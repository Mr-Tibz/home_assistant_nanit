package utils_test

import (
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/indiefan/home_assistant_nanit/pkg/utils"
)

// recorder collects the order of events from several goroutines.
type recorder struct {
	mu  sync.Mutex
	out string
}

func (r *recorder) add(event string) {
	r.mu.Lock()
	r.out += event
	r.mu.Unlock()
}

func (r *recorder) String() string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.out
}

// The tests below order their events with channels, not with sleeps, so
// that they do not depend on how fast the machine schedules goroutines.
// Every wait has a limit, so that a broken runner fails the test with a
// message and does not hang it.

const testTimeout = 5 * time.Second

// within reports a failure when ch does not deliver in time. It is safe
// to call from any goroutine.
func within(t *testing.T, ch <-chan struct{}, what string) bool {
	t.Helper()

	select {
	case <-ch:
		return true
	case <-time.After(testTimeout):
		t.Errorf("timed out waiting for %s", what)
		return false
	}
}

// cancelWithin calls runner.Cancel() and fails the test if it does not
// return in time.
func cancelWithin(t *testing.T, runner utils.GracefulRunner) {
	t.Helper()

	done := make(chan struct{})
	go func() {
		runner.Cancel()
		close(done)
	}()

	if !within(t, done, "Cancel() to return") {
		t.FailNow()
	}
}

// waitWithin calls runner.Wait() and fails the test if it does not return
// in time.
func waitWithin(t *testing.T, runner utils.GracefulRunner) (bool, error) {
	t.Helper()

	var cancelled bool
	var err error

	done := make(chan struct{})
	go func() {
		cancelled, err = runner.Wait()
		close(done)
	}()

	if !within(t, done, "Wait() to return") {
		t.FailNow()
	}

	return cancelled, err
}

// A cancelled runner waits for its handler and for the children of that
// handler, also when a child finishes after the handler itself.
func TestGracefulRunner(t *testing.T) {
	out := &recorder{}
	started := make(chan struct{})
	mainDone := make(chan struct{})

	runner := utils.RunWithGracefulCancel(func(ctx utils.GracefulContext) {
		ctx.RunAsChild(func(childCtx utils.GracefulContext) {
			if within(t, childCtx.Done(), "the child to be cancelled") && within(t, mainDone, "the handler to finish") {
				out.add(" sub_finished")
			}
		})

		close(started)
		if within(t, ctx.Done(), "the handler to be cancelled") {
			out.add(" main_finished")
		}
		close(mainDone)
	})

	if !within(t, started, "the handler to start") {
		t.FailNow()
	}
	cancelWithin(t, runner)
	out.add(" after_cancel")

	assert.Equal(t, " main_finished sub_finished after_cancel", out.String())
}

// Wait() returns only after the children have finished, also when the
// handler returned first.
func TestGracefulRunnerFinished(t *testing.T) {
	out := &recorder{}
	mainDone := make(chan struct{})

	runner := utils.RunWithGracefulCancel(func(ctx utils.GracefulContext) {
		ctx.RunAsChild(func(childCtx utils.GracefulContext) {
			if within(t, mainDone, "the handler to finish") {
				out.add(" sub_finished")
			}
		})

		out.add(" main_finished")
		close(mainDone)
	})

	_, err := waitWithin(t, runner)
	assert.NoError(t, err)
	assert.Equal(t, " main_finished sub_finished", out.String())
}

// A handler that fails cancels its children, and Wait() reports the error.
func TestGracefulRunnerFail(t *testing.T) {
	out := &recorder{}
	mainDone := make(chan struct{})

	runner := utils.RunWithGracefulCancel(func(ctx utils.GracefulContext) {
		ctx.RunAsChild(func(childCtx utils.GracefulContext) {
			if within(t, childCtx.Done(), "the child to be cancelled") && within(t, mainDone, "the handler to finish") {
				out.add(" sub_finished")
			}
		})

		ctx.Fail(errors.New("simulated failure"))
		if within(t, ctx.Done(), "the failed handler to be done") {
			out.add(" main_finished")
		}
		close(mainDone)
	})

	_, err := waitWithin(t, runner)
	assert.EqualError(t, err, "simulated failure")
	assert.Equal(t, " main_finished sub_finished", out.String())
}

// A child that returns while its parent is being cancelled must not race
// with that cancellation when its result is read. RunWithPerseverance
// reads a child's result in exactly this way. The race detector reports
// the unsynchronised read if the lock in Wait() is removed.
func TestGracefulChildWaitDuringParentCancel(t *testing.T) {
	for i := 0; i < 200; i++ {
		ready := make(chan struct{})
		release := make(chan struct{})

		runner := utils.RunWithGracefulCancel(func(ctx utils.GracefulContext) {
			child := ctx.RunAsChild(func(childCtx utils.GracefulContext) {
				within(t, release, "the child to be released")
			})

			close(ready)
			child.Wait()
		})

		if !within(t, ready, "the handler to start its child") {
			t.FailNow()
		}

		go close(release)
		cancelWithin(t, runner)
	}
}
