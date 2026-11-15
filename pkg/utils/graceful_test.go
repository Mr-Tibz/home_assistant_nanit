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
// that a slow machine cannot make a correct runner fail them.
//
// Each test holds a child back until the test goroutine has seen that
// Cancel() or Wait() is still blocked, and releases it only then. A runner
// that stops waiting for its children therefore fails every time.
//
// Goroutines started by a runner never touch the testing.T. They wait on
// channels that the test closes when it ends, so nothing is reported after
// the test has completed.

const (
	// testTimeout limits every wait for something that must happen.
	testTimeout = 5 * time.Second

	// stillBlocked is how long a call is watched to show that it has not
	// returned. A correct runner blocks for as long as the child is held
	// back, so this check cannot fail by timing.
	stillBlocked = 100 * time.Millisecond
)

// scenario gives the goroutines of a test a way to wait that ends when the
// test ends.
type scenario struct {
	t    *testing.T
	stop chan struct{}
}

func newScenario(t *testing.T) *scenario {
	s := &scenario{t: t, stop: make(chan struct{})}
	t.Cleanup(func() { close(s.stop) })
	return s
}

// await blocks until ch delivers. It returns false if the test ended
// first. It is for goroutines started by a runner.
func (s *scenario) await(ch <-chan struct{}) bool {
	select {
	case <-ch:
		return true
	case <-s.stop:
		return false
	}
}

// must waits for ch on the test goroutine and fails the test if it does
// not deliver in time.
func (s *scenario) must(ch <-chan struct{}, what string) {
	s.t.Helper()

	select {
	case <-ch:
	case <-time.After(testTimeout):
		s.t.Fatalf("timed out waiting for %s", what)
	}
}

// mustStillBlock fails the test if ch delivers although the call behind it
// has to wait for a child that is still held back.
func (s *scenario) mustStillBlock(ch <-chan struct{}, what string) {
	s.t.Helper()

	select {
	case <-ch:
		s.t.Fatalf("%s returned before the child had finished", what)
	case <-time.After(stillBlocked):
	}
}

// A cancelled runner waits for its handler and for the children of that
// handler, also when a child finishes after the handler itself.
func TestGracefulRunner(t *testing.T) {
	s := newScenario(t)
	out := &recorder{}
	started := make(chan struct{})
	mainDone := make(chan struct{})
	releaseChild := make(chan struct{})

	runner := utils.RunWithGracefulCancel(func(ctx utils.GracefulContext) {
		ctx.RunAsChild(func(childCtx utils.GracefulContext) {
			if s.await(childCtx.Done()) && s.await(releaseChild) {
				out.add(" sub_finished")
			}
		})

		close(started)
		if s.await(ctx.Done()) {
			out.add(" main_finished")
		}
		close(mainDone)
	})

	s.must(started, "the handler to start")

	cancelled := make(chan struct{})
	go func() {
		runner.Cancel()
		close(cancelled)
	}()

	s.must(mainDone, "the handler to finish")
	s.mustStillBlock(cancelled, "Cancel()")

	close(releaseChild)
	s.must(cancelled, "Cancel() to return")
	out.add(" after_cancel")

	assert.Equal(t, " main_finished sub_finished after_cancel", out.String())
}

// Wait() returns only after the children have finished, also when the
// handler returned first.
func TestGracefulRunnerFinished(t *testing.T) {
	s := newScenario(t)
	out := &recorder{}
	mainDone := make(chan struct{})
	releaseChild := make(chan struct{})

	runner := utils.RunWithGracefulCancel(func(ctx utils.GracefulContext) {
		ctx.RunAsChild(func(childCtx utils.GracefulContext) {
			if s.await(releaseChild) {
				out.add(" sub_finished")
			}
		})

		out.add(" main_finished")
		close(mainDone)
	})

	var err error
	waited := make(chan struct{})
	go func() {
		_, err = runner.Wait()
		close(waited)
	}()

	s.must(mainDone, "the handler to finish")
	s.mustStillBlock(waited, "Wait()")

	close(releaseChild)
	s.must(waited, "Wait() to return")

	assert.NoError(t, err)
	assert.Equal(t, " main_finished sub_finished", out.String())
}

// A handler that fails cancels its children, and Wait() reports the error
// after the children have finished.
func TestGracefulRunnerFail(t *testing.T) {
	s := newScenario(t)
	out := &recorder{}
	mainDone := make(chan struct{})
	childCancelled := make(chan struct{})
	releaseChild := make(chan struct{})

	runner := utils.RunWithGracefulCancel(func(ctx utils.GracefulContext) {
		ctx.RunAsChild(func(childCtx utils.GracefulContext) {
			if !s.await(childCtx.Done()) {
				return
			}
			close(childCancelled)

			if s.await(releaseChild) {
				out.add(" sub_finished")
			}
		})

		ctx.Fail(errors.New("simulated failure"))
		if s.await(ctx.Done()) {
			out.add(" main_finished")
		}
		close(mainDone)
	})

	var err error
	waited := make(chan struct{})
	go func() {
		_, err = runner.Wait()
		close(waited)
	}()

	s.must(mainDone, "the failed handler to finish")
	s.must(childCancelled, "the failure to cancel the child")
	s.mustStillBlock(waited, "Wait()")

	close(releaseChild)
	s.must(waited, "Wait() to return")

	assert.EqualError(t, err, "simulated failure")
	assert.Equal(t, " main_finished sub_finished", out.String())
}

// Wait() reads the runner's error under the lock that Fail() writes it
// with. A parent that is cancelled cancels its children, also one whose
// handler returns at that moment, and RunWithPerseverance reads a child's
// result in exactly this way.
//
// This is a race-detector test: without -race it only checks that the
// result is one of the two valid ones. It repeats the overlap many times,
// because the detector reports a race only when it happens. With the lock
// removed it fails within the first iterations.
func TestGracefulChildWaitDuringParentCancel(t *testing.T) {
	s := newScenario(t)

	for i := 0; i < 1000; i++ {
		ready := make(chan struct{})
		release := make(chan struct{})
		results := make(chan error, 1)

		runner := utils.RunWithGracefulCancel(func(ctx utils.GracefulContext) {
			child := ctx.RunAsChild(func(childCtx utils.GracefulContext) {
				s.await(release)
			})

			close(ready)
			_, err := child.Wait()
			results <- err
		})

		s.must(ready, "the handler to start its child")

		cancelled := make(chan struct{})
		go func() {
			runner.Cancel()
			close(cancelled)
		}()
		close(release)

		s.must(cancelled, "Cancel() to return")

		// The child either finished before the cancellation reached it,
		// or it was cancelled. Nothing else is a valid result.
		if err := <-results; err != nil {
			assert.EqualError(t, err, "cancelled execution")
		}
	}
}
