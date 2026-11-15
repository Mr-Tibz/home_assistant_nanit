package utils_test

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/indiefan/home_assistant_nanit/pkg/utils"
)

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

// waitResult is what GracefulRunner.Wait() returned.
type waitResult struct {
	cancelled bool
	err       error
}

// wait calls runner.Wait() on another goroutine. The returned channel
// delivers once, when Wait() has returned.
func wait(runner utils.GracefulRunner) (<-chan struct{}, *waitResult) {
	res := &waitResult{}
	done := make(chan struct{})

	go func() {
		res.cancelled, res.err = runner.Wait()
		close(done)
	}()

	return done, res
}

// A cancelled runner waits for its handler and for the children of that
// handler, also when a child finishes after the handler itself.
func TestGracefulRunner(t *testing.T) {
	s := newScenario(t)
	started := make(chan struct{})
	handlerCancelled := make(chan struct{})
	childCancelled := make(chan struct{})
	releaseChild := make(chan struct{})
	childFinished := make(chan struct{})

	runner := utils.RunWithGracefulCancel(func(ctx utils.GracefulContext) {
		ctx.RunAsChild(func(childCtx utils.GracefulContext) {
			if !s.await(childCtx.Done()) {
				return
			}
			close(childCancelled)

			if s.await(releaseChild) {
				close(childFinished)
			}
		})

		close(started)
		if s.await(ctx.Done()) {
			close(handlerCancelled)
		}
	})

	s.must(started, "the handler to start")

	cancelReturned := make(chan struct{})
	go func() {
		runner.Cancel()
		close(cancelReturned)
	}()

	s.must(handlerCancelled, "the cancellation to reach the handler")
	s.must(childCancelled, "the cancellation to reach the child")
	s.mustStillBlock(cancelReturned, "Cancel()")

	close(releaseChild)
	s.must(cancelReturned, "Cancel() to return")
	s.must(childFinished, "the child to have finished when Cancel() returned")

	done, res := wait(runner)
	s.must(done, "Wait() to return")
	assert.True(t, res.cancelled)
	assert.Error(t, res.err)
}

// Wait() returns only after the children have finished, also when the
// handler returned first. A handler that returned without being failed is
// reported as neither cancelled nor failed.
func TestGracefulRunnerFinished(t *testing.T) {
	s := newScenario(t)
	handlerReturning := make(chan struct{})
	releaseChild := make(chan struct{})
	childFinished := make(chan struct{})

	runner := utils.RunWithGracefulCancel(func(ctx utils.GracefulContext) {
		ctx.RunAsChild(func(childCtx utils.GracefulContext) {
			if s.await(releaseChild) {
				close(childFinished)
			}
		})

		close(handlerReturning)
	})

	done, res := wait(runner)

	s.must(handlerReturning, "the handler to return")
	s.mustStillBlock(done, "Wait()")

	close(releaseChild)
	s.must(done, "Wait() to return")
	s.must(childFinished, "the child to have finished when Wait() returned")

	assert.False(t, res.cancelled)
	assert.NoError(t, res.err)
}

// A handler that fails cancels its children, and Wait() reports the error
// after the children have finished.
func TestGracefulRunnerFail(t *testing.T) {
	s := newScenario(t)
	failure := errors.New("simulated failure")
	handlerReturning := make(chan struct{})
	childCancelled := make(chan struct{})
	releaseChild := make(chan struct{})
	childFinished := make(chan struct{})

	runner := utils.RunWithGracefulCancel(func(ctx utils.GracefulContext) {
		ctx.RunAsChild(func(childCtx utils.GracefulContext) {
			if !s.await(childCtx.Done()) {
				return
			}
			close(childCancelled)

			if s.await(releaseChild) {
				close(childFinished)
			}
		})

		ctx.Fail(failure)
		s.await(ctx.Done())
		close(handlerReturning)
	})

	done, res := wait(runner)

	s.must(handlerReturning, "the failed handler to return")
	s.must(childCancelled, "the failure to cancel the child")
	s.mustStillBlock(done, "Wait()")

	close(releaseChild)
	s.must(done, "Wait() to return")
	s.must(childFinished, "the child to have finished when Wait() returned")

	assert.False(t, res.cancelled)
	assert.Same(t, failure, res.err)
}

// Cancelling a runner whose handler has already returned does not change
// its result. The parent of a child does exactly that when it is cancelled
// at the moment the child returns, and RunWithPerseverance decides from
// this result whether an attempt was cancelled.
func TestGracefulRunnerCancelAfterHandlerReturned(t *testing.T) {
	s := newScenario(t)
	results := make(chan [2]waitResult, 1)

	runner := utils.RunWithGracefulCancel(func(ctx utils.GracefulContext) {
		child := ctx.RunAsChild(func(childCtx utils.GracefulContext) {})

		var before, after waitResult
		before.cancelled, before.err = child.Wait()
		child.Cancel()
		after.cancelled, after.err = child.Wait()

		results <- [2]waitResult{before, after}
	})

	done, _ := wait(runner)
	s.must(done, "Wait() to return")

	res := <-results
	assert.Equal(t, waitResult{}, res[0], "result of a child that returned by itself")
	assert.Equal(t, waitResult{}, res[1], "result after a late Cancel()")
}

// Wait() must not race with the cancellation that a parent sends to a
// child whose handler returns at the same moment.
//
// The outcome of that overlap is one of two: the child returned first, or
// the cancellation reached it first. This test checks that the two values
// Wait() returns always agree with each other, and gives the race detector
// many overlaps to look at. Reading the result without the lock is
// reported within the first iterations.
func TestGracefulChildWaitDuringParentCancel(t *testing.T) {
	s := newScenario(t)

	for i := 0; i < 1000; i++ {
		ready := make(chan struct{})
		release := make(chan struct{})
		results := make(chan waitResult, 1)

		runner := utils.RunWithGracefulCancel(func(ctx utils.GracefulContext) {
			child := ctx.RunAsChild(func(childCtx utils.GracefulContext) {
				s.await(release)
			})

			close(ready)

			var res waitResult
			res.cancelled, res.err = child.Wait()
			results <- res
		})

		s.must(ready, "the handler to start its child")

		cancelReturned := make(chan struct{})
		go func() {
			runner.Cancel()
			close(cancelReturned)
		}()
		close(release)

		s.must(cancelReturned, "Cancel() to return")

		res := <-results
		if res.cancelled {
			assert.Error(t, res.err, "a cancelled child reports the cancellation as its error")
		} else {
			assert.NoError(t, res.err, "a child that was not cancelled or failed reports no error")
		}
	}
}
