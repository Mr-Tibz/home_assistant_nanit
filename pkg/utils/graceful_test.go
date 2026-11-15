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
// Each test holds the handler or a child back until the test goroutine
// has seen that Cancel() or Wait() is still blocked, and releases it only
// then. A runner that stops waiting for its handler or for its children
// therefore fails every time.
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

// mustAlready fails the test if ch has not delivered yet. It does not wait.
func (s *scenario) mustAlready(ch <-chan struct{}, what string) {
	s.t.Helper()

	select {
	case <-ch:
	default:
		s.t.Fatalf("%s", what)
	}
}

// mustStillBlock fails the test with problem if ch delivers. It is for a
// call that has to wait for work the test still holds back.
func (s *scenario) mustStillBlock(ch <-chan struct{}, problem string) {
	s.t.Helper()

	select {
	case <-ch:
		s.t.Fatal(problem)
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
// handler, whichever of them finishes last.
func TestGracefulRunner(t *testing.T) {
	s := newScenario(t)
	started := make(chan struct{})
	handlerCancelled := make(chan struct{})
	releaseHandler := make(chan struct{})
	handlerFinished := make(chan struct{})
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
		if !s.await(ctx.Done()) {
			return
		}
		close(handlerCancelled)

		if s.await(releaseHandler) {
			close(handlerFinished)
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
	s.mustStillBlock(cancelReturned, "Cancel() returned while the handler and the child were still working")

	close(releaseChild)
	s.must(childFinished, "the child to finish")
	s.mustStillBlock(cancelReturned, "Cancel() returned while the handler was still working")

	close(releaseHandler)
	s.must(cancelReturned, "Cancel() to return")
	s.mustAlready(handlerFinished, "Cancel() returned although the handler had not finished")

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
	s.mustStillBlock(done, "Wait() returned while the child was still working")

	close(releaseChild)
	s.must(done, "Wait() to return")
	s.mustAlready(childFinished, "Wait() returned although the child had not finished")

	assert.False(t, res.cancelled)
	assert.NoError(t, res.err)
}

// A handler that fails cancels its children. Wait() reports the error
// after the handler and the children have finished, whichever of them
// finishes last.
func TestGracefulRunnerFail(t *testing.T) {
	s := newScenario(t)
	failure := errors.New("simulated failure")
	handlerFailed := make(chan struct{})
	releaseHandler := make(chan struct{})
	handlerFinished := make(chan struct{})
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
		if !s.await(ctx.Done()) {
			return
		}
		close(handlerFailed)

		if s.await(releaseHandler) {
			close(handlerFinished)
		}
	})

	done, res := wait(runner)

	s.must(handlerFailed, "the failure to cancel the handler's own context")
	s.must(childCancelled, "the failure to cancel the child")
	s.mustStillBlock(done, "Wait() returned while the handler and the child were still working")

	close(releaseChild)
	s.must(childFinished, "the child to finish")
	s.mustStillBlock(done, "Wait() returned while the handler was still working")

	close(releaseHandler)
	s.must(done, "Wait() to return")
	s.mustAlready(handlerFinished, "Wait() returned although the handler had not finished")

	assert.False(t, res.cancelled)
	assert.Same(t, failure, res.err)
}

// Wait() must not race with the cancellation that a parent sends to a
// child whose handler returns at the same moment. RunWithPerseverance
// reads a child's result in exactly this way.
//
// Only the race detector can see this defect, so the test is skipped
// without it. The detector reports a race only when it happens, so the
// overlap is repeated many times. With the lock in Wait() removed, it is
// reported within the first iterations.
func TestGracefulChildWaitDuringParentCancel(t *testing.T) {
	if !raceDetectorEnabled {
		t.Skip("needs the race detector (go test -race)")
	}

	s := newScenario(t)
	finishedFirst, cancelledFirst := 0, 0

	for i := 0; i < 1000; i++ {
		ready := make(chan struct{})
		release := make(chan struct{})
		results := make(chan bool, 1)

		runner := utils.RunWithGracefulCancel(func(ctx utils.GracefulContext) {
			child := ctx.RunAsChild(func(childCtx utils.GracefulContext) {
				s.await(release)
			})

			close(ready)

			cancelled, _ := child.Wait()
			results <- cancelled
		})

		s.must(ready, "the handler to start its child")

		cancelReturned := make(chan struct{})
		go func() {
			runner.Cancel()
			close(cancelReturned)
		}()
		close(release)

		s.must(cancelReturned, "Cancel() to return")

		if <-results {
			cancelledFirst++
		} else {
			finishedFirst++
		}
	}

	t.Logf("child returned first: %d times, cancellation reached it first: %d times", finishedFirst, cancelledFirst)
}
