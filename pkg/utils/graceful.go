package utils

import (
	"errors"
	"sync"
)

// GracefulContext - a context carries channel factory for cancelation
type GracefulContext interface {
	// Done - blocks until cancelled
	Done() <-chan struct{}

	// RunAsChild - runs handler within child context
	RunAsChild(callback func(GracefulContext)) GracefulRunner

	// Fail - cancels run from the inside and propagates cancel to all children
	// Does not await the cancellation (obviously)
	Fail(err error)
}

// GracefulRunner - outter API for controlling gracefully run handlers
type GracefulRunner interface {
	// Wait - blocks until finishes execution
	Wait() (bool, error)

	// Cancel - notifies handler to cancel the execution and awaits graceful return (clean up)
	Cancel()
}

// RunWithGracefulCancel - runs callback as a go routine and returns cancel routine
// This is inspired by context but with the key difference that the cancel function waits until
// the handler finishes all the cleanup
// @see https://blog.golang.org/context
func RunWithGracefulCancel(callback func(GracefulContext)) GracefulRunner {
	ctx := newGracefulCtx()
	ctx.wg.Add(1)

	go func() {
		callback(ctx)
		ctx.handlerReturned()
		ctx.wg.Done()
	}()

	return newGracefulRunner(ctx)
}

// -----------------------------

type gracefulRunner struct {
	ctx *gracefulCtx
}

func newGracefulRunner(ctx *gracefulCtx) *gracefulRunner {
	return &gracefulRunner{ctx}
}

// Wait - blocks until the handler and its children have finished. It
// reports how the handler ended: whether it had been cancelled, and the
// error it had been failed with, at the moment it returned.
func (runner *gracefulRunner) Wait() (bool, error) {
	runner.ctx.wg.Wait()

	runner.ctx.mutex.Lock()
	err := runner.ctx.result
	runner.ctx.mutex.Unlock()

	return err == errCancel, err
}

func (runner *gracefulRunner) Cancel() {
	runner.ctx.Fail(errCancel)
	runner.ctx.wg.Wait()
}

// -----------------------------

type gracefulCtx struct {
	cancelC         chan struct{}
	wg              sync.WaitGroup
	mutex           sync.Mutex
	hasBeenCanceled bool
	err             error

	// result is the value of err at the moment the handler returned. A
	// context can still be failed after that, because the cancellation of
	// a parent reaches its children also when their handler has just
	// returned. That must not turn a handler that finished into one that
	// was cancelled, so Wait() reports this copy and not err.
	result    error
	resultSet bool
}

func newGracefulCtx() *gracefulCtx {
	return &gracefulCtx{
		cancelC:         make(chan struct{}),
		hasBeenCanceled: false,
	}
}

func (c *gracefulCtx) Done() <-chan struct{} {
	return c.cancelC
}

func (c *gracefulCtx) RunAsChild(callback func(GracefulContext)) GracefulRunner {
	// Parent should wait for 1 more child
	c.wg.Add(1)

	// Do not even start if context has been already cancelled
	c.mutex.Lock()
	if c.hasBeenCanceled {
		c.wg.Done()
		c.mutex.Unlock()
		return newCancelledGracefulRunner()
	}
	c.mutex.Unlock()

	// Create channel for cancel callback
	// Note: this is necessary so that we don't leak if child finishes first
	cancelC := make(chan struct{}, 1)

	// Start child runner
	runner := RunWithGracefulCancel(func(childCtx GracefulContext) {
		callback(childCtx)

		// Record the child's result before the parent is told. From here
		// on the parent may cancel this child without changing it.
		if c, ok := childCtx.(*gracefulCtx); ok {
			c.handlerReturned()
		}

		// Notify parent that child is done
		c.wg.Done()

		// Unblock cancel callback
		close(cancelC)
	})

	// Cancel child when parent gets cancelled
	go func() {
		select {
		case <-c.Done():
			runner.Cancel()
		case <-cancelC:
			return
		}
	}()

	return runner
}

// handlerReturned records how the handler ended. Only the first call
// counts. See gracefulCtx.result.
func (c *gracefulCtx) handlerReturned() {
	c.mutex.Lock()
	if !c.resultSet {
		c.result = c.err
		c.resultSet = true
	}
	c.mutex.Unlock()
}

func (c *gracefulCtx) Fail(err error) {
	c.mutex.Lock()
	if c.hasBeenCanceled {
		c.mutex.Unlock()
		return
	}

	c.hasBeenCanceled = true
	c.err = err
	close(c.cancelC)
	c.mutex.Unlock()
}

// -----------------------------

type cancelledGracefulRunner struct{}

func newCancelledGracefulRunner() *cancelledGracefulRunner {
	return &cancelledGracefulRunner{}
}

func (*cancelledGracefulRunner) Wait() (bool, error) {
	return true, errCancel
}

func (*cancelledGracefulRunner) Cancel() {}

var errCancel error = errors.New("cancelled execution")
