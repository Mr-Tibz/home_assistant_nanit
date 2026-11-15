package utils_test

import (
	"errors"
	"sync"
	"testing"

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

// A cancelled runner waits for its handler and for the children of that
// handler, also when a child finishes after the handler itself.
func TestGracefulRunner(t *testing.T) {
	out := &recorder{}
	started := make(chan struct{})
	mainDone := make(chan struct{})

	runner := utils.RunWithGracefulCancel(func(ctx utils.GracefulContext) {
		ctx.RunAsChild(func(childCtx utils.GracefulContext) {
			<-childCtx.Done()
			<-mainDone
			out.add(" sub_finished")
		})

		close(started)
		<-ctx.Done()
		out.add(" main_finished")
		close(mainDone)
	})

	<-started
	runner.Cancel()
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
			<-mainDone
			out.add(" sub_finished")
		})

		out.add(" main_finished")
		close(mainDone)
	})

	_, err := runner.Wait()
	assert.NoError(t, err)
	assert.Equal(t, " main_finished sub_finished", out.String())
}

// A handler that fails cancels its children, and Wait() reports the error.
func TestGracefulRunnerFail(t *testing.T) {
	out := &recorder{}
	mainDone := make(chan struct{})

	runner := utils.RunWithGracefulCancel(func(ctx utils.GracefulContext) {
		ctx.RunAsChild(func(childCtx utils.GracefulContext) {
			<-childCtx.Done()
			<-mainDone
			out.add(" sub_finished")
		})

		ctx.Fail(errors.New("simulated failure"))
		<-ctx.Done()
		out.add(" main_finished")
		close(mainDone)
	})

	_, err := runner.Wait()
	assert.EqualError(t, err, "simulated failure")
	assert.Equal(t, " main_finished sub_finished", out.String())
}
