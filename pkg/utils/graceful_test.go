package utils_test

import (
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/indiefan/home_assistant_nanit/pkg/utils"
)

// recorder collects the order of events from several goroutines. The tests
// used a plain string for this, which the race detector reports.
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

func TestGracefulRunner(t *testing.T) {
	out := &recorder{}

	runner := utils.RunWithGracefulCancel(func(ctx utils.GracefulContext) {
		ctx.RunAsChild(func(childCtx utils.GracefulContext) {
			<-childCtx.Done()
			time.Sleep(500 * time.Millisecond)
			out.add(" sub_finished")
		})

		<-ctx.Done()
		time.Sleep(200 * time.Millisecond)
		out.add(" main_finished")
	})

	time.Sleep(100 * time.Millisecond)
	runner.Cancel()
	out.add(" after_cancel")

	assert.Equal(t, " main_finished sub_finished after_cancel", out.String())
}

func TestGracefulRunnerFinished(t *testing.T) {
	out := &recorder{}
	runner := utils.RunWithGracefulCancel(func(ctx utils.GracefulContext) {
		ctx.RunAsChild(func(childCtx utils.GracefulContext) {
			time.Sleep(300 * time.Millisecond)
			out.add(" sub_finished")
		})

		time.Sleep(200 * time.Millisecond)
		out.add(" main_finished")
	})

	_, err := runner.Wait()
	assert.NoError(t, err)
	assert.Equal(t, " main_finished sub_finished", out.String())
}

func TestGracefulRunnerFail(t *testing.T) {
	out := &recorder{}
	runner := utils.RunWithGracefulCancel(func(ctx utils.GracefulContext) {
		ctx.RunAsChild(func(childCtx utils.GracefulContext) {
			<-childCtx.Done()
			time.Sleep(200 * time.Millisecond)
			out.add(" sub_finished")
		})

		for {
			select {
			case <-time.After(200 * time.Millisecond):
				ctx.Fail(errors.New("simulated failure"))
			case <-ctx.Done():
				time.Sleep(100 * time.Millisecond)
				out.add(" main_finished")
				return
			}
		}
	})

	_, err := runner.Wait()
	assert.EqualError(t, err, "simulated failure")
	assert.Equal(t, " main_finished sub_finished", out.String())
}
