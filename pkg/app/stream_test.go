package app

import (
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/indiefan/home_assistant_nanit/pkg/baby"
	"github.com/indiefan/home_assistant_nanit/pkg/client"
	"github.com/indiefan/home_assistant_nanit/pkg/utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const testBabyUID = "baby-1"
const testTargetURL = "rtmp://192.0.2.1:1935/local/baby-1"

var errLimit = errors.New(streamLimitMessage)

// fakeCamera is a stand-in for the websocket connection to the camera. It
// answers the requests about the stream.
type fakeCamera struct {
	mu sync.Mutex

	// answers - answer to each request to start the stream, in order.
	// The last entry repeats. nil accepts the request.
	answers []error
	starts  int

	// stopAnswer - answer to every request to stop the stream
	stopAnswer error

	// requests - every request, in the order sent
	requests chan client.Streaming_Status

	// urls - the address of every request
	urls []string

	// gaveUp - the reason, when the keeper has given up the connection
	gaveUp chan error

	// done - closed when the connection has ended
	done    chan struct{}
	endOnce sync.Once

	// watched - what Watched reports
	watched bool

	// hold - when set, the answer to a request to start the stream is
	// kept back until this channel is closed
	hold chan struct{}

	// streamEndsAtOnce - a stream that arrives ends right away
	streamEndsAtOnce bool

	// streamArrives - the stream arrives as soon as the camera accepts
	// a request for it
	streamArrives bool
	states        *baby.StateManager
}

func newFakeCamera(answers ...error) *fakeCamera {
	return &fakeCamera{
		answers:  answers,
		requests: make(chan client.Streaming_Status, 64),
		gaveUp:   make(chan error, 1),
		done:     make(chan struct{}),
	}
}

func (camera *fakeCamera) SendRequest(reqType client.RequestType, request *client.Request) func(time.Duration) (*client.Response, error) {
	status := request.GetStreaming().GetStatus()

	camera.mu.Lock()
	camera.urls = append(camera.urls, request.GetStreaming().GetRtmpUrl())

	answer := camera.stopAnswer
	if status == client.Streaming_STARTED {
		answer = camera.answers[min(camera.starts, len(camera.answers)-1)]
		camera.starts++
	}
	streamArrives, streamEndsAtOnce, states, hold := camera.streamArrives, camera.streamEndsAtOnce, camera.states, camera.hold
	camera.mu.Unlock()

	camera.requests <- status

	if status == client.Streaming_STARTED && answer == nil && streamArrives {
		states.Update(testBabyUID, *baby.NewState().SetStreamState(baby.StreamState_Alive))

		if streamEndsAtOnce {
			states.Update(testBabyUID, *baby.NewState().SetStreamState(baby.StreamState_Unhealthy))
		}
	}

	return func(time.Duration) (*client.Response, error) {
		if status == client.Streaming_STARTED && hold != nil {
			<-hold
		}

		// A request ends in this way only on a connection that has ended
		if errors.Is(answer, client.ErrConnectionClosed) {
			camera.endOnce.Do(func() { close(camera.done) })
		}

		return nil, answer
	}
}

func (camera *fakeCamera) Watched() bool {
	camera.mu.Lock()
	defer camera.mu.Unlock()

	return camera.watched
}

// GiveUp ends the connection, as the real one does.
func (camera *fakeCamera) GiveUp(reason error) {
	camera.endOnce.Do(func() { close(camera.done) })
	camera.gaveUp <- reason
}

func (camera *fakeCamera) Done() <-chan struct{} {
	return camera.done
}

// next returns the next request, or fails the test when none comes.
func (camera *fakeCamera) next(t *testing.T) client.Streaming_Status {
	select {
	case status := <-camera.requests:
		return status
	case <-time.After(5 * time.Second):
		t.Fatal("no request was sent")
		panic("unreachable")
	}
}

// expect checks that the next requests are these, in this order.
func (camera *fakeCamera) expect(t *testing.T, statuses ...client.Streaming_Status) {
	for i, want := range statuses {
		require.Equal(t, want, camera.next(t), "request %d of %v", i+1, statuses)
	}
}

// expectNoRequest checks that no request is sent for a while.
func (camera *fakeCamera) expectNoRequest(t *testing.T) {
	select {
	case status := <-camera.requests:
		t.Fatalf("a request was sent: %v", status)
	case <-time.After(150 * time.Millisecond):
	}
}

const (
	stopped = client.Streaming_STOPPED
	started = client.Streaming_STARTED
)

// keeperTest runs a stream keeper against a fake camera.
type keeperTest struct {
	camera *fakeCamera
	states *baby.StateManager
	runner utils.GracefulRunner
}

func startKeeper(t *testing.T, camera *fakeCamera, prepare func(states *baby.StateManager, keeper *streamKeeper)) *keeperTest {
	states := baby.NewStateManager()

	camera.mu.Lock()
	camera.states = states
	camera.mu.Unlock()

	keeper := newStreamKeeper(testBabyUID, testTargetURL, camera, states, &streamPace{})
	keeper.retryPauses = []time.Duration{20 * time.Millisecond}
	keeper.publisherTimeout = 40 * time.Millisecond
	keeper.stableAfter = 30 * time.Millisecond

	if prepare != nil {
		prepare(states, keeper)
	}

	test := &keeperTest{camera: camera, states: states}
	test.runner = utils.RunWithGracefulCancel(keeper.run)
	t.Cleanup(test.runner.Cancel)

	return test
}

func (test *keeperTest) setStream(state baby.StreamState) {
	test.states.Update(testBabyUID, *baby.NewState().SetStreamState(state))
}

// stop tells the keeper to stop and waits until it has returned.
func (test *keeperTest) stop(t *testing.T) {
	finished := make(chan struct{})
	go func() {
		test.runner.Cancel()
		close(finished)
	}()

	select {
	case <-finished:
	case <-time.After(5 * time.Second):
		t.Fatal("the keeper did not stop")
	}
}

// The keeper asks for the stream when the connection is there, and it
// asks the camera to stop the stream to our address first. When the
// stream has arrived, it asks for nothing more.
func TestKeeperAsksForTheStream(t *testing.T) {
	camera := newFakeCamera(nil)

	camera.streamArrives = true
	startKeeper(t, camera, nil)

	camera.expect(t, stopped, started)
	camera.expectNoRequest(t)

	camera.mu.Lock()
	defer camera.mu.Unlock()
	assert.Equal(t, []string{testTargetURL, testTargetURL}, camera.urls, "both requests name the same address: the camera stops a stream only for the exact address")
}

// A stream that runs when the connection comes back needs no request. The
// stream does not depend on the websocket connection.
func TestKeeperDoesNotAskWhenTheStreamRuns(t *testing.T) {
	camera := newFakeCamera(nil)

	startKeeper(t, camera, func(states *baby.StateManager, _ *streamKeeper) {
		states.Update(testBabyUID, *baby.NewState().SetStreamState(baby.StreamState_Alive))
	})

	camera.expectNoRequest(t)
}

// The camera has no place left: the keeper asks again after a short
// pause, until a place is free. Before, it waited 5 minutes each time.
func TestKeeperAsksAgainAfterARefusal(t *testing.T) {
	camera := newFakeCamera(errLimit, errLimit, nil)

	camera.streamArrives = true
	startKeeper(t, camera, nil)

	camera.expect(t, stopped, started, stopped, started, stopped, started)
	camera.expectNoRequest(t)
}

// Every refusal is followed by one more request, not only the refusal for
// lack of places. Before, another refusal ended the requests until the
// connection was made again.
func TestKeeperAsksAgainAfterAnotherError(t *testing.T) {
	camera := newFakeCamera(errors.New("Internal error"), nil)

	camera.streamArrives = true
	startKeeper(t, camera, nil)

	camera.expect(t, stopped, started, stopped, started)
	camera.expectNoRequest(t)
}

// The pauses grow with the failures in a row, up to the last entry of the
// list.
func TestKeeperPausesGrow(t *testing.T) {
	camera := newFakeCamera(errLimit)

	startKeeper(t, camera, func(_ *baby.StateManager, keeper *streamKeeper) {
		keeper.retryPauses = []time.Duration{10 * time.Millisecond, time.Hour}
	})

	// The second request follows after the first, short pause. The third
	// one would follow after an hour.
	camera.expect(t, stopped, started, stopped, started)
	camera.expectNoRequest(t)
}

// The camera accepts the request, and no stream arrives. The keeper asks
// again. Before, nothing happened until the connection was made again.
func TestKeeperAsksAgainWhenNoStreamArrives(t *testing.T) {
	camera := newFakeCamera(nil)

	startKeeper(t, camera, nil)

	camera.expect(t, stopped, started, stopped, started, stopped, started)
}

// The stream ends: the keeper asks for it again.
func TestKeeperAsksAgainWhenTheStreamEnds(t *testing.T) {
	camera := newFakeCamera(nil)

	camera.streamArrives = true
	test := startKeeper(t, camera, nil)

	camera.expect(t, stopped, started)
	camera.expectNoRequest(t)

	test.setStream(baby.StreamState_Unhealthy)

	camera.expect(t, stopped, started)
	camera.expectNoRequest(t)
}

// A stream that starts and ends again and again gets requests with the
// pauses between them, like requests that are refused. Only a stream that
// has run for a while lets the next request follow at once.
func TestKeeperPausesWhenTheStreamEndsAtOnce(t *testing.T) {
	camera := newFakeCamera(nil)
	camera.streamArrives = true
	camera.streamEndsAtOnce = true

	startKeeper(t, camera, func(_ *baby.StateManager, keeper *streamKeeper) {
		keeper.retryPauses = []time.Duration{10 * time.Millisecond, time.Hour}
		keeper.stableAfter = time.Hour
	})

	// The second request follows after the first, short pause. The third
	// one would follow after an hour.
	camera.expect(t, stopped, started, stopped, started)
	camera.expectNoRequest(t)
}

// A stop of the program does not wait for the answer to a request that is
// on its way. The camera may act on that request, so it is asked to stop
// the stream.
func TestKeeperStopsDuringARequest(t *testing.T) {
	camera := newFakeCamera(nil)
	camera.hold = make(chan struct{})
	defer close(camera.hold)

	test := startKeeper(t, camera, nil)

	camera.expect(t, stopped, started)
	test.stop(t)

	camera.expect(t, stopped)
}

// At a stop of the program, the keeper asks the camera to stop the
// stream, so that the place it takes is free at once.
func TestKeeperStopsTheStreamAtAStop(t *testing.T) {
	camera := newFakeCamera(nil)

	camera.streamArrives = true
	test := startKeeper(t, camera, nil)

	camera.expect(t, stopped, started)
	camera.expectNoRequest(t)

	test.stop(t)

	camera.expect(t, stopped)
	camera.expectNoRequest(t)
}

// A stream that this program did not ask for on this connection, but
// that runs, is stopped at a stop of the program too: it is the stream
// of an earlier connection.
func TestKeeperStopsAStreamOfAnEarlierConnection(t *testing.T) {
	camera := newFakeCamera(nil)

	test := startKeeper(t, camera, func(states *baby.StateManager, _ *streamKeeper) {
		states.Update(testBabyUID, *baby.NewState().SetStreamState(baby.StreamState_Alive))
	})

	camera.expectNoRequest(t)
	test.stop(t)

	camera.expect(t, stopped)
}

// When the connection was lost, the keeper ends without a request to stop
// the stream: it could not be sent, and the stream goes on without the
// websocket connection.
func TestKeeperSendsNothingWhenTheConnectionWasLost(t *testing.T) {
	camera := newFakeCamera(nil)
	camera.streamArrives = true
	test := startKeeper(t, camera, nil)

	camera.expect(t, stopped, started)

	camera.endOnce.Do(func() { close(camera.done) })
	test.stop(t)

	camera.expectNoRequest(t)
}

// A stop during the pause after a refusal does not wait for the pause.
// Nothing was accepted, so there is nothing to stop.
func TestKeeperStopsDuringAPause(t *testing.T) {
	camera := newFakeCamera(errLimit)
	paused := make(chan struct{}, 1)

	test := startKeeper(t, camera, func(_ *baby.StateManager, keeper *streamKeeper) {
		keeper.retryPauses = []time.Duration{time.Hour}
		keeper.onPause = func() {
			select {
			case paused <- struct{}{}:
			default:
			}
		}
	})

	camera.expect(t, stopped, started)

	// The request is on its way at this point. Wait until the keeper has
	// taken the refusal and has started its pause: a stop during the
	// request itself is another case, with another result.
	select {
	case <-paused:
	case <-time.After(5 * time.Second):
		t.Fatal("the keeper did not start its pause")
	}

	test.stop(t)

	camera.expectNoRequest(t)
}

// Requests that get no answer can mean that the connection is dead. After
// three in a row the keeper gives the connection up, and the manager
// makes a new one.
func TestKeeperGivesUpAConnectionWithoutAnswers(t *testing.T) {
	camera := newFakeCamera(client.ErrRequestTimeout)
	camera.stopAnswer = client.ErrRequestTimeout

	startKeeper(t, camera, nil)

	select {
	case reason := <-camera.gaveUp:
		assert.Equal(t, errNoAnswers, reason)
	case <-time.After(5 * time.Second):
		t.Fatal("the connection was not given up")
	}

	camera.mu.Lock()
	defer camera.mu.Unlock()
	assert.Equal(t, streamTimeoutsBeforeReconnect, camera.starts)
}

// A connection that the idle limit watches over reaches the service. When
// the requests get no answer on it, the cause lies behind the service,
// and most often it is a camera that is switched off. Such a connection
// is given up too, but after more requests.
func TestKeeperKeepsAWatchedConnectionForLonger(t *testing.T) {
	camera := newFakeCamera(client.ErrRequestTimeout)
	camera.stopAnswer = client.ErrRequestTimeout
	camera.watched = true

	startKeeper(t, camera, nil)

	select {
	case reason := <-camera.gaveUp:
		assert.Equal(t, errNoAnswers, reason)
	case <-time.After(5 * time.Second):
		t.Fatal("the connection was not given up")
	}

	camera.mu.Lock()
	defer camera.mu.Unlock()
	assert.Equal(t, streamTimeoutsBeforeReconnectWatched, camera.starts)
}

// The time of the next request belongs to the camera, not to one
// connection. A connection that is made again and again does not lead to
// a request each time.
func TestKeeperOfTheNextConnectionKeepsThePause(t *testing.T) {
	pace := &streamPace{}
	states := baby.NewStateManager()
	paused := make(chan struct{}, 4)

	run := func(camera *fakeCamera) utils.GracefulRunner {
		camera.states = states

		keeper := newStreamKeeper(testBabyUID, testTargetURL, camera, states, pace)
		keeper.retryPauses = []time.Duration{time.Hour}
		keeper.onPause = func() { paused <- struct{}{} }

		return utils.RunWithGracefulCancel(keeper.run)
	}

	// The first connection: the request is refused, and the connection
	// ends
	first := newFakeCamera(errLimit)
	firstRunner := run(first)

	first.expect(t, stopped, started)

	awaitPause(t, paused)

	first.endOnce.Do(func() { close(first.done) })
	firstRunner.Cancel()

	// The next connection: the pause of an hour is not over
	second := newFakeCamera(nil)
	secondRunner := run(second)

	second.expectNoRequest(t)

	// The program stops in that pause. The first request was refused, so
	// the camera sends nothing, and there is nothing to stop.
	secondRunner.Cancel()
	second.expectNoRequest(t)
}

// A request without an answer can still make the camera send its stream.
// When the program stops, the camera is asked to stop the stream, also
// when the request went out on an earlier connection.
func TestKeeperStopsAStreamThatAnEarlierConnectionAskedFor(t *testing.T) {
	pace := &streamPace{}
	states := baby.NewStateManager()
	paused := make(chan struct{}, 4)

	run := func(camera *fakeCamera) utils.GracefulRunner {
		camera.states = states

		keeper := newStreamKeeper(testBabyUID, testTargetURL, camera, states, pace)
		keeper.retryPauses = []time.Duration{time.Hour}
		keeper.onPause = func() { paused <- struct{}{} }

		return utils.RunWithGracefulCancel(keeper.run)
	}

	// The first connection: the request gets no answer, and the
	// connection ends
	first := newFakeCamera(client.ErrRequestTimeout)
	firstRunner := run(first)

	first.expect(t, stopped, started)
	awaitPause(t, paused)

	// The connection was given up on purpose, and the keeper has asked
	// the camera to stop
	firstRunner.Cancel()
	first.expect(t, stopped)

	// The next connection is in the pause when the program stops
	second := newFakeCamera(nil)
	secondRunner := run(second)

	awaitPause(t, paused)
	secondRunner.Cancel()

	second.expect(t, stopped)
}

// Requests that got no answer on a connection that has ended were most
// likely lost with it. They do not count, and the keeper of the next
// connection asks at once.
func TestKeeperOfTheNextConnectionAsksAtOnceAfterALostConnection(t *testing.T) {
	pace := &streamPace{}
	states := baby.NewStateManager()
	paused := make(chan struct{}, 4)

	run := func(camera *fakeCamera) utils.GracefulRunner {
		camera.states = states

		keeper := newStreamKeeper(testBabyUID, testTargetURL, camera, states, pace)
		keeper.retryPauses = []time.Duration{time.Hour}
		keeper.onPause = func() { paused <- struct{}{} }

		return utils.RunWithGracefulCancel(keeper.run)
	}

	// The first connection is dead: nothing gets an answer, and then it
	// ends
	first := newFakeCamera(client.ErrRequestTimeout)
	first.stopAnswer = client.ErrRequestTimeout
	firstRunner := run(first)

	first.expect(t, stopped, started)
	awaitPause(t, paused)

	first.endOnce.Do(func() { close(first.done) })
	firstRunner.Cancel()

	// The next connection does not wait for the pause of an hour
	second := newFakeCamera(nil)
	second.streamArrives = true
	secondRunner := run(second)
	defer secondRunner.Cancel()

	second.expect(t, stopped, started)
}

// A request for the stream that gets no answer, on a connection on which
// the camera has just answered the stop: the connection works, so it is
// kept.
func TestKeeperKeepsAConnectionOnWhichTheStopIsAnswered(t *testing.T) {
	camera := newFakeCamera(client.ErrRequestTimeout)

	startKeeper(t, camera, nil)

	for i := 0; i < 2*streamTimeoutsBeforeReconnectWatched; i++ {
		camera.expect(t, stopped, started)
	}

	select {
	case <-camera.gaveUp:
		t.Fatal("the connection was given up")
	default:
	}
}

func awaitPause(t *testing.T, paused <-chan struct{}) {
	select {
	case <-paused:
	case <-time.After(5 * time.Second):
		t.Fatal("the keeper did not start its pause")
	}
}

// An answer between requests without an answer starts the count again.
func TestKeeperCountsOnlyRequestsWithoutAnswerInARow(t *testing.T) {
	camera := newFakeCamera(client.ErrRequestTimeout, client.ErrRequestTimeout, errLimit, client.ErrRequestTimeout, client.ErrRequestTimeout, errLimit)
	camera.stopAnswer = client.ErrRequestTimeout

	startKeeper(t, camera, nil)

	for i := 0; i < 8; i++ {
		camera.expect(t, stopped, started)
	}

	select {
	case <-camera.gaveUp:
		t.Fatal("the connection was given up")
	default:
	}
}

// The connection ends: the keeper returns and leaves the state as it is.
// The keeper of the next connection goes on.
func TestKeeperReturnsWhenTheConnectionEnds(t *testing.T) {
	camera := newFakeCamera(client.ErrConnectionClosed)

	startKeeper(t, camera, nil)

	camera.expect(t, stopped, started)
	camera.expectNoRequest(t)

	select {
	case <-camera.gaveUp:
		t.Fatal("the connection was given up")
	default:
	}
}
