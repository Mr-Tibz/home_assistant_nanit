package app

import (
	"errors"
	"time"

	"github.com/indiefan/home_assistant_nanit/pkg/baby"
	"github.com/indiefan/home_assistant_nanit/pkg/client"
	"github.com/indiefan/home_assistant_nanit/pkg/utils"
	"github.com/rs/zerolog/log"
)

// streamLimitMessage - what the camera answers when it has no place left
// for one more app connection. A stream that it sends on a request of
// this program takes one of these places, like a phone that shows the
// video.
const streamLimitMessage = "Forbidden: Number of Mobile App connections above limit, declining connection"

// streamRetryPauses - time between two requests for the stream: after the
// first request of a row the first entry, and so on, the last entry from
// then on. A row ends when a stream has run for streamStableAfter.
//
// A place at the camera becomes free when a phone stops to show the
// video, and the camera gives no sign of that. Asking again is the only
// way to find out, so the pauses are short: the time without video after
// a place has become free is at most one pause. Before, the pause after
// a refusal was 5 minutes.
var streamRetryPauses = []time.Duration{
	20 * time.Second,
	40 * time.Second,
	60 * time.Second,
}

const (
	// streamRequestTimeout - time to wait for the answer of the camera
	// to a request for the stream
	streamRequestTimeout = 30 * time.Second

	// streamPublisherTimeout - time that the camera gets to start its
	// stream after it has accepted the request
	streamPublisherTimeout = 15 * time.Second

	// streamStopTimeout - time to wait for the answer of the camera to a
	// request to stop the stream. Stopping is done on the way to
	// something else, so the wait is short.
	streamStopTimeout = 3 * time.Second

	// streamStableAfter - a stream that has run for this long was a good
	// one. The request after it follows at once when it ends. A stream
	// that ends sooner is followed by the pauses of streamRetryPauses,
	// so that a stream that starts and ends again and again does not
	// lead to requests without a pause.
	streamStableAfter = 60 * time.Second

	// streamTimeoutsBeforeReconnect - after this many requests in a row
	// without an answer, the connection is given up and made again,
	// unless the idle limit watches over it (see
	// streamConnection.Watched). The connection itself may be the
	// cause, and without the idle limit a connection that is dead gives
	// no other sign.
	streamTimeoutsBeforeReconnect = 3

	// streamTimeoutsBeforeReconnectWatched - the same for a connection
	// that the idle limit watches over. Such a connection reaches the
	// service. What lies behind the service, on the way to the camera,
	// can still be at fault, and a new connection can repair that. If
	// the camera is switched off, a new connection changes nothing, so
	// this number is larger.
	streamTimeoutsBeforeReconnectWatched = 10
)

// errNoAnswers - the reason for giving up a connection on which the
// requests for the stream get no answer
var errNoAnswers = errors.New("the requests for the stream get no answer")

// errKeeperStopped - the keeper was told to stop while it waited for the
// answer to a request
var errKeeperStopped = errors.New("the stream keeper was told to stop")

// streamConnection - what the stream keeper needs of a websocket
// connection
type streamConnection interface {
	SendRequest(reqType client.RequestType, requestData *client.Request) func(time.Duration) (*client.Response, error)

	// GiveUp ends the connection, and the manager makes a new one
	GiveUp(reason error)

	// Done is closed when the connection has ended
	Done() <-chan struct{}

	// Watched reports whether the idle limit watches over this
	// connection, so that it is given up when it stops carrying data
	Watched() bool
}

// streamPace holds what the keepers of one camera know about its stream
// across the websocket connections: when the next request is due, and
// whether the camera may be sending. It outlives the connections, so that
// a connection that is made again and again does not lead to a request
// each time, and so that a stream that was asked for on an earlier
// connection is still stopped at a stop of the program.
type streamPace struct {
	// requests - requests in a row, see streamRetryPauses
	requests int

	// nextRequest - no request before this moment
	nextRequest time.Time

	// mayStream - the camera may send its stream on a request of this
	// program, or be about to: a request was sent, and the camera has
	// not refused it. That includes a request without an answer.
	mayStream bool

	// aliveSince - when the stream that runs was first seen. Zero when
	// no stream runs.
	aliveSince time.Time
}

// requested records that a request for the stream is sent, and sets the
// time of the next one.
func (pace *streamPace) requested(pauses []time.Duration) {
	pace.mayStream = true
	pace.requests++
	pace.nextRequest = time.Now().Add(pauses[min(pace.requests, len(pauses))-1])
}

// streamIsGood records that a stream has run for a while. The request
// after it follows at once.
func (pace *streamPace) streamIsGood() {
	pace.requests = 0
	pace.nextRequest = time.Time{}
}

// streamKeeper asks the camera to send its stream to this program, for
// as long as one websocket connection lasts, and asks again whenever the
// stream is not there.
//
// One keeper runs for each connection, and it is the only place that
// asks for the stream. It ends with the connection.
type streamKeeper struct {
	babyUID   string
	targetURL string
	conn      streamConnection
	states    *baby.StateManager

	// pace - shared with the keepers of the connections before and after
	// this one. They run one after the other, never at the same time.
	pace *streamPace

	retryPauses      []time.Duration
	requestTimeout   time.Duration
	publisherTimeout time.Duration
	stopTimeout      time.Duration
	stableAfter      time.Duration

	// onPause - called when the keeper starts to wait for the time of
	// the next request. For tests.
	onPause func()
}

func newStreamKeeper(babyUID string, targetURL string, conn streamConnection, states *baby.StateManager, pace *streamPace) *streamKeeper {
	return &streamKeeper{
		babyUID:   babyUID,
		targetURL: targetURL,
		conn:      conn,
		states:    states,
		pace:      pace,

		retryPauses:      streamRetryPauses,
		requestTimeout:   streamRequestTimeout,
		publisherTimeout: streamPublisherTimeout,
		stopTimeout:      streamStopTimeout,
		stableAfter:      streamStableAfter,
	}
}

// streamAlive reports whether the camera sends its stream to this
// program right now.
func (keeper *streamKeeper) streamAlive() bool {
	return keeper.states.GetBabyState(keeper.babyUID).GetStreamState() == baby.StreamState_Alive
}

// send sends one request about the stream and waits for the answer. It
// returns errKeeperStopped when ctx is done first. A nil ctx waits for
// the answer in every case.
func (keeper *streamKeeper) send(ctx utils.GracefulContext, status client.Streaming_Status, timeout time.Duration) error {
	awaitResponse := keeper.conn.SendRequest(client.RequestType_PUT_STREAMING, &client.Request{
		Streaming: &client.Streaming{
			Id:       client.StreamIdentifier(client.StreamIdentifier_MOBILE).Enum(),
			RtmpUrl:  utils.ConstRefStr(keeper.targetURL),
			Status:   client.Streaming_Status(status).Enum(),
			Attempts: utils.ConstRefInt32(1),
		},
	})

	if ctx == nil {
		_, err := awaitResponse(timeout)
		return err
	}

	// The wait for the answer has its own time limit, so this goroutine
	// ends by itself also when nobody takes its result
	answer := make(chan error, 1)
	go func() {
		_, err := awaitResponse(timeout)
		answer <- err
	}()

	select {
	case err := <-answer:
		return err
	case <-ctx.Done():
		return errKeeperStopped
	}
}

// start asks the camera for the stream.
//
// It asks the camera to stop the stream to our address first. The camera
// can still count a stream that it started for an earlier request of
// this program, for example one that broke without the camera noticing.
// That stream takes a place, and with it the camera can refuse the new
// request for lack of places. The camera stops a stream only for the
// exact address that it was started with, so this stops no stream of
// another program or phone.
func (keeper *streamKeeper) start(ctx utils.GracefulContext) error {
	err := keeper.send(ctx, client.Streaming_STOPPED, keeper.stopTimeout)
	if errors.Is(err, client.ErrConnectionClosed) || errors.Is(err, errKeeperStopped) {
		return err
	}

	log.Info().Str("target", keeper.targetURL).Msg("Requesting local streaming")

	// From here on the camera may act on the request, and the pause
	// before the next one starts. A request that could not be sent,
	// because the connection had ended, uses up no pause.
	keeper.pace.requested(keeper.retryPauses)

	return keeper.send(ctx, client.Streaming_STARTED, keeper.requestTimeout)
}

// stop asks the camera to stop the stream to our address, so that the
// place that it takes is free at once. Without this the camera keeps the
// place until it notices by itself that nothing takes the stream any
// more.
//
// It does nothing when the connection has ended: the request cannot be
// sent then. That is the case when the connection was lost, and not given
// up on purpose.
func (keeper *streamKeeper) stop() {
	select {
	case <-keeper.conn.Done():
		return
	default:
	}

	log.Info().Str("target", keeper.targetURL).Msg("Stopping local streaming")

	if err := keeper.send(nil, client.Streaming_STOPPED, keeper.stopTimeout); err != nil && !errors.Is(err, client.ErrConnectionClosed) {
		log.Warn().Err(err).Msg("Unable to stop local streaming")
	}
}

// What ended a wait
const (
	waitStopped = iota // the keeper was told to stop
	waitChanged        // the state of the stream may have changed
	waitElapsed        // the time is over
)

// wait waits until the state of the stream may have changed, until ctx is
// done, or for timeout. Without a timeout (0) it waits for one of the
// first two.
func (keeper *streamKeeper) wait(ctx utils.GracefulContext, changed <-chan struct{}, timeout time.Duration) int {
	var elapsed <-chan time.Time

	if timeout > 0 {
		timer := time.NewTimer(timeout)
		defer timer.Stop()
		elapsed = timer.C
	}

	select {
	case <-ctx.Done():
		return waitStopped
	case <-changed:
		return waitChanged
	case <-elapsed:
		return waitElapsed
	}
}

// run keeps the stream going until ctx is done, which is when the
// connection is given up. When the connection is given up on purpose,
// like at a stop of the program, it still works at that moment, and run
// stops the stream.
func (keeper *streamKeeper) run(ctx utils.GracefulContext) {
	// changed - the state of the stream may have changed
	changed := make(chan struct{}, 1)

	unsubscribe := keeper.states.Subscribe(func(babyUID string, update baby.State) {
		if babyUID == keeper.babyUID && update.StreamState != nil {
			select {
			case changed <- struct{}{}:
			default:
			}
		}
	})
	defer unsubscribe()

	// The one way out. A stream that the camera may send on our request,
	// or that runs, is stopped, see stop.
	defer func() {
		if keeper.pace.mayStream || keeper.streamAlive() {
			keeper.stop()
		}
	}()

	// timeouts - requests in a row without an answer on this connection
	timeouts := 0

	for {
		// While the stream runs, there is nothing to ask
		if keeper.streamAlive() {
			if keeper.pace.aliveSince.IsZero() {
				keeper.pace.aliveSince = time.Now()
			}

			// A stream that has run for a while ends the row of
			// requests. The time counts from when the stream was first
			// seen, also by the keeper of an earlier connection.
			untilGood := time.Duration(0)
			if keeper.pace.requests > 0 {
				untilGood = max(keeper.stableAfter-time.Since(keeper.pace.aliveSince), time.Nanosecond)
			}

			switch keeper.wait(ctx, changed, untilGood) {
			case waitStopped:
				return
			case waitElapsed:
				keeper.pace.streamIsGood()
				timeouts = 0
			}

			continue
		}

		keeper.pace.aliveSince = time.Time{}

		// The pause since the last request. Whatever ends the wait, the
		// top of the loop looks again whether the stream is there: it
		// can have started at the very end of the pause.
		if pause := time.Until(keeper.pace.nextRequest); pause > 0 {
			if keeper.onPause != nil {
				keeper.onPause()
			}

			if keeper.wait(ctx, changed, pause) == waitStopped {
				return
			}

			continue
		}

		select {
		case <-ctx.Done():
			return
		default:
		}

		err := keeper.start(ctx)

		switch {
		case err == nil:
			timeouts = 0
			log.Info().Msg("Local streaming successfully requested")

			if !keeper.waitForStream(ctx, changed) {
				select {
				case <-ctx.Done():
					return
				default:
				}

				log.Warn().Str("waited", keeper.publisherTimeout.String()).Msg("The camera has accepted the request, but no stream has arrived. Asking again")
			}

		case errors.Is(err, errKeeperStopped):
			return

		case errors.Is(err, client.ErrConnectionClosed):
			// The connection has ended. That says nothing about the
			// stream, and the keeper of the next connection goes on.
			return

		case errors.Is(err, client.ErrRequestTimeout):
			timeouts++

			limit := streamTimeoutsBeforeReconnect
			if keeper.conn.Watched() {
				limit = streamTimeoutsBeforeReconnectWatched
			}

			if timeouts >= limit {
				log.Warn().Int("requests", timeouts).Msg("The requests for the stream get no answer. Making the websocket connection again")
				keeper.conn.GiveUp(errNoAnswers)
				return
			}

			log.Warn().Msg("Streaming request timeout, trying again")

		default:
			// The camera has refused the request, so it sends no stream
			// for it. A stream of an earlier request was stopped before
			// this request was sent.
			keeper.pace.mayStream = false
			timeouts = 0

			if err.Error() == streamLimitMessage {
				log.Warn().Err(err).Msg("Too many app connections, waiting for local connection to become available...")
			} else {
				log.Error().Err(err).Msg("Failed to request local streaming")
			}
		}
	}
}

// waitForStream waits until the stream is there, for the time that the
// camera gets to start it. It reports whether the stream is there.
func (keeper *streamKeeper) waitForStream(ctx utils.GracefulContext, changed <-chan struct{}) bool {
	deadline := time.Now().Add(keeper.publisherTimeout)

	for !keeper.streamAlive() {
		remaining := time.Until(deadline)
		if remaining <= 0 || keeper.wait(ctx, changed, remaining) == waitStopped {
			return false
		}
	}

	return true
}
