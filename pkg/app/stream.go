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
	// unless the connection has shown that it works (see
	// streamConnection.AnswersPings). The connection itself may be the
	// cause, and a connection that is dead gives no other sign when the
	// service answers no ping.
	streamTimeoutsBeforeReconnect = 3
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

	// AnswersPings reports whether the other end has answered a ping on
	// this connection. Such a connection is watched for its end by the
	// idle limit.
	AnswersPings() bool
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

	retryPauses      []time.Duration
	requestTimeout   time.Duration
	publisherTimeout time.Duration
	stopTimeout      time.Duration
	stableAfter      time.Duration
}

func newStreamKeeper(babyUID string, targetURL string, conn streamConnection, states *baby.StateManager) *streamKeeper {
	return &streamKeeper{
		babyUID:   babyUID,
		targetURL: targetURL,
		conn:      conn,
		states:    states,

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

	// mayStream - the camera may send its stream on a request of this
	// connection, or be about to: a request was sent, and the camera has
	// not refused it. That includes a request without an answer.
	mayStream := false

	// The one way out. A stream that the camera may send on our request,
	// or that runs, is stopped, see stop.
	defer func() {
		if mayStream || keeper.streamAlive() {
			keeper.stop()
		}
	}()

	// requests - requests in a row, see streamRetryPauses
	// timeouts - requests in a row without an answer
	// nextRequest - no request before this moment
	requests, timeouts := 0, 0
	var nextRequest time.Time

	for {
		// While the stream runs, there is nothing to ask
		if keeper.streamAlive() {
			stableAfter := keeper.stableAfter
			if requests == 0 {
				stableAfter = 0
			}

			switch keeper.wait(ctx, changed, stableAfter) {
			case waitStopped:
				return
			case waitElapsed:
				requests, timeouts = 0, 0
				nextRequest = time.Time{}
			}

			continue
		}

		// The pause since the last request
		if pause := time.Until(nextRequest); pause > 0 {
			if keeper.wait(ctx, changed, pause) == waitStopped {
				return
			}

			// The pause is over, or the stream may be there. The top of
			// the loop finds out which.
			if time.Now().Before(nextRequest) {
				continue
			}
		}

		select {
		case <-ctx.Done():
			return
		default:
		}

		mayStream = true
		requests++
		nextRequest = time.Now().Add(keeper.retryPauses[min(requests, len(keeper.retryPauses))-1])

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

			// A connection that answers pings works, and the idle limit
			// watches over it. Then the camera is what gives no answer,
			// and a new connection would not change that.
			if timeouts >= streamTimeoutsBeforeReconnect && !keeper.conn.AnswersPings() {
				log.Warn().Int("requests", timeouts).Msg("The requests for the stream get no answer. Making the websocket connection again")
				keeper.conn.GiveUp(errNoAnswers)
				return
			}

			log.Warn().Msg("Streaming request timeout, trying again")

		default:
			// The camera has refused the request, so it sends no stream
			// for it
			mayStream = false
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
