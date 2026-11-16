package client

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/indiefan/home_assistant_nanit/pkg/baby"
	"github.com/indiefan/home_assistant_nanit/pkg/utils"
	"github.com/rs/zerolog/log"
)

// defaultWebsocketBaseURL - address of the websocket service of Nanit
const defaultWebsocketBaseURL = "wss://api.nanit.com"

const (
	// DefaultHandshakeTimeout - time limit for making the connection
	DefaultHandshakeTimeout = 15 * time.Second

	// DefaultPingInterval - time between two websocket pings
	DefaultPingInterval = 20 * time.Second

	// DefaultIdleTimeout - a connection on which nothing has arrived for
	// this long has ended. It is longer than three ping intervals.
	DefaultIdleTimeout = 70 * time.Second

	// handlersStopTimeout - time that the handlers of a connection get
	// to finish their work when the connection is given up, before the
	// socket is closed
	handlersStopTimeout = 5 * time.Second
)

// authorizeWaits - pauses between tries to authorize before a websocket
// attempt, when the failure can pass by itself
var authorizeWaits = []time.Duration{
	5 * time.Second,
	10 * time.Second,
	20 * time.Second,
	30 * time.Second,
}

// reconnectCooldown - pauses after attempts that failed in a row, see
// reconnectResetThreshold. The list is short on purpose: the camera can
// only be asked to stream through this connection, so a long pause here
// is a long time without video after an outage has passed.
var reconnectCooldown = []time.Duration{
	2 * time.Second,
	5 * time.Second,
	10 * time.Second,
	20 * time.Second,
	30 * time.Second,
}

// reconnectResetThreshold - when an attempt that lasted this long ends,
// the next attempt starts at once and counts as the first one. The time
// counts from the start of the attempt, so it includes the authorization
// and the making of the connection.
const reconnectResetThreshold = 60 * time.Second

// shortConnection - a connection that the server closes within this time
// counts as refused, like a handshake that is answered with a refusal
const shortConnection = 10 * time.Second

// refusedByClose reports whether reason says that the server has closed
// the connection with a close message that names a refusal: a violation
// of its policy, or a code of the service itself. Every other end is no
// refusal: a connection that breaks, reaches its idle limit or cannot be
// written to, and a close that names a restart, a fault of the server or
// a normal end.
func refusedByClose(reason error) bool {
	var closeErr *websocket.CloseError
	if !errors.As(reason, &closeErr) {
		return false
	}

	// 4000 to 4999 are the codes that a service defines for itself
	return closeErr.Code == websocket.ClosePolicyViolation || (closeErr.Code >= 4000 && closeErr.Code <= 4999)
}

// refusalPauses - extra pause after attempts in a row that the server has
// refused: none after the first one, refusalPauses[0] after the second
// one, and so on, the last entry from then on. The server has refused an
// attempt when it answers the handshake with a 4xx status other than 408,
// or accepts the connection and closes it within shortConnection (see
// refusedByClose).
//
// The short pauses of reconnectCooldown are for failures that pass, like
// an outage of the network. A refusal is an answer, and asking again at
// once does not change it. When the answer names the token as the cause,
// the next attempt comes at once, with a new token if the refused one can
// be replaced. When the server refuses again, something else is the
// cause, for example a camera that is switched off or that the account
// may no longer use, and asking every 30 seconds for as long as that
// lasts could get the account blocked.
var refusalPauses = []time.Duration{
	30 * time.Second,
	time.Minute,
	2 * time.Minute,
	5 * time.Minute,
}

// errAttemptEnded - the attempt was told to stop from outside
var errAttemptEnded = errors.New("the connection attempt was ended")

type readyState struct {
	Context    utils.GracefulContext
	Connection *WebsocketConnection

	// handlers - the handlers that were started for this connection
	handlers []utils.GracefulRunner
}

// WebsocketConnectionHandler - handler of ready connection
type WebsocketConnectionHandler func(*WebsocketConnection, utils.GracefulContext)

// WebsocketConnectionManager - connection manager
type WebsocketConnectionManager struct {
	BabyUID          string
	CameraUID        string
	API              *NanitClient
	BabyStateManager *baby.StateManager

	// BaseURL - address of the websocket service. Empty means the real one.
	BaseURL string

	// HandshakeTimeout, PingInterval, IdleTimeout - see the constants
	// with the defaults. Zero means the default. A negative IdleTimeout
	// turns the idle limit off.
	HandshakeTimeout time.Duration
	PingInterval     time.Duration
	IdleTimeout      time.Duration

	// Cooldown - pauses after failed attempts. Empty means the default.
	Cooldown []time.Duration

	// RefusalPauses - see refusalPauses. Empty means the default.
	RefusalPauses []time.Duration

	mu               sync.Mutex
	readyState       *readyState
	readySubscribers []WebsocketConnectionHandler

	// refusedToken - the token that the server has refused. The next
	// attempt replaces it first.
	refusedToken string

	// lastRefusedToken - the last token that the server has refused by
	// name. The first refusal of a token gets no pause.
	lastRefusedToken string

	// refusals - attempts in a row that the server has refused, see
	// refusalPauses
	refusals int

	// ctx - the context that RunWithinContext runs in
	ctx utils.GracefulContext
}

// NewWebsocketConnectionManager - constructor
func NewWebsocketConnectionManager(babyUID string, cameraUID string, api *NanitClient, babyStateManager *baby.StateManager) *WebsocketConnectionManager {
	manager := &WebsocketConnectionManager{
		BabyUID:          babyUID,
		CameraUID:        cameraUID,
		API:              api,
		BabyStateManager: babyStateManager,
	}

	manager.WithReadyConnection(func(conn *WebsocketConnection, ctx utils.GracefulContext) {
		ticker := time.NewTicker(20 * time.Second)

		for {
			select {
			case <-ctx.Done():
				ticker.Stop()
				return
			case <-ticker.C:
				conn.SendMessage(&Message{
					Type: Message_Type(Message_KEEPALIVE).Enum(),
				})
			}
		}
	})

	return manager
}

// WithReadyConnection - registers handler which will be called as a go routine upon ready connection
func (manager *WebsocketConnectionManager) WithReadyConnection(handler WebsocketConnectionHandler) {
	manager.mu.Lock()
	defer manager.mu.Unlock()

	manager.readySubscribers = append(manager.readySubscribers, handler)

	if manager.readyState != nil {
		log.Debug().Msg("Immediately notifying ready handler")
		manager.readyState.start(handler)
	}
}

// start runs handler for the connection. The caller holds manager.mu.
func (state *readyState) start(handler WebsocketConnectionHandler) {
	state.handlers = append(state.handlers, state.Context.RunAsChild(func(childCtx utils.GracefulContext) {
		handler(state.Connection, childCtx)
	}))
}

// RunWithinContext - starts websocket connection attempt loop
func (manager *WebsocketConnectionManager) RunWithinContext(ctx utils.GracefulContext) {
	manager.ctx = ctx

	cooldown := manager.Cooldown
	if len(cooldown) == 0 {
		cooldown = reconnectCooldown
	}

	utils.RunWithPerseverance(manager.run, ctx, utils.PerseverenceOpts{
		RunnerID:       fmt.Sprintf("websocket-%v", manager.CameraUID),
		ResetThreshold: reconnectResetThreshold,
		Cooldown:       cooldown,
	})
}

// stopped - closed when the manager is told to stop
func (manager *WebsocketConnectionManager) stopped() <-chan struct{} {
	return manager.ctx.Done()
}

func (manager *WebsocketConnectionManager) url() string {
	base := manager.BaseURL
	if base == "" {
		base = defaultWebsocketBaseURL
	}

	return fmt.Sprintf("%v/focus/cameras/%v/user_connect", base, manager.CameraUID)
}

// noteRefusal records that the server has refused an attempt, see
// refusalPauses. refusedToken is the token of the attempt when the answer
// names it as the cause (401, 403), and "" otherwise. It returns the
// pause to make before the next attempt.
func (manager *WebsocketConnectionManager) noteRefusal(refusedToken string) time.Duration {
	manager.mu.Lock()
	defer manager.mu.Unlock()

	manager.refusals++

	if refusedToken != "" {
		manager.refusedToken = refusedToken

		if refusedToken != manager.lastRefusedToken {
			// The first refusal of this token. The next attempt can
			// repair that with a new token, so it gets no pause. The
			// refusal counts all the same: when the server refuses the
			// new token too, the pauses go on where they were.
			manager.lastRefusedToken = refusedToken

			return 0
		}
	}

	if manager.refusals < 2 {
		return 0
	}

	pauses := manager.RefusalPauses
	if len(pauses) == 0 {
		pauses = refusalPauses
	}

	return pauses[min(manager.refusals-2, len(pauses)-1)]
}

// noteNoRefusal records that an attempt has ended in another way than a
// refusal: with a connection that lasted, or with a failure that does not
// show whether the server accepts the connection, like a server error or
// a network that is down. The refusals are counted in a row, so this
// starts the count again.
func (manager *WebsocketConnectionManager) noteNoRefusal() {
	manager.mu.Lock()
	defer manager.mu.Unlock()

	manager.refusals = 0
}

// takeRefusedToken returns the token that the server has refused since
// the last call, or "".
func (manager *WebsocketConnectionManager) takeRefusedToken() string {
	manager.mu.Lock()
	defer manager.mu.Unlock()

	refused := manager.refusedToken
	manager.refusedToken = ""

	return refused
}

func (manager *WebsocketConnectionManager) run(attempt utils.AttemptContext) {
	// Reauthorize if we assume we don't have a valid token, or if the
	// server has refused the token.
	//
	// A failure that can pass (no network, a timeout, a server error) is
	// tried again here, with short pauses, and does not fail the attempt.
	refused := manager.takeRefusedToken()
	err := RetryTemporary(attempt.Done(), authorizeWaits, "authorize", func() error {
		if refused != "" {
			return manager.API.RenewRefusedToken(refused)
		}

		return manager.API.MaybeAuthorize(false)
	})
	if errors.Is(err, ErrRetryCancelled) {
		return
	}
	if err != nil {
		log.Fatal().Err(err).Msg("Unable to authorize")
	}

	// Like a request, the attempt waits for an authorization that is
	// running and takes the token that it gives.
	token, _ := manager.API.tokenForRequest()

	conn, refusalPause, err := manager.connect(attempt, token)
	if err != nil {
		manager.pauseAfterRefusal(refusalPause)

		select {
		case <-attempt.Done():
			// Told to stop while connecting: that is not a failure
		default:
			attempt.Fail(err)
		}

		return
	}

	connected := time.Now()
	log.Info().Str("url", manager.url()).Msg("Connected to websocket")

	// The state says that the connection is alive before the handlers
	// start
	manager.BabyStateManager.Update(manager.BabyUID, *baby.NewState().SetWebsocketAlive(true))

	// Start the handlers
	state := &readyState{Context: attempt, Connection: conn}

	manager.mu.Lock()
	manager.readyState = state
	log.Trace().Int("num_handlers", len(manager.readySubscribers)).Msg("Notifying websocket ready handlers")
	for _, handler := range manager.readySubscribers {
		state.start(handler)
	}
	manager.mu.Unlock()

	// Read until the connection ends, by itself or because we end it
	readDone := make(chan struct{})
	go func() {
		conn.read(manager.idleTimeout())
		close(readDone)
	}()
	go conn.ping(manager.pingInterval())

	var refusalPauseAfter time.Duration

	select {
	case <-attempt.Done():
		// Told to stop. The connection still works, and the state says
		// so until the handlers have finished and the socket is closed.
	case <-conn.Done():
		reason := conn.endReason()
		log.Error().Err(reason).Msg("Disconnected from server")

		if refusedByClose(reason) && time.Since(connected) < shortConnection {
			refusalPauseAfter = manager.noteRefusal("")
		} else {
			manager.noteNoRefusal()
		}

		manager.BabyStateManager.Update(manager.BabyUID, *baby.NewState().SetWebsocketAlive(false))

		// This also tells the handlers to stop
		attempt.Fail(reason)
	}

	// No handler is started for this connection from here on
	manager.mu.Lock()
	manager.readyState = nil
	handlers := state.handlers
	manager.mu.Unlock()

	// Let the handlers finish before the socket is closed: a handler may
	// have a last message to send, for example to stop the stream.
	waitForHandlers(handlers, handlersStopTimeout)

	manager.BabyStateManager.Update(manager.BabyUID, *baby.NewState().SetWebsocketAlive(false))

	log.Debug().Msg("Closing websocket")
	conn.end(errAttemptEnded)
	<-readDone

	manager.pauseAfterRefusal(refusalPauseAfter)
}

// pauseAfterRefusal waits for pause, see refusalPauses, or until the
// program is told to stop.
//
// The pause is made inside the attempt. RunWithPerseverance has one list
// of pauses for every kind of failure and counts the time of the attempt
// against it, so this pause takes the place of its pause and does not
// come on top of it.
func (manager *WebsocketConnectionManager) pauseAfterRefusal(pause time.Duration) {
	if pause <= 0 {
		return
	}

	log.Warn().Str("pause", pause.String()).Msg("The server has refused the connection again. Waiting before the next attempt")

	// The attempt has failed at this point when the server has closed a
	// connection, so its own signal is of no use to wait on. The parent
	// of the attempt tells when the program stops.
	select {
	case <-manager.stopped():
	case <-time.After(pause):
	}
}

// connect makes the connection. It gives up when the attempt is told to
// stop, and after the handshake time limit. When the server has refused
// the connection, it also returns the pause to make before the next
// attempt.
func (manager *WebsocketConnectionManager) connect(attempt utils.AttemptContext, token string) (*WebsocketConnection, time.Duration, error) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// finished - connect has returned, with or without a connection
	finished := make(chan struct{})
	defer close(finished)

	go func() {
		select {
		case <-attempt.Done():
			cancel()
		case <-finished:
		}
	}()

	dialer := websocket.Dialer{
		HandshakeTimeout: cmp.Or(manager.HandshakeTimeout, DefaultHandshakeTimeout),

		NetDialContext: func(ctx context.Context, network string, addr string) (net.Conn, error) {
			netConn, err := (&net.Dialer{}).DialContext(ctx, network, addr)
			if err != nil {
				return nil, err
			}

			// The websocket library looks at the context only until
			// the TCP connection is there. Closing that connection
			// also ends the TLS handshake and the wait for the answer
			// of the server, so a stop does not wait for their limit.
			go func() {
				select {
				case <-attempt.Done():
					// When both are ready, the handshake is over and
					// the connection is no longer ours to close: a
					// handler may still send a last message on it.
					select {
					case <-finished:
					default:
						netConn.Close()
					}
				case <-finished:
				}
			}()

			return netConn, nil
		},
	}

	header := http.Header{}
	header.Set("Authorization", fmt.Sprintf("Bearer %v", token))

	log.Trace().Msg("Connecting to websocket")

	socket, response, err := dialer.DialContext(ctx, manager.url(), header)
	if err != nil {
		status := 0
		if response != nil {
			status = response.StatusCode
		}

		select {
		case <-attempt.Done():
			// Told to stop while connecting. That is no failure, and it
			// says nothing about the server.
			return nil, 0, err
		default:
		}

		log.Error().Str("url", manager.url()).Int("status", status).Err(err).Msg("Unable to establish websocket connection")

		switch {
		case status == http.StatusUnauthorized || status == http.StatusForbidden:
			// Only a refusal of the token leads to a new token. A
			// renewal replaces the refresh token, so it is not done on
			// a guess.
			return nil, manager.noteRefusal(token), err
		case status >= 400 && status < 500 && status != http.StatusRequestTimeout:
			return nil, manager.noteRefusal(""), err
		}

		manager.noteNoRefusal()

		return nil, 0, err
	}

	return NewWebsocketConnection(socket), 0, nil
}

func (manager *WebsocketConnectionManager) pingInterval() time.Duration {
	return cmp.Or(manager.PingInterval, DefaultPingInterval)
}

// idleTimeout returns the idle limit, or 0 when it is turned off. The
// default is DefaultIdleTimeout, or three ping intervals if that is more,
// so that a longer interval alone gives a pair of values that fit.
func (manager *WebsocketConnectionManager) idleTimeout() time.Duration {
	if manager.IdleTimeout < 0 {
		return 0
	}

	return cmp.Or(manager.IdleTimeout, max(DefaultIdleTimeout, 3*manager.pingInterval()))
}

// waitForHandlers waits until all handlers have returned, but not longer
// than timeout.
func waitForHandlers(handlers []utils.GracefulRunner, timeout time.Duration) {
	finished := make(chan struct{})

	go func() {
		for _, handler := range handlers {
			handler.Wait()
		}
		close(finished)
	}()

	select {
	case <-finished:
	case <-time.After(timeout):
		log.Warn().Msg("The handlers of the websocket connection did not finish in time")
	}
}
