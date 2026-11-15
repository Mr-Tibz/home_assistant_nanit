package client

import (
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

// refusalPauses - extra pause after handshakes in a row that the server
// has answered with a refusal (a 4xx status): none after the first one,
// refusalPauses[0] after the second one, and so on, the last entry from
// then on.
//
// The short pauses of reconnectCooldown are for failures that pass, like
// an outage of the network. A refusal is an answer, and asking again at
// once does not change it. After the first one the token is replaced if
// it can be the cause. When the server refuses again, something else is
// the cause, for example a camera that the account may no longer use, and
// asking every 30 seconds for as long as that lasts could get the account
// blocked.
var refusalPauses = []time.Duration{
	30 * time.Second,
	time.Minute,
	2 * time.Minute,
	5 * time.Minute,
	10 * time.Minute,
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

	// Cooldown - pauses after failed attempts. Nil means the default.
	Cooldown []time.Duration

	// RefusalPauses - see refusalPauses. Nil means the default.
	RefusalPauses []time.Duration

	mu               sync.Mutex
	readyState       *readyState
	readySubscribers []WebsocketConnectionHandler

	// refusedToken - the token that the server has refused. The next
	// attempt replaces it first.
	refusedToken string

	// refusals - handshakes in a row that the server has answered with
	// a refusal
	refusals int
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
	cooldown := manager.Cooldown
	if cooldown == nil {
		cooldown = reconnectCooldown
	}

	utils.RunWithPerseverance(manager.run, ctx, utils.PerseverenceOpts{
		RunnerID:       fmt.Sprintf("websocket-%v", manager.CameraUID),
		ResetThreshold: reconnectResetThreshold,
		Cooldown:       cooldown,
	})
}

func (manager *WebsocketConnectionManager) url() string {
	base := manager.BaseURL
	if base == "" {
		base = defaultWebsocketBaseURL
	}

	return fmt.Sprintf("%v/focus/cameras/%v/user_connect", base, manager.CameraUID)
}

func orDefault(value time.Duration, defaultValue time.Duration) time.Duration {
	if value == 0 {
		return defaultValue
	}

	return value
}

// noteRefusal records that the server has answered the handshake with a
// refusal. tokenRefused says that the answer names the token as the
// cause (401, 403). It returns the pause to make before the next attempt.
func (manager *WebsocketConnectionManager) noteRefusal(token string, tokenRefused bool) time.Duration {
	manager.mu.Lock()
	defer manager.mu.Unlock()

	if tokenRefused {
		manager.refusedToken = token
	}

	manager.refusals++
	if manager.refusals < 2 {
		return 0
	}

	pauses := manager.RefusalPauses
	if pauses == nil {
		pauses = refusalPauses
	}

	return pauses[utils.MinInt(manager.refusals-2, len(pauses)-1)]
}

// noteConnection records that the server has accepted a handshake.
func (manager *WebsocketConnectionManager) noteConnection() {
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
		if refusalPause > 0 {
			log.Warn().Str("pause", refusalPause.String()).Msg("The server has refused the connection again. Waiting before the next attempt")

			select {
			case <-attempt.Done():
			case <-time.After(refusalPause):
			}
		}

		select {
		case <-attempt.Done():
			// Told to stop while connecting: that is not a failure
		default:
			attempt.Fail(err)
		}

		return
	}

	manager.noteConnection()
	log.Info().Str("url", manager.url()).Msg("Connected to websocket")

	// Start the handlers
	state := &readyState{Context: attempt, Connection: conn}

	manager.mu.Lock()
	manager.readyState = state
	log.Trace().Int("num_handlers", len(manager.readySubscribers)).Msg("Notifying websocket ready handlers")
	for _, handler := range manager.readySubscribers {
		state.start(handler)
	}
	manager.mu.Unlock()

	manager.BabyStateManager.Update(manager.BabyUID, *baby.NewState().SetWebsocketAlive(true))

	// Read until the connection ends, by itself or because we end it
	readResult := make(chan error, 1)
	go func() { readResult <- conn.read(manager.idleTimeout()) }()
	go conn.ping(orDefault(manager.PingInterval, DefaultPingInterval))

	select {
	case <-attempt.Done():
		// Told to stop. The connection still works, and the state says
		// so until the handlers have finished: a handler may send a last
		// message only on a connection that is alive.
	case <-conn.Done():
		reason := conn.endReason()
		log.Error().Err(reason).Msg("Disconnected from server")

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
	<-readResult
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
		HandshakeTimeout: orDefault(manager.HandshakeTimeout, DefaultHandshakeTimeout),

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
					netConn.Close()
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

		log.Error().Str("url", manager.url()).Int("status", status).Err(err).Msg("Unable to establish websocket connection")

		var pause time.Duration
		if status >= 400 && status < 500 {
			// Only a refusal of the token leads to a new token. A
			// renewal replaces the refresh token, so it is not done on
			// a guess.
			tokenRefused := status == http.StatusUnauthorized || status == http.StatusForbidden
			pause = manager.noteRefusal(token, tokenRefused)
		}

		return nil, pause, err
	}

	return NewWebsocketConnection(socket), 0, nil
}

func (manager *WebsocketConnectionManager) idleTimeout() time.Duration {
	if manager.IdleTimeout < 0 {
		return 0
	}

	return orDefault(manager.IdleTimeout, DefaultIdleTimeout)
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
