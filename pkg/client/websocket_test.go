package client

import (
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/indiefan/home_assistant_nanit/pkg/baby"
	"github.com/indiefan/home_assistant_nanit/pkg/session"
	"github.com/indiefan/home_assistant_nanit/pkg/utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

// fakeRelay is a stand-in for the websocket service of Nanit.
type fakeRelay struct {
	server *httptest.Server

	// handshakeStatuses - status to answer each handshake with, in
	// order. 0 accepts the connection. After the list, every connection
	// is accepted.
	handshakeStatuses []int
	handshakes        int32

	// silent - answer no ping, like a connection whose other end is
	// gone, or like a service that takes no part in the pings. Tests
	// change it while connections are open.
	silent atomic.Bool

	// pings - pings that were answered
	pings int32

	// closeAtOnce - close this many accepted connections right away
	closeAtOnce int32

	// tokens - Authorization header of each handshake
	tokens chan string

	// connections - each accepted connection
	connections chan *relayConnection
}

// relayConnection is the relay's end of one connection.
type relayConnection struct {
	socket *websocket.Conn

	// received - every message that arrived
	received chan *Message

	// closed - closed when the connection has ended
	closed chan struct{}
}

func newFakeRelay(t *testing.T) *fakeRelay {
	relay := &fakeRelay{
		tokens:      make(chan string, 16),
		connections: make(chan *relayConnection, 16),
	}

	upgrader := websocket.Upgrader{}

	relay.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		call := int(atomic.AddInt32(&relay.handshakes, 1)) - 1
		relay.tokens <- r.Header.Get("Authorization")

		if call < len(relay.handshakeStatuses) && relay.handshakeStatuses[call] != 0 {
			w.WriteHeader(relay.handshakeStatuses[call])
			return
		}

		socket, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}

		if atomic.AddInt32(&relay.closeAtOnce, -1) >= 0 {
			socket.WriteControl(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.ClosePolicyViolation, "not now"), time.Now().Add(time.Second))
			socket.Close()
			return
		}

		conn := &relayConnection{socket: socket, received: make(chan *Message, 16), closed: make(chan struct{})}
		relay.connections <- conn

		socket.SetPingHandler(func(message string) error {
			if relay.silent.Load() {
				return nil
			}

			atomic.AddInt32(&relay.pings, 1)
			return socket.WriteControl(websocket.PongMessage, []byte(message), time.Now().Add(time.Second))
		})

		for {
			_, data, err := socket.ReadMessage()
			if err != nil {
				close(conn.closed)
				return
			}

			m := &Message{}
			if proto.Unmarshal(data, m) == nil {
				conn.received <- m
			}
		}
	}))

	t.Cleanup(relay.server.Close)

	return relay
}

func (conn *relayConnection) send(t *testing.T, m *Message) {
	data, err := proto.Marshal(m)
	require.NoError(t, err)
	require.NoError(t, conn.socket.WriteMessage(websocket.BinaryMessage, data))
}

// nextRequest returns the next request that arrives, and skips the
// keep-alive messages.
func (conn *relayConnection) nextRequest(t *testing.T) *Request {
	for {
		select {
		case m := <-conn.received:
			if m.GetType() == Message_REQUEST {
				return m.Request
			}
		case <-time.After(5 * time.Second):
			t.Fatal("no request arrived")
		}
	}
}

// testConnection is a manager that runs against a fake relay.
type testConnection struct {
	manager *WebsocketConnectionManager
	relay   *fakeRelay
	api     *fakeAPI
	runner  utils.GracefulRunner

	// ready - every connection that the manager has made
	ready chan *WebsocketConnection
}

// startConnection runs a manager against relay. It is stopped when the
// test ends. Change the manager in prepare.
func startConnection(t *testing.T, relay *fakeRelay, prepare func(manager *WebsocketConnectionManager)) *testConnection {
	api := newFakeAPI(t)
	c := newTestClient(t, api)

	// Old enough to be replaced after a refusal, and young enough to
	// need no renewal by itself
	setTokenAge(c, refusedTokenMinAge+5*time.Minute)

	manager := NewWebsocketConnectionManager("baby-1", "camera-1", c, baby.NewStateManager())
	manager.BaseURL = "ws" + strings.TrimPrefix(relay.server.URL, "http")
	manager.Cooldown = []time.Duration{10 * time.Millisecond}
	manager.RefusalPauses = []time.Duration{20 * time.Millisecond}

	if prepare != nil {
		prepare(manager)
	}

	tc := &testConnection{manager: manager, relay: relay, api: api, ready: make(chan *WebsocketConnection, 16)}

	manager.WithReadyConnection(func(conn *WebsocketConnection, ctx utils.GracefulContext) {
		tc.ready <- conn
		<-ctx.Done()
	})

	tc.runner = utils.RunWithGracefulCancel(func(ctx utils.GracefulContext) {
		manager.RunWithinContext(ctx)
	})
	t.Cleanup(tc.runner.Cancel)

	return tc
}

// setTokenAge makes the auth token of the session look as if it was
// issued that long ago.
func setTokenAge(c *NanitClient, age time.Duration) {
	c.SessionStore.Update(func(s *session.Session) { s.AuthTime = time.Now().Add(-age) })
}

// websocketAlive reads what the manager has reported about the connection.
// It reads the field itself: State.GetIsWebsocketAlive looks at another
// field (a defect that the change of the stream handling corrects).
func websocketAlive(manager *WebsocketConnectionManager) bool {
	alive := manager.BabyStateManager.GetBabyState(manager.BabyUID).IsWebsocketAlive
	return alive != nil && *alive
}

func await[T any](t *testing.T, events <-chan T, what string) T {
	select {
	case event := <-events:
		return event
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for %v", what)
		panic("unreachable")
	}
}

func TestWebsocketConnects(t *testing.T) {
	relay := newFakeRelay(t)
	tc := startConnection(t, relay, nil)

	assert.Equal(t, "Bearer access-1", await(t, relay.tokens, "the handshake"))

	conn := await(t, tc.ready, "the ready connection")
	relayConn := await(t, relay.connections, "the connection at the relay")

	// A message of the relay reaches the handlers of the connection
	arrived := make(chan *Message, 1)
	conn.RegisterMessageHandler(func(m *Message, _ *WebsocketConnection) { arrived <- m })

	relayConn.send(t, &Message{Type: Message_Type(Message_KEEPALIVE).Enum()})

	assert.Equal(t, Message_KEEPALIVE, await(t, arrived, "the message").GetType())
	assert.Eventually(t, func() bool { return websocketAlive(tc.manager) }, 5*time.Second, 5*time.Millisecond)
}

func TestWebsocketRequestGetsItsResponse(t *testing.T) {
	relay := newFakeRelay(t)
	tc := startConnection(t, relay, nil)

	conn := await(t, tc.ready, "the ready connection")
	relayConn := await(t, relay.connections, "the connection at the relay")

	awaitResponse := conn.SendRequest(RequestType_GET_SENSOR_DATA, &Request{})

	request := relayConn.nextRequest(t)
	relayConn.send(t, &Message{
		Type: Message_Type(Message_RESPONSE).Enum(),
		Response: &Response{
			RequestId:   request.Id,
			RequestType: request.Type,
			StatusCode:  utils.ConstRefInt32(200),
		},
	})

	_, err := awaitResponse(5 * time.Second)
	assert.NoError(t, err)
}

// The status message of a refusal is the error of the request. The
// request for the stream depends on that text.
func TestWebsocketRequestReportsARefusal(t *testing.T) {
	relay := newFakeRelay(t)
	tc := startConnection(t, relay, nil)

	conn := await(t, tc.ready, "the ready connection")
	relayConn := await(t, relay.connections, "the connection at the relay")

	awaitResponse := conn.SendRequest(RequestType_PUT_STREAMING, &Request{})

	request := relayConn.nextRequest(t)
	relayConn.send(t, &Message{
		Type: Message_Type(Message_RESPONSE).Enum(),
		Response: &Response{
			RequestId:     request.Id,
			RequestType:   request.Type,
			StatusCode:    utils.ConstRefInt32(403),
			StatusMessage: utils.ConstRefStr("Forbidden: Number of Mobile App connections above limit, declining connection"),
		},
	})

	_, err := awaitResponse(5 * time.Second)
	assert.EqualError(t, err, "Forbidden: Number of Mobile App connections above limit, declining connection")
}

func TestWebsocketRequestTimesOut(t *testing.T) {
	relay := newFakeRelay(t)
	tc := startConnection(t, relay, nil)

	conn := await(t, tc.ready, "the ready connection")

	_, err := conn.SendRequest(RequestType_GET_SENSOR_DATA, &Request{})(50 * time.Millisecond)

	assert.True(t, errors.Is(err, ErrRequestTimeout), "got: %v", err)
	assert.EqualError(t, err, "Request timeout", "callers compare this text")
}

// A request does not wait for its full time when the connection ends: no
// response can arrive any more.
func TestWebsocketRequestEndsWithTheConnection(t *testing.T) {
	relay := newFakeRelay(t)
	tc := startConnection(t, relay, nil)

	conn := await(t, tc.ready, "the ready connection")
	relayConn := await(t, relay.connections, "the connection at the relay")

	awaitResponse := conn.SendRequest(RequestType_GET_SENSOR_DATA, &Request{})
	relayConn.nextRequest(t)
	relayConn.socket.Close()

	started := time.Now()
	_, err := awaitResponse(time.Minute)

	assert.True(t, errors.Is(err, ErrConnectionClosed), "got: %v", err)
	assert.True(t, time.Since(started) < 10*time.Second, "the request waited for its full time")
}

// A connection that carries no data any more gives no sign of that. The
// manager notices that nothing arrives, not even an answer to a ping, and
// makes a new connection.
func TestWebsocketReconnectsWhenNothingArrives(t *testing.T) {
	relay := newFakeRelay(t)

	tc := startConnection(t, relay, func(manager *WebsocketConnectionManager) {
		manager.PingInterval = 20 * time.Millisecond
		manager.IdleTimeout = 300 * time.Millisecond
	})

	first := await(t, tc.ready, "the first connection")

	// The connection works, and then it stops carrying data
	require.Eventually(t, func() bool { return atomic.LoadInt32(&relay.pings) >= 2 }, 5*time.Second, 5*time.Millisecond)
	relay.silent.Store(true)

	second := await(t, tc.ready, "the second connection")

	assert.NotSame(t, first, second)

	select {
	case <-first.Done():
	default:
		t.Fatal("the first connection was not ended")
	}
}

// The idle limit applies from the first answer to a ping. A service that
// takes no part in the pings keeps its connections: ending them when the
// camera is quiet for a while would make and end connections without end.
func TestWebsocketKeepsAConnectionToAServiceThatAnswersNoPing(t *testing.T) {
	relay := newFakeRelay(t)
	relay.silent.Store(true)

	tc := startConnection(t, relay, func(manager *WebsocketConnectionManager) {
		manager.PingInterval = 20 * time.Millisecond
		manager.IdleTimeout = 100 * time.Millisecond
	})

	conn := await(t, tc.ready, "the connection")

	select {
	case <-conn.Done():
		t.Fatalf("the connection ended: %v", conn.endReason())
	case <-tc.ready:
		t.Fatal("a second connection was made")
	case <-time.After(500 * time.Millisecond):
	}
}

// As long as the other end answers the pings, a connection without
// messages stays.
func TestWebsocketStaysWhileThePingsAreAnswered(t *testing.T) {
	relay := newFakeRelay(t)

	// The limit is long against the pings, so that a slow machine does
	// not end the connection, and the test watches for three limits.
	tc := startConnection(t, relay, func(manager *WebsocketConnectionManager) {
		manager.PingInterval = 25 * time.Millisecond
		manager.IdleTimeout = 500 * time.Millisecond
	})

	conn := await(t, tc.ready, "the connection")

	select {
	case <-conn.Done():
		t.Fatalf("the connection ended: %v", conn.endReason())
	case <-tc.ready:
		t.Fatal("a second connection was made")
	case <-time.After(1500 * time.Millisecond):
	}

	assert.EqualValues(t, 1, atomic.LoadInt32(&relay.handshakes))
}

// A ping of the other end shows that the connection carries data, also
// when that end has stopped to answer our pings.
func TestWebsocketStaysWhileTheOtherEndSendsPings(t *testing.T) {
	relay := newFakeRelay(t)

	tc := startConnection(t, relay, func(manager *WebsocketConnectionManager) {
		manager.PingInterval = 20 * time.Millisecond
		manager.IdleTimeout = 500 * time.Millisecond
	})

	conn := await(t, tc.ready, "the connection")
	relayConn := await(t, relay.connections, "the connection at the relay")

	// The idle limit applies from the first answer to a ping
	require.Eventually(t, func() bool { return atomic.LoadInt32(&relay.pings) >= 1 }, 5*time.Second, 5*time.Millisecond)
	relay.silent.Store(true)

	stop := time.After(1500 * time.Millisecond)

	for {
		select {
		case <-conn.Done():
			t.Fatalf("the connection ended: %v", conn.endReason())
		case <-stop:
			return
		case <-time.After(25 * time.Millisecond):
			require.NoError(t, relayConn.socket.WriteControl(websocket.PingMessage, nil, time.Now().Add(time.Second)))
		}
	}
}

// Only a refusal of the token at the handshake leads to a new token at
// once. A renewal replaces the refresh token, so it is not done on a
// guess.
func TestWebsocketRenewsTheTokenAfterItWasRefused(t *testing.T) {
	for name, tc := range map[string]struct {
		status    int
		wantToken string
		renewals  int32
	}{
		"unauthorized": {http.StatusUnauthorized, "Bearer access-2", 1},
		"forbidden":    {http.StatusForbidden, "Bearer access-2", 1},
		"server error": {http.StatusBadGateway, "Bearer access-1", 0},
		"bad request":  {http.StatusBadRequest, "Bearer access-1", 0},
	} {
		t.Run(name, func(t *testing.T) {
			relay := newFakeRelay(t)
			relay.handshakeStatuses = []int{tc.status}

			conn := startConnection(t, relay, nil)

			assert.Equal(t, "Bearer access-1", await(t, relay.tokens, "the first handshake"))
			assert.Equal(t, tc.wantToken, await(t, relay.tokens, "the second handshake"))

			await(t, conn.ready, "the connection")
			assert.Equal(t, tc.renewals, atomic.LoadInt32(&conn.api.refreshCalls))
		})
	}
}

// When the server refuses every token, the cause is not the token. One
// renewal is made, and then no further one while the new token is young.
// Before, every attempt after a failed one made a renewal.
func TestWebsocketDoesNotRenewATokenThatIsRefusedWhenNew(t *testing.T) {
	relay := newFakeRelay(t)
	relay.handshakeStatuses = []int{401, 401, 401, 401, 401}

	conn := startConnection(t, relay, nil)

	assert.Equal(t, "Bearer access-1", await(t, relay.tokens, "the first handshake"))
	for i := 0; i < 4; i++ {
		assert.Equal(t, "Bearer access-2", await(t, relay.tokens, "a later handshake"))
	}

	await(t, conn.ready, "the connection")
	assert.EqualValues(t, 1, atomic.LoadInt32(&conn.api.refreshCalls))
}

// A refusal is an answer, and asking again at once does not change it.
// After the second refusal in a row, the manager waits longer before each
// further attempt than after a failure of the network.
func TestWebsocketWaitsLongerWhenTheServerKeepsRefusing(t *testing.T) {
	relay := newFakeRelay(t)
	relay.handshakeStatuses = []int{403, 403, 403, 403, 403, 403, 403, 403}

	startConnection(t, relay, func(manager *WebsocketConnectionManager) {
		manager.RefusalPauses = []time.Duration{time.Hour}
	})

	// A token that is refused for the first time gets no pause: the next
	// attempt can repair that with a new token. So the second attempt
	// follows at once with the new token, and the third one too, because
	// the new token was refused for the first time. Then comes the pause.
	assert.Equal(t, "Bearer access-1", await(t, relay.tokens, "the first handshake"))
	assert.Equal(t, "Bearer access-2", await(t, relay.tokens, "the second handshake"))
	assert.Equal(t, "Bearer access-2", await(t, relay.tokens, "the third handshake"))

	select {
	case <-relay.tokens:
		t.Fatal("a fourth attempt was made without the pause")
	case <-time.After(300 * time.Millisecond):
	}
}

// A server that closes the connection because it restarts has not refused
// it, and neither has a connection that breaks or reaches its idle limit.
func TestWhichEndsOfAConnectionCountAsRefused(t *testing.T) {
	assert.False(t, refusedByClose(&websocket.CloseError{Code: websocket.CloseGoingAway}))
	assert.False(t, refusedByClose(&websocket.CloseError{Code: websocket.CloseServiceRestart}))
	assert.False(t, refusedByClose(&websocket.CloseError{Code: websocket.CloseAbnormalClosure}), "a connection that breaks")
	assert.False(t, refusedByClose(errors.New("i/o timeout")), "the idle limit")
	assert.True(t, refusedByClose(&websocket.CloseError{Code: websocket.ClosePolicyViolation}))
	assert.True(t, refusedByClose(&websocket.CloseError{Code: 4001}), "a code of the service")
}

// The default idle limit follows the ping interval.
func TestWebsocketDefaultIdleLimit(t *testing.T) {
	manager := &WebsocketConnectionManager{}
	assert.Equal(t, DefaultIdleTimeout, manager.idleTimeout())

	manager.PingInterval = 2 * time.Minute
	assert.Equal(t, 6*time.Minute, manager.idleTimeout())

	manager.IdleTimeout = 5 * time.Minute
	assert.Equal(t, 5*time.Minute, manager.idleTimeout())

	manager.IdleTimeout = -1
	assert.Zero(t, manager.idleTimeout(), "turned off")
}

// A server that accepts the connection and closes it at once refuses it
// too, in another way.
func TestWebsocketWaitsLongerWhenTheServerKeepsClosingAtOnce(t *testing.T) {
	relay := newFakeRelay(t)
	relay.closeAtOnce = 8

	tc := startConnection(t, relay, func(manager *WebsocketConnectionManager) {
		manager.RefusalPauses = []time.Duration{time.Hour}
	})

	await(t, relay.tokens, "the first handshake")
	await(t, relay.tokens, "the second handshake")

	select {
	case <-relay.tokens:
		t.Fatal("a third attempt was made without the pause")
	case <-time.After(300 * time.Millisecond):
	}

	// The pause does not hold up a stop
	stopped := make(chan struct{})
	go func() {
		tc.runner.Cancel()
		close(stopped)
	}()

	await(t, stopped, "the stop of the manager")
	assert.EqualValues(t, 0, atomic.LoadInt32(&tc.api.refreshCalls), "no token is replaced on a guess")
}

// A failure that is no refusal, like a server error, gets the short
// pauses only, and it starts the count of the refusals again.
func TestWebsocketDoesNotWaitLongerAfterServerErrors(t *testing.T) {
	relay := newFakeRelay(t)
	relay.handshakeStatuses = []int{400, 502, 400, 502, 400, 502}

	tc := startConnection(t, relay, func(manager *WebsocketConnectionManager) {
		manager.RefusalPauses = []time.Duration{time.Hour}
	})

	await(t, tc.ready, "the connection after refusals that did not follow each other")
}

// A handler may have a last message to send when the connection is given
// up, for example to stop the stream. The socket stays open until the
// handlers have finished.
func TestWebsocketLetsTheHandlersFinishBeforeItCloses(t *testing.T) {
	relay := newFakeRelay(t)

	aliveAtCleanup := make(chan bool, 1)

	tc := startConnection(t, relay, func(manager *WebsocketConnectionManager) {
		manager.WithReadyConnection(func(conn *WebsocketConnection, ctx utils.GracefulContext) {
			<-ctx.Done()
			aliveAtCleanup <- websocketAlive(manager)
			conn.SendRequest(RequestType_PUT_STREAMING, &Request{})
		})
	})

	await(t, tc.ready, "the ready connection")
	relayConn := await(t, relay.connections, "the connection at the relay")
	require.Eventually(t, func() bool { return websocketAlive(tc.manager) }, 5*time.Second, 5*time.Millisecond)

	tc.runner.Cancel()

	assert.True(t, await(t, aliveAtCleanup, "the clean up of the handler"), "the state still says that the connection is alive, so the handler sends")
	assert.Equal(t, RequestType_PUT_STREAMING, relayConn.nextRequest(t).GetType())
	await(t, relayConn.closed, "the end of the connection")
	assert.False(t, websocketAlive(tc.manager))
}

// The manager stops also while it cannot connect.
func TestWebsocketStopsWhileItCannotConnect(t *testing.T) {
	relay := newFakeRelay(t)
	relay.handshakeStatuses = []int{500, 500, 500, 500, 500, 500, 500, 500}

	tc := startConnection(t, relay, func(manager *WebsocketConnectionManager) {
		manager.Cooldown = []time.Duration{time.Hour}
	})

	await(t, relay.tokens, "the first handshake")

	stopped := make(chan struct{})
	go func() {
		tc.runner.Cancel()
		close(stopped)
	}()

	await(t, stopped, "the stop of the manager")
}

// A server that takes the connection and then says nothing must not hold
// up a stop for the time limit of the handshake.
func TestWebsocketStopsDuringAHandshakeThatHangs(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { listener.Close() })

	accepted := make(chan net.Conn, 4)
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			accepted <- conn
		}
	}()

	relay := newFakeRelay(t)

	tc := startConnection(t, relay, func(manager *WebsocketConnectionManager) {
		manager.BaseURL = "ws://" + listener.Addr().String()
		manager.HandshakeTimeout = time.Minute
	})

	hanging := await(t, accepted, "the connection")
	defer hanging.Close()

	stopped := make(chan struct{})
	go func() {
		tc.runner.Cancel()
		close(stopped)
	}()

	await(t, stopped, "the stop of the manager")
}
