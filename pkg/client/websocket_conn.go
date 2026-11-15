package client

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
	"github.com/indiefan/home_assistant_nanit/pkg/utils"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"google.golang.org/protobuf/proto"
)

var (
	// ErrRequestTimeout - no response to a request has arrived in time
	ErrRequestTimeout = errors.New("Request timeout")

	// ErrConnectionClosed - the connection ended before the response to a
	// request arrived
	ErrConnectionClosed = errors.New("Websocket connection closed")
)

// writeTimeout - time limit for writing one message to the socket
const writeTimeout = 10 * time.Second

// WebsocketMessageHandler - message handler
type WebsocketMessageHandler func(*Message, *WebsocketConnection)

// WebsocketConnection - ready websocket connection
type WebsocketConnection struct {
	socket *websocket.Conn

	// writeMu - the socket allows one writer of messages at a time
	writeMu sync.Mutex

	// done is closed when the connection has ended, and closeErr says why
	done      chan struct{}
	closeOnce sync.Once
	closeErr  error

	msgHandlersMu sync.RWMutex
	msgHandlers   []WebsocketMessageHandler

	// resHandlers - the requests that wait for their response. It is nil
	// when the connection has ended: no response can arrive any more.
	resHandlersMu sync.Mutex
	resHandlers   map[int32]unhandledRequest

	lastRequestID int32
}

// NewWebsocketConnection - constructor
func NewWebsocketConnection(socket *websocket.Conn) *WebsocketConnection {
	return &WebsocketConnection{
		socket:        socket,
		done:          make(chan struct{}),
		resHandlers:   make(map[int32]unhandledRequest),
		lastRequestID: 0,
	}
}

// Done - closed when the connection has ended. Nothing can be sent after
// that, and no response will arrive.
func (conn *WebsocketConnection) Done() <-chan struct{} {
	return conn.done
}

// end closes the connection. The first call decides the reason.
func (conn *WebsocketConnection) end(reason error) {
	conn.closeOnce.Do(func() {
		conn.closeErr = reason
		close(conn.done)

		if errors.Is(reason, errAttemptEnded) {
			// The connection works and is given up on purpose: tell the
			// other end so. It is not waited for an answer.
			conn.socket.WriteControl(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""), time.Now().Add(time.Second))
		}

		conn.socket.Close()

		conn.resHandlersMu.Lock()
		conn.resHandlers = nil
		conn.resHandlersMu.Unlock()
	})
}

// endReason returns why the connection has ended. Call it after Done.
func (conn *WebsocketConnection) endReason() error {
	<-conn.done
	return conn.closeErr
}

// RegisterMessageHandler - registers handler which will be called whenever new message is received
func (conn *WebsocketConnection) RegisterMessageHandler(handler WebsocketMessageHandler) {
	conn.msgHandlersMu.Lock()
	conn.msgHandlers = append(conn.msgHandlers, handler)
	conn.msgHandlersMu.Unlock()
}

// SendMessage - low-level helper for sending raw message
// Note: Use SendRequest() for requests
//
// A message that cannot be written ends the connection: the socket is of
// no use any more, and the manager makes a new one.
func (conn *WebsocketConnection) SendMessage(m *Message) {
	var msg *zerolog.Event

	if m.GetType() == Message_KEEPALIVE {
		msg = log.Trace()
	} else {
		msg = log.Debug()
	}

	msg.Stringer("data", m).Msg("Sending message")

	bytes := getMessageBytes(m)
	log.Trace().Bytes("rawdata", bytes).Msg("Sending data")

	conn.writeMu.Lock()
	conn.socket.SetWriteDeadline(time.Now().Add(writeTimeout))
	err := conn.socket.WriteMessage(websocket.BinaryMessage, bytes)
	conn.writeMu.Unlock()

	if err != nil {
		select {
		case <-conn.done:
			// The connection has ended already, and its end was
			// reported. Say what is lost with it. The keep-alive is no
			// loss.
			if m.GetType() != Message_KEEPALIVE {
				log.Warn().Stringer("data", m).Msg("A message was not sent, because the websocket connection has ended")
			}
		default:
			log.Warn().Err(err).Msg("Unable to send a message, closing the websocket")
			conn.end(fmt.Errorf("unable to send a message: %w", err))
		}
	}
}

// SendRequest - sends request to the cam and returns await function. Await function waits for the response and returns it
//
// The await function returns ErrRequestTimeout when no response arrives
// in the given time, and ErrConnectionClosed as soon as the connection
// ends.
func (conn *WebsocketConnection) SendRequest(reqType RequestType, requestData *Request) func(time.Duration) (*Response, error) {
	// Build request
	id := atomic.AddInt32(&conn.lastRequestID, 1)

	requestData.Id = utils.ConstRefInt32(id)
	requestData.Type = RequestType(reqType).Enum()

	m := &Message{
		Type:    Message_Type(Message_REQUEST).Enum(),
		Request: requestData,
	}

	// Response handling
	resC := make(chan *Response, 1)

	conn.resHandlersMu.Lock()
	if conn.resHandlers != nil {
		conn.resHandlers[id] = unhandledRequest{
			Request: m.Request,
			HandleResponse: func(res *Response) {
				select {
				case resC <- res:
				default:
					// A response is waiting already
				}
			},
		}
	}
	conn.resHandlersMu.Unlock()

	// Send request
	conn.SendMessage(m)

	finish := func(res *Response) (*Response, error) {
		if res.StatusCode == nil {
			return res, errors.New("No status code received")
		} else if *res.StatusCode != 200 {
			if res.GetStatusMessage() != "" {
				return res, errors.New(res.GetStatusMessage())
			}

			return res, fmt.Errorf("Unexpected status code %v", *res.StatusCode)
		}

		return res, nil
	}

	forget := func() {
		conn.resHandlersMu.Lock()
		delete(conn.resHandlers, id)
		conn.resHandlersMu.Unlock()
	}

	// Return awaiter
	return func(timeout time.Duration) (*Response, error) {
		// A response that is there counts, whatever else has happened
		select {
		case res := <-resC:
			return finish(res)
		default:
		}

		timer := time.NewTimer(timeout)
		defer timer.Stop()

		select {
		case res := <-resC:
			return finish(res)
		case <-conn.done:
			forget()
			return nil, ErrConnectionClosed
		case <-timer.C:
			forget()
			return nil, ErrRequestTimeout
		}
	}
}

type unhandledRequest struct {
	Request        *Request
	HandleResponse func(response *Response)
}

func (conn *WebsocketConnection) handleResponse(r *Response) {
	if r.RequestId == nil || r.RequestType == nil {
		return
	}

	requestID := *r.RequestId
	requestType := *r.RequestType

	conn.resHandlersMu.Lock()
	unhandledReqCandidate, ok := conn.resHandlers[requestID]
	ok = ok && requestType == unhandledReqCandidate.Request.GetType()
	if ok {
		delete(conn.resHandlers, requestID)
	}
	conn.resHandlersMu.Unlock()

	if ok {
		unhandledReqCandidate.HandleResponse(r)
	}
}

func (conn *WebsocketConnection) handleMessage(m *Message) {
	if m.GetType() == Message_RESPONSE && m.Response != nil {
		conn.handleResponse(m.Response)
	}

	conn.msgHandlersMu.RLock()
	subscribedHandlers := make([]WebsocketMessageHandler, len(conn.msgHandlers))
	copy(subscribedHandlers, conn.msgHandlers)
	conn.msgHandlersMu.RUnlock()

	for _, handler := range subscribedHandlers {
		handler(m, conn)
	}
}

// read reads messages until the connection ends.
//
// With an idle limit, the connection ends when nothing has arrived for
// that long: no message, no ping and no answer to a ping. A connection
// can stop carrying data without any sign, for example when a router
// between the two ends restarts. Only the missing data shows that.
//
// The limit starts to apply when the first ping or answer to a ping of
// the other end has arrived. Until then nothing says that the other end
// takes part in the pings at all, and a camera can be quiet for longer
// than the limit: without this, such a connection would be ended and made
// again without end.
func (conn *WebsocketConnection) read(idleTimeout time.Duration) {
	pingsSeen := false

	extend := func() {
		if idleTimeout > 0 && pingsSeen {
			conn.socket.SetReadDeadline(time.Now().Add(idleTimeout))
		}
	}

	conn.socket.SetPongHandler(func(string) error {
		pingsSeen = true
		extend()
		return nil
	})

	// A ping of the other end is answered, as the library does by itself
	// without this handler. An answer that cannot be written needs no
	// handling here: the connection is broken then, and the next read
	// reports that.
	conn.socket.SetPingHandler(func(message string) error {
		pingsSeen = true
		extend()
		conn.socket.WriteControl(websocket.PongMessage, []byte(message), time.Now().Add(writeTimeout))
		return nil
	})

	for {
		extend()

		messageType, data, err := conn.socket.ReadMessage()
		if err != nil {
			conn.end(err)
			return
		}

		if messageType != websocket.BinaryMessage {
			continue
		}

		m := &Message{}
		if err := proto.Unmarshal(data, m); err != nil {
			log.Error().Err(err).Bytes("rawdata", data).Msg("Received malformed binary message")
			continue
		}

		log.Debug().Stringer("data", m).Msg("Received message")

		go conn.handleMessage(m)
	}
}

// ping sends a websocket ping at every interval until the connection
// ends. The answers keep an idle connection from reaching its idle limit.
func (conn *WebsocketConnection) ping(interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-conn.done:
			return
		case <-ticker.C:
			// A ping that cannot be written needs no handling of its
			// own: the idle limit ends the connection.
			conn.socket.WriteControl(websocket.PingMessage, nil, time.Now().Add(writeTimeout))
		}
	}
}

func getMessageBytes(data *Message) []byte {
	out, err := proto.Marshal(data)
	if err != nil {
		log.Fatal().Err(err).Msg("Unable to marshal data")
	}

	return out
}
