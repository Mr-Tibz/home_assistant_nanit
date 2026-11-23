package push

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protowire"
)

// login - a login of the client, as the service reads it
type login struct {
	ID, Domain, User, Resource, AuthToken, DeviceID string
	Setting                                         keyValue
	ReceivedIDs                                     []string
	AdaptiveHeartbeat, UseRmq2                      bool
	AuthService, NetworkType                        uint64
}

func decodeLoginRequest(t *testing.T, message []byte) login {
	var l login

	require.NoError(t, eachField(message, func(field protowire.Number, data []byte, number uint64) error {
		switch field {
		case 1:
			l.ID = string(data)
		case 2:
			l.Domain = string(data)
		case 3:
			l.User = string(data)
		case 4:
			l.Resource = string(data)
		case 5:
			l.AuthToken = string(data)
		case 6:
			l.DeviceID = string(data)
		case 8:
			return eachField(data, func(field protowire.Number, data []byte, _ uint64) error {
				if field == 1 {
					l.Setting.Key = string(data)
				} else {
					l.Setting.Value = string(data)
				}
				return nil
			})
		case 10:
			l.ReceivedIDs = append(l.ReceivedIDs, string(data))
		case 12:
			l.AdaptiveHeartbeat = number == 1
		case 14:
			l.UseRmq2 = number == 1
		case 16:
			l.AuthService = number
		case 17:
			l.NetworkType = number
		default:
			t.Errorf("the login has a field %v that the service does not expect", field)
		}
		return nil
	}))

	return l
}

// encodeLoginResponse builds the answer to a login. refusal is nil for a
// login that is accepted.
func encodeLoginResponse(refusal *loginRefusal) []byte {
	m := appendString(nil, 1, "chrome-63.0.3234.0") // id
	m = appendNumber(m, 5, 1)                       // stream_id
	m = appendNumber(m, 8, 1763856000000)           // server_timestamp

	if refusal != nil {
		m = appendMessage(m, 3, appendString(appendNumber(nil, 1, uint64(refusal.Code)), 2, refusal.Message))
	}

	return m
}

// encodeDataMessage builds a message that the service delivers, with
// fields around the ones that the client reads.
func encodeDataMessage(persistentID string, key string, value string) []byte {
	m := appendString(nil, 2, "0:1763856000000000")                                // id
	m = appendString(m, 3, "25705829844")                                          // from
	m = appendString(m, 5, "com.nanit.baby")                                       // category
	m = appendMessage(m, 7, appendString(appendString(nil, 1, "ignored"), 2, "x")) // app_data
	m = appendMessage(m, 7, appendString(appendString(nil, 1, key), 2, value))
	m = appendNumber(m, 8, 1) // from_trusted_server

	if persistentID != "" {
		m = appendString(m, 9, persistentID)
	}

	m = appendNumber(m, 17, 86400)                                                    // ttl
	m = appendNumber(m, 18, 1763856000000)                                            // sent
	m = protowire.AppendFixed32(protowire.AppendTag(m, 99, protowire.Fixed32Type), 7) // a field of a kind that no message here has

	return m
}

func notificationMessage(persistentID string, notification string) []byte {
	return encodeDataMessage(persistentID, "notification", notification)
}

type frameData struct {
	tag     byte
	payload []byte
}

// fakeService is a stand-in for the Firebase service, at the other end of
// a connection in memory. It reads what the client sends all the time,
// as the real service does, because a write to such a connection waits
// for the reader.
type fakeService struct {
	t      *testing.T
	conn   net.Conn
	client net.Conn

	version chan byte
	frames  chan frameData
}

func newFakeService(t *testing.T) *fakeService {
	client, server := net.Pipe()

	s := &fakeService{
		t:       t,
		conn:    server,
		client:  client,
		version: make(chan byte, 1),
		frames:  make(chan frameData, 64),
	}

	t.Cleanup(func() {
		server.Close()
		client.Close()
	})

	go func() {
		reader := bufio.NewReader(server)

		version, err := reader.ReadByte()
		if err != nil {
			return
		}
		s.version <- version

		for {
			tag, err := reader.ReadByte()
			if err != nil {
				return
			}

			size, err := binary.ReadUvarint(reader)
			if err != nil {
				return
			}

			payload := make([]byte, size)
			if _, err := io.ReadFull(reader, payload); err != nil {
				return
			}

			s.frames <- frameData{tag, payload}
		}
	}()

	return s
}

// dial is the Dial of a client that connects to this service.
func (s *fakeService) dial() (net.Conn, error) {
	return s.client, nil
}

func (s *fakeService) next() frameData {
	select {
	case f := <-s.frames:
		return f
	case <-time.After(5 * time.Second):
		s.t.Fatal("the client sent nothing")
		return frameData{}
	}
}

// login waits for the login of the client and returns it.
func (s *fakeService) login() login {
	select {
	case version := <-s.version:
		require.Equal(s.t, byte(mcsVersion), version)
	case <-time.After(5 * time.Second):
		s.t.Fatal("the client sent no version")
	}

	f := s.next()
	require.Equal(s.t, byte(tagLoginRequest), f.tag)

	return decodeLoginRequest(s.t, f.payload)
}

func (s *fakeService) sendVersion() {
	_, err := s.conn.Write([]byte{mcsVersion})
	require.NoError(s.t, err)
}

func (s *fakeService) send(tag byte, payload []byte) {
	_, err := s.conn.Write(frame(tag, payload))
	require.NoError(s.t, err)
}

// accept answers the login of the client the way the real service does.
func (s *fakeService) accept() login {
	request := s.login()
	s.sendVersion()
	s.send(tagLoginResponse, encodeLoginResponse(nil))

	return request
}

// listen runs Listen of the client and returns the channel with its result.
func listen(c *mcsClient, stop <-chan struct{}, receivedIDs []string) chan error {
	result := make(chan error, 1)
	go func() { result <- c.Listen(stop, receivedIDs) }()

	return result
}

func waitFor(t *testing.T, result chan error) error {
	select {
	case err := <-result:
		return err
	case <-time.After(5 * time.Second):
		t.Fatal("Listen did not return")
		return nil
	}
}

func TestListenLogsInAndDeliversMessages(t *testing.T) {
	service := newFakeService(t)

	connected := make(chan struct{})
	messages := make(chan dataMessage, 4)
	stop := make(chan struct{})

	client := &mcsClient{
		AndroidID:     1234567890123456789,
		SecurityToken: 987654321,
		Dial:          service.dial,
		OnConnected:   func() { close(connected) },
		OnMessage:     func(m dataMessage) { messages <- m },
	}
	result := listen(client, stop, []string{"id-1", "id-2", "id-3"})

	request := service.accept()

	assert.Equal(t, login{
		ID:          "chrome-63.0.3234.0",
		Domain:      "mcs.android.com",
		User:        "1234567890123456789",
		Resource:    "1234567890123456789",
		AuthToken:   "987654321",
		DeviceID:    "android-112210f47de98115",
		Setting:     keyValue{"new_vc", "1"},
		ReceivedIDs: []string{"id-1", "id-2", "id-3"},
		UseRmq2:     true,
		AuthService: 2,
		NetworkType: 1,
	}, request)

	select {
	case <-connected:
	case <-time.After(5 * time.Second):
		t.Fatal("OnConnected was not called")
	}

	service.send(tagDataMessage, notificationMessage("id-4", `{"type":"SOUND"}`))

	select {
	case m := <-messages:
		assert.Equal(t, dataMessage{
			From:         "25705829844",
			PersistentID: "id-4",
			Data:         []keyValue{{"ignored", "x"}, {"notification", `{"type":"SOUND"}`}},
		}, m)
	case <-time.After(5 * time.Second):
		t.Fatal("the message did not arrive")
	}

	close(stop)
	assert.EqualError(t, waitFor(t, result), "stopped")
}

// A login with many IDs is longer than 16 kilobytes, so its length takes
// three bytes.
func TestLoginWithManyIDs(t *testing.T) {
	service := newFakeService(t)

	ids := make([]string, rememberedIDs)
	for i := range ids {
		ids[i] = fmt.Sprintf("0:1763856000000000%%%032d", i)
	}

	stop := make(chan struct{})
	result := listen(&mcsClient{AndroidID: 1, SecurityToken: 2, Dial: service.dial}, stop, ids)

	assert.Equal(t, ids, service.accept().ReceivedIDs)

	close(stop)
	waitFor(t, result)
}

func TestListenAnswersAPingOfTheService(t *testing.T) {
	service := newFakeService(t)

	stop := make(chan struct{})
	result := listen(&mcsClient{AndroidID: 1, SecurityToken: 2, Dial: service.dial}, stop, nil)

	service.accept()
	service.send(tagHeartbeatPing, nil)

	assert.Equal(t, byte(tagHeartbeatAck), service.next().tag)

	close(stop)
	waitFor(t, result)
}

// The connection can die without a sign, for example when a router
// restarts. The service then answers no ping.
func TestListenGivesUpAConnectionThatAnswersNoPing(t *testing.T) {
	service := newFakeService(t)

	client := &mcsClient{AndroidID: 1, SecurityToken: 2, Dial: service.dial, PingInterval: 30 * time.Millisecond, AnswerTimeout: 60 * time.Millisecond}
	result := listen(client, make(chan struct{}), nil)

	service.accept()
	assert.Equal(t, byte(tagHeartbeatPing), service.next().tag)

	err := waitFor(t, result)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "the connection ended")
}

func TestListenKeepsAConnectionThatAnswersThePings(t *testing.T) {
	service := newFakeService(t)

	client := &mcsClient{AndroidID: 1, SecurityToken: 2, Dial: service.dial, PingInterval: 30 * time.Millisecond, AnswerTimeout: 60 * time.Millisecond}
	stop := make(chan struct{})
	result := listen(client, stop, nil)

	service.accept()

	// Several times the time after which a silent connection is given up
	for i := 0; i < 10; i++ {
		assert.Equal(t, byte(tagHeartbeatPing), service.next().tag)
		service.send(tagHeartbeatAck, nil)
	}

	select {
	case err := <-result:
		t.Fatalf("the connection was given up: %v", err)
	default:
	}

	close(stop)
	waitFor(t, result)
}

func TestListenReportsARefusedLogin(t *testing.T) {
	service := newFakeService(t)

	connected := false
	client := &mcsClient{AndroidID: 1, SecurityToken: 2, Dial: service.dial, OnConnected: func() { connected = true }}
	result := listen(client, make(chan struct{}), nil)

	service.login()
	service.sendVersion()
	service.send(tagLoginResponse, encodeLoginResponse(&loginRefusal{Code: 401, Message: "unauthorized"}))

	err := waitFor(t, result)
	assert.True(t, errors.Is(err, errLoginRefused), "got: %v", err)
	assert.Contains(t, err.Error(), "code 401 unauthorized")
	assert.False(t, connected)
}

func TestListenGivesUpALoginWithoutAnswer(t *testing.T) {
	service := newFakeService(t)

	client := &mcsClient{AndroidID: 1, SecurityToken: 2, Dial: service.dial, AnswerTimeout: 50 * time.Millisecond}
	result := listen(client, make(chan struct{}), nil)

	service.login()

	err := waitFor(t, result)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no answer to the login")
}

func TestListenEndsWhenTheServiceClosesTheConnection(t *testing.T) {
	for name, send := range map[string]func(s *fakeService){
		"with a message that says so": func(s *fakeService) { s.send(tagClose, nil) },
		"with an error":               func(s *fakeService) { s.send(tagStreamError, appendString(nil, 1, "host-gone")) },
		"without a word":              func(s *fakeService) { s.conn.Close() },
	} {
		t.Run(name, func(t *testing.T) {
			service := newFakeService(t)
			result := listen(&mcsClient{AndroidID: 1, SecurityToken: 2, Dial: service.dial}, make(chan struct{}), nil)

			service.accept()
			send(service)

			assert.Error(t, waitFor(t, result))
		})
	}
}

func TestListenReportsTheErrorOfTheService(t *testing.T) {
	service := newFakeService(t)
	result := listen(&mcsClient{AndroidID: 1, SecurityToken: 2, Dial: service.dial}, make(chan struct{}), nil)

	service.accept()
	service.send(tagStreamError, appendString(appendString(nil, 1, "host-gone"), 2, "try later"))

	assert.EqualError(t, waitFor(t, result), "the service reports an error: host-gone try later")
}

// A message of a kind that this client does not know, and a message that
// cannot be read, do not end the connection: the next message arrives.
func TestListenSkipsWhatItCannotUse(t *testing.T) {
	service := newFakeService(t)

	messages := make(chan dataMessage, 4)
	stop := make(chan struct{})
	client := &mcsClient{AndroidID: 1, SecurityToken: 2, Dial: service.dial, OnMessage: func(m dataMessage) { messages <- m }}
	result := listen(client, stop, nil)

	service.accept()

	_, err := service.conn.Write(frame(7, []byte{1, 2, 3}))
	require.NoError(t, err)
	_, err = service.conn.Write(frame(tagDataMessage, []byte{0xff, 0xff, 0xff}))
	require.NoError(t, err)
	// No message at all: a length of zero
	_, err = service.conn.Write(frame(tagHeartbeatAck, nil))
	require.NoError(t, err)

	service.send(tagDataMessage, notificationMessage("id-1", `{"type":"MOTION"}`))

	select {
	case m := <-messages:
		assert.Equal(t, "id-1", m.PersistentID)
	case <-time.After(5 * time.Second):
		t.Fatal("the message did not arrive")
	}

	close(stop)
	waitFor(t, result)
}

func TestListenRefusesAMessageOfAnImpossibleLength(t *testing.T) {
	service := newFakeService(t)
	result := listen(&mcsClient{AndroidID: 1, SecurityToken: 2, Dial: service.dial}, make(chan struct{}), nil)

	service.accept()

	header := binary.AppendUvarint([]byte{tagDataMessage}, maxFrameSize+1)
	_, err := service.conn.Write(header)
	require.NoError(t, err)

	err = waitFor(t, result)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "longer than this protocol allows")
}

func TestListenReportsAFailedDial(t *testing.T) {
	client := &mcsClient{Dial: func() (net.Conn, error) { return nil, errors.New("no network") }}

	err := client.Listen(make(chan struct{}), nil)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "no network")
}
