package push

import (
	"bufio"
	"context"
	"crypto/tls"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"time"

	"github.com/rs/zerolog/log"
)

// The connection on which the Firebase service delivers notifications to a
// device. Firebase calls it the mobile connection server. Each side sends
// frames: one byte that tells the kind of the message, the length of the
// message, and the message in the protocol buffers format. Before its
// first frame, each side sends one byte with the version of the protocol.
//
// Nothing here may end the program or wait without end, whatever the
// service sends: this is an extra, and the video must run without it.

const (
	mcsAddress = "mtalk.google.com:5228"
	mcsVersion = 41

	tagHeartbeatPing = 0
	tagHeartbeatAck  = 1
	tagLoginRequest  = 2
	tagLoginResponse = 3
	tagClose         = 4
	tagDataMessage   = 8
	tagStreamError   = 10

	// maxFrameSize - the service sends messages of a few kilobytes. A
	// longer one is a fault of the connection.
	maxFrameSize = 1 << 20
)

const (
	// defaultPingInterval - time between two pings to the service. The
	// service answers each one, and that is how a connection that has
	// died is noticed.
	defaultPingInterval = 4 * time.Minute

	// defaultAnswerTimeout - time that the service gets to answer the
	// login or a ping, and the time that one frame may take to arrive
	defaultAnswerTimeout = 30 * time.Second

	dialTimeout  = 15 * time.Second
	writeTimeout = 15 * time.Second
)

// errLoginRefused - the service does not accept the device
var errLoginRefused = errors.New("the Firebase service refuses the login of the device")

// mcsClient connects a registered device to the service.
type mcsClient struct {
	AndroidID     uint64
	SecurityToken uint64

	// OnMessage gets every message that arrives, from one goroutine
	OnMessage func(dataMessage)

	// OnConnected is called when the service has accepted the login
	OnConnected func()

	// Dial makes the connection. The default connects to the service.
	Dial func() (net.Conn, error)

	// PingInterval and AnswerTimeout - see the defaults
	PingInterval  time.Duration
	AnswerTimeout time.Duration
}

func dialService() (net.Conn, error) {
	ctx, cancel := context.WithTimeout(context.Background(), dialTimeout)
	defer cancel()

	dialer := &tls.Dialer{NetDialer: &net.Dialer{}}
	return dialer.DialContext(ctx, "tcp", mcsAddress)
}

// Listen connects, logs in, and hands the messages that arrive to
// OnMessage until the connection ends or stop is closed. It always
// returns an error that says why the connection ended.
//
// receivedIDs are the IDs of the messages that have arrived on earlier
// connections. The login tells them to the service, which then does not
// send those messages again. They count as told when OnConnected runs.
func (c *mcsClient) Listen(stop <-chan struct{}, receivedIDs []string) error {
	pingInterval, answerTimeout := c.PingInterval, c.AnswerTimeout
	if pingInterval <= 0 {
		pingInterval = defaultPingInterval
	}
	if answerTimeout <= 0 {
		answerTimeout = defaultAnswerTimeout
	}

	dial := c.Dial
	if dial == nil {
		dial = dialService
	}

	conn, err := dial()
	if err != nil {
		return fmt.Errorf("unable to connect: %w", err)
	}

	// Everything that waits for the connection ends when it is closed.
	// It is closed when this function returns, and when stop is closed.
	var helpers sync.WaitGroup
	done := make(chan struct{})

	defer func() {
		close(done)
		conn.Close()
		helpers.Wait()
	}()

	helpers.Add(1)
	go func() {
		defer helpers.Done()

		select {
		case <-stop:
			conn.Close()
		case <-done:
		}
	}()

	link := &mcsLink{conn: conn, reader: bufio.NewReader(conn)}

	if err := link.login(c.AndroidID, c.SecurityToken, receivedIDs, answerTimeout); err != nil {
		return err
	}

	if c.OnConnected != nil {
		c.OnConnected()
	}

	helpers.Add(1)
	go func() {
		defer helpers.Done()

		ticker := time.NewTicker(pingInterval)
		defer ticker.Stop()

		for {
			select {
			case <-done:
				return
			case <-ticker.C:
				if err := link.write(tagHeartbeatPing, nil); err != nil {
					// The reader gets the error of the closed connection
					conn.Close()
					return
				}
			}
		}
	}()

	for {
		// Every ping is answered, so a connection on which nothing
		// arrives for longer than one interval and the time for the
		// answer has died
		tag, payload, err := link.read(pingInterval+answerTimeout, answerTimeout)
		if err != nil {
			select {
			case <-stop:
				return errors.New("stopped")
			default:
			}

			return fmt.Errorf("the connection ended: %w", err)
		}

		switch tag {
		case tagHeartbeatPing:
			if err := link.write(tagHeartbeatAck, nil); err != nil {
				return fmt.Errorf("unable to answer a ping: %w", err)
			}

		case tagClose:
			return errors.New("the service closed the connection")

		case tagStreamError:
			kind, text := decodeStreamError(payload)
			return fmt.Errorf("the service reports an error: %v %v", kind, text)

		case tagDataMessage:
			message, err := decodeDataMessage(payload)
			if err != nil {
				// The frame has been read as a whole, so the connection
				// is still in order. Ending it would only make the
				// service send the same message again.
				log.Warn().Err(err).Msg("Push message that cannot be read, ignored")
				continue
			}

			if c.OnMessage != nil {
				c.OnMessage(message)
			}
		}

		// Every other kind of message needs no answer, among them the
		// answers to our pings
	}
}

// mcsLink reads and writes the frames of one connection.
type mcsLink struct {
	conn   net.Conn
	reader *bufio.Reader

	// writeMutex - the pings and the answers to pings of the service
	// come from two goroutines
	writeMutex sync.Mutex
}

// login sends the login of the device and waits for the answer.
func (l *mcsLink) login(androidID uint64, securityToken uint64, receivedIDs []string, timeout time.Duration) error {
	request := encodeLoginRequest(androidID, securityToken, receivedIDs)

	if err := l.writeBytes(append([]byte{mcsVersion}, frame(tagLoginRequest, request)...)); err != nil {
		return fmt.Errorf("unable to send the login: %w", err)
	}

	l.conn.SetReadDeadline(time.Now().Add(timeout))

	// The version of the service. Any version that answers the login in
	// the form below will do.
	if _, err := l.reader.ReadByte(); err != nil {
		return fmt.Errorf("no answer to the login: %w", err)
	}

	tag, payload, err := l.read(timeout, timeout)
	if err != nil {
		return fmt.Errorf("no answer to the login: %w", err)
	}

	if tag != tagLoginResponse {
		return fmt.Errorf("the answer to the login is a message of kind %v", tag)
	}

	refusal, err := decodeLoginResponse(payload)
	if err != nil {
		return fmt.Errorf("unable to read the answer to the login: %w", err)
	}

	if refusal != nil {
		return fmt.Errorf("%w: code %v %v", errLoginRefused, refusal.Code, refusal.Message)
	}

	return nil
}

// frame puts the kind and the length in front of a message.
func frame(tag byte, payload []byte) []byte {
	data := make([]byte, 0, 1+binary.MaxVarintLen32+len(payload))
	data = append(data, tag)
	data = binary.AppendUvarint(data, uint64(len(payload)))

	return append(data, payload...)
}

func (l *mcsLink) write(tag byte, payload []byte) error {
	return l.writeBytes(frame(tag, payload))
}

func (l *mcsLink) writeBytes(data []byte) error {
	l.writeMutex.Lock()
	defer l.writeMutex.Unlock()

	l.conn.SetWriteDeadline(time.Now().Add(writeTimeout))
	_, err := l.conn.Write(data)

	return err
}

// read returns the next frame. It waits for the start of the frame for
// idle at most, and for the rest of it for rest at most.
func (l *mcsLink) read(idle time.Duration, rest time.Duration) (tag byte, payload []byte, err error) {
	l.conn.SetReadDeadline(time.Now().Add(idle))

	tag, err = l.reader.ReadByte()
	if err != nil {
		return 0, nil, err
	}

	l.conn.SetReadDeadline(time.Now().Add(rest))

	size, err := binary.ReadUvarint(l.reader)
	if err != nil {
		return 0, nil, fmt.Errorf("unable to read the length of a message: %w", err)
	}

	if size > maxFrameSize {
		return 0, nil, fmt.Errorf("a message of %v bytes is longer than this protocol allows", size)
	}

	payload = make([]byte, size)
	if _, err := io.ReadFull(l.reader, payload); err != nil {
		return 0, nil, fmt.Errorf("unable to read a message: %w", err)
	}

	return tag, payload, nil
}
