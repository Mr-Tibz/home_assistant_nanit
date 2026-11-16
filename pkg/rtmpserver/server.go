package rtmpserver

import (
	"io"
	"net"
	"regexp"
	"sync"
	"sync/atomic"
	"time"

	"github.com/notedit/rtmp/av"
	"github.com/notedit/rtmp/format/rtmp"
	"github.com/rs/zerolog/log"
	"github.com/indiefan/home_assistant_nanit/pkg/baby"
)

// DefaultReadTimeout - a publisher from which nothing has arrived for
// this long has ended. A stream can stop without a sign, for example when
// the camera loses its network for good. Without a limit such a stream
// counted as running for ever, and nothing asked the camera for a new
// one.
//
// The limit is long on purpose. A gap of the network that is shorter
// passes without any loss but a picture that stands still, because the
// connection goes on after it. Ending the stream means asking the camera
// for a new one, and that request can be refused.
const DefaultReadTimeout = 30 * time.Second

// subscriberWriteTimeout - time limit for writing one packet to a
// subscriber
const subscriberWriteTimeout = 10 * time.Second

// idleConn is a connection whose reads can get a limit for the time
// without any data. The limit starts again with every read that brings
// data, whatever that data is. It is switched on for publishers only: a
// subscriber sends nothing while it takes the stream.
type idleConn struct {
	net.Conn

	// limit - the time limit in nanoseconds. 0 means none.
	limit atomic.Int64
}

func (conn *idleConn) Read(p []byte) (int, error) {
	if limit := time.Duration(conn.limit.Load()); limit > 0 {
		conn.Conn.SetReadDeadline(time.Now().Add(limit))
	}

	return conn.Conn.Read(p)
}

// limitIdleTime ends the reads of nc when nothing arrives for limit.
func limitIdleTime(nc net.Conn, limit time.Duration) {
	if conn, ok := nc.(*idleConn); ok {
		conn.limit.Store(int64(limit))
	}
}

type rtmpHandler struct {
	// readTimeout - see DefaultReadTimeout. 0 means no limit.
	readTimeout time.Duration

	babyStateManager  *baby.StateManager
	broadcastersMu    sync.RWMutex
	broadcastersByUID map[string]*broadcaster
}

// StartRTMPServer - Blocking server
//
// readTimeout ends a publisher from which nothing arrives, see
// DefaultReadTimeout. 0 means the default, and a negative value means no
// limit.
func StartRTMPServer(addr string, babyStateManager *baby.StateManager, readTimeout time.Duration) {
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatal().Str("addr", addr).Err(err).Msg("Unable to start RTMP server")
		panic(err)
	}

	log.Info().Str("addr", addr).Msg("RTMP server started")

	handler := newRtmpHandler(babyStateManager)

	switch {
	case readTimeout == 0:
		handler.readTimeout = DefaultReadTimeout
	case readTimeout > 0:
		handler.readTimeout = readTimeout
	}

	s := rtmp.NewServer()
	s.ReplaceRawConn = func(nc net.Conn) net.Conn { return &idleConn{Conn: nc} }
	s.HandleConn = handler.handleConnection

	for {
		nc, err := lis.Accept()
		if err != nil {
			time.Sleep(time.Second)
			continue
		}
		go s.HandleNetConn(nc)
	}
}

func newRtmpHandler(babyStateManager *baby.StateManager) *rtmpHandler {
	return &rtmpHandler{
		broadcastersByUID: make(map[string]*broadcaster),
		babyStateManager:  babyStateManager,
	}
}

var rtmpURLRX = regexp.MustCompile(`^/local/([a-z0-9_-]+)$`)

func (s *rtmpHandler) handleConnection(c *rtmp.Conn, nc net.Conn) {
	sublog := log.With().Stringer("client_addr", nc.RemoteAddr()).Logger()

	submatch := rtmpURLRX.FindStringSubmatch(c.URL.Path)
	if len(submatch) != 2 {
		sublog.Warn().Str("path", c.URL.Path).Msg("Invalid RTMP stream requested")
		nc.Close()
		return
	}

	babyUID := submatch[1]
	sublog = sublog.With().Str("baby_uid", babyUID).Logger()

	if c.Publishing {
		sublog.Info().Msg("New stream publisher connected")
		publisher := s.getNewPublisher(babyUID, nc)

		limitIdleTime(nc, s.readTimeout)
		err := readPackets(c, publisher)

		if s.closePublisher(babyUID, publisher) {
			sublog.Warn().Err(err).Msg("Publisher stream closed unexpectedly")
		} else {
			sublog.Info().Err(err).Msg("Publisher stream closed. A newer publisher has taken its place")
		}

		nc.Close()

		return
	}

	sublog.Debug().Msg("New stream subscriber connected")
	subscriber, unsubscribe := s.getNewSubscriber(babyUID)

	if subscriber == nil {
		sublog.Warn().Msg("No stream publisher registered yet, closing subscriber stream")
		nc.Close()
		return
	}

	defer nc.Close()
	defer unsubscribe()

	closeC := c.CloseNotify()

	for {
		select {
		case pkt, open := <-subscriber.pktC:
			if !open {
				if subscriber.tooSlow.Load() {
					sublog.Warn().Msg("Closing subscriber because it did not take the stream fast enough. It can connect again")
				} else {
					sublog.Debug().Msg("Closing subscriber because publisher quit")
				}

				return
			}

			nc.SetWriteDeadline(time.Now().Add(subscriberWriteTimeout))
			if err := c.WritePacket(pkt); err != nil {
				sublog.Debug().Err(err).Msg("Unable to write to stream subscriber, closing it")
				return
			}

		case <-closeC:
			sublog.Debug().Msg("Stream subscriber disconnected")
			return
		}
	}
}

// readPackets hands the packets of a publisher to its broadcaster until
// reading fails, and returns that failure.
func readPackets(c av.PacketReader, publisher *broadcaster) error {
	for {
		pkt, err := c.ReadPacket()
		if err != nil {
			return err
		}

		publisher.broadcast(pkt)
	}
}

// getNewPublisher registers a new publisher for the baby and marks the
// stream as alive.
//
// The state of the stream changes under the same lock as the list of
// publishers, here and in closePublisher. A new publisher that comes at
// the moment an old one ends can therefore not be followed by the state
// "ended" of the old one.
//
// conn is the connection of the publisher. It is closed when a newer
// publisher takes its place: the camera has started a new stream then,
// and the old one is of no use any more.
func (s *rtmpHandler) getNewPublisher(babyUID string, conn io.Closer) *broadcaster {
	broadcaster := newBroadcaster(conn, subscriberBufferFor(s.readTimeout))

	s.broadcastersMu.Lock()
	existingBroadcaster, hadExistingBroadcaster := s.broadcastersByUID[babyUID]
	s.broadcastersByUID[babyUID] = broadcaster
	s.babyStateManager.Update(babyUID, *baby.NewState().SetStreamState(baby.StreamState_Alive))
	s.broadcastersMu.Unlock()

	if hadExistingBroadcaster {
		log.Warn().Msg("Baby already has active publisher, closing it and its subscribers")
		existingBroadcaster.closeSubscribers()
		existingBroadcaster.publisher.Close()
	}

	return broadcaster
}

func (s *rtmpHandler) getNewSubscriber(babyUID string) (*subscriber, func()) {
	s.broadcastersMu.RLock()
	broadcaster, hasBroadcaster := s.broadcastersByUID[babyUID]
	s.broadcastersMu.RUnlock()

	if !hasBroadcaster {
		return nil, nil
	}

	sub := broadcaster.newSubscriber()

	return sub, func() { broadcaster.unsubscribe(sub) }
}

// closePublisher ends the publisher b. If b was the publisher of the baby
// at that moment, the stream is marked as ended, and the result is true.
// A publisher that a newer one has replaced says nothing about the stream
// when it ends: the newer one sends it.
func (s *rtmpHandler) closePublisher(babyUID string, b *broadcaster) bool {
	s.broadcastersMu.Lock()
	currBroadcaster, hasExistingBroadcaster := s.broadcastersByUID[babyUID]
	wasCurrent := hasExistingBroadcaster && currBroadcaster == b
	if wasCurrent {
		delete(s.broadcastersByUID, babyUID)
		s.babyStateManager.Update(babyUID, *baby.NewState().SetStreamState(baby.StreamState_Unhealthy))
	}
	s.broadcastersMu.Unlock()

	b.closeSubscribers()

	return wasCurrent
}
