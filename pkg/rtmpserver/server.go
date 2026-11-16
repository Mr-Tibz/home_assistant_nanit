package rtmpserver

import (
	"net"
	"regexp"
	"sync"
	"time"

	"github.com/notedit/rtmp/format/rtmp"
	"github.com/rs/zerolog/log"
	"github.com/indiefan/home_assistant_nanit/pkg/baby"
)

type rtmpHandler struct {
	babyStateManager  *baby.StateManager
	broadcastersMu    sync.RWMutex
	broadcastersByUID map[string]*broadcaster
}

// StartRTMPServer - Blocking server
func StartRTMPServer(addr string, babyStateManager *baby.StateManager) {
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatal().Str("addr", addr).Err(err).Msg("Unable to start RTMP server")
		panic(err)
	}

	log.Info().Str("addr", addr).Msg("RTMP server started")

	s := rtmp.NewServer()
	s.HandleConn = newRtmpHandler(babyStateManager).handleConnection

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
		publisher := s.getNewPublisher(babyUID)

		for {
			pkt, err := c.ReadPacket()
			if err != nil {
				sublog.Warn().Err(err).Msg("Publisher stream closed unexpectedly")
				s.closePublisher(babyUID, publisher)
				return
			}

			publisher.broadcast(pkt)
		}

	} else {
		sublog.Debug().Msg("New stream subscriber connected")
		subscriber, unsubscribe := s.getNewSubscriber(babyUID)

		if subscriber == nil {
			sublog.Warn().Msg("No stream publisher registered yet, closing subscriber stream")
			nc.Close()
			return
		}

		closeC := c.CloseNotify()
		for {
			select {
			case pkt, open := <-subscriber.pktC:
				if !open {
					sublog.Debug().Msg("Closing subscriber because publisher quit")
					nc.Close()
					return
				}

				c.WritePacket(pkt)

			case <-closeC:
				sublog.Debug().Msg("Stream subscriber disconnected")
				unsubscribe()
			}
		}
	}
}

// getNewPublisher registers a new publisher for the baby and marks the
// stream as alive.
//
// The state of the stream changes under the same lock as the list of
// publishers, here and in closePublisher. A new publisher that comes at
// the moment an old one ends can therefore not be followed by the state
// "ended" of the old one.
func (s *rtmpHandler) getNewPublisher(babyUID string) *broadcaster {
	broadcaster := newBroadcaster()

	s.broadcastersMu.Lock()
	existingBroadcaster, hadExistingBroadcaster := s.broadcastersByUID[babyUID]
	s.broadcastersByUID[babyUID] = broadcaster
	s.babyStateManager.Update(babyUID, *baby.NewState().SetStreamState(baby.StreamState_Alive))
	s.broadcastersMu.Unlock()

	if hadExistingBroadcaster {
		log.Warn().Msg("Baby already has active publisher, closing existing subscribers")
		go existingBroadcaster.closeSubscribers()
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
// at that moment, the stream is marked as ended. A publisher that a newer
// one has replaced says nothing about the stream when it ends: the newer
// one sends it.
func (s *rtmpHandler) closePublisher(babyUID string, b *broadcaster) {
	s.broadcastersMu.Lock()
	if currBroadcaster, hasExistingBroadcaster := s.broadcastersByUID[babyUID]; hasExistingBroadcaster && currBroadcaster == b {
		delete(s.broadcastersByUID, babyUID)
		s.babyStateManager.Update(babyUID, *baby.NewState().SetStreamState(baby.StreamState_Unhealthy))
	}
	s.broadcastersMu.Unlock()

	b.closeSubscribers()
}
