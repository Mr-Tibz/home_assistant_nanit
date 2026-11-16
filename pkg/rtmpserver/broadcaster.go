package rtmpserver

import (
	"bytes"
	"io"
	"sync"
	"sync/atomic"
	"time"

	"github.com/notedit/rtmp/av"
	"github.com/rs/zerolog/log"
)

// minSubscriberBuffer - the least number of packets that can wait for one
// subscriber, see subscriberBufferFor
const minSubscriberBuffer = 2048

// packetsPerSecond - an upper estimate of the packets of a stream in a
// second, pictures and sound together
const packetsPerSecond = 100

// subscriberBufferFor returns the number of packets that can wait for one
// subscriber. A subscriber that falls this far behind is dropped, see
// send.
//
// The number is large on purpose. After a gap of its network, the camera
// can send what it has kept in one go, and a subscriber that is healthy
// must not be dropped for that. The longest gap that the stream survives
// is the read limit of the publisher, so the room follows that limit.
func subscriberBufferFor(readTimeout time.Duration) int {
	return max(minSubscriberBuffer, int((readTimeout+10*time.Second).Seconds())*packetsPerSecond)
}

type subscriber struct {
	// pktC - closed when the subscriber gets no more packets: the
	// publisher has ended, or the subscriber was dropped
	pktC chan av.Packet

	// tooSlow - the subscriber was dropped because it did not take its
	// packets fast enough
	tooSlow atomic.Bool
}

// broadcaster hands the packets of one publisher to its subscribers.
//
// The lock guards everything in it but publisher, which never changes.
// Packets are put into the channels of the subscribers and the channels
// are closed only under the lock, so a packet is never put into a closed
// channel and no channel is closed twice.
type broadcaster struct {
	// publisher - the connection of the publisher
	publisher io.Closer

	// buffer - packets that can wait for one subscriber
	buffer int

	mu          sync.Mutex
	closed      bool
	headerPkts  []av.Packet
	subscribers map[*subscriber]struct{}

	// lastTime - the time of the last picture or sound packet
	lastTime time.Duration

	// opusSeen - sound in the Opus format has arrived, see broadcast
	opusSeen bool
}

func newBroadcaster(publisher io.Closer, buffer int) *broadcaster {
	return &broadcaster{
		publisher:   publisher,
		buffer:      buffer,
		subscribers: make(map[*subscriber]struct{}),
	}
}

// isHeader reports whether a packet describes the stream. A viewer needs
// these packets before the first picture and sound.
func isHeader(pkt av.Packet) bool {
	switch pkt.Type {
	case av.H264DecoderConfig, av.H264SPSPPSNALU, av.AACDecoderConfig, av.Metadata:
		return true
	}

	return false
}

// newSubscriber adds a subscriber and gives it the header packets of the
// stream. When the publisher has ended already, the subscriber gets a
// channel that is closed.
func (b *broadcaster) newSubscriber() *subscriber {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.closed {
		sub := &subscriber{pktC: make(chan av.Packet)}
		close(sub.pktC)

		return sub
	}

	sub := &subscriber{pktC: make(chan av.Packet, b.buffer)}

	b.subscribers[sub] = struct{}{}
	b.send(sub, b.headerPkts...)

	return sub
}

// unsubscribe removes a subscriber that takes no more packets.
func (b *broadcaster) unsubscribe(sub *subscriber) {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.drop(sub)
}

// drop removes sub and closes its channel, if it is still a subscriber.
// The subscriber still gets the packets that wait in the channel, and
// then sees the end. The caller holds the lock.
func (b *broadcaster) drop(sub *subscriber) {
	if _, subscribed := b.subscribers[sub]; !subscribed {
		return
	}

	delete(b.subscribers, sub)
	close(sub.pktC)
}

// send puts packets into the channel of sub. If there is no room for all
// of them, the subscriber is dropped. The caller holds the lock.
//
// A subscriber is never waited for. One that does not take its packets
// fast enough is dropped: its connection is closed, and it can connect
// again. Waiting for it would hold up the publisher, and with it the
// stream for every other subscriber. Leaving out single packets would
// give that subscriber a damaged picture.
func (b *broadcaster) send(sub *subscriber, packets ...av.Packet) {
	if cap(sub.pktC)-len(sub.pktC) < len(packets) {
		sub.tooSlow.Store(true)

		// Throw away what waits for it, so that it sees the end at once
		// and not after it has taken all of that. Those packets are old
		// by now, and the subscriber is to connect again for the live
		// stream.
		for len(sub.pktC) > 0 {
			select {
			case <-sub.pktC:
			default:
			}
		}

		b.drop(sub)

		return
	}

	for _, pkt := range packets {
		sub.pktC <- pkt
	}
}

// broadcast hands a packet of the publisher to every subscriber.
func (b *broadcaster) broadcast(pkt av.Packet) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.closed {
		return
	}

	switch {
	case pkt.Type == av.OPUS:
		// The library that writes the packets to the subscribers cannot
		// write sound in the Opus format: it ends the program on such a
		// packet. The camera sends its sound in the AAC format.
		if !b.opusSeen {
			b.opusSeen = true
			log.Warn().Msg("The stream carries sound in the Opus format, which cannot be handed on. The viewers get no sound")
		}

		return

	case isHeader(pkt):
		if !b.keepHeader(pkt) {
			return
		}

		// The library gives a header packet no time, and it would be
		// written with the time 0. In the middle of a stream that is a
		// step back for a viewer, so the header gets the time of the
		// packet before it.
		pkt.Time = b.lastTime

	default:
		b.lastTime = pkt.Time
	}

	for sub := range b.subscribers {
		b.send(sub, pkt)
	}
}

// keepHeader stores a header packet for the subscribers that come later.
// A newer header packet of a kind takes the place of the older one, so
// the list does not grow with the time that the stream runs. The result
// is false when the packet is the same as the one that is stored: the
// subscribers have it already, and it need not be sent again. The caller
// holds the lock.
func (b *broadcaster) keepHeader(pkt av.Packet) bool {
	for i, headerPkt := range b.headerPkts {
		if headerPkt.Type == pkt.Type {
			if bytes.Equal(headerPkt.Data, pkt.Data) {
				return false
			}

			b.headerPkts[i] = pkt

			return true
		}
	}

	b.headerPkts = append(b.headerPkts, pkt)

	return true
}

// closeSubscribers ends the broadcaster: every subscriber is closed, and
// later packets and subscribers are turned away. It can be called more
// than once.
func (b *broadcaster) closeSubscribers() {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.closed = true

	for sub := range b.subscribers {
		b.drop(sub)
	}
}
