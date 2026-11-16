package rtmpserver

import (
	"bytes"
	"io"
	"sync"
	"sync/atomic"

	"github.com/notedit/rtmp/av"
)

// subscriberBuffer - packets that can wait for one subscriber. A
// subscriber that falls this far behind is dropped, see send.
//
// The number is large on purpose. After a gap of its network, the camera
// can send what it has kept in one go, and a subscriber that is healthy
// must not be dropped for that. With about 75 packets a second, this is
// about half a minute.
const subscriberBuffer = 2048

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

	mu          sync.Mutex
	closed      bool
	headerPkts  []av.Packet
	subscribers map[*subscriber]struct{}
}

func newBroadcaster(publisher io.Closer) *broadcaster {
	return &broadcaster{
		publisher:   publisher,
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
	sub := &subscriber{
		pktC: make(chan av.Packet, subscriberBuffer),
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	if b.closed {
		close(sub.pktC)
		return sub
	}

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
// The packets that wait in the channel are thrown away first, so that the
// subscriber sees the end at once and not after it has taken them all.
// The caller holds the lock.
func (b *broadcaster) drop(sub *subscriber) {
	if _, subscribed := b.subscribers[sub]; !subscribed {
		return
	}

	delete(b.subscribers, sub)

	for len(sub.pktC) > 0 {
		select {
		case <-sub.pktC:
		default:
		}
	}

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
		b.drop(sub)
		return
	}

	for _, pkt := range packets {
		sub.pktC <- pkt
	}
}

// broadcast hands a packet of the publisher to every subscriber.
func (b *broadcaster) broadcast(pkt av.Packet) {
	// The library that writes the packets to the subscribers cannot
	// write sound in the Opus format: it ends the program on such a
	// packet. The camera sends its sound in the AAC format.
	if pkt.Type == av.OPUS {
		return
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	if b.closed {
		return
	}

	if isHeader(pkt) && !b.keepHeader(pkt) {
		return
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
