package rtmpserver

import (
	"sync"

	"github.com/notedit/rtmp/av"
)

// subscriberBuffer - packets that can wait for one subscriber. A
// subscriber that falls this far behind is dropped, see broadcast. At 30
// pictures a second, with the sound, this is several seconds.
const subscriberBuffer = 512

type subscriber struct {
	// initialized - the header packets were sent. Used by the publisher
	// only, under the lock of the broadcaster.
	initialized bool

	// pktC - closed when the subscriber gets no more packets: the
	// publisher has ended, or the subscriber was dropped
	pktC chan av.Packet
}

// broadcaster hands the packets of one publisher to its subscribers.
//
// The lock guards everything in it. Packets are put into the channels of
// the subscribers and the channels are closed only under the lock, so a
// packet is never put into a closed channel and no channel is closed
// twice.
type broadcaster struct {
	mu          sync.Mutex
	closed      bool
	headerPkts  []av.Packet
	subscribers map[*subscriber]struct{}

	// stop ends the publisher, see closePublisher. It may be nil.
	stop func()
}

func newBroadcaster() *broadcaster {
	return &broadcaster{
		subscribers: make(map[*subscriber]struct{}),
	}
}

// newSubscriber adds a subscriber. When the publisher has ended already,
// the subscriber gets a channel that is closed.
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

	return sub
}

// unsubscribe removes a subscriber that takes no more packets.
func (b *broadcaster) unsubscribe(sub *subscriber) {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.drop(sub)
}

// drop removes sub and closes its channel, if it is still a subscriber.
// The caller holds the lock.
func (b *broadcaster) drop(sub *subscriber) {
	if _, subscribed := b.subscribers[sub]; subscribed {
		delete(b.subscribers, sub)
		close(sub.pktC)
	}
}

// broadcast hands a packet of the publisher to every subscriber.
//
// It never waits for a subscriber. A subscriber that does not take its
// packets fast enough is dropped: its connection is closed, and it can
// connect again. Waiting for it would hold up the publisher, and with it
// the stream for every other subscriber. Leaving out single packets would
// give that subscriber a damaged picture.
func (b *broadcaster) broadcast(pkt av.Packet) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.closed {
		return
	}

	// Header packets. A newer one of a type takes the place of the older
	// one, so the list does not grow with the time that the stream runs.
	if pkt.Type > 2 {
		for i, headerPkt := range b.headerPkts {
			if headerPkt.Type == pkt.Type {
				b.headerPkts[i] = pkt
				return
			}
		}

		b.headerPkts = append(b.headerPkts, pkt)
		return
	}

	// Audio / Video packets
	for sub := range b.subscribers {
		// Send header packets before sending any data
		needed := 1
		if !sub.initialized {
			needed += len(b.headerPkts)
		}

		if cap(sub.pktC)-len(sub.pktC) < needed {
			b.drop(sub)
			continue
		}

		if !sub.initialized {
			sub.initialized = true
			for _, headerPkt := range b.headerPkts {
				sub.pktC <- headerPkt
			}
		}

		sub.pktC <- pkt
	}
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
