package rtmpserver

import (
	"io"
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

	if isHeader(pkt) {
		// A newer header packet of a kind takes the place of the older
		// one, so the list does not grow with the time that the stream
		// runs.
		replaced := false
		for i, headerPkt := range b.headerPkts {
			if headerPkt.Type == pkt.Type {
				b.headerPkts[i] = pkt
				replaced = true
				break
			}
		}

		if !replaced {
			b.headerPkts = append(b.headerPkts, pkt)
		}

		// Subscribers that have their headers already get the new one
		// too: the packets after it are made for it. The others get all
		// headers with their first picture or sound.
		for sub := range b.subscribers {
			if sub.initialized {
				b.send(sub, pkt)
			}
		}

		return
	}

	// Audio / Video packets
	for sub := range b.subscribers {
		if sub.initialized {
			b.send(sub, pkt)
			continue
		}

		// Send header packets before sending any data
		sub.initialized = true
		b.send(sub, append(append([]av.Packet{}, b.headerPkts...), pkt)...)
	}
}

// closeSubscribers ends the broadcaster: every subscriber is closed, and
// later packets and subscribers are turned away. It can be called more
// than once. It reports whether this call was the one that ended it.
func (b *broadcaster) closeSubscribers() bool {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.closed {
		return false
	}

	b.closed = true

	for sub := range b.subscribers {
		b.drop(sub)
	}

	return true
}
