package rtmpserver

import (
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/indiefan/home_assistant_nanit/pkg/baby"
	"github.com/notedit/rtmp/av"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func video(n int) av.Packet {
	return av.Packet{Type: av.H264, Time: time.Duration(n) * time.Millisecond}
}

func header(packetType int, n int) av.Packet {
	return av.Packet{Type: packetType, Time: time.Duration(n) * time.Millisecond}
}

// take returns the packets that wait for the subscriber right now.
func take(sub *subscriber) []av.Packet {
	var packets []av.Packet

	for {
		select {
		case pkt, open := <-sub.pktC:
			if !open {
				return packets
			}
			packets = append(packets, pkt)
		default:
			return packets
		}
	}
}

func isClosed(sub *subscriber) bool {
	for {
		select {
		case _, open := <-sub.pktC:
			if !open {
				return true
			}
		default:
			return false
		}
	}
}

// A subscriber gets the header packets before the first picture, also
// when it joins while the stream runs. Of each kind of header packet it
// gets the newest one.
func TestBroadcasterSendsTheHeadersFirst(t *testing.T) {
	b := newBroadcaster()

	b.broadcast(header(av.H264DecoderConfig, 1))
	b.broadcast(header(av.AACDecoderConfig, 2))
	b.broadcast(video(3))

	sub := b.newSubscriber()

	b.broadcast(header(av.H264DecoderConfig, 4))
	b.broadcast(video(5))
	b.broadcast(video(6))

	packets := take(sub)
	require.Len(t, packets, 4)
	assert.Equal(t, header(av.H264DecoderConfig, 4), packets[0], "the newer one has taken the place of the older one")
	assert.Equal(t, header(av.AACDecoderConfig, 2), packets[1])
	assert.Equal(t, video(5), packets[2])
	assert.Equal(t, video(6), packets[3])

	b.mu.Lock()
	defer b.mu.Unlock()
	assert.Len(t, b.headerPkts, 2, "the list of header packets does not grow")
}

// A subscriber that does not take its packets is dropped. The publisher
// is not held up, and the other subscribers get every packet. Before, the
// publisher waited for the slowest subscriber, and the stream stopped for
// all.
func TestBroadcasterDropsASubscriberThatIsTooSlow(t *testing.T) {
	b := newBroadcaster()

	slow := b.newSubscriber()
	fast := b.newSubscriber()

	received := 0
	finished := make(chan struct{})

	go func() {
		defer close(finished)
		for range fast.pktC {
			received++
		}
	}()

	const packets = 3 * subscriberBuffer

	broadcastDone := make(chan struct{})
	go func() {
		defer close(broadcastDone)
		for i := 0; i < packets; i++ {
			b.broadcast(video(i))

			// Let the fast subscriber keep up
			for len(fast.pktC) > subscriberBuffer/2 {
				time.Sleep(time.Millisecond)
			}
		}
	}()

	select {
	case <-broadcastDone:
	case <-time.After(10 * time.Second):
		t.Fatal("the publisher was held up")
	}

	assert.True(t, isClosed(slow), "the slow subscriber was dropped")

	b.closeSubscribers()
	<-finished

	assert.Equal(t, packets, received, "the fast subscriber got every packet")
}

// The end of a broadcaster can come from two sides at the same moment:
// the publisher ends, and a newer publisher takes its place. Closing the
// channel of a subscriber twice, or handing a packet to a closed one,
// ended the program.
func TestBroadcasterCanBeClosedFromEverySide(t *testing.T) {
	b := newBroadcaster()

	subs := make([]*subscriber, 16)
	for i := range subs {
		subs[i] = b.newSubscriber()
	}

	var group sync.WaitGroup

	// The publisher goes on sending
	group.Add(1)
	go func() {
		defer group.Done()
		for i := 0; i < 2000; i++ {
			b.broadcast(video(i))
		}
	}()

	// Subscribers leave
	for _, sub := range subs[:8] {
		group.Add(1)
		go func(sub *subscriber) {
			defer group.Done()
			b.unsubscribe(sub)
			b.unsubscribe(sub)
		}(sub)
	}

	// Two sides end the broadcaster
	for i := 0; i < 2; i++ {
		group.Add(1)
		go func() {
			defer group.Done()
			b.closeSubscribers()
		}()
	}

	group.Wait()

	for _, sub := range subs {
		assert.True(t, isClosed(sub))
	}

	// What comes after the end is turned away
	b.broadcast(video(1))
	assert.True(t, isClosed(b.newSubscriber()), "a subscriber that comes after the end gets a closed channel")
}

// fakePublisher is a stand-in for the connection of a publisher. It sends
// a number of packets and then nothing more, without closing.
type fakePublisher struct {
	packets  int
	deadline time.Time
}

var errTimeout = errors.New("i/o timeout")

func (publisher *fakePublisher) SetReadDeadline(t time.Time) error {
	publisher.deadline = t
	return nil
}

func (publisher *fakePublisher) ReadPacket() (av.Packet, error) {
	if publisher.packets > 0 {
		publisher.packets--
		return video(publisher.packets), nil
	}

	// Nothing arrives any more: the read ends at its deadline
	time.Sleep(time.Until(publisher.deadline))

	return av.Packet{}, errTimeout
}

// A stream can stop without a sign, for example when the camera loses its
// network. The read then ends at its time limit. Without a limit the
// stream counted as running for ever.
func TestReadPacketsEndsWhenThePublisherSendsNothing(t *testing.T) {
	publisher := &fakePublisher{packets: 5}
	b := newBroadcaster()
	sub := b.newSubscriber()

	result := make(chan error, 1)
	go func() { result <- readPackets(publisher, publisher, b, 50*time.Millisecond) }()

	select {
	case err := <-result:
		assert.Equal(t, errTimeout, err)
	case <-time.After(5 * time.Second):
		t.Fatal("reading did not end")
	}

	assert.Len(t, take(sub), 5, "the packets before the silence were handed on")
}

// When the camera starts a new stream, the old one ends a moment later.
// That end must not mark the stream as ended: the newer publisher sends
// it.
func TestOnlyTheEndOfTheCurrentPublisherEndsTheStream(t *testing.T) {
	states := baby.NewStateManager()
	handler := newRtmpHandler(states)

	streamState := func() baby.StreamState { return states.GetBabyState("baby-1").GetStreamState() }

	older := handler.getNewPublisher("baby-1", nil)
	assert.Equal(t, baby.StreamState_Alive, streamState())

	newer := handler.getNewPublisher("baby-1", nil)

	handler.closePublisher("baby-1", older)
	assert.Equal(t, baby.StreamState_Alive, streamState(), "a newer publisher has replaced it")

	handler.closePublisher("baby-1", newer)
	assert.Equal(t, baby.StreamState_Unhealthy, streamState())

	// A publisher that comes after the end of the last one
	again := handler.getNewPublisher("baby-1", nil)
	handler.closePublisher("baby-1", newer)
	assert.Equal(t, baby.StreamState_Alive, streamState(), "the end of a publisher that is gone already changes nothing")

	handler.closePublisher("baby-1", again)
	assert.Equal(t, baby.StreamState_Unhealthy, streamState())
}

// A newer publisher ends the older one: its connection is closed, and its
// subscribers are closed, so that they connect again and get the new
// stream.
func TestANewPublisherEndsTheOlderOne(t *testing.T) {
	handler := newRtmpHandler(baby.NewStateManager())

	stopped := 0
	older := handler.getNewPublisher("baby-1", func() { stopped++ })

	sub, unsubscribe := handler.getNewSubscriber("baby-1")
	require.NotNil(t, sub)

	handler.getNewPublisher("baby-1", nil)

	assert.Equal(t, 1, stopped, "the connection of the older publisher is closed")
	assert.True(t, isClosed(sub), "its subscriber is closed")

	// The older publisher ends now, and its subscriber leaves. Both
	// close what is closed already.
	handler.closePublisher("baby-1", older)
	unsubscribe()
}
