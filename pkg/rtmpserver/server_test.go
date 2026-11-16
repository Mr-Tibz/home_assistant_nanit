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

// fakeConn is a stand-in for the connection of a publisher.
type fakeConn struct {
	mu     sync.Mutex
	closed int
}

func (conn *fakeConn) Close() error {
	conn.mu.Lock()
	defer conn.mu.Unlock()

	conn.closed++

	return nil
}

func (conn *fakeConn) timesClosed() int {
	conn.mu.Lock()
	defer conn.mu.Unlock()

	return conn.closed
}

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
	b := newBroadcaster(&fakeConn{})

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

// A header packet that comes while the stream runs goes to the viewers
// that are there too: the pictures after it are made for it.
func TestBroadcasterSendsANewHeaderToItsSubscribers(t *testing.T) {
	b := newBroadcaster(&fakeConn{})

	b.broadcast(header(av.H264DecoderConfig, 1))

	sub := b.newSubscriber()

	b.broadcast(video(2))
	b.broadcast(header(av.H264DecoderConfig, 3))
	b.broadcast(video(4))

	assert.Equal(t, []av.Packet{
		header(av.H264DecoderConfig, 1),
		video(2),
		header(av.H264DecoderConfig, 3),
		video(4),
	}, take(sub))
}

// Sound in the Opus format is sound, not a header. The old rule took every
// packet type above 2 for a header, and Opus is 3.
func TestWhichPacketsAreHeaders(t *testing.T) {
	for _, packetType := range []int{av.H264DecoderConfig, av.H264SPSPPSNALU, av.AACDecoderConfig, av.Metadata} {
		assert.True(t, isHeader(av.Packet{Type: packetType}), av.PacketTypeString[packetType])
	}

	for _, packetType := range []int{av.H264, av.AAC, av.OPUS} {
		assert.False(t, isHeader(av.Packet{Type: packetType}), av.PacketTypeString[packetType])
	}
}

// A subscriber that does not take its packets is dropped. The publisher
// is not held up, and the other subscribers get every packet. Before, the
// publisher waited for the slowest subscriber, and the stream stopped for
// all.
func TestBroadcasterDropsASubscriberThatIsTooSlow(t *testing.T) {
	b := newBroadcaster(&fakeConn{})

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

	// A subscriber that is dropped sees that at once. The packets that
	// waited for it are thrown away.
	_, open := <-slow.pktC
	assert.False(t, open, "the slow subscriber was dropped, and nothing waits for it")

	b.closeSubscribers()
	<-finished

	assert.Equal(t, packets, received, "the fast subscriber got every packet")
}

// The end of a broadcaster can come from two sides at the same moment:
// the publisher ends, and a newer publisher takes its place. Closing the
// channel of a subscriber twice, or handing a packet to a closed one,
// ended the program.
func TestBroadcasterCanBeClosedFromEverySide(t *testing.T) {
	b := newBroadcaster(&fakeConn{})

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
// a number of packets, one every interval, and then nothing more, without
// closing.
type fakePublisher struct {
	packets  int
	interval time.Duration

	deadline  time.Time
	deadlines int
}

var errTimeout = errors.New("i/o timeout")

func (publisher *fakePublisher) SetReadDeadline(t time.Time) error {
	publisher.deadline = t
	publisher.deadlines++

	return nil
}

func (publisher *fakePublisher) ReadPacket() (av.Packet, error) {
	if publisher.packets > 0 {
		time.Sleep(publisher.interval)

		if !publisher.deadline.IsZero() && time.Now().After(publisher.deadline) {
			return av.Packet{}, errTimeout
		}

		publisher.packets--

		return video(publisher.packets), nil
	}

	if publisher.deadline.IsZero() {
		return av.Packet{}, errors.New("the test ends a publisher without a time limit here")
	}

	// Nothing arrives any more: the read ends at its deadline
	time.Sleep(time.Until(publisher.deadline))

	return av.Packet{}, errTimeout
}

// A stream can stop without a sign, for example when the camera loses its
// network. The read then ends at its time limit. Without a limit the
// stream counted as running for ever.
//
// The limit is for the time without a packet, not for the stream as a
// whole: the packets here take longer together than the limit.
func TestReadPacketsEndsWhenThePublisherSendsNothing(t *testing.T) {
	publisher := &fakePublisher{packets: 10, interval: 20 * time.Millisecond}
	b := newBroadcaster(&fakeConn{})
	sub := b.newSubscriber()

	result := make(chan error, 1)
	go func() { result <- readPackets(publisher, publisher, b, 100*time.Millisecond) }()

	select {
	case err := <-result:
		assert.Equal(t, errTimeout, err)
	case <-time.After(5 * time.Second):
		t.Fatal("reading did not end")
	}

	assert.Len(t, take(sub), 10, "every packet before the silence was handed on")
	assert.Equal(t, 11, publisher.deadlines, "the time limit started again with every packet")
}

// Without a time limit, no deadline is set.
func TestReadPacketsWithoutATimeLimit(t *testing.T) {
	publisher := &fakePublisher{packets: 3}
	b := newBroadcaster(&fakeConn{})

	assert.Error(t, readPackets(publisher, publisher, b, 0))
	assert.Equal(t, 0, publisher.deadlines)
}

// When the camera starts a new stream, the old one ends a moment later.
// That end must not mark the stream as ended: the newer publisher sends
// it.
func TestOnlyTheEndOfTheCurrentPublisherEndsTheStream(t *testing.T) {
	states := baby.NewStateManager()
	handler := newRtmpHandler(states)

	streamState := func() baby.StreamState { return states.GetBabyState("baby-1").GetStreamState() }

	older := handler.getNewPublisher("baby-1", &fakeConn{})
	assert.Equal(t, baby.StreamState_Alive, streamState())

	newer := handler.getNewPublisher("baby-1", &fakeConn{})

	assert.False(t, handler.closePublisher("baby-1", older))
	assert.Equal(t, baby.StreamState_Alive, streamState(), "a newer publisher has replaced it")

	assert.True(t, handler.closePublisher("baby-1", newer))
	assert.Equal(t, baby.StreamState_Unhealthy, streamState())

	// A publisher that comes after the end of the last one
	again := handler.getNewPublisher("baby-1", &fakeConn{})
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

	olderConn := &fakeConn{}
	older := handler.getNewPublisher("baby-1", olderConn)

	sub, unsubscribe := handler.getNewSubscriber("baby-1")
	require.NotNil(t, sub)

	newerConn := &fakeConn{}
	handler.getNewPublisher("baby-1", newerConn)

	assert.Equal(t, 1, olderConn.timesClosed(), "the connection of the older publisher is closed")
	assert.Equal(t, 0, newerConn.timesClosed())
	assert.True(t, isClosed(sub), "its subscriber is closed")

	// The older publisher ends now, and its subscriber leaves. Both
	// close what is closed already.
	handler.closePublisher("baby-1", older)
	unsubscribe()
}
