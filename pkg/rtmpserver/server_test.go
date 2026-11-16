package rtmpserver

import (
	"errors"
	"net"
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

// header returns a header packet whose content is n.
func header(packetType int, n int) av.Packet {
	return av.Packet{Type: packetType, Data: []byte{byte(n)}}
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
	b := newBroadcaster(&fakeConn{}, minSubscriberBuffer)

	b.broadcast(header(av.H264DecoderConfig, 1))
	b.broadcast(header(av.AACDecoderConfig, 2))
	b.broadcast(video(3))
	b.broadcast(header(av.H264DecoderConfig, 4))

	sub := b.newSubscriber()

	b.broadcast(video(5))
	b.broadcast(video(6))

	assert.Equal(t, []av.Packet{
		header(av.H264DecoderConfig, 4),
		header(av.AACDecoderConfig, 2),
		video(5),
		video(6),
	}, take(sub))

	b.mu.Lock()
	defer b.mu.Unlock()
	assert.Len(t, b.headerPkts, 2, "the list of header packets does not grow")
}

// A header packet that comes while the stream runs goes to the viewers
// that are there too, if it differs from the one before: the pictures
// after it are made for it. A header packet that only repeats the one
// before is not sent again.
func TestBroadcasterSendsAChangedHeaderToItsSubscribers(t *testing.T) {
	b := newBroadcaster(&fakeConn{}, minSubscriberBuffer)

	b.broadcast(header(av.H264DecoderConfig, 1))

	sub := b.newSubscriber()

	b.broadcast(video(2))
	b.broadcast(header(av.H264DecoderConfig, 1))
	b.broadcast(video(3))
	b.broadcast(header(av.H264DecoderConfig, 4))
	b.broadcast(video(5))

	// The library gives a header packet no time. It gets the time of the
	// packet before it, so that the times of a viewer never go back.
	changedConfig := header(av.H264DecoderConfig, 4)
	changedConfig.Time = video(3).Time

	assert.Equal(t, []av.Packet{
		header(av.H264DecoderConfig, 1),
		video(2),
		video(3),
		changedConfig,
		video(5),
	}, take(sub))
}

// When the publisher ends, a viewer still gets the packets that wait for
// it, and then sees the end. Only a viewer that is dropped for being too
// slow loses what waits for it.
func TestBroadcasterLeavesTheLastPacketsToItsSubscribers(t *testing.T) {
	b := newBroadcaster(&fakeConn{}, minSubscriberBuffer)
	sub := b.newSubscriber()

	b.broadcast(video(1))
	b.broadcast(video(2))
	b.closeSubscribers()

	var packets []av.Packet
	for pkt := range sub.pktC {
		packets = append(packets, pkt)
	}

	assert.Equal(t, []av.Packet{video(1), video(2)}, packets)
	assert.False(t, sub.tooSlow.Load())
}

// The room for the packets that wait for a viewer follows the time that a
// silent stream is kept: after a gap of that length the camera can send
// what it has kept in one go.
func TestSubscriberBufferFollowsTheReadLimit(t *testing.T) {
	assert.Equal(t, 4000, subscriberBufferFor(DefaultReadTimeout))
	assert.Equal(t, 13000, subscriberBufferFor(2*time.Minute))
	assert.Equal(t, minSubscriberBuffer, subscriberBufferFor(0), "no limit")
}

// Sound in the Opus format is sound, not a header. The old rule took
// every packet type above 2 for a header, and Opus is 3. Such sound is
// not handed on at all: the library that writes the packets ends the
// program on it.
func TestWhichPacketsAreHeaders(t *testing.T) {
	for _, packetType := range []int{av.H264DecoderConfig, av.H264SPSPPSNALU, av.AACDecoderConfig, av.Metadata} {
		assert.True(t, isHeader(av.Packet{Type: packetType}), av.PacketTypeString[packetType])
	}

	for _, packetType := range []int{av.H264, av.AAC, av.OPUS} {
		assert.False(t, isHeader(av.Packet{Type: packetType}), av.PacketTypeString[packetType])
	}

	b := newBroadcaster(&fakeConn{}, minSubscriberBuffer)
	sub := b.newSubscriber()

	b.broadcast(av.Packet{Type: av.OPUS})
	b.broadcast(video(1))

	assert.Equal(t, []av.Packet{video(1)}, take(sub))

	b.mu.Lock()
	defer b.mu.Unlock()
	assert.Empty(t, b.headerPkts)
}

// A subscriber that does not take its packets is dropped. The publisher
// is not held up, and the other subscribers get every packet. Before, the
// publisher waited for the slowest subscriber, and the stream stopped for
// all.
func TestBroadcasterDropsASubscriberThatIsTooSlow(t *testing.T) {
	b := newBroadcaster(&fakeConn{}, minSubscriberBuffer)

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

	const packets = 3 * minSubscriberBuffer

	broadcastDone := make(chan struct{})
	go func() {
		defer close(broadcastDone)
		for i := 0; i < packets; i++ {
			b.broadcast(video(i))

			// Let the fast subscriber keep up
			for len(fast.pktC) > minSubscriberBuffer/2 {
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
	assert.True(t, slow.tooSlow.Load())

	// The end of the broadcaster throws away what waits, so let the fast
	// subscriber take everything first
	require.Eventually(t, func() bool { return len(fast.pktC) == 0 }, 10*time.Second, time.Millisecond)

	b.closeSubscribers()
	<-finished

	assert.False(t, fast.tooSlow.Load())

	assert.Equal(t, packets, received, "the fast subscriber got every packet")
}

// The end of a broadcaster can come from two sides at the same moment:
// the publisher ends, and a newer publisher takes its place. Closing the
// channel of a subscriber twice, or handing a packet to a closed one,
// ended the program.
func TestBroadcasterCanBeClosedFromEverySide(t *testing.T) {
	b := newBroadcaster(&fakeConn{}, minSubscriberBuffer)

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

// A stream can stop without a sign, for example when the camera loses its
// network. With a limit, a read on the connection of the publisher then
// ends. Without one, the stream counted as running for ever.
//
// The limit is for the time without any data, not for the stream as a
// whole: the data here arrives over a longer time than the limit.
func TestIdleConnEndsAReadWhenNothingArrives(t *testing.T) {
	publisher, receiver := net.Pipe()
	defer publisher.Close()
	defer receiver.Close()

	conn := &idleConn{Conn: receiver}
	limitIdleTime(conn, 300*time.Millisecond)

	// Data every 30 ms for 600 ms, and then silence
	go func() {
		for i := 0; i < 20; i++ {
			if _, err := publisher.Write([]byte{byte(i)}); err != nil {
				return
			}
			time.Sleep(30 * time.Millisecond)
		}
	}()

	received := 0
	buffer := make([]byte, 16)

	for {
		n, err := conn.Read(buffer)
		received += n

		if err != nil {
			var netErr net.Error
			require.True(t, errors.As(err, &netErr) && netErr.Timeout(), "got: %v", err)
			break
		}
	}

	assert.Equal(t, 20, received, "everything before the silence has arrived")
}

// Without a limit, a read waits for as long as it takes. That is what a
// subscriber needs, which sends nothing while it takes the stream.
func TestIdleConnWithoutALimit(t *testing.T) {
	publisher, receiver := net.Pipe()
	defer publisher.Close()
	defer receiver.Close()

	conn := &idleConn{Conn: receiver}

	go func() {
		time.Sleep(200 * time.Millisecond)
		publisher.Write([]byte{1})
	}()

	n, err := conn.Read(make([]byte, 16))

	assert.NoError(t, err)
	assert.Equal(t, 1, n)
}

// scriptedReader hands out a number of packets, and then fails.
type scriptedReader struct {
	packets int
}

var errReadFailed = errors.New("i/o timeout")

func (reader *scriptedReader) ReadPacket() (av.Packet, error) {
	if reader.packets == 0 {
		return av.Packet{}, errReadFailed
	}

	reader.packets--

	return video(reader.packets), nil
}

// readPackets hands every packet on, and returns the failure that ends
// the stream.
func TestReadPackets(t *testing.T) {
	b := newBroadcaster(&fakeConn{}, minSubscriberBuffer)
	sub := b.newSubscriber()

	err := readPackets(&scriptedReader{packets: 5}, b)

	assert.Equal(t, errReadFailed, err)
	assert.Len(t, take(sub), 5)
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
