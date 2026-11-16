package rtmpserver

import (
	"testing"

	"github.com/indiefan/home_assistant_nanit/pkg/baby"
	"github.com/stretchr/testify/assert"
)

// When the camera starts a new stream, the old one ends a moment later.
// That end must not mark the stream as ended: the newer publisher sends
// it.
func TestOnlyTheEndOfTheCurrentPublisherEndsTheStream(t *testing.T) {
	states := baby.NewStateManager()
	handler := newRtmpHandler(states)

	streamState := func() baby.StreamState { return states.GetBabyState("baby-1").GetStreamState() }

	older := handler.getNewPublisher("baby-1")
	assert.Equal(t, baby.StreamState_Alive, streamState())

	newer := handler.getNewPublisher("baby-1")

	handler.closePublisher("baby-1", older)
	assert.Equal(t, baby.StreamState_Alive, streamState(), "a newer publisher has replaced it")

	handler.closePublisher("baby-1", newer)
	assert.Equal(t, baby.StreamState_Unhealthy, streamState())

	// A publisher that comes after the end of the last one
	again := handler.getNewPublisher("baby-1")
	handler.closePublisher("baby-1", newer)
	assert.Equal(t, baby.StreamState_Alive, streamState(), "the end of a publisher that is gone already changes nothing")

	handler.closePublisher("baby-1", again)
	assert.Equal(t, baby.StreamState_Unhealthy, streamState())
}
