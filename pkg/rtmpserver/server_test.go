package rtmpserver

import (
	"testing"

	"github.com/indiefan/home_assistant_nanit/pkg/baby"
	"github.com/stretchr/testify/assert"
)

// When the camera starts a new stream, the old one ends a moment later.
// That end must not count as the end of the stream: the newer publisher
// sends it. closePublisher tells the two cases apart.
func TestClosePublisherTellsWhetherItWasTheCurrentOne(t *testing.T) {
	handler := newRtmpHandler(baby.NewStateManager())

	older := handler.getNewPublisher("baby-1")
	newer := handler.getNewPublisher("baby-1")

	assert.False(t, handler.closePublisher("baby-1", older), "a newer publisher has replaced it")
	assert.True(t, handler.closePublisher("baby-1", newer))
	assert.False(t, handler.closePublisher("baby-1", newer), "it is gone already")
}
