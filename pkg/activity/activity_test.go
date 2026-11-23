package activity

import (
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/indiefan/home_assistant_nanit/pkg/utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type published struct {
	uid      string
	backfill bool
}

// fakeList is a stand-in for the activity list and for the receiver of
// the events.
type fakeList struct {
	mutex sync.Mutex

	// rounds - the answer to each request, in order. The last one repeats.
	rounds  []func() ([]Event, error)
	fetches int

	// refuse - UIDs for which the next Publish fails, once each
	refuse map[string]bool

	out []published
}

func list(events ...Event) func() ([]Event, error) {
	return func() ([]Event, error) { return append([]Event(nil), events...), nil }
}

func (f *fakeList) fetch() ([]Event, error) {
	f.mutex.Lock()
	round := f.rounds[len(f.rounds)-1]
	if f.fetches < len(f.rounds) {
		round = f.rounds[f.fetches]
	}
	f.fetches++
	f.mutex.Unlock()

	return round()
}

func (f *fakeList) publish(event Event, backfill bool) error {
	f.mutex.Lock()
	defer f.mutex.Unlock()

	if f.refuse[event.UID] {
		delete(f.refuse, event.UID)
		return errors.New("no connection to the broker")
	}

	f.out = append(f.out, published{event.UID, backfill})

	return nil
}

func (f *fakeList) published() []published {
	f.mutex.Lock()
	defer f.mutex.Unlock()

	return append([]published(nil), f.out...)
}

func (f *fakeList) requests() int {
	f.mutex.Lock()
	defer f.mutex.Unlock()

	return f.fetches
}

// run runs a poller on f until f has had the given number of requests,
// and returns what was handed on.
func run(t *testing.T, f *fakeList, requests int) []published {
	poller := &Poller{Fetch: f.fetch, Publish: f.publish, Interval: time.Millisecond}
	runner := utils.RunWithGracefulCancel(poller.Run)

	require.Eventually(t, func() bool { return f.requests() >= requests }, 10*time.Second, time.Millisecond)
	runner.Cancel()

	return f.published()
}

var (
	a = Event{UID: "a", Key: "MOTION", Time: 100}
	b = Event{UID: "b", Key: "SOUND", Time: 200}
	c = Event{UID: "c", Key: "FELL_ASLEEP", Title: "Baby fell asleep", Time: 300}
	// The service adds an entry about sleep late, with the time of the
	// event: it stands behind entries that were there before
	late = Event{UID: "late", Key: "WOKE_UP", Title: "Baby woke up", Time: 150}
)

func TestWhatIsInTheListAtTheStartIsBackfillOldestFirst(t *testing.T) {
	f := &fakeList{rounds: []func() ([]Event, error){list(c, a, b)}}

	assert.Equal(t, []published{{"a", true}, {"b", true}, {"c", true}}, run(t, f, 4))
}

func TestANewEntryIsHandedOnOnce(t *testing.T) {
	f := &fakeList{rounds: []func() ([]Event, error){list(b, a), list(c, b, a), list(c, b, late, a)}}

	assert.Equal(t, []published{{"a", true}, {"b", true}, {"c", false}, {"late", false}}, run(t, f, 6))
}

func TestAnEventIsHandedOnAgainWhenThatFailed(t *testing.T) {
	f := &fakeList{rounds: []func() ([]Event, error){list(c, b, a)}, refuse: map[string]bool{"b": true}}

	// b fails in the first round. c waits for it, and both count as
	// backfill: the round in which the list was first read is not over.
	assert.Equal(t, []published{{"a", true}, {"b", true}, {"c", true}}, run(t, f, 4))
}

func TestAFailedRequestIsFollowedByTheNext(t *testing.T) {
	failed := func() ([]Event, error) { return nil, errors.New("HTTP request failed") }
	f := &fakeList{rounds: []func() ([]Event, error){failed, failed, list(a), list(b, a)}}

	assert.Equal(t, []published{{"a", true}, {"b", false}}, run(t, f, 5))
}

// A fault in this part must not end the program, which would stop the
// stream of the camera.
func TestAPanicDoesNotEndTheProgram(t *testing.T) {
	f := &fakeList{rounds: []func() ([]Event, error){func() ([]Event, error) { panic("a fault") }, list(a)}}

	assert.Equal(t, []published{{"a", true}}, run(t, f, 3))
}

func TestAnEntryWithoutAnIDIsLeftOut(t *testing.T) {
	f := &fakeList{rounds: []func() ([]Event, error){list(Event{Key: "MOTION", Time: 50}, a)}}

	assert.Equal(t, []published{{"a", true}}, run(t, f, 3))
}

func TestText(t *testing.T) {
	assert.Equal(t, "Motion", Event{Key: "MOTION", Title: "MOTION"}.Text())
	assert.Equal(t, "Sound", Event{Key: "SOUND", Title: "SOUND"}.Text())
	assert.Equal(t, "Baby fell asleep", Event{Key: "FELL_ASLEEP", Title: "Baby fell asleep"}.Text())
	assert.Equal(t, "SOMETHING_NEW", Event{Key: "SOMETHING_NEW"}.Text())
}
