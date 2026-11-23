package activity

import (
	"errors"
	"sort"
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
	rounds  []func(limit int) ([]Event, error)
	fetches int

	// limits - how many entries each request asked for
	limits []int

	// refuse - UIDs for which the next Publish fails, once each
	refuse map[string]bool

	out []published
}

// list answers a request the way the service does: with the newest
// entries, as many as the request asks for.
func list(events ...Event) func(limit int) ([]Event, error) {
	return func(limit int) ([]Event, error) {
		newest := append([]Event(nil), events...)
		sort.SliceStable(newest, func(i, j int) bool { return newest[i].Time > newest[j].Time })

		if len(newest) > limit {
			newest = newest[:limit]
		}

		return newest, nil
	}
}

func (f *fakeList) fetch(limit int) ([]Event, error) {
	f.mutex.Lock()
	round := f.rounds[len(f.rounds)-1]
	if f.fetches < len(f.rounds) {
		round = f.rounds[f.fetches]
	}
	f.fetches++
	f.limits = append(f.limits, limit)
	f.mutex.Unlock()

	return round(limit)
}

func (f *fakeList) asked() []int {
	f.mutex.Lock()
	defer f.mutex.Unlock()

	return append([]int(nil), f.limits...)
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
	return runWith(t, f, requests, time.Hour)
}

// runWith is run with a time between two requests for a long list.
func runWith(t *testing.T, f *fakeList, requests int, fullInterval time.Duration) []published {
	poller := &Poller{Fetch: f.fetch, Publish: f.publish, Interval: time.Millisecond, FullInterval: fullInterval}
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
	f := &fakeList{rounds: []func(limit int) ([]Event, error){list(c, a, b)}}

	assert.Equal(t, []published{{"a", true}, {"b", true}, {"c", true}}, run(t, f, 4))
}

func TestANewEntryIsHandedOnOnce(t *testing.T) {
	f := &fakeList{rounds: []func(limit int) ([]Event, error){list(b, a), list(c, b, a), list(c, b, late, a)}}

	assert.Equal(t, []published{{"a", true}, {"b", true}, {"c", false}, {"late", false}}, run(t, f, 6))
}

func TestAnEventIsHandedOnAgainWhenThatFailed(t *testing.T) {
	f := &fakeList{rounds: []func(limit int) ([]Event, error){list(c, b, a)}, refuse: map[string]bool{"b": true}}

	// b fails in the first round. c waits for it, and both count as
	// backfill: the round in which the list was first read is not over.
	assert.Equal(t, []published{{"a", true}, {"b", true}, {"c", true}}, run(t, f, 4))
}

func TestAFailedRequestIsFollowedByTheNext(t *testing.T) {
	failed := func(int) ([]Event, error) { return nil, errors.New("HTTP request failed") }
	f := &fakeList{rounds: []func(limit int) ([]Event, error){failed, failed, list(a), list(b, a)}}

	assert.Equal(t, []published{{"a", true}, {"b", false}}, run(t, f, 5))
}

// A fault in this part must not end the program, which would stop the
// stream of the camera.
func TestAPanicDoesNotEndTheProgram(t *testing.T) {
	f := &fakeList{rounds: []func(limit int) ([]Event, error){func(int) ([]Event, error) { panic("a fault") }, list(a)}}

	assert.Equal(t, []published{{"a", true}}, run(t, f, 3))
}

func TestAnEntryWithoutAnIDIsLeftOut(t *testing.T) {
	f := &fakeList{rounds: []func(limit int) ([]Event, error){list(Event{Key: "MOTION", Time: 50}, a)}}

	assert.Equal(t, []published{{"a", true}}, run(t, f, 3))
}

// numbered returns events with the IDs and the times from..to.
func numbered(from int, to int) []Event {
	var events []Event
	for i := from; i <= to; i++ {
		events = append(events, Event{UID: string(rune('A' + i)), Key: "MOTION", Time: float64(1000 + i)})
	}

	return events
}

func uids(out []published) string {
	text := ""
	for _, p := range out {
		text += p.uid
	}

	return text
}

// The service sends much for each entry, so only the first request and
// one now and then ask for a long list.
func TestMostRequestsAskForAShortList(t *testing.T) {
	f := &fakeList{rounds: []func(limit int) ([]Event, error){list(numbered(0, 9)...)}}

	out := run(t, f, 5)

	assert.Equal(t, "ABCDEFGHIJ", uids(out), "the first request reaches back")
	asked := f.asked()
	assert.Equal(t, FullLimit, asked[0])
	for _, limit := range asked[1:5] {
		assert.Equal(t, QuickLimit, limit)
	}
}

// An entry about sleep is added late, with the time of the event. When
// newer entries were added before it, a short list does not show it.
func TestALongListFindsAnEntryBehindNewerOnes(t *testing.T) {
	hidden := Event{UID: "hidden", Key: "FELL_ASLEEP", Time: 1000.5}
	before := list(numbered(0, 9)...)
	after := list(append(numbered(0, 9), hidden)...)

	f := &fakeList{rounds: []func(limit int) ([]Event, error){before, after}}
	out := runWith(t, f, 12, 5*time.Millisecond)

	assert.Equal(t, published{"hidden", false}, out[len(out)-1])
	assert.Len(t, out, 11)
	assert.Contains(t, f.asked()[1:], FullLimit, "a long list is asked for again after the time for it")
}

// More entries than a short list holds were added since the last
// request: all of the short list is new, and the rest is asked for at once.
func TestAShortListOfNewEntriesOnlyIsFollowedByALongOne(t *testing.T) {
	f := &fakeList{rounds: []func(limit int) ([]Event, error){
		list(numbered(0, 1)...),
		list(numbered(0, 9)...), // the short list of the second request
		list(numbered(0, 9)...), // the long list that follows it at once
	}}

	out := run(t, f, 5)

	assert.Equal(t, "ABCDEFGHIJ", uids(out))
	assert.Equal(t, []int{FullLimit, QuickLimit, FullLimit}, f.asked()[:3])
	for _, p := range out[2:] {
		assert.False(t, p.backfill)
	}
}

// An event that could not be handed on may not be in a short list, so
// the next request asks for a long list again.
func TestAfterAFailureTheNextRequestAsksForALongListAgain(t *testing.T) {
	f := &fakeList{rounds: []func(limit int) ([]Event, error){list(numbered(0, 9)...)}, refuse: map[string]bool{"C": true}}

	out := run(t, f, 4)

	assert.Equal(t, "ABCDEFGHIJ", uids(out))
	assert.Equal(t, []int{FullLimit, FullLimit, QuickLimit}, f.asked()[:3])
}

// The same after a failure in a request for a short list.
func TestAfterAFailureWithAShortListTheNextRequestAsksForALongList(t *testing.T) {
	f := &fakeList{
		rounds: []func(limit int) ([]Event, error){list(a, b), list(a, b, c)},
		refuse: map[string]bool{"c": true},
	}

	out := run(t, f, 5)

	assert.Equal(t, []published{{"a", true}, {"b", true}, {"c", false}}, out)
	assert.Equal(t, []int{FullLimit, QuickLimit, FullLimit, QuickLimit}, f.asked()[:4])
}

func TestText(t *testing.T) {
	assert.Equal(t, "Motion", Event{Key: "MOTION", Title: "MOTION"}.Text())
	assert.Equal(t, "Sound", Event{Key: "SOUND", Title: "SOUND"}.Text())
	assert.Equal(t, "Baby fell asleep", Event{Key: "FELL_ASLEEP", Title: "Baby fell asleep"}.Text())
	assert.Equal(t, "SOMETHING_NEW", Event{Key: "SOMETHING_NEW"}.Text())
}
