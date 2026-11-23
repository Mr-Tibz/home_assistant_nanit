// Package activity follows the activity list of the Nanit service: the
// list of events that the Nanit app shows for a baby, such as motion,
// sound, "Baby fell asleep" and "Parent took baby out".
//
// The service keeps this list whether or not the account gets
// notifications, and it has no way to tell a program about a new entry.
// So the list is asked for at an interval, and every entry that has not
// been seen before is handed on.
package activity

import (
	"fmt"
	"sort"
	"time"

	"github.com/indiefan/home_assistant_nanit/pkg/utils"
	"github.com/rs/zerolog/log"
)

// Event - one entry of the activity list (matching the Nanit API)
type Event struct {
	// UID - the ID of the entry. It never changes.
	UID string `json:"uid"`

	// Key - the kind of the event, for example MOTION or FELL_ASLEEP
	Key string `json:"key"`

	// InternalKey - a finer kind where the service has one, for example
	// VISIT_WOKE_UP for the kind VISIT. Else the same as Key.
	InternalKey string `json:"internal_key"`

	// Title - the text of the Nanit app for the event, for example
	// "Baby fell asleep". For motion and sound it is the kind itself.
	Title string `json:"title"`

	// Time - when the event happened, in seconds since 1970
	Time float64 `json:"time"`

	// CreatedAt - when the service added the entry to the list. For the
	// events about sleep that is some 15 minutes after the event.
	CreatedAt string `json:"created_at"`

	// Clip - the Nanit app has a video clip of the event
	Clip bool `json:"clip"`
}

// titles - a text for the kinds whose Title is the kind itself
var titles = map[string]string{
	"MOTION": "Motion",
	"SOUND":  "Sound",
}

// Text returns a text for the event that a person can read.
func (e Event) Text() string {
	if title, found := titles[e.Key]; found {
		return title
	}

	if e.Title != "" {
		return e.Title
	}

	return e.Key
}

// The service sends some 5 kilobytes for each entry, most of it addresses
// of video clips, and does not compress the answer. So most requests ask
// for a few entries only, and a long list is asked for now and then.
const (
	// DefaultInterval - time between two requests for the list
	DefaultInterval = time.Minute

	// QuickLimit - number of entries that most requests ask for
	QuickLimit = 5

	// FullLimit - number of entries of a long list. An entry about sleep
	// is added late, with the time of the event, so it can stand behind
	// entries that are newer, where a short list does not show it. One
	// day has some 25 entries, so this number reaches back about a day.
	FullLimit = 30

	// DefaultFullInterval - time between two requests for a long list
	DefaultFullInterval = 15 * time.Minute

	// maxSlowdown - after failures in a row, the pause between two
	// requests grows up to this many intervals
	maxSlowdown = 10
)

// Poller asks for the activity list of one baby and hands on what is new.
type Poller struct {
	// Fetch returns the newest entries of the list, limit of them at most
	Fetch func(limit int) ([]Event, error)

	// Publish hands one event on. backfill tells that the entry was in
	// the list already when the program started: the program cannot know
	// whether it has handed that one on in an earlier run, so the
	// receiver gets it again and must expect that. An event for which
	// Publish fails is handed on again at the next request.
	Publish func(event Event, backfill bool) error

	// Interval - time between two requests. Zero means DefaultInterval.
	Interval time.Duration

	// FullInterval - time between two requests for a long list. Zero
	// means DefaultFullInterval.
	FullInterval time.Duration
}

// Run asks for the list until ctx is done. It never ends the program: a
// failure is logged, and the next request comes later.
func (p *Poller) Run(ctx utils.GracefulContext) {
	interval := p.Interval
	if interval <= 0 {
		interval = DefaultInterval
	}

	fullInterval := p.FullInterval
	if fullInterval <= 0 {
		fullInterval = DefaultFullInterval
	}

	// The IDs of the events that have been handed on. One day adds some
	// 25 of them, so the list stays small for years.
	seen := make(map[string]bool)
	backfill := true
	failures := 0

	// lastFull - when a long list was last handed on as a whole. The
	// zero time makes the first request one for a long list.
	var lastFull time.Time

	for {
		started := time.Now()
		long, err := p.round(seen, backfill, started.Sub(lastFull) >= fullInterval)

		if err != nil {
			failures++
			log.Warn().Err(err).Msg("Unable to follow the activity list, will try again")

			// An event that could not be handed on may not be in the
			// next short list, so the next request is for a long one
			lastFull = time.Time{}
		} else {
			failures = 0
			// From here on, an entry that is not known is a new one
			backfill = false

			if long {
				lastFull = started
			}
		}

		slowdown := failures + 1
		if slowdown > maxSlowdown {
			slowdown = maxSlowdown
		}

		select {
		case <-ctx.Done():
			return
		case <-time.After(interval * time.Duration(slowdown)):
		}
	}
}

// round asks for the list and hands on the events that are not in seen,
// the oldest first. It asks for a long list when long is set, and also
// when a short list holds new entries only: more new ones can then stand
// behind them. It returns whether a long list was handed on, and an
// error when the request failed or an event could not be handed on.
func (p *Poller) round(seen map[string]bool, backfill bool, long bool) (wasLong bool, err error) {
	// A fault in here must not end the program, which would stop the stream
	defer func() {
		if cause := recover(); cause != nil {
			wasLong, err = false, fmt.Errorf("panic: %v", cause)
		}
	}()

	limit := QuickLimit
	if long {
		limit = FullLimit
	}

	events, err := p.Fetch(limit)
	if err != nil {
		return false, err
	}

	if !long && len(events) >= QuickLimit && allNew(events, seen) {
		long = true

		if events, err = p.Fetch(FullLimit); err != nil {
			return false, err
		}
	}

	sort.SliceStable(events, func(i, j int) bool { return events[i].Time < events[j].Time })

	for _, event := range events {
		if event.UID == "" || seen[event.UID] {
			continue
		}

		if err := p.Publish(event, backfill); err != nil {
			// The rest waits too, so that the order is kept
			return false, fmt.Errorf("unable to hand on the event %v: %w", event.Key, err)
		}

		seen[event.UID] = true
		log.Info().Str("kind", event.Key).Bool("backfill", backfill).Msg("Event of the activity list")
	}

	return long, nil
}

// allNew reports whether no event of the list is in seen.
func allNew(events []Event, seen map[string]bool) bool {
	for _, event := range events {
		if seen[event.UID] {
			return false
		}
	}

	return true
}
