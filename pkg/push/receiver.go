// Package push receives the push notifications that the Nanit service
// sends to its mobile apps: motion, sound, a crying baby, temperature and
// humidity out of range, and whatever else the account is set to get.
//
// The Nanit service does not send them itself. It hands them to the
// Firebase service of Google, which delivers them to the devices that an
// app has registered. So this program registers as one more device with
// the Nanit app for Android, tells the Nanit service the address of that
// device, and keeps a connection to the Firebase service open.
package push

import (
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/indiefan/home_assistant_nanit/pkg/session"
	"github.com/indiefan/home_assistant_nanit/pkg/utils"
	"github.com/rs/zerolog/log"
)

// Notification - one push notification of the Nanit service
type Notification struct {
	// Type - the kind, as the service names it, for example SOUND
	Type string

	// BabyUID - the baby that the notification is about. Empty when the
	// notification names none.
	BabyUID string

	// Time - when the service made the notification. Zero when the
	// notification carries no time.
	Time time.Time

	// Fields - everything that the notification carries, as it arrived
	Fields map[string]interface{}
}

// kinds - the kinds of notifications that are known by name, and the name
// that Kind gives them
var kinds = map[string]string{
	"SOUND":                "sound",
	"MOTION":               "motion",
	"CAMERA_CRY_DETECTION": "cry",
	"TEMPERATURE":          "temperature",
	"HUMIDITY":             "humidity",
}

// KindOther - what Kind returns for a kind that is not known by name
const KindOther = "other"

// Kinds returns every value that Kind can return.
func Kinds() []string {
	return []string{"sound", "motion", "cry", "temperature", "humidity", KindOther}
}

// Kind returns a short name for the kind of the notification, from a
// fixed list. Type holds the name of the service for every kind.
func (n Notification) Kind() string {
	if kind, found := kinds[n.Type]; found {
		return kind
	}

	return KindOther
}

// Registrar tells the Nanit service where to send the notifications.
type Registrar interface {
	// RegisterPushDevice gives the service the token of a device and
	// returns the number that the service gave the registration
	RegisterPushDevice(token string) (int64, error)
}

const (
	// maxAge - a notification that is older than this is dropped. After
	// a time without a connection, the Firebase service delivers what it
	// has kept. Something that reacts to a notification should not do so
	// for a cry of an hour ago.
	maxAge = 5 * time.Minute

	// rememberedIDs - number of messages that are remembered, to drop
	// one that arrives a second time
	rememberedIDs = 500
)

// reconnectWaits - pauses between two connections to the Firebase service
// that ended early. The last one repeats.
var reconnectWaits = []time.Duration{5 * time.Second, 15 * time.Second, time.Minute, 5 * time.Minute}

// registerWaits - pauses between two tries of the registration. The last
// one repeats. A registration makes a new device at two services, so it
// is not tried often.
var registerWaits = []time.Duration{time.Minute, 5 * time.Minute, 15 * time.Minute, time.Hour}

// stableAfter - a connection that held this long was a good one, and the
// next pause starts short again
const stableAfter = time.Minute

// Receiver gets the push notifications of one Nanit account.
type Receiver struct {
	// Store keeps the registration, so that a new start connects as the
	// same device
	Store *session.Store

	// Nanit - the service that is told where to send the notifications
	Nanit Registrar

	// FirebaseAPIKey - the API key of the Nanit app for Android
	FirebaseAPIKey string

	// OnNotification gets every notification that is new and not too
	// old, from one goroutine
	OnNotification func(Notification)

	// For the tests: stand-ins for the Firebase service
	register func(apiKey string) (session.Push, error)
	client   func(registration session.Push) *mcsClient
	now      func() time.Time

	mutex sync.Mutex

	// unconfirmed - IDs of messages that have arrived and that the
	// Firebase service has not been told about yet. The next login
	// tells them.
	unconfirmed []string

	// seen - IDs of the last messages, in the order of arrival
	seen []string
}

// Run registers when the session holds no registration, then keeps a
// connection to the Firebase service until ctx is done. It never ends the
// program: a failure is logged and tried again later.
func (r *Receiver) Run(ctx utils.GracefulContext) {
	registration, ok := r.registration(ctx.Done())
	if !ok {
		return
	}

	failures := 0

	for {
		started := r.time()
		err := r.listen(ctx.Done(), registration)

		select {
		case <-ctx.Done():
			return
		default:
		}

		if r.time().Sub(started) >= stableAfter {
			failures = 0
		}

		wait := pause(reconnectWaits, failures)
		failures++

		event := log.Warn()
		if errors.Is(err, errLoginRefused) {
			// Trying again does not help here. A new registration might,
			// but it would make one more device at the Nanit service
			// each time, so it is left to the person who runs this.
			event = log.Error().Str("hint", "to register anew, stop the program and remove \"push\" from the session file")
		}
		event.Err(err).Str("retry_in", wait.String()).Msg("The connection for push notifications ended")

		select {
		case <-ctx.Done():
			return
		case <-time.After(wait):
		}
	}
}

func pause(waits []time.Duration, failures int) time.Duration {
	if failures >= len(waits) {
		return waits[len(waits)-1]
	}

	return waits[failures]
}

func (r *Receiver) time() time.Time {
	if r.now != nil {
		return r.now()
	}

	return time.Now()
}

// registration returns the registration of the session. It makes what is
// missing of it first, and tries again after a failure. It returns false
// when done is closed before there is a registration.
func (r *Receiver) registration(done <-chan struct{}) (session.Push, bool) {
	for failures := 0; ; failures++ {
		// The requests cannot be cancelled and can take a minute, so a
		// stop of the program does not wait for them. What a try has
		// reached is stored in the session by the try itself.
		result := make(chan error, 1)
		go func() {
			defer func() {
				if cause := recover(); cause != nil {
					result <- fmt.Errorf("panic: %v", cause)
				}
			}()

			result <- r.completeRegistration()
		}()

		select {
		case <-done:
			return session.Push{}, false
		case err := <-result:
			if err == nil {
				return *r.Store.Snapshot().Push, true
			}

			wait := pause(registerWaits, failures)
			log.Error().Err(err).Str("retry_in", wait.String()).Msg("Unable to register for push notifications")

			select {
			case <-done:
				return session.Push{}, false
			case <-time.After(wait):
			}
		}
	}
}

// completeRegistration makes the parts of the registration that the
// session lacks: the device at the Firebase service, and the entry at the
// Nanit service.
func (r *Receiver) completeRegistration() error {
	registration := r.Store.Snapshot().Push

	if registration == nil || registration.Token == "" {
		register := r.register
		if register == nil {
			register = registerWithFirebase
		}

		log.Info().Msg("Registering a device for push notifications at the Firebase service")

		created, err := register(r.FirebaseAPIKey)
		if err != nil {
			return fmt.Errorf("Firebase service: %w", err)
		}

		registration = &created
		r.Store.Update(func(s *session.Session) { s.Push = &created })
	}

	if !registration.Registered {
		deviceID, err := r.Nanit.RegisterPushDevice(registration.Token)
		if err != nil {
			return fmt.Errorf("Nanit service: %w", err)
		}

		registered := *registration
		registered.Registered = true
		registered.DeviceID = deviceID
		r.Store.Update(func(s *session.Session) { s.Push = &registered })

		log.Info().Int64("device_id", deviceID).Msg("The Nanit service will send its push notifications to this program")
	}

	return nil
}

// listen holds one connection to the Firebase service, until it ends or
// done is closed. A panic in the handling of a message ends the
// connection, and not the program.
func (r *Receiver) listen(done <-chan struct{}, registration session.Push) (err error) {
	defer func() {
		if cause := recover(); cause != nil {
			err = fmt.Errorf("panic: %v", cause)
		}
	}()

	var client *mcsClient
	if r.client != nil {
		client = r.client(registration)
	} else {
		client = &mcsClient{AndroidID: registration.AndroidID, SecurityToken: registration.SecurityToken}
	}

	r.mutex.Lock()
	told := append([]string(nil), r.unconfirmed...)
	r.mutex.Unlock()

	client.OnConnected = func() {
		// The service has the IDs now
		r.mutex.Lock()
		if len(told) <= len(r.unconfirmed) {
			r.unconfirmed = r.unconfirmed[len(told):]
		}
		r.mutex.Unlock()

		log.Info().Msg("Connected to the Firebase service, waiting for push notifications")
	}
	client.OnMessage = r.handleMessage

	return client.Listen(done, told)
}

// handleMessage takes one message of the Firebase service.
func (r *Receiver) handleMessage(message dataMessage) {
	if id := message.PersistentID; id != "" && !r.remember(id) {
		log.Debug().Str("id", id).Msg("Push message arrived a second time, dropped")
		return
	}

	content, found := "", false
	keys := make([]string, 0, len(message.Data))

	for _, data := range message.Data {
		keys = append(keys, data.Key)
		log.Debug().Str("key", data.Key).Str("value", data.Value).Msg("Push message data")

		if data.Key == "notification" {
			content, found = data.Value, true
		}
	}

	if !found {
		log.Warn().Strs("keys", keys).Str("from", message.From).Msg("Push message without a notification, ignored")
		return
	}

	notification, err := parseNotification(content)
	if err != nil {
		log.Warn().Err(err).Str("content", content).Msg("Push notification that cannot be read, ignored")
		return
	}

	if age := r.time().Sub(notification.Time); !notification.Time.IsZero() && age > maxAge {
		log.Debug().Str("type", notification.Type).Str("age", age.Round(time.Second).String()).Msg("Push notification is too old, dropped")
		return
	}

	if r.OnNotification != nil {
		r.OnNotification(notification)
	}
}

// remember records the ID of a message. It returns false when the message
// has arrived before.
func (r *Receiver) remember(id string) bool {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	for _, known := range r.seen {
		if known == id {
			return false
		}
	}

	r.seen = append(r.seen, id)
	if len(r.seen) > rememberedIDs {
		r.seen = r.seen[len(r.seen)-rememberedIDs:]
	}

	// Without a login for a long time this list would grow without end.
	// An ID that is left out is of a message that the service may send
	// once more, and by then it is too old to count.
	r.unconfirmed = append(r.unconfirmed, id)
	if len(r.unconfirmed) > rememberedIDs {
		r.unconfirmed = r.unconfirmed[len(r.unconfirmed)-rememberedIDs:]
	}

	return true
}

// parseNotification reads the content of a push message of the Nanit
// service: a JSON object with the kind, the baby and the time, like
//
//	{"type":"SOUND","baby_uid":"abc123","time":1763856000,"id":42}
func parseNotification(content string) (Notification, error) {
	var fields map[string]interface{}
	if err := json.Unmarshal([]byte(content), &fields); err != nil {
		return Notification{}, err
	}

	// Some messages carry the object inside another one
	if _, found := fields["type"]; !found {
		if inner, ok := fields["notification"].(map[string]interface{}); ok {
			fields = inner
		}
	}

	notification := Notification{Fields: fields}
	notification.Type, _ = fields["type"].(string)
	notification.BabyUID, _ = fields["baby_uid"].(string)

	if notification.Type == "" {
		return Notification{}, errors.New("it names no type")
	}

	if seconds, ok := fields["time"].(float64); ok && seconds > 0 {
		notification.Time = time.Unix(0, int64(seconds*float64(time.Second))).UTC()
	}

	return notification, nil
}
