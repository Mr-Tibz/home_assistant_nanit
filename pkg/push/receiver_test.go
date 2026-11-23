package push

import (
	"errors"
	"fmt"
	"net"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/indiefan/home_assistant_nanit/pkg/session"
	"github.com/indiefan/home_assistant_nanit/pkg/utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseNotification(t *testing.T) {
	n, err := parseNotification(`{"type":"CAMERA_CRY_DETECTION","baby_uid":"abc123","time":1763856000.5,"id":42,"extra":"kept"}`)

	require.NoError(t, err)
	assert.Equal(t, "CAMERA_CRY_DETECTION", n.Type)
	assert.Equal(t, "cry", n.Kind())
	assert.Equal(t, "abc123", n.BabyUID)
	assert.Equal(t, time.Unix(1763856000, 500000000).UTC(), n.Time)
	assert.Equal(t, "kept", n.Fields["extra"], "everything that the notification carries is passed on")
	assert.Equal(t, float64(42), n.Fields["id"])
}

func TestParseNotificationInsideAnotherObject(t *testing.T) {
	n, err := parseNotification(`{"notification":{"type":"MOTION","baby_uid":"abc123","time":1763856000}}`)

	require.NoError(t, err)
	assert.Equal(t, "MOTION", n.Type)
	assert.Equal(t, "abc123", n.BabyUID)
}

func TestParseNotificationOfAKindWithoutAName(t *testing.T) {
	n, err := parseNotification(`{"type":"SOMETHING_NEW"}`)

	require.NoError(t, err)
	assert.Equal(t, "SOMETHING_NEW", n.Type)
	assert.Equal(t, KindOther, n.Kind())
	assert.True(t, n.Time.IsZero())
	assert.Contains(t, Kinds(), n.Kind())
}

func TestParseNotificationRefusesWhatIsNone(t *testing.T) {
	for _, content := range []string{``, `not JSON`, `[]`, `{}`, `{"type":7}`, `{"baby_uid":"abc123"}`} {
		_, err := parseNotification(content)
		assert.Error(t, err, content)
	}
}

func TestEveryKnownKindIsInKinds(t *testing.T) {
	for _, kind := range kinds {
		assert.Contains(t, Kinds(), kind)
	}
}

// fakeNanit is a stand-in for the Nanit service.
type fakeNanit struct {
	mutex  sync.Mutex
	tokens []string
	err    error
}

func (f *fakeNanit) RegisterPushDevice(token string) (int64, error) {
	f.mutex.Lock()
	defer f.mutex.Unlock()

	if f.err != nil {
		return 0, f.err
	}

	f.tokens = append(f.tokens, token)

	return 77, nil
}

func (f *fakeNanit) calls() []string {
	f.mutex.Lock()
	defer f.mutex.Unlock()

	return append([]string(nil), f.tokens...)
}

func newStore(t *testing.T) *session.Store {
	store := session.NewSessionStore()
	store.Filename = filepath.Join(t.TempDir(), "session.json")

	return store
}

// shortWaits makes the pauses of the receiver short for one test.
func shortWaits(t *testing.T) {
	reconnect, register := reconnectWaits, registerWaits
	reconnectWaits = []time.Duration{time.Millisecond}
	registerWaits = []time.Duration{time.Millisecond}

	t.Cleanup(func() { reconnectWaits, registerWaits = reconnect, register })
}

// services hands out one fake service for each connection of a receiver.
type services struct {
	t    *testing.T
	next chan *fakeService
}

func newServices(t *testing.T) *services {
	return &services{t: t, next: make(chan *fakeService, 16)}
}

func (s *services) client(registration session.Push) *mcsClient {
	return &mcsClient{
		AndroidID:     registration.AndroidID,
		SecurityToken: registration.SecurityToken,
		Dial: func() (net.Conn, error) {
			service := newFakeService(s.t)
			s.next <- service

			return service.client, nil
		},
	}
}

func (s *services) connection() *fakeService {
	select {
	case service := <-s.next:
		return service
	case <-time.After(5 * time.Second):
		s.t.Fatal("the receiver did not connect")
		return nil
	}
}

func receive(t *testing.T, notifications chan Notification) Notification {
	select {
	case n := <-notifications:
		return n
	case <-time.After(5 * time.Second):
		t.Fatal("no notification arrived")
		return Notification{}
	}
}

func TestReceiverRegistersOnceAndDeliversNotifications(t *testing.T) {
	shortWaits(t)

	store := newStore(t)
	nanit := &fakeNanit{}
	fake := newServices(t)
	notifications := make(chan Notification, 8)
	registrations := 0

	now := time.Unix(1763856060, 0)

	receiver := &Receiver{
		Store:          store,
		Nanit:          nanit,
		FirebaseAPIKey: "the-key",
		OnNotification: func(n Notification) { notifications <- n },
		register: func(apiKey string) (session.Push, error) {
			registrations++
			assert.Equal(t, "the-key", apiKey)

			return session.Push{AndroidID: 11, SecurityToken: 22, Token: "token-1"}, nil
		},
		client: fake.client,
		now:    func() time.Time { return now },
	}

	runner := utils.RunWithGracefulCancel(receiver.Run)

	// First connection
	service := fake.connection()
	request := service.accept()

	assert.Equal(t, "11", request.User)
	assert.Equal(t, "22", request.AuthToken)
	assert.Empty(t, request.ReceivedIDs)
	assert.Equal(t, []string{"token-1"}, nanit.calls())

	stored := store.Snapshot().Push
	require.NotNil(t, stored)
	assert.Equal(t, session.Push{AndroidID: 11, SecurityToken: 22, Token: "token-1", Registered: true, DeviceID: 77}, *stored)

	service.send(tagDataMessage, notificationMessage("id-1", `{"type":"SOUND","baby_uid":"abc123","time":1763856000}`))

	n := receive(t, notifications)
	assert.Equal(t, "SOUND", n.Type)
	assert.Equal(t, "abc123", n.BabyUID)

	// The connection is lost. The receiver connects again, and tells the
	// service which message it has.
	service.conn.Close()

	service = fake.connection()
	assert.Equal(t, []string{"id-1"}, service.accept().ReceivedIDs)

	// The service sends the same message again all the same, and a new one
	service.send(tagDataMessage, notificationMessage("id-1", `{"type":"SOUND","baby_uid":"abc123","time":1763856000}`))
	service.send(tagDataMessage, notificationMessage("id-2", `{"type":"MOTION","baby_uid":"abc123","time":1763856030}`))

	assert.Equal(t, "MOTION", receive(t, notifications).Type, "the message that arrived a second time is dropped")

	// After a login that the service accepted, an ID is not told again
	service.conn.Close()

	service = fake.connection()
	assert.Equal(t, []string{"id-2"}, service.accept().ReceivedIDs)

	runner.Cancel()

	assert.Equal(t, 1, registrations)
	assert.Equal(t, []string{"token-1"}, nanit.calls(), "the Nanit service is told the token once")
}

func TestReceiverUsesTheRegistrationOfTheSession(t *testing.T) {
	shortWaits(t)

	store := newStore(t)
	require.NoError(t, store.Update(func(s *session.Session) {
		s.Push = &session.Push{AndroidID: 11, SecurityToken: 22, Token: "token-1", Registered: true, DeviceID: 77}
	}))

	nanit := &fakeNanit{}
	fake := newServices(t)

	receiver := &Receiver{
		Store: store,
		Nanit: nanit,
		register: func(string) (session.Push, error) {
			t.Error("a new device was registered")
			return session.Push{}, errors.New("not expected")
		},
		client: fake.client,
	}

	runner := utils.RunWithGracefulCancel(receiver.Run)

	assert.Equal(t, "11", fake.connection().accept().User)

	runner.Cancel()

	assert.Empty(t, nanit.calls())
}

// The device is made, and then the Nanit service fails. The next try
// keeps the device, and only tells the Nanit service again.
func TestReceiverKeepsTheDeviceWhenTheNanitServiceFails(t *testing.T) {
	shortWaits(t)

	store := newStore(t)
	nanit := &fakeNanit{err: errors.New("HTTP request failed: server responded with status code 503")}
	fake := newServices(t)

	var mutex sync.Mutex
	registrations := 0

	receiver := &Receiver{
		Store: store,
		Nanit: nanit,
		register: func(string) (session.Push, error) {
			mutex.Lock()
			defer mutex.Unlock()

			registrations++

			return session.Push{AndroidID: 11, SecurityToken: 22, Token: fmt.Sprintf("token-%d", registrations)}, nil
		},
		client: fake.client,
	}

	runner := utils.RunWithGracefulCancel(receiver.Run)

	// Let some tries fail
	require.Eventually(t, func() bool {
		stored := store.Snapshot().Push
		return stored != nil && stored.Token == "token-1"
	}, 5*time.Second, time.Millisecond)
	time.Sleep(20 * time.Millisecond)

	stored := store.Snapshot().Push
	assert.False(t, stored.Registered)

	nanit.mutex.Lock()
	nanit.err = nil
	nanit.mutex.Unlock()

	fake.connection().accept()

	runner.Cancel()

	mutex.Lock()
	defer mutex.Unlock()

	assert.Equal(t, 1, registrations)
	assert.Equal(t, []string{"token-1"}, nanit.calls())
	assert.True(t, store.Snapshot().Push.Registered)
}

func TestReceiverStopsWhileItRegisters(t *testing.T) {
	store := newStore(t)
	release := make(chan struct{})
	defer close(release)

	receiver := &Receiver{
		Store: store,
		Nanit: &fakeNanit{},
		register: func(string) (session.Push, error) {
			<-release
			return session.Push{}, errors.New("too late")
		},
	}

	runner := utils.RunWithGracefulCancel(receiver.Run)

	stopped := make(chan struct{})
	go func() {
		runner.Cancel()
		close(stopped)
	}()

	select {
	case <-stopped:
	case <-time.After(5 * time.Second):
		t.Fatal("the stop waited for the registration")
	}
}

func TestReceiverDropsANotificationThatIsTooOld(t *testing.T) {
	shortWaits(t)

	store := newStore(t)
	require.NoError(t, store.Update(func(s *session.Session) {
		s.Push = &session.Push{AndroidID: 11, SecurityToken: 22, Token: "token-1", Registered: true}
	}))

	fake := newServices(t)
	notifications := make(chan Notification, 8)
	now := time.Unix(1763856000, 0)

	receiver := &Receiver{
		Store:          store,
		Nanit:          &fakeNanit{},
		OnNotification: func(n Notification) { notifications <- n },
		client:         fake.client,
		now:            func() time.Time { return now },
	}

	runner := utils.RunWithGracefulCancel(receiver.Run)
	defer runner.Cancel()

	service := fake.connection()
	service.accept()

	old := now.Add(-maxAge - time.Second).Unix()
	recent := now.Add(-maxAge + time.Second).Unix()

	service.send(tagDataMessage, notificationMessage("id-1", fmt.Sprintf(`{"type":"SOUND","baby_uid":"abc123","time":%d}`, old)))
	service.send(tagDataMessage, notificationMessage("id-2", fmt.Sprintf(`{"type":"MOTION","baby_uid":"abc123","time":%d}`, recent)))
	service.send(tagDataMessage, notificationMessage("id-3", `{"type":"TEMPERATURE","baby_uid":"abc123"}`))

	assert.Equal(t, "MOTION", receive(t, notifications).Type)
	assert.Equal(t, "TEMPERATURE", receive(t, notifications).Type, "a notification without a time is passed on")
}

// A fault in what takes the notifications must not end the program, and
// the receiver keeps going.
func TestReceiverSurvivesAPanicOfItsHandler(t *testing.T) {
	shortWaits(t)

	store := newStore(t)
	require.NoError(t, store.Update(func(s *session.Session) {
		s.Push = &session.Push{AndroidID: 11, SecurityToken: 22, Token: "token-1", Registered: true}
	}))

	fake := newServices(t)
	notifications := make(chan Notification, 8)

	receiver := &Receiver{
		Store: store,
		Nanit: &fakeNanit{},
		OnNotification: func(n Notification) {
			if n.Type == "SOUND" {
				panic("a fault in the handler")
			}
			notifications <- n
		},
		client: fake.client,
	}

	runner := utils.RunWithGracefulCancel(receiver.Run)
	defer runner.Cancel()

	service := fake.connection()
	service.accept()
	service.send(tagDataMessage, notificationMessage("id-1", `{"type":"SOUND","baby_uid":"abc123"}`))

	// The connection is given up, and a new one is made
	service = fake.connection()
	service.accept()
	service.send(tagDataMessage, notificationMessage("id-2", `{"type":"MOTION","baby_uid":"abc123"}`))

	assert.Equal(t, "MOTION", receive(t, notifications).Type)
}

func TestReceiverIgnoresAMessageWithoutANotification(t *testing.T) {
	shortWaits(t)

	store := newStore(t)
	require.NoError(t, store.Update(func(s *session.Session) {
		s.Push = &session.Push{AndroidID: 11, SecurityToken: 22, Token: "token-1", Registered: true}
	}))

	fake := newServices(t)
	notifications := make(chan Notification, 8)

	receiver := &Receiver{
		Store:          store,
		Nanit:          &fakeNanit{},
		OnNotification: func(n Notification) { notifications <- n },
		client:         fake.client,
	}

	runner := utils.RunWithGracefulCancel(receiver.Run)
	defer runner.Cancel()

	service := fake.connection()
	service.accept()

	service.send(tagDataMessage, encodeDataMessage("id-1", "something-else", "ignored"))
	service.send(tagDataMessage, encodeDataMessage("", "notification", `{"type":"SOUND","baby_uid":"abc123"}`))
	service.send(tagDataMessage, notificationMessage("id-2", `not JSON`))
	service.send(tagDataMessage, notificationMessage("id-3", `{"type":"HUMIDITY","baby_uid":"abc123"}`))

	assert.Equal(t, "SOUND", receive(t, notifications).Type, "a message without an ID is passed on")
	assert.Equal(t, "HUMIDITY", receive(t, notifications).Type)
}

func TestPause(t *testing.T) {
	waits := []time.Duration{1, 2, 3}

	assert.Equal(t, time.Duration(1), pause(waits, 0))
	assert.Equal(t, time.Duration(3), pause(waits, 2))
	assert.Equal(t, time.Duration(3), pause(waits, 9), "the last pause repeats")
}
