package push

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/indiefan/home_assistant_nanit/pkg/session"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protowire"
)

// fakeFirebase is a stand-in for the three addresses of the Firebase
// service that a registration uses.
type fakeFirebase struct {
	server *httptest.Server

	mutex sync.Mutex

	installStatus int
	installBody   string
	installHeader http.Header
	installed     map[string]string

	checkinBody   []byte
	checkinHeader http.Header
	checkinSent   []byte

	// tokenAnswers - the answer to each request for a token, in order.
	// The last one repeats.
	tokenAnswers []string
	tokenCalls   int
	tokenHeader  http.Header
	tokenForm    url.Values
}

func newFakeFirebase(t *testing.T) *fakeFirebase {
	f := &fakeFirebase{
		installStatus: http.StatusOK,
		installBody:   `{"name":"projects/1/installations/x","fid":"x","refreshToken":"r","authToken":{"token":"installation-token","expiresIn":"604800s"}}`,
		checkinBody:   appendFixed64(appendFixed64(appendNumber(nil, 1, 1), 7, 1234567890123456789), 8, 987654321),
		tokenAnswers:  []string{"token=fcm-token:APA91b-abc_def\n"},
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/installations", func(w http.ResponseWriter, r *http.Request) {
		f.mutex.Lock()
		defer f.mutex.Unlock()

		f.installHeader = r.Header.Clone()
		json.NewDecoder(r.Body).Decode(&f.installed)

		w.WriteHeader(f.installStatus)
		w.Write([]byte(f.installBody))
	})
	mux.HandleFunc("/checkin", func(w http.ResponseWriter, r *http.Request) {
		f.mutex.Lock()
		defer f.mutex.Unlock()

		f.checkinHeader = r.Header.Clone()
		f.checkinSent, _ = io.ReadAll(r.Body)

		w.Write(f.checkinBody)
	})
	mux.HandleFunc("/register3", func(w http.ResponseWriter, r *http.Request) {
		f.mutex.Lock()
		defer f.mutex.Unlock()

		f.tokenHeader = r.Header.Clone()
		r.ParseForm()
		f.tokenForm = r.PostForm

		answer := f.tokenAnswers[len(f.tokenAnswers)-1]
		if f.tokenCalls < len(f.tokenAnswers) {
			answer = f.tokenAnswers[f.tokenCalls]
		}
		f.tokenCalls++

		w.Write([]byte(answer))
	})

	f.server = httptest.NewServer(mux)
	t.Cleanup(f.server.Close)

	return f
}

func (f *fakeFirebase) client() *firebase {
	return &firebase{
		APIKey:           "the-key",
		InstallationsURL: f.server.URL + "/installations",
		CheckinURL:       f.server.URL + "/checkin",
		RegisterURL:      f.server.URL + "/register3",
		pause:            time.Millisecond,
	}
}

func TestRegister(t *testing.T) {
	fake := newFakeFirebase(t)

	registration, err := fake.client().Register()

	require.NoError(t, err)
	assert.Equal(t, session.Push{AndroidID: 1234567890123456789, SecurityToken: 987654321, Token: "fcm-token:APA91b-abc_def"}, registration)

	// The installation names the app and carries the key
	assert.Equal(t, "the-key", fake.installHeader.Get("x-goog-api-key"))
	assert.Equal(t, "com.nanit.baby", fake.installHeader.Get("X-Android-Package"))
	assert.Equal(t, androidCertSHA1, fake.installHeader.Get("X-Android-Cert"))
	assert.Equal(t, firebaseAppID, fake.installed["appId"])
	assert.Equal(t, "FIS_v2", fake.installed["authVersion"])
	assert.Len(t, fake.installed["fid"], 22)

	// The check-in asks for a new device
	var id, token, version uint64 = 99, 99, 0
	var checkin []byte
	require.NoError(t, eachField(fake.checkinSent, func(field protowire.Number, data []byte, number uint64) error {
		switch field {
		case 2:
			id = number
		case 4:
			checkin = data
		case 13:
			token = number
		case 14:
			version = number
		}
		return nil
	}))
	assert.Zero(t, id)
	assert.Zero(t, token)
	assert.Equal(t, uint64(3), version)
	assert.NotEmpty(t, checkin)
	assert.Equal(t, "application/x-protobuf", fake.checkinHeader.Get("Content-Type"))
	assert.Empty(t, fake.checkinHeader.Get("User-Agent"))

	// The token is asked for as that device, with the proof of the installation
	assert.Equal(t, "AidLogin 1234567890123456789:987654321", fake.tokenHeader.Get("Authorization"))
	assert.Equal(t, "1234567890123456789", fake.tokenForm.Get("device"))
	assert.Equal(t, "installation-token", fake.tokenForm.Get("X-Goog-Firebase-Installations-Auth"))
	assert.Equal(t, firebaseSenderID, fake.tokenForm.Get("sender"))
	assert.Equal(t, firebaseSenderID, fake.tokenForm.Get("X-subtype"))
	assert.Equal(t, "com.nanit.baby", fake.tokenForm.Get("app"))
	assert.Equal(t, firebaseAppID, fake.tokenForm.Get("X-gms_app_id"))
}

func TestRegisterTriesTheTokenAgain(t *testing.T) {
	fake := newFakeFirebase(t)
	fake.tokenAnswers = []string{"Error=PHONE_REGISTRATION_ERROR", "Error=PHONE_REGISTRATION_ERROR", "token=fcm-token"}

	registration, err := fake.client().Register()

	require.NoError(t, err)
	assert.Equal(t, "fcm-token", registration.Token)
	assert.Equal(t, 3, fake.tokenCalls)
}

func TestRegisterGivesUpATokenThatIsRefused(t *testing.T) {
	fake := newFakeFirebase(t)
	fake.tokenAnswers = []string{"Error=PHONE_REGISTRATION_ERROR"}

	_, err := fake.client().Register()

	require.Error(t, err)
	assert.Contains(t, err.Error(), "PHONE_REGISTRATION_ERROR")
	assert.Equal(t, registerTries, fake.tokenCalls)
}

func TestRegisterReportsFailures(t *testing.T) {
	for name, tc := range map[string]struct {
		change func(f *fakeFirebase)
		want   string
	}{
		"a key that the service refuses": {
			func(f *fakeFirebase) {
				f.installStatus = http.StatusBadRequest
				f.installBody = `{"error":{"code":400,"message":"API key not valid. Please pass a valid API key."}}`
			},
			"API key not valid",
		},
		"an installation without a token": {func(f *fakeFirebase) { f.installBody = `{}` }, "installation: the answer holds no token"},
		"an installation that is no JSON": {func(f *fakeFirebase) { f.installBody = `<html>` }, "installation:"},
		"a check-in without a device":     {func(f *fakeFirebase) { f.checkinBody = appendNumber(nil, 1, 1) }, "check-in: the answer names no device"},
		"a check-in that cannot be read":  {func(f *fakeFirebase) { f.checkinBody = []byte{0xff, 0xff} }, "check-in:"},
		"an answer without a token":       {func(f *fakeFirebase) { f.tokenAnswers = []string{"something=else"} }, "token: the answer holds no token"},
	} {
		t.Run(name, func(t *testing.T) {
			fake := newFakeFirebase(t)
			tc.change(fake)

			_, err := fake.client().Register()

			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.want)
		})
	}
}

func TestNewInstallationID(t *testing.T) {
	seen := map[string]bool{}

	for i := 0; i < 200; i++ {
		id, err := newInstallationID()

		require.NoError(t, err)
		require.Len(t, id, 22)
		// The first four bits are 0111, which gives one of these characters
		assert.True(t, strings.ContainsRune("cdef", rune(id[0])), id)
		assert.False(t, strings.ContainsAny(id, "+/="), id)
		assert.False(t, seen[id])
		seen[id] = true
	}
}
