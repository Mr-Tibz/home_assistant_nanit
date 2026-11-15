package client

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/indiefan/home_assistant_nanit/pkg/session"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fakeAPI is a stand-in for the Nanit API.
type fakeAPI struct {
	server *httptest.Server

	refreshStatus int
	refreshBody   string
	refreshCalls  int32
	lastRefresh   atomic.Value // refresh token of the last request

	loginStatus int
	loginBody   string
	loginCalls  int32
}

func newFakeAPI(t *testing.T) *fakeAPI {
	api := &fakeAPI{
		refreshStatus: http.StatusOK,
		refreshBody:   `{"access_token":"access-2","refresh_token":"refresh-2"}`,
		loginStatus:   http.StatusCreated,
		loginBody:     `{"access_token":"access-login","refresh_token":"refresh-login"}`,
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/tokens/refresh", func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&api.refreshCalls, 1)

		var body map[string]string
		json.NewDecoder(r.Body).Decode(&body)
		api.lastRefresh.Store(body["refresh_token"])

		w.WriteHeader(api.refreshStatus)
		w.Write([]byte(api.refreshBody))
	})
	mux.HandleFunc("/login", func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&api.loginCalls, 1)
		w.WriteHeader(api.loginStatus)
		w.Write([]byte(api.loginBody))
	})

	api.server = httptest.NewServer(mux)
	t.Cleanup(api.server.Close)

	return api
}

// newTestClient returns a client that talks to api and keeps its session
// in a file of its own.
func newTestClient(t *testing.T, api *fakeAPI) *NanitClient {
	store := session.NewSessionStore()
	store.Filename = filepath.Join(t.TempDir(), "session.json")
	store.Session.AuthToken = "access-1"
	store.Session.RefreshToken = "refresh-1"
	store.Session.AuthTime = time.Now().Add(-2 * AuthTokenTimelife)

	return &NanitClient{SessionStore: store, BaseURL: api.server.URL}
}

func storedSession(t *testing.T, c *NanitClient) session.Session {
	data, err := os.ReadFile(c.SessionStore.Filename)
	require.NoError(t, err)

	var stored session.Session
	require.NoError(t, json.Unmarshal(data, &stored))

	return stored
}

func TestRenewSessionStoresTheNewTokens(t *testing.T) {
	api := newFakeAPI(t)
	c := newTestClient(t, api)

	require.NoError(t, c.RenewSession())

	assert.Equal(t, "refresh-1", api.lastRefresh.Load(), "the request carries the current refresh token")
	assert.Equal(t, "access-2", c.SessionStore.Session.AuthToken)
	assert.Equal(t, "refresh-2", c.SessionStore.Session.RefreshToken)
	assert.WithinDuration(t, time.Now(), c.SessionStore.Session.AuthTime, time.Minute)

	stored := storedSession(t, c)
	assert.Equal(t, "access-2", stored.AuthToken)
	assert.Equal(t, "refresh-2", stored.RefreshToken)
}

// A failure that can pass by itself must come back as a temporary error.
// The program used to end on every one of these.
func TestRenewSessionReportsTemporaryFailures(t *testing.T) {
	for name, breakIt := range map[string]func(api *fakeAPI){
		"server unreachable":   func(api *fakeAPI) { api.server.Close() },
		"server error":         func(api *fakeAPI) { api.refreshStatus = http.StatusBadGateway },
		"too many requests":    func(api *fakeAPI) { api.refreshStatus = http.StatusTooManyRequests },
		"unreadable answer":    func(api *fakeAPI) { api.refreshBody = `{"access_token":` },
		"answer without token": func(api *fakeAPI) { api.refreshBody = `{}` },
	} {
		t.Run(name, func(t *testing.T) {
			api := newFakeAPI(t)
			c := newTestClient(t, api)
			breakIt(api)

			err := c.RenewSession()

			require.Error(t, err)
			assert.True(t, IsTemporary(err), "expected a temporary error, got: %v", err)
			assert.Equal(t, "access-1", c.SessionStore.Session.AuthToken, "the session keeps its tokens")
			assert.Equal(t, "refresh-1", c.SessionStore.Session.RefreshToken, "the session keeps its tokens")
		})
	}
}

func TestRenewSessionReportsARefusalAsPermanent(t *testing.T) {
	api := newFakeAPI(t)
	c := newTestClient(t, api)
	api.refreshStatus = http.StatusBadRequest

	err := c.RenewSession()

	require.Error(t, err)
	assert.False(t, IsTemporary(err))
	assert.False(t, errors.Is(err, ErrExpiredRefreshToken))
}

func TestAuthorizeLogsInWhenTheRefreshTokenHasExpired(t *testing.T) {
	api := newFakeAPI(t)
	c := newTestClient(t, api)
	api.refreshStatus = http.StatusNotFound

	require.NoError(t, c.Authorize())

	assert.EqualValues(t, 1, atomic.LoadInt32(&api.loginCalls))
	assert.Equal(t, "access-login", c.SessionStore.Session.AuthToken)
	assert.Equal(t, "refresh-login", c.SessionStore.Session.RefreshToken)
}

// A temporary failure of the renewal must not fall through to a login
// with e-mail address and password.
func TestAuthorizeDoesNotLogInAfterATemporaryFailure(t *testing.T) {
	api := newFakeAPI(t)
	c := newTestClient(t, api)
	api.refreshStatus = http.StatusServiceUnavailable

	err := c.Authorize()

	require.Error(t, err)
	assert.True(t, IsTemporary(err))
	assert.EqualValues(t, 0, atomic.LoadInt32(&api.loginCalls))
}

func TestLoginReportsRefusedCredentialsAsPermanent(t *testing.T) {
	api := newFakeAPI(t)
	c := newTestClient(t, api)
	api.loginStatus = http.StatusUnauthorized

	err := c.Login()

	require.Error(t, err)
	assert.False(t, IsTemporary(err))
}

func TestMaybeAuthorizeKeepsAFreshToken(t *testing.T) {
	api := newFakeAPI(t)
	c := newTestClient(t, api)
	c.SessionStore.Session.AuthTime = time.Now()

	require.NoError(t, c.MaybeAuthorize(false))
	assert.EqualValues(t, 0, atomic.LoadInt32(&api.refreshCalls), "a fresh token needs no request")

	require.NoError(t, c.MaybeAuthorize(true))
	assert.EqualValues(t, 1, atomic.LoadInt32(&api.refreshCalls), "force renews also a fresh token")
}

// scriptedAuthorizer returns one prepared result per call.
type scriptedAuthorizer struct {
	results []error
	calls   int
}

func (a *scriptedAuthorizer) MaybeAuthorize(force bool) error {
	result := a.results[a.calls]
	a.calls++
	return result
}

func TestAuthorizeWithRetryTriesAgainAfterTemporaryFailures(t *testing.T) {
	a := &scriptedAuthorizer{results: []error{temporary("no network"), temporary("no network"), nil}}

	err := AuthorizeWithRetry(a, false, make(chan struct{}), []time.Duration{time.Millisecond})

	assert.NoError(t, err)
	assert.Equal(t, 3, a.calls)
}

func TestAuthorizeWithRetryStopsAtAPermanentFailure(t *testing.T) {
	refused := errors.New("credentials refused")
	a := &scriptedAuthorizer{results: []error{temporary("no network"), refused, nil}}

	err := AuthorizeWithRetry(a, false, make(chan struct{}), []time.Duration{time.Millisecond})

	assert.Same(t, refused, err)
	assert.Equal(t, 2, a.calls)
}

func TestAuthorizeWithRetryStopsWhenDone(t *testing.T) {
	a := &scriptedAuthorizer{results: []error{temporary("no network"), nil}}
	done := make(chan struct{})
	close(done)

	err := AuthorizeWithRetry(a, false, done, []time.Duration{time.Hour})

	require.Error(t, err)
	assert.True(t, IsTemporary(err))
	assert.Equal(t, 1, a.calls)
}
