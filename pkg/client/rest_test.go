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

	// babiesStatuses - status of each request for the baby list, in
	// order. The last entry repeats.
	babiesStatuses []int
	babiesCalls    int32
	babiesTokens   chan string // Authorization header of each request
}

func newFakeAPI(t *testing.T) *fakeAPI {
	api := &fakeAPI{
		refreshStatus: http.StatusOK,
		refreshBody:   `{"access_token":"access-2","refresh_token":"refresh-2"}`,
		loginStatus:   http.StatusCreated,
		loginBody:     `{"access_token":"access-login","refresh_token":"refresh-login"}`,

		babiesStatuses: []int{http.StatusOK},
		babiesTokens:   make(chan string, 16),
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

	mux.HandleFunc("/babies", func(w http.ResponseWriter, r *http.Request) {
		call := int(atomic.AddInt32(&api.babiesCalls, 1)) - 1
		api.babiesTokens <- r.Header.Get("Authorization")

		status := api.babiesStatuses[len(api.babiesStatuses)-1]
		if call < len(api.babiesStatuses) {
			status = api.babiesStatuses[call]
		}

		w.WriteHeader(status)
		if status == http.StatusOK {
			w.Write([]byte(`{"babies":[{"uid":"baby-1","name":"Baby","camera_uid":"camera-1"}]}`))
		}
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
		"server unreachable": func(api *fakeAPI) { api.server.Close() },
		"server error":       func(api *fakeAPI) { api.refreshStatus = http.StatusBadGateway },
		"too many requests":  func(api *fakeAPI) { api.refreshStatus = http.StatusTooManyRequests },
		"unreadable answer":  func(api *fakeAPI) { api.refreshBody = `{"access_token":` },
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

// An answer that reports success but carries no access token is not
// tried again: that would send the credentials again and again. A new
// refresh token in it is kept, because the server may already have retired
// the old one.
func TestAuthResponseWithoutAccessToken(t *testing.T) {
	api := newFakeAPI(t)
	c := newTestClient(t, api)
	api.refreshBody = `{"refresh_token":"refresh-2"}`

	err := c.RenewSession()

	require.Error(t, err)
	assert.False(t, IsTemporary(err))
	assert.Equal(t, "access-1", c.SessionStore.Session.AuthToken, "the old access token stays")
	assert.Equal(t, "refresh-2", c.SessionStore.Session.RefreshToken, "the new refresh token is kept")
	assert.Equal(t, "refresh-2", storedSession(t, c).RefreshToken, "the new refresh token is saved")
}

func TestFetchBabies(t *testing.T) {
	api := newFakeAPI(t)
	c := newTestClient(t, api)

	babies, err := c.FetchBabies()

	require.NoError(t, err)
	require.Len(t, babies, 1)
	assert.Equal(t, "baby-1", babies[0].UID)
	assert.Equal(t, "access-1", <-api.babiesTokens)
	assert.Len(t, storedSession(t, c).Babies, 1, "the baby list is saved in the session")
}

// A 401 answer means the token has expired: renew it and ask once more.
func TestFetchAuthorizedRenewsAnExpiredToken(t *testing.T) {
	api := newFakeAPI(t)
	c := newTestClient(t, api)
	api.babiesStatuses = []int{http.StatusUnauthorized, http.StatusOK}

	_, err := c.FetchBabies()

	require.NoError(t, err)
	assert.Equal(t, "access-1", <-api.babiesTokens)
	assert.Equal(t, "access-2", <-api.babiesTokens, "the second request carries the renewed token")
	assert.EqualValues(t, 1, atomic.LoadInt32(&api.refreshCalls))
}

func TestFetchAuthorizedGivesUpAfterTwoRefusals(t *testing.T) {
	api := newFakeAPI(t)
	c := newTestClient(t, api)
	api.babiesStatuses = []int{http.StatusUnauthorized}

	_, err := c.FetchBabies()

	require.Error(t, err)
	assert.False(t, IsTemporary(err))
	assert.EqualValues(t, 2, atomic.LoadInt32(&api.babiesCalls))
}

// The program used to end on every failed request for the baby list or
// for the messages.
func TestFetchAuthorizedReportsTemporaryFailures(t *testing.T) {
	for name, breakIt := range map[string]func(api *fakeAPI){
		"server unreachable": func(api *fakeAPI) { api.server.Close() },
		"server error":       func(api *fakeAPI) { api.babiesStatuses = []int{http.StatusBadGateway} },
		"renewal fails": func(api *fakeAPI) {
			api.babiesStatuses = []int{http.StatusUnauthorized}
			api.refreshStatus = http.StatusBadGateway
		},
	} {
		t.Run(name, func(t *testing.T) {
			api := newFakeAPI(t)
			c := newTestClient(t, api)
			breakIt(api)

			_, err := c.FetchBabies()

			require.Error(t, err)
			assert.True(t, IsTemporary(err), "expected a temporary error, got: %v", err)
		})
	}
}

func TestFetchAuthorizedReportsARefusalAsPermanent(t *testing.T) {
	api := newFakeAPI(t)
	c := newTestClient(t, api)
	api.babiesStatuses = []int{http.StatusForbidden}

	_, err := c.FetchBabies()

	require.Error(t, err)
	assert.False(t, IsTemporary(err))
}

func TestEnsureBabiesUsesTheSession(t *testing.T) {
	api := newFakeAPI(t)
	c := newTestClient(t, api)

	first, err := c.EnsureBabies()
	require.NoError(t, err)

	second, err := c.EnsureBabies()
	require.NoError(t, err)

	assert.Equal(t, first, second)
	assert.EqualValues(t, 1, atomic.LoadInt32(&api.babiesCalls), "the list is fetched once")
}

// scripted returns a function that gives one prepared result per call.
func scripted(calls *int, results ...error) func() error {
	return func() error {
		result := results[*calls]
		*calls++
		return result
	}
}

func TestRetryTemporaryTriesAgainAfterTemporaryFailures(t *testing.T) {
	calls := 0
	fn := scripted(&calls, temporary("no network"), temporary("no network"), nil)

	err := RetryTemporary(make(chan struct{}), []time.Duration{time.Millisecond}, "test", fn)

	assert.NoError(t, err)
	assert.Equal(t, 3, calls)
}

func TestRetryTemporaryStopsAtAPermanentFailure(t *testing.T) {
	calls := 0
	refused := errors.New("credentials refused")
	fn := scripted(&calls, temporary("no network"), refused, nil)

	err := RetryTemporary(make(chan struct{}), []time.Duration{time.Millisecond}, "test", fn)

	assert.Same(t, refused, err)
	assert.Equal(t, 2, calls)
}

func TestRetryTemporaryStopsWhileWaiting(t *testing.T) {
	failed := make(chan struct{})
	fn := func() error {
		close(failed)
		return temporary("no network")
	}

	done := make(chan struct{})
	result := make(chan error, 1)
	go func() { result <- RetryTemporary(done, []time.Duration{time.Hour}, "test", fn) }()

	<-failed
	close(done)

	select {
	case err := <-result:
		assert.True(t, errors.Is(err, ErrRetryCancelled), "got: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("RetryTemporary kept waiting after done had closed")
	}
}

// A call that hangs, like a request to a network that drops packets, must
// not hold up a shutdown.
func TestRetryTemporaryStopsDuringACall(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	defer close(release)

	fn := func() error {
		close(started)
		<-release
		return nil
	}

	done := make(chan struct{})
	result := make(chan error, 1)
	go func() { result <- RetryTemporary(done, nil, "test", fn) }()

	<-started
	close(done)

	select {
	case err := <-result:
		assert.True(t, errors.Is(err, ErrRetryCancelled), "got: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("RetryTemporary waited for the hanging call")
	}
}

// Without a list of waits there is a default pause, not a panic.
func TestRetryTemporaryWithoutWaits(t *testing.T) {
	failed := make(chan struct{})
	fn := func() error {
		close(failed)
		return temporary("no network")
	}

	done := make(chan struct{})
	result := make(chan error, 1)
	go func() { result <- RetryTemporary(done, nil, "test", fn) }()

	// The call has failed and the helper is in its default pause, or
	// about to start it. Ending it here keeps the test short.
	<-failed
	close(done)

	select {
	case err := <-result:
		assert.True(t, errors.Is(err, ErrRetryCancelled), "got: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("RetryTemporary did not return")
	}
}
