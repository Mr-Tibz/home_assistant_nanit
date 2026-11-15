package client

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
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
	// refreshStatusFor - status for a given refresh token. It takes
	// precedence over refreshStatus.
	refreshStatusFor map[string]int
	refreshCalls     int32
	lastRefresh      atomic.Value // refresh token of the last request

	// rotate - behave like the real server: accept a refresh token once
	// and answer with a new pair of tokens. A refresh token that was used
	// before gets the answer 404.
	rotate       bool
	rotateMutex  sync.Mutex
	validRefresh string
	rotations    int

	loginStatus int
	loginBody   string
	loginCalls  int32
	loginDrop   bool // receive the login, then close the connection without an answer

	// babiesStatuses - status of each request for the baby list, in
	// order. The last entry repeats.
	babiesStatuses []int
	babiesBody     string
	babiesCalls    int32
	babiesTokens   chan string // Authorization header of each request
}

// rotateTokens answers a renewal the way the real server does.
func (api *fakeAPI) rotateTokens(w http.ResponseWriter, refreshToken string) {
	// Let requests that were sent at the same moment arrive before the
	// first of them is answered.
	time.Sleep(20 * time.Millisecond)

	api.rotateMutex.Lock()
	defer api.rotateMutex.Unlock()

	if refreshToken != api.validRefresh {
		w.WriteHeader(http.StatusNotFound)
		return
	}

	api.rotations++
	api.validRefresh = fmt.Sprintf("rotated-refresh-%d", api.rotations)

	w.WriteHeader(http.StatusOK)
	fmt.Fprintf(w, `{"access_token":"rotated-access-%d","refresh_token":"%s"}`, api.rotations, api.validRefresh)
}

func newFakeAPI(t *testing.T) *fakeAPI {
	api := &fakeAPI{
		refreshStatus: http.StatusOK,
		refreshBody:   `{"access_token":"access-2","refresh_token":"refresh-2"}`,
		loginStatus:   http.StatusCreated,
		loginBody:     `{"access_token":"access-login","refresh_token":"refresh-login"}`,

		validRefresh: "refresh-1",

		babiesStatuses: []int{http.StatusOK},
		babiesBody:     `{"babies":[{"uid":"baby-1","name":"Baby","camera_uid":"camera-1"}]}`,
		babiesTokens:   make(chan string, 16),
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/tokens/refresh", func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&api.refreshCalls, 1)

		var body map[string]string
		json.NewDecoder(r.Body).Decode(&body)
		api.lastRefresh.Store(body["refresh_token"])

		if api.rotate {
			api.rotateTokens(w, body["refresh_token"])
			return
		}

		if status, found := api.refreshStatusFor[body["refresh_token"]]; found {
			w.WriteHeader(status)
			return
		}

		w.WriteHeader(api.refreshStatus)
		w.Write([]byte(api.refreshBody))
	})
	mux.HandleFunc("/login", func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&api.loginCalls, 1)

		if api.loginDrop {
			if conn, _, err := w.(http.Hijacker).Hijack(); err == nil {
				conn.Close()
			}
			return
		}

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
			w.Write([]byte(api.babiesBody))
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

	require.NoError(t, store.Update(func(s *session.Session) {
		s.AuthToken = "access-1"
		s.RefreshToken = "refresh-1"
		s.AuthTime = time.Now().Add(-2 * AuthTokenTimelife)
	}))

	return &NanitClient{Email: "parent@example.com", Password: "password", SessionStore: store, BaseURL: api.server.URL}
}

// renew renews the session of c with the refresh token that it holds.
func renew(c *NanitClient) error {
	return c.renewSession(c.SessionStore.Snapshot().RefreshToken)
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

	require.NoError(t, renew(c))

	assert.Equal(t, "refresh-1", api.lastRefresh.Load(), "the request carries the current refresh token")
	assert.Equal(t, "access-2", c.SessionStore.Snapshot().AuthToken)
	assert.Equal(t, "refresh-2", c.SessionStore.Snapshot().RefreshToken)
	assert.WithinDuration(t, time.Now(), c.SessionStore.Snapshot().AuthTime, time.Minute)

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

			err := renew(c)

			require.Error(t, err)
			assert.True(t, IsTemporary(err), "expected a temporary error, got: %v", err)
			assert.Equal(t, "access-1", c.SessionStore.Snapshot().AuthToken, "the session keeps its tokens")
			assert.Equal(t, "refresh-1", c.SessionStore.Snapshot().RefreshToken, "the session keeps its tokens")
		})
	}
}

func TestRenewSessionReportsARefusalAsPermanent(t *testing.T) {
	api := newFakeAPI(t)
	c := newTestClient(t, api)
	api.refreshStatus = http.StatusBadRequest

	err := renew(c)

	require.Error(t, err)
	assert.False(t, IsTemporary(err))
	assert.False(t, errors.Is(err, ErrExpiredRefreshToken))
}

func TestAuthorizeLogsInWhenTheRefreshTokenHasExpired(t *testing.T) {
	api := newFakeAPI(t)
	c := newTestClient(t, api)
	api.refreshStatus = http.StatusNotFound

	require.NoError(t, c.authorize())

	assert.EqualValues(t, 1, atomic.LoadInt32(&api.loginCalls))
	assert.Equal(t, "access-login", c.SessionStore.Snapshot().AuthToken)
	assert.Equal(t, "refresh-login", c.SessionStore.Snapshot().RefreshToken)
}

// A temporary failure of the renewal must not fall through to a login
// with e-mail address and password.
func TestAuthorizeDoesNotLogInAfterATemporaryFailure(t *testing.T) {
	api := newFakeAPI(t)
	c := newTestClient(t, api)
	api.refreshStatus = http.StatusServiceUnavailable

	err := c.authorize()

	require.Error(t, err)
	assert.True(t, IsTemporary(err))
	assert.EqualValues(t, 0, atomic.LoadInt32(&api.loginCalls))
}

// Every answer of the server to a login is final, also one that could
// pass. Trying again would send the e-mail address and the password again
// and again.
func TestLoginTakesEveryAnswerOfTheServerAsFinal(t *testing.T) {
	for _, status := range []int{http.StatusUnauthorized, http.StatusTooManyRequests, http.StatusServiceUnavailable} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			api := newFakeAPI(t)
			c := newTestClient(t, api)
			api.loginStatus = status

			err := c.login()

			require.Error(t, err)
			assert.False(t, IsTemporary(err), "got: %v", err)
		})
	}
}

// The same holds when the server accepts the login and its answer cannot
// be used, for example because it was cut off.
func TestLoginTakesAnAcceptedLoginWithAnUnusableAnswerAsFinal(t *testing.T) {
	for name, body := range map[string]string{
		"cut off":    `{"access_token":`,
		"not JSON":   `<html>`,
		"no content": ``,
		"no token":   `{}`,
	} {
		t.Run(name, func(t *testing.T) {
			api := newFakeAPI(t)
			c := newTestClient(t, api)
			api.loginBody = body

			err := c.login()

			require.Error(t, err)
			assert.False(t, IsTemporary(err), "got: %v", err)
		})
	}
}

// A login that got no answer is not sent again either: the server may
// have received it all the same.
func TestLoginThatTheServerMayHaveReceivedIsFinal(t *testing.T) {
	api := newFakeAPI(t)
	c := newTestClient(t, api)
	api.loginDrop = true

	err := c.login()

	require.Error(t, err)
	assert.False(t, IsTemporary(err), "got: %v", err)
	assert.EqualValues(t, 1, atomic.LoadInt32(&api.loginCalls), "the server did receive the login")
}

// The one failure of a login that is tried again: no connection could be
// made, so the server cannot have received the credentials. This is the
// case of a start before the network is up.
func TestLoginForWhichNoConnectionCouldBeMadeIsTemporary(t *testing.T) {
	api := newFakeAPI(t)
	c := newTestClient(t, api)
	api.server.Close()

	err := c.login()

	require.Error(t, err)
	assert.True(t, IsTemporary(err), "got: %v", err)
}

// Most setups have only a refresh token, because the account needs a
// second factor for a login. When that token is no longer valid, the
// error says so. A login without e-mail address and password is not sent:
// its refusal would name the wrong cause.
func TestAuthorizeWithoutCredentialsDoesNotLogIn(t *testing.T) {
	api := newFakeAPI(t)
	c := newTestClient(t, api)
	c.Email, c.Password = "", ""
	api.refreshStatus = http.StatusNotFound

	err := c.authorize()

	require.Error(t, err)
	assert.False(t, IsTemporary(err))
	assert.Contains(t, err.Error(), "refresh token is no longer valid")
	assert.EqualValues(t, 0, atomic.LoadInt32(&api.loginCalls))
}

// When the server no longer accepts the refresh token of the session, a
// configured one that differs is tried: the owner may have just set a new
// one.
func TestAuthorizeTriesTheConfiguredRefreshTokenAfterTheOneOfTheSession(t *testing.T) {
	api := newFakeAPI(t)
	api.rotate = true
	api.validRefresh = "configured"
	c := newTestClient(t, api)
	c.Email, c.Password = "", ""
	c.RefreshToken = "configured"

	require.NoError(t, c.authorize())

	assert.EqualValues(t, 2, atomic.LoadInt32(&api.refreshCalls), "the token of the session first, then the configured one")
	assert.Equal(t, "rotated-refresh-1", c.SessionStore.Snapshot().RefreshToken)
	assert.EqualValues(t, 0, atomic.LoadInt32(&api.loginCalls))
}

// The configured token is a second chance only. When the server refuses
// it for any reason, the login is still tried.
func TestAuthorizeLogsInWhenTheConfiguredRefreshTokenIsRefusedToo(t *testing.T) {
	api := newFakeAPI(t)
	c := newTestClient(t, api)
	c.RefreshToken = "malformed"

	// The token of the session gets 404, the configured one 400
	api.refreshStatusFor = map[string]int{"refresh-1": http.StatusNotFound, "malformed": http.StatusBadRequest}

	require.NoError(t, c.authorize())

	assert.EqualValues(t, 2, atomic.LoadInt32(&api.refreshCalls))
	assert.EqualValues(t, 1, atomic.LoadInt32(&api.loginCalls))
	assert.Equal(t, "access-login", c.AuthToken())
}

// The renewal before a new attempt to connect is a precaution. If the
// server answers it with an error of its own, the caller goes on with the
// token of the session, as long as that token should still be valid.
func TestAuthorizeForAttempt(t *testing.T) {
	for name, tc := range map[string]struct {
		renew     bool
		fresh     bool
		breakIt   func(api *fakeAPI)
		wantError bool
	}{
		"server error, fresh token":      {true, true, func(api *fakeAPI) { api.refreshStatus = http.StatusServiceUnavailable }, false},
		"too many requests, fresh token": {true, true, func(api *fakeAPI) { api.refreshStatus = http.StatusTooManyRequests }, false},
		"server error, old token":        {true, false, func(api *fakeAPI) { api.refreshStatus = http.StatusServiceUnavailable }, true},
		"no answer, fresh token":         {true, true, func(api *fakeAPI) { api.server.Close() }, true},
		"refusal, fresh token":           {true, true, func(api *fakeAPI) { api.refreshStatus = http.StatusBadRequest }, true},
		"renewal works":                  {true, true, func(api *fakeAPI) {}, false},
		"no renewal asked, old token":    {false, false, func(api *fakeAPI) { api.refreshStatus = http.StatusServiceUnavailable }, true},
		"no renewal asked, fresh token":  {false, true, func(api *fakeAPI) { api.server.Close() }, false},
	} {
		t.Run(name, func(t *testing.T) {
			api := newFakeAPI(t)
			c := newTestClient(t, api)
			if tc.fresh {
				c.SessionStore.Update(func(s *session.Session) { s.AuthTime = time.Now() })
			}
			tc.breakIt(api)

			err := c.AuthorizeForAttempt(tc.renew)

			assert.Equal(t, tc.wantError, err != nil, "got: %v", err)
		})
	}
}

// The answer to a login normally carries a new refresh token. If it does
// not, the refresh token that the server has just refused is dropped, so
// that the next authorization does not send it again.
func TestLoginWithoutANewRefreshTokenDropsTheRefusedOne(t *testing.T) {
	api := newFakeAPI(t)
	c := newTestClient(t, api)
	api.refreshStatus = http.StatusNotFound
	api.loginBody = `{"access_token":"access-login"}`

	require.NoError(t, c.authorize())

	assert.Equal(t, "access-login", c.SessionStore.Snapshot().AuthToken)
	assert.Equal(t, "", c.SessionStore.Snapshot().RefreshToken)
	assert.Equal(t, "", storedSession(t, c).RefreshToken)

	// The next authorization goes to the login directly
	require.NoError(t, c.authorize())
	assert.EqualValues(t, 1, atomic.LoadInt32(&api.refreshCalls))
	assert.EqualValues(t, 2, atomic.LoadInt32(&api.loginCalls))
}

func TestMaybeAuthorizeKeepsAFreshToken(t *testing.T) {
	api := newFakeAPI(t)
	c := newTestClient(t, api)
	c.SessionStore.Update(func(s *session.Session) { s.AuthTime = time.Now() })

	require.NoError(t, c.MaybeAuthorize(false))
	assert.EqualValues(t, 0, atomic.LoadInt32(&api.refreshCalls), "a fresh token needs no request")

	require.NoError(t, c.MaybeAuthorize(true))
	assert.EqualValues(t, 1, atomic.LoadInt32(&api.refreshCalls), "force renews also a fresh token")
}

// A session without a refresh token starts with the one of the
// configuration.
func TestAuthorizeUsesTheConfiguredRefreshToken(t *testing.T) {
	api := newFakeAPI(t)
	c := newTestClient(t, api)
	c.SessionStore.Update(func(s *session.Session) { s.RefreshToken = "" })
	c.RefreshToken = "configured"

	require.NoError(t, c.authorize())

	assert.Equal(t, "configured", api.lastRefresh.Load())
	assert.Equal(t, "refresh-2", c.SessionStore.Snapshot().RefreshToken)
	assert.EqualValues(t, 0, atomic.LoadInt32(&api.loginCalls))
}

// The configured refresh token reaches the session only when the server
// has accepted it. If the answer carries no new one, it stays the valid
// one.
func TestAcceptedRefreshTokenIsKeptWhenTheAnswerCarriesNoNewOne(t *testing.T) {
	api := newFakeAPI(t)
	c := newTestClient(t, api)
	c.SessionStore.Update(func(s *session.Session) { s.RefreshToken = "" })
	c.RefreshToken = "configured"
	api.refreshBody = `{"access_token":"access-2"}`

	require.NoError(t, c.authorize())

	assert.Equal(t, "configured", c.SessionStore.Snapshot().RefreshToken)
	assert.Equal(t, "configured", storedSession(t, c).RefreshToken)
}

// A refresh token that the server did not accept is not stored.
func TestRefusedConfiguredRefreshTokenIsNotStored(t *testing.T) {
	api := newFakeAPI(t)
	c := newTestClient(t, api)
	c.SessionStore.Update(func(s *session.Session) { s.RefreshToken = "" })
	c.RefreshToken = "configured"
	api.refreshStatus = http.StatusBadGateway

	require.Error(t, c.authorize())

	assert.Equal(t, "", c.SessionStore.Snapshot().RefreshToken)
	assert.Equal(t, "", storedSession(t, c).RefreshToken)
}

// A request reads the token together with the number of authorizations
// that had ended. If another caller gets a new token before the refusal
// of the request arrives, that token is used and no further renewal is
// made.
func TestFetchAuthorizedUsesATokenThatAnotherCallerGotInTheMeantime(t *testing.T) {
	api := newFakeAPI(t)
	c := newTestClient(t, api)

	token, seen := c.tokenForRequest()
	assert.Equal(t, "access-1", token)

	require.NoError(t, c.MaybeAuthorize(true)) // another caller renews while the request is on its way
	require.NoError(t, c.authorizeAfter(seen)) // the request comes back refused

	assert.EqualValues(t, 1, atomic.LoadInt32(&api.refreshCalls))
	assert.Equal(t, "access-2", c.AuthToken())
}

// All cameras share one session, and the server accepts a refresh token
// only once. Authorizations that are asked for at the same moment must
// run one after the other, each with the refresh token that is valid
// then. Without that, the second one is refused (404), falls through to a
// login and ends the program.
func TestAuthorizationsAtTheSameMomentDoNotShareARefreshToken(t *testing.T) {
	api := newFakeAPI(t)
	api.rotate = true
	api.loginStatus = http.StatusUnauthorized
	c := newTestClient(t, api)

	const callers = 6
	start := make(chan struct{})
	results := make(chan error, callers)

	for i := 0; i < callers; i++ {
		go func() {
			<-start
			results <- c.MaybeAuthorize(true)
		}()
	}

	close(start)

	for i := 0; i < callers; i++ {
		assert.NoError(t, <-results)
	}

	assert.EqualValues(t, 0, atomic.LoadInt32(&api.loginCalls), "no renewal was refused")

	api.rotateMutex.Lock()
	defer api.rotateMutex.Unlock()
	assert.GreaterOrEqual(t, api.rotations, 1)
	assert.Equal(t, api.validRefresh, c.SessionStore.Snapshot().RefreshToken, "the session holds the refresh token that the server expects next")
	assert.Equal(t, api.validRefresh, storedSession(t, c).RefreshToken, "and so does the session file")
}

// A caller that waited for its turn while another caller got a new token
// uses that token. It does not ask for one more.
func TestACallerThatWaitedSharesTheNewToken(t *testing.T) {
	api := newFakeAPI(t)
	c := newTestClient(t, api)

	seen := c.authRuns.Load() // a caller is about to wait for its turn

	require.NoError(t, c.MaybeAuthorize(true)) // another caller is served first
	require.NoError(t, c.authorizeAfter(seen))

	assert.EqualValues(t, 1, atomic.LoadInt32(&api.refreshCalls), "the caller that waited did not renew again")

	// A caller that comes later gets a renewal of its own
	require.NoError(t, c.authorizeAfter(c.authRuns.Load()))
	assert.EqualValues(t, 2, atomic.LoadInt32(&api.refreshCalls))
}

// A caller that waited while the authorization of another caller failed
// gets that failure. Without this, every camera would repeat a request
// that has just failed, one after the other, each for the full time
// limit of a request.
func TestACallerThatWaitedSharesTheFailure(t *testing.T) {
	api := newFakeAPI(t)
	c := newTestClient(t, api)
	api.refreshStatus = http.StatusBadGateway

	seen := c.authRuns.Load() // a caller is about to wait for its turn

	first := c.MaybeAuthorize(true) // another caller is served first, and fails
	require.Error(t, first)

	second := c.authorizeAfter(seen)

	assert.Same(t, first, second)
	assert.True(t, IsTemporary(second))
	assert.EqualValues(t, 1, atomic.LoadInt32(&api.refreshCalls), "the caller that waited made no request")

	// A caller that comes later tries for itself
	api.refreshStatus = http.StatusOK
	require.NoError(t, c.MaybeAuthorize(true))
	assert.EqualValues(t, 2, atomic.LoadInt32(&api.refreshCalls))
}

// A token that should still be valid needs no authorization, whatever
// happened to the authorization of another caller in the meantime.
func TestAFreshTokenIsEnoughWithoutForce(t *testing.T) {
	api := newFakeAPI(t)
	c := newTestClient(t, api)
	c.SessionStore.Update(func(s *session.Session) { s.AuthTime = time.Now() })
	api.refreshStatus = http.StatusBadGateway

	require.Error(t, c.MaybeAuthorize(true))
	require.NoError(t, c.MaybeAuthorize(false))

	assert.EqualValues(t, 1, atomic.LoadInt32(&api.refreshCalls))
}

// An answer that is JSON, but not of the expected form, means that the
// API has changed. Asking again does not help.
func TestAuthResponseOfAnUnexpectedFormIsPermanent(t *testing.T) {
	api := newFakeAPI(t)
	c := newTestClient(t, api)
	api.refreshBody = `{"access_token":{"value":"access-2"}}`

	err := renew(c)

	require.Error(t, err)
	assert.False(t, IsTemporary(err), "got: %v", err)
	assert.Equal(t, "access-1", c.SessionStore.Snapshot().AuthToken, "the session keeps its tokens")
}

// An answer of an unexpected form can still carry a new refresh token.
// It is kept: the server may already have retired the old one.
func TestAuthResponseOfAnUnexpectedFormKeepsItsRefreshToken(t *testing.T) {
	api := newFakeAPI(t)
	c := newTestClient(t, api)
	api.refreshBody = `{"access_token":{"value":"access-2"},"refresh_token":"refresh-2"}`

	err := renew(c)

	require.Error(t, err)
	assert.False(t, IsTemporary(err), "got: %v", err)
	assert.Equal(t, "access-1", c.SessionStore.Snapshot().AuthToken, "the old access token stays")
	assert.Equal(t, "refresh-2", c.SessionStore.Snapshot().RefreshToken, "the new refresh token is kept")
	assert.Equal(t, "refresh-2", storedSession(t, c).RefreshToken, "the new refresh token is saved")
}

// An answer that reports success but carries no access token is not
// tried again: that would send the credentials again and again. A new
// refresh token in it is kept, because the server may already have retired
// the old one.
func TestAuthResponseWithoutAccessToken(t *testing.T) {
	api := newFakeAPI(t)
	c := newTestClient(t, api)
	api.refreshBody = `{"refresh_token":"refresh-2"}`

	err := renew(c)

	require.Error(t, err)
	assert.False(t, IsTemporary(err))
	assert.Equal(t, "access-1", c.SessionStore.Snapshot().AuthToken, "the old access token stays")
	assert.Equal(t, "refresh-2", c.SessionStore.Snapshot().RefreshToken, "the new refresh token is kept")
	assert.Equal(t, "refresh-2", storedSession(t, c).RefreshToken, "the new refresh token is saved")
}

// As before this change, the first JSON value of an answer counts, and
// what follows it is ignored.
func TestFetchAuthorizedIgnoresWhatFollowsTheAnswer(t *testing.T) {
	api := newFakeAPI(t)
	c := newTestClient(t, api)
	api.babiesBody += "\n{\"more\":true} and more"

	babies, err := c.FetchBabies()

	require.NoError(t, err)
	assert.Len(t, babies, 1)
}

// An answer that is far longer than any answer of the API is not read to
// its end, and it is not asked for again.
func TestFetchAuthorizedDoesNotReadAnEndlessAnswer(t *testing.T) {
	api := newFakeAPI(t)
	c := newTestClient(t, api)
	api.babiesBody = `{"babies":[{"uid":"` + strings.Repeat("x", maxResponseSize) + `"}]}`

	_, err := c.FetchBabies()

	require.Error(t, err)
	assert.False(t, IsTemporary(err), "got: %.200v", err)
	assert.Contains(t, err.Error(), "longer than")
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
	assert.EqualValues(t, 1, atomic.LoadInt32(&api.refreshCalls), "no renewal follows the second refusal, because no request would use it")
}

// What the answer of a request can look like when the status says
// success, and what each case means for the caller.
func TestFetchAuthorizedSortsAnswersItCannotUse(t *testing.T) {
	for name, tc := range map[string]struct {
		body      string
		temporary bool
	}{
		"not JSON, something else answered": {`<html>Sign in to the network</html>`, true},
		"cut off":                           {`{"babies":[{"uid":"baby-1"`, true},
		"no content":                        {``, true},
		"JSON of another form":              {`{"babies":"none"}`, false},
	} {
		t.Run(name, func(t *testing.T) {
			api := newFakeAPI(t)
			c := newTestClient(t, api)
			api.babiesBody = tc.body

			_, err := c.FetchBabies()

			require.Error(t, err)
			assert.Equal(t, tc.temporary, IsTemporary(err), "got: %v", err)
		})
	}
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

// A call that is running is not abandoned: a renewal that nobody waits
// for could lose the new refresh token. The helper returns when the call
// has ended.
func TestRetryTemporaryWaitsForARunningCall(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})

	fn := func() error {
		close(started)
		<-release
		return temporary("no network")
	}

	done := make(chan struct{})
	result := make(chan error, 1)
	go func() { result <- RetryTemporary(done, []time.Duration{time.Hour}, "test", fn) }()

	<-started
	close(done)

	select {
	case err := <-result:
		t.Fatalf("RetryTemporary returned (%v) while the call was still running", err)
	case <-time.After(50 * time.Millisecond):
	}

	close(release)

	select {
	case err := <-result:
		assert.True(t, errors.Is(err, ErrRetryCancelled), "got: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("RetryTemporary did not return after the call had ended")
	}
}

// A call that succeeds counts, also when done has closed in the meantime.
func TestRetryTemporaryKeepsASuccessAfterDone(t *testing.T) {
	done := make(chan struct{})
	fn := func() error {
		close(done)
		return nil
	}

	assert.NoError(t, RetryTemporary(done, nil, "test", fn))
}

func TestRetryTemporaryDoesNotCallWhenDoneHasClosed(t *testing.T) {
	done := make(chan struct{})
	close(done)

	calls := 0
	err := RetryTemporary(done, nil, "test", scripted(&calls, nil))

	assert.True(t, errors.Is(err, ErrRetryCancelled), "got: %v", err)
	assert.Equal(t, 0, calls)
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

// The polls of all cameras share the time of the newest message seen. It
// moves forward only: a poll that stores an older time would make another
// camera report its newest messages a second time.
func TestRememberMessageTimeNeverGoesBack(t *testing.T) {
	api := newFakeAPI(t)
	c := newTestClient(t, api)

	t3 := time.Date(2026, 1, 1, 0, 0, 3, 0, time.UTC)
	t5 := time.Date(2026, 1, 1, 0, 0, 5, 0, time.UTC)
	t7 := time.Date(2026, 1, 1, 0, 0, 7, 0, time.UTC)

	c.rememberMessageTime(t5)
	assert.Equal(t, t5, c.SessionStore.Snapshot().LastSeenMessageTime)

	c.rememberMessageTime(t3)
	assert.Equal(t, t5, c.SessionStore.Snapshot().LastSeenMessageTime, "an older time does not replace a later one")

	c.rememberMessageTime(t7)
	assert.Equal(t, t7, c.SessionStore.Snapshot().LastSeenMessageTime)
	assert.True(t, t7.Equal(storedSession(t, c).LastSeenMessageTime), "the time is saved")
}
