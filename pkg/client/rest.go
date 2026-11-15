package client

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptrace"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/indiefan/home_assistant_nanit/pkg/baby"
	"github.com/indiefan/home_assistant_nanit/pkg/message"
	"github.com/indiefan/home_assistant_nanit/pkg/session"
	"github.com/indiefan/home_assistant_nanit/pkg/utils"
	"github.com/rs/zerolog/log"
)

var myClient = &http.Client{Timeout: 10 * time.Second}
var ErrExpiredRefreshToken = errors.New("Refresh token has expired. Relogin required.")

// defaultAPIBaseURL - address of the Nanit API
const defaultAPIBaseURL = "https://api.nanit.com"

// TemporaryError marks a failure that can pass without any change on our
// side: no network, no DNS, a timeout, or a server that answers with an
// error of its own. The caller should try again later.
//
// Every other error means that trying again as we are will not help, for
// example credentials the server refuses.
//
// Every request that does not get through counts as temporary, also for a
// cause that may never pass, such as a certificate that cannot be
// verified. The two cannot be told apart safely: a network that answers
// in the place of the server during an outage fails in the same way. The
// cost is that such a failure is tried again for as long as it lasts, with
// an error in the log each time, and does not end the program.
type TemporaryError struct {
	Err error
}

func (e *TemporaryError) Error() string { return e.Err.Error() }
func (e *TemporaryError) Unwrap() error { return e.Err }

// IsTemporary reports whether err is, or wraps, a TemporaryError.
func IsTemporary(err error) bool {
	var temporary *TemporaryError
	return errors.As(err, &temporary)
}

func temporary(format string, args ...interface{}) error {
	return &TemporaryError{Err: fmt.Errorf(format, args...)}
}

// statusError turns an unexpected HTTP status into an error. Answers that
// tell us the server could not serve the request right now (5xx, 408,
// 429) are temporary. Everything else is the server refusing what we sent.
func statusError(what string, code int) error {
	err := fmt.Errorf("%v: server responded with status code %v", what, code)

	if code >= 500 || code == http.StatusRequestTimeout || code == http.StatusTooManyRequests {
		return &TemporaryError{Err: err}
	}

	return err
}

// ------------------------------------------

type authResponsePayload struct {
	AccessToken  string `json:"access_token,omitempty"`
	RefreshToken string `json:"refresh_token,omitempty"` // We can store this to renew a session, avoiding the need to re-auth with MFA
}

type babiesResponsePayload struct {
	Babies []baby.Baby `json:"babies"`
}

type messagesResponsePayload struct {
	Messages []message.Message `json:"messages"`
}

// ------------------------------------------

// ErrRetryCancelled - RetryTemporary was told to stop before fn succeeded
var ErrRetryCancelled = errors.New("cancelled while waiting for the next try")

// defaultRetryWait is used when RetryTemporary gets no list of waits.
const defaultRetryWait = 30 * time.Second

// RetryTemporary calls fn until it succeeds. After a temporary failure
// (see TemporaryError) it waits and calls fn again: first after waits[0],
// then after waits[1], and so on, and after the last entry from then on.
//
// It returns nil after a success. It returns the error of a failure that
// is not temporary at once, because trying again as we are does not help
// then. It returns ErrRetryCancelled when done closes during a wait, or
// has closed when the next call of fn is due.
//
// A call of fn that is running is never abandoned. fn must end by itself
// in a limited time, as the requests of NanitClient do. A renewal of the
// token that is abandoned half-way could lose the new refresh token: the
// server has replaced the old one, and nobody is left to store the new.
//
// This is not utils.RunWithPerseverance: that one restarts a long-running
// handler and counts the time the handler ran against the cooldown. Here
// the pause always follows the failure in full.
func RetryTemporary(done <-chan struct{}, waits []time.Duration, what string, fn func() error) error {
	for try := 0; ; try++ {
		select {
		case <-done:
			return ErrRetryCancelled
		default:
		}

		err := fn()
		if err == nil || !IsTemporary(err) {
			return err
		}

		wait := defaultRetryWait
		if len(waits) > 0 {
			wait = waits[utils.MinInt(try, len(waits)-1)]
		}

		log.Error().Err(err).Int("failed_tries", try+1).Str("retry_in", wait.String()).Msgf("Unable to %v, will try again", what)

		timer := time.NewTimer(wait)
		select {
		case <-done:
			timer.Stop()
			return ErrRetryCancelled
		case <-timer.C:
		}
	}
}

// NanitClient - client context
type NanitClient struct {
	Email        string
	Password     string
	RefreshToken string
	SessionStore *session.Store

	// BaseURL - address of the Nanit API. Empty means the real one.
	BaseURL string

	// authMutex lets one authorization run at a time. All cameras share
	// one session, and the server accepts a refresh token only once: of
	// two renewals at the same moment, the second one is refused.
	authMutex sync.Mutex

	// authRuns counts the authorizations that have ended, and lastAuthErr
	// is the result of the latest one. A caller that had to wait for its
	// turn sees from the count that an authorization has run in the
	// meantime. It takes that result and makes no request of its own.
	// authMutex guards lastAuthErr.
	authRuns    atomic.Uint64
	lastAuthErr error
}

func (c *NanitClient) url(path string) string {
	if c.BaseURL != "" {
		return c.BaseURL + path
	}

	return defaultAPIBaseURL + path
}

// AuthToken returns the auth token that the session has now.
func (c *NanitClient) AuthToken() string {
	return c.SessionStore.Snapshot().AuthToken
}

// MaybeAuthorize - Performs authorization if we don't have token or we assume it is expired
//
// With force it asks for a new token also when the session has one that
// should still be valid.
//
// Callers at the same moment share one authorization: a caller that had
// to wait for its turn gets the result of the authorization that ran in
// the meantime, a new token or the failure, and makes no request of its
// own.
//
// It returns an error if the authorization was needed and failed. Use
// IsTemporary to tell a failure that is worth another try from one that
// is not.
func (c *NanitClient) MaybeAuthorize(force bool) error {
	seen := c.authRuns.Load()

	if !force && c.hasFreshToken() {
		return nil
	}

	return c.authorizeAfter(seen)
}

// hasFreshToken reports whether the session holds an auth token that
// should still be valid.
func (c *NanitClient) hasFreshToken() bool {
	current := c.SessionStore.Snapshot()

	return current.AuthToken != "" && time.Since(current.AuthTime) <= AuthTokenTimelife
}

// refusedTokenMinAge - a token that the server refuses is replaced only
// when it is at least this old. A token that is refused right after it
// was issued is not refused for its age, and one more renewal would not
// help: it would only use up one more refresh token. This also limits
// the renewals when the server refuses every token, to one in this time.
const refusedTokenMinAge = 15 * time.Minute

// RenewRefusedToken gets a new auth token in the place of refused, which
// the server did not accept. It does nothing when the session holds
// another token by now, or when the token is too young to be the cause
// (see refusedTokenMinAge).
func (c *NanitClient) RenewRefusedToken(refused string) error {
	// The token and the count belong together: an authorization that
	// ends after this point is seen by authorizeAfter, which then makes
	// no further one.
	token, seen := c.tokenForRequest()

	if token != refused {
		return nil
	}

	if age := time.Since(c.SessionStore.Snapshot().AuthTime); age < refusedTokenMinAge {
		log.Debug().Str("age", age.String()).Msg("The server has refused a token that is new. It is not renewed again yet")
		return nil
	}

	return c.authorizeAfter(seen)
}

// authorizeAfter performs an authorization. If one has ended since the
// caller read seen from authRuns, it returns the result of that one.
func (c *NanitClient) authorizeAfter(seen uint64) error {
	c.authMutex.Lock()
	defer c.authMutex.Unlock()

	if c.authRuns.Load() != seen {
		return c.lastAuthErr
	}

	c.lastAuthErr = c.authorize()
	c.authRuns.Add(1)

	return c.lastAuthErr
}

// tokenForRequest returns the auth token of the session and the number
// of authorizations that had ended when it was read. The lock makes the
// two belong together, and it makes a request wait for an authorization
// that is running, so that it does not go out with a token that is about
// to be replaced.
func (c *NanitClient) tokenForRequest() (token string, seen uint64) {
	c.authMutex.Lock()
	defer c.authMutex.Unlock()

	return c.AuthToken(), c.authRuns.Load()
}

// authorize gets a new auth token: with a refresh token, or with the
// e-mail address and password when there is no refresh token or the
// server says that none is valid any more. The caller holds authMutex.
func (c *NanitClient) authorize() error {
	// The refresh token of the session comes first. The configured one
	// starts a session that has none, and it is tried when the server no
	// longer accepts the one of the session: it may be a new one that
	// the owner has just set.
	refreshTokens := []string{c.SessionStore.Snapshot().RefreshToken}
	if c.RefreshToken != refreshTokens[0] {
		refreshTokens = append(refreshTokens, c.RefreshToken)
	}

	tried := 0

	for _, refreshToken := range refreshTokens {
		if refreshToken == "" {
			continue
		}

		tried++

		err := c.renewSession(refreshToken) // We have a refresh token, so we'll use that to extend our session
		if err == nil {
			return nil
		}
		if errors.Is(err, ErrExpiredRefreshToken) {
			continue
		}
		if tried > 1 && !IsTemporary(err) {
			// The configured token was a second chance only. Whatever
			// the server has against it, the login is still open.
			log.Warn().Err(err).Msg("The configured refresh token was not accepted either")
			continue
		}

		return err
	}

	// We don't have a refresh token, e.g. initial login so we need to supply username/password
	if c.Email == "" || c.Password == "" {
		// A login without them cannot work, and its refusal would hide
		// the cause.
		if tried > 0 {
			return errors.New("the refresh token is no longer valid, and no e-mail address and password are configured for a login. Get a new refresh token with init-nanit.sh (see the README)")
		}

		return errors.New("there is no refresh token, and no e-mail address and password are configured for a login. Get a refresh token with init-nanit.sh (see the README)")
	}

	return c.login()
}

// Renews an existing session using a valid refresh token
// If the refresh token has also expired, we need to perform a full re-login
// The caller holds authMutex.
func (c *NanitClient) renewSession(refreshToken string) error {
	requestBody, requestBodyErr := json.Marshal(map[string]string{
		"refresh_token": refreshToken,
	})

	if requestBodyErr != nil {
		return fmt.Errorf("unable to marshal auth body: %w", requestBodyErr)
	}

	r, clientErr := myClient.Post(c.url("/tokens/refresh"), "application/json", bytes.NewBuffer(requestBody))
	if clientErr != nil {
		return temporary("unable to renew session: %w", clientErr)
	}

	defer r.Body.Close()
	if r.StatusCode == 404 {
		log.Warn().Msg("Server responded with code 404. This typically means your refresh token has expired")
		return ErrExpiredRefreshToken
	} else if r.StatusCode > 299 || r.StatusCode < 200 {
		return statusError("unable to renew session", r.StatusCode)
	}

	// If the answer carries no new refresh token, the one that the
	// server has just accepted stays the valid one.
	return c.storeAuthResponse(r, refreshToken)
}

// login gets an auth token with the e-mail address and password. The
// caller holds authMutex.
//
// This program does not send the e-mail address and the password a second
// time by itself after the server may have received them. Every failure
// of a login ends the program, as it did before. How often the login is
// sent again is then up to whatever restarts the program, as before. The
// one exception is a login for which no connection could be made: that
// one is tried again here.
func (c *NanitClient) login() error {
	log.Info().Str("email", c.Email).Str("password", utils.AnonymizeToken(c.Password, 0)).Msg("Authorizing using user credentials")
	requestBody, requestBodyErr := json.Marshal(map[string]string{
		"email":    c.Email,
		"password": c.Password,
	})

	if requestBodyErr != nil {
		return fmt.Errorf("unable to marshal auth body: %w", requestBodyErr)
	}

	//nanit-api-version: 1
	req, reqErr := http.NewRequest("POST", c.url("/login"), bytes.NewBuffer(requestBody))
	if reqErr != nil {
		return fmt.Errorf("unable to create request: %w", reqErr)
	}
	req.Header.Add("Content-Type", "application/json")
	req.Header.Add("nanit-api-version", "1") // required if you have MFA enabled or it'll reject the request

	// connected - a connection to the server was there for this request.
	// Without one, the server cannot have received the credentials.
	var connected atomic.Bool
	req = req.WithContext(httptrace.WithClientTrace(req.Context(), &httptrace.ClientTrace{
		GotConn: func(httptrace.GotConnInfo) { connected.Store(true) },
	}))

	r, clientErr := myClient.Do(req)
	if clientErr != nil {
		if !connected.Load() {
			return temporary("unable to fetch auth token: %w", clientErr)
		}

		// No answer has arrived, but the server may have received the
		// request all the same.
		return fmt.Errorf("unable to fetch auth token: %v", clientErr)
	}

	defer r.Body.Close()

	if r.StatusCode == 401 {
		return errors.New("server responded with code 401. Provided credentials has not been accepted by the server. Please check if your e-mail address and password is entered correctly and that 2FA is disabled on your account")
	} else if r.StatusCode != 201 {
		return fmt.Errorf("unable to fetch auth token: server responded with status code %v", r.StatusCode)
	}

	// If the answer carries an access token and no refresh token, the
	// session keeps no refresh token: the one it had, if any, is the one
	// that the server has just refused.
	if err := c.storeAuthResponse(r, ""); err != nil {
		// %v and not %w: the error must not count as temporary
		return fmt.Errorf("unable to use the answer to the login: %v", err)
	}

	return nil
}

// maxResponseSize - no answer of the API comes near this size. A longer
// answer is not read to its end, so that it cannot fill the memory, and
// it is not tried again.
const maxResponseSize = 4 << 20

// decodeBody reads the JSON body of an answer whose status said success.
// As before, the first JSON value of the body counts, and what follows it
// is ignored.
//
// A body that is not a complete JSON value (no content, cut off, or not
// JSON at all) is a fault of the transfer or of something that answered
// in the place of the server, and can pass. A JSON value of another form
// than this program expects comes from the API and means that the API
// has changed: asking again does not help.
//
// When the form does not fit, data still holds the fields that did fit.
func decodeBody(r *http.Response, data interface{}) error {
	body, readErr := io.ReadAll(io.LimitReader(r.Body, maxResponseSize+1))
	if readErr != nil {
		return temporary("unable to read the response: %w", readErr)
	}
	if len(body) > maxResponseSize {
		return fmt.Errorf("the response is longer than %v bytes", maxResponseSize)
	}

	jsonErr := json.NewDecoder(bytes.NewReader(body)).Decode(data)
	if jsonErr == nil {
		return nil
	}

	var syntaxErr *json.SyntaxError
	if errors.As(jsonErr, &syntaxErr) || errors.Is(jsonErr, io.EOF) || errors.Is(jsonErr, io.ErrUnexpectedEOF) {
		return temporary("the response is not a complete JSON value: %w", jsonErr)
	}

	return fmt.Errorf("the response has an unexpected form: %w", jsonErr)
}

// storeAuthResponse reads the tokens of a successful answer and saves
// them in the session, with one save. If the answer is complete and
// carries no refresh token, the session gets fallbackRefreshToken. The
// caller holds authMutex.
func (c *NanitClient) storeAuthResponse(r *http.Response, fallbackRefreshToken string) error {
	authResponse := new(authResponsePayload)

	decodeErr := decodeBody(r, authResponse)
	if decodeErr != nil {
		// Forget an access token of an answer that cannot be trusted as
		// a whole. A refresh token in it is kept, see below.
		authResponse.AccessToken = ""
	}

	if authResponse.RefreshToken != "" {
		log.Info().Str("refresh_token", utils.AnonymizeToken(authResponse.RefreshToken, 4)).Msg("Retreived")
	}

	if authResponse.RefreshToken != "" || authResponse.AccessToken != "" {
		c.SessionStore.Update(func(s *session.Session) {
			// The server may retire the old refresh token as soon as it
			// has sent a new one. Keep the new one whatever else the
			// answer holds, also when another part of it cannot be used.
			if authResponse.RefreshToken != "" {
				s.RefreshToken = authResponse.RefreshToken
			} else {
				s.RefreshToken = fallbackRefreshToken
			}

			if authResponse.AccessToken != "" {
				s.AuthToken = authResponse.AccessToken
				s.AuthTime = time.Now()
			}
		})
	}

	if decodeErr != nil {
		return fmt.Errorf("unable to use the auth response: %w", decodeErr)
	}

	if authResponse.AccessToken == "" {
		// Not temporary: the server answers in a way this program does
		// not understand, and asking again would send the credentials
		// again and again.
		return errors.New("auth response carries no access token")
	}

	log.Info().Str("token", utils.AnonymizeToken(authResponse.AccessToken, 4)).Msg("Authorized")

	return nil
}

// FetchAuthorized - makes authorized http request
//
// When the server refuses the token (status code 401), the token is
// renewed and the request is sent once more.
//
// Use IsTemporary on the returned error to tell a failure that is worth
// another try from one that is not.
func (c *NanitClient) FetchAuthorized(req *http.Request, data interface{}) error {
	token, seen := c.tokenForRequest()

	if token != "" {
		unauthorized, err := c.fetchOnce(req, token, data)
		if err != nil || !unauthorized {
			return err
		}

		log.Info().Msg("Token might be expired. Will try to re-authenticate.")
	}

	// There is no token, or the server has refused it. If an
	// authorization has ended since the token was read, its result counts
	// and no further one is made.
	if err := c.authorizeAfter(seen); err != nil {
		return err
	}

	unauthorized, err := c.fetchOnce(req, c.AuthToken(), data)
	if err != nil {
		return err
	}

	if unauthorized {
		// No further renewal: no request would use it
		return errors.New("unable to make request: the server refuses it with a new token too (2 attempts)")
	}

	return nil
}

// fetchOnce sends req with token and decodes the answer into data. It
// reports a 401 answer as unauthorized and not as an error.
func (c *NanitClient) fetchOnce(req *http.Request, token string, data interface{}) (unauthorized bool, err error) {
	req.Header.Set("Authorization", token)

	res, clientErr := myClient.Do(req)
	if clientErr != nil {
		return false, temporary("HTTP request failed: %w", clientErr)
	}

	defer res.Body.Close()

	if res.StatusCode == 401 {
		return true, nil
	}

	if res.StatusCode != 200 {
		return false, statusError("HTTP request failed", res.StatusCode)
	}

	if err := decodeBody(res, data); err != nil {
		return false, fmt.Errorf("HTTP request failed: %w", err)
	}

	return false, nil
}

// FetchBabies - fetches baby list
func (c *NanitClient) FetchBabies() ([]baby.Baby, error) {
	log.Info().Msg("Fetching babies list")
	req, reqErr := http.NewRequest("GET", c.url("/babies"), nil)

	if reqErr != nil {
		return nil, fmt.Errorf("unable to create request: %w", reqErr)
	}

	data := new(babiesResponsePayload)
	if err := c.FetchAuthorized(req, data); err != nil {
		return nil, err
	}

	c.SessionStore.Update(func(s *session.Session) { s.Babies = data.Babies })
	return data.Babies, nil
}

// FetchMessages - fetches message list
func (c *NanitClient) FetchMessages(babyUID string, limit int) ([]message.Message, error) {
	req, reqErr := http.NewRequest("GET", c.url(fmt.Sprintf("/babies/%s/messages?limit=%d", babyUID, limit)), nil)

	if reqErr != nil {
		return nil, fmt.Errorf("unable to create request: %w", reqErr)
	}

	data := new(messagesResponsePayload)
	if err := c.FetchAuthorized(req, data); err != nil {
		return nil, err
	}

	return data.Messages, nil
}

// EnsureBabies - fetches baby list if not fetched already
func (c *NanitClient) EnsureBabies() ([]baby.Baby, error) {
	if babies := c.SessionStore.Snapshot().Babies; len(babies) > 0 {
		return babies, nil
	}

	return c.FetchBabies()
}

// FetchNewMessages - fetches 10 newest messages, ignores any messages which were already fetched or which are older than 5 minutes
func (c *NanitClient) FetchNewMessages(babyUID string, defaultMessageTimeout time.Duration) ([]message.Message, error) {
	fetchedMessages, err := c.FetchMessages(babyUID, 10)
	if err != nil {
		return nil, err
	}

	newMessages := make([]message.Message, 0)

	// return empty [] if there are no fetchedMessages
	if len(fetchedMessages) == 0 {
		log.Debug().Msg("No messages fetched")
		return newMessages, nil
	}

	// sort fetechedMessages starting with most recent
	sort.Slice(fetchedMessages, func(i, j int) bool {
		return fetchedMessages[i].Time.Time().After(fetchedMessages[j].Time.Time())
	})

	lastSeenMessageTime := c.SessionStore.Snapshot().LastSeenMessageTime
	messageTimeoutTime := lastSeenMessageTime
	log.Debug().Msgf("Last seen message time was %s", lastSeenMessageTime)

	// Don't know when last message was, set messageTimeout to default
	if lastSeenMessageTime.IsZero() {
		messageTimeoutTime = time.Now().UTC().Add(-defaultMessageTimeout)
	}

	// lastSeenMessageTime is older than most recent fetchedMessage, or is unset
	if lastSeenMessageTime.Before(fetchedMessages[0].Time.Time()) {
		c.rememberMessageTime(fetchedMessages[0].Time.Time())
	}

	// Only keep messages that are more recent than messageTimeoutTime
	filteredMessages := message.FilterMessages(fetchedMessages, func(message message.Message) bool {
		return message.Time.Time().After(messageTimeoutTime)
	})

	log.Debug().Msgf("Found %d new messages", len(filteredMessages))
	log.Debug().Msgf("%+v\n", filteredMessages)

	return filteredMessages, nil
}

// rememberMessageTime stores the time of the newest message seen, unless
// the session already holds a later one. The polls of all cameras share
// this time, and the poll of another camera may have stored a later time
// since this one read it. The time must not go back, or that camera
// would report its newest messages a second time.
func (c *NanitClient) rememberMessageTime(newest time.Time) {
	c.SessionStore.Update(func(s *session.Session) {
		if s.LastSeenMessageTime.Before(newest) {
			s.LastSeenMessageTime = newest
		}
	})
}
