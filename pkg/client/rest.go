package client

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sort"
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
// Every other error of the authorization means that trying again as we
// are will not help, for example credentials the server refuses.
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

// Authorizer - gives the session a valid auth token
type Authorizer interface {
	// MaybeAuthorize - performs authorization if there is no token, if the
	// token is assumed to be expired, or if force is set
	MaybeAuthorize(force bool) error
}

// AuthorizeWithRetry calls MaybeAuthorize until it succeeds. After a
// temporary failure it waits and tries again: first for waits[0], then for
// waits[1], and so on, and for the last entry from then on.
//
// It returns nil after a success. It returns the error of a failure that
// is not temporary at once, because trying again as we are does not help
// then. It returns the last temporary error when done closes first.
func AuthorizeWithRetry(a Authorizer, force bool, done <-chan struct{}, waits []time.Duration) error {
	for try := 0; ; try++ {
		err := a.MaybeAuthorize(force)
		if err == nil || !IsTemporary(err) {
			return err
		}

		wait := waits[utils.MinInt(try, len(waits)-1)]
		log.Error().Err(err).Str("retry_in", wait.String()).Msg("Unable to authorize, will try again")

		timer := time.NewTimer(wait)
		select {
		case <-done:
			timer.Stop()
			return err
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
}

func (c *NanitClient) url(path string) string {
	if c.BaseURL != "" {
		return c.BaseURL + path
	}

	return defaultAPIBaseURL + path
}

// MaybeAuthorize - Performs authorization if we don't have token or we assume it is expired
//
// It returns an error if the authorization was needed and failed. Use
// IsTemporary to tell a failure that is worth another try from one that
// is not.
func (c *NanitClient) MaybeAuthorize(force bool) error {
	if force || c.SessionStore.Session.AuthToken == "" || time.Since(c.SessionStore.Session.AuthTime) > AuthTokenTimelife {
		return c.Authorize()
	}

	return nil
}

// Authorize - performs authorization attempt
func (c *NanitClient) Authorize() error {
	if len(c.SessionStore.Session.RefreshToken) == 0 {
		c.SessionStore.Session.RefreshToken = c.RefreshToken
	}

	if len(c.SessionStore.Session.RefreshToken) > 0 {
		err := c.RenewSession() // We have a refresh token, so we'll use that to extend our session
		if err == nil {
			return nil
		}
		if !errors.Is(err, ErrExpiredRefreshToken) {
			return err
		}
	}

	return c.Login() // We don't have a refresh token, e.g. initial login so we need to supply username/password
}

// Renews an existing session using a valid refresh token
// If the refresh token has also expired, we need to perform a full re-login
func (c *NanitClient) RenewSession() error {
	requestBody, requestBodyErr := json.Marshal(map[string]string{
		"refresh_token": c.SessionStore.Session.RefreshToken,
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
		log.Warn().Msg("Server responded with code 404. This typically means your refresh token has expired. Will try to login with username/password")
		return ErrExpiredRefreshToken
	} else if r.StatusCode > 299 || r.StatusCode < 200 {
		return statusError("unable to renew session", r.StatusCode)
	}

	return c.storeAuthResponse(r)
}

func (c *NanitClient) Login() error {
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
	r, clientErr := myClient.Do(req)
	if clientErr != nil {
		return temporary("unable to fetch auth token: %w", clientErr)
	}

	defer r.Body.Close()

	if r.StatusCode == 401 {
		return errors.New("server responded with code 401. Provided credentials has not been accepted by the server. Please check if your e-mail address and password is entered correctly and that 2FA is disabled on your account")
	} else if r.StatusCode != 201 {
		return statusError("unable to fetch auth token", r.StatusCode)
	}

	return c.storeAuthResponse(r)
}

// storeAuthResponse reads the tokens of a successful answer and saves
// them in the session.
func (c *NanitClient) storeAuthResponse(r *http.Response) error {
	authResponse := new(authResponsePayload)

	jsonErr := json.NewDecoder(r.Body).Decode(authResponse)
	if jsonErr != nil {
		// The status said success, so a body we cannot read is a fault
		// of the transfer or of the server, not of what we sent.
		return temporary("unable to decode auth response: %w", jsonErr)
	}

	if authResponse.AccessToken == "" {
		return temporary("auth response carries no access token")
	}

	log.Info().Str("token", utils.AnonymizeToken(authResponse.AccessToken, 4)).Msg("Authorized")
	log.Info().Str("refresh_token", utils.AnonymizeToken(authResponse.RefreshToken, 4)).Msg("Retreived")
	c.SessionStore.Session.AuthToken = authResponse.AccessToken
	if authResponse.RefreshToken != "" {
		c.SessionStore.Session.RefreshToken = authResponse.RefreshToken
	}
	c.SessionStore.Session.AuthTime = time.Now()
	c.SessionStore.Save()

	return nil
}

// FetchAuthorized - makes authorized http request
func (c *NanitClient) FetchAuthorized(req *http.Request, data interface{}) {
	for i := 0; i < 2; i++ {
		if c.SessionStore.Session.AuthToken != "" {
			req.Header.Set("Authorization", c.SessionStore.Session.AuthToken)

			res, clientErr := myClient.Do(req)
			if clientErr != nil {
				log.Fatal().Err(clientErr).Msg("HTTP request failed")
			}

			defer res.Body.Close()

			if res.StatusCode != 401 {
				if res.StatusCode != 200 {
					log.Fatal().Int("code", res.StatusCode).Msg("Server responded with unexpected status code")
				}

				jsonErr := json.NewDecoder(res.Body).Decode(data)
				if jsonErr != nil {
					log.Fatal().Err(jsonErr).Msg("Unable to decode response")
				}

				return
			}

			log.Info().Msg("Token might be expired. Will try to re-authenticate.")
		}

		if err := c.Authorize(); err != nil {
			log.Fatal().Err(err).Msg("Unable to authorize")
		}
	}

	log.Fatal().Msg("Unable to make request due failed authorization (2 attempts).")
}

// FetchBabies - fetches baby list
func (c *NanitClient) FetchBabies() []baby.Baby {
	log.Info().Msg("Fetching babies list")
	req, reqErr := http.NewRequest("GET", c.url("/babies"), nil)

	if reqErr != nil {
		log.Fatal().Err(reqErr).Msg("Unable to create request")
	}

	data := new(babiesResponsePayload)
	c.FetchAuthorized(req, data)

	c.SessionStore.Session.Babies = data.Babies
	c.SessionStore.Save()
	return data.Babies
}

// FetchMessages - fetches message list
func (c *NanitClient) FetchMessages(babyUID string, limit int) []message.Message {
	req, reqErr := http.NewRequest("GET", c.url(fmt.Sprintf("/babies/%s/messages?limit=%d", babyUID, limit)), nil)

	if reqErr != nil {
		log.Fatal().Err(reqErr).Msg("Unable to create request")
	}

	data := new(messagesResponsePayload)
	c.FetchAuthorized(req, data)

	return data.Messages
}

// EnsureBabies - fetches baby list if not fetched already
func (c *NanitClient) EnsureBabies() []baby.Baby {
	if len(c.SessionStore.Session.Babies) == 0 {
		return c.FetchBabies()
	}

	return c.SessionStore.Session.Babies
}

// FetchNewMessages - fetches 10 newest messages, ignores any messages which were already fetched or which are older than 5 minutes
func (c *NanitClient) FetchNewMessages(babyUID string, defaultMessageTimeout time.Duration) []message.Message {
	fetchedMessages := c.FetchMessages(babyUID, 10)
	newMessages := make([]message.Message, 0)

	// return empty [] if there are no fetchedMessages
	if len(fetchedMessages) == 0 {
		log.Debug().Msg("No messages fetched")
		return newMessages
	}

	// sort fetechedMessages starting with most recent
	sort.Slice(fetchedMessages, func(i, j int) bool {
		return fetchedMessages[i].Time.Time().After(fetchedMessages[j].Time.Time())
	})

	lastSeenMessageTime := c.SessionStore.Session.LastSeenMessageTime
	messageTimeoutTime := lastSeenMessageTime
	log.Debug().Msgf("Last seen message time was %s", lastSeenMessageTime)

	// Don't know when last message was, set messageTimeout to default
	if lastSeenMessageTime.IsZero() {
		messageTimeoutTime = time.Now().UTC().Add(-defaultMessageTimeout)
	}

	// lastSeenMessageTime is older than most recent fetchedMessage, or is unset
	if lastSeenMessageTime.Before(fetchedMessages[0].Time.Time()) {
		lastSeenMessageTime = fetchedMessages[0].Time.Time()
		c.SessionStore.Session.LastSeenMessageTime = lastSeenMessageTime
		c.SessionStore.Save()
	}

	// Only keep messages that are more recent than messageTimeoutTime
	filteredMessages := message.FilterMessages(fetchedMessages, func(message message.Message) bool {
		return message.Time.Time().After(messageTimeoutTime)
	})

	log.Debug().Msgf("Found %d new messages", len(filteredMessages))
	log.Debug().Msgf("%+v\n", filteredMessages)

	return filteredMessages
}
