package push

import (
	"bytes"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/indiefan/home_assistant_nanit/pkg/session"
)

// The Nanit app for Android, as the Firebase service knows it. The Nanit
// service sends its notifications to devices that are registered for this
// app, so this program registers as one. The API key of the app belongs
// to the same set. It is a setting, and not in this file.
const (
	firebaseAppID     = "1:25705829844:android:83a90cd56c7431e0"
	firebaseProjectID = "nanit-144706"
	firebaseSenderID  = "25705829844"
	androidPackage    = "com.nanit.baby"
	androidCertSHA1   = "5099ace03019d88c8115fa90a37b2ed014bbea42"
)

// What the Firebase library of an Android app says about itself. The
// service expects these with a registration for an Android app. The
// values are those of a phone, as other programs that receive push
// notifications send them.
const (
	firebaseClient   = "android-min-sdk/23 fire-core/20.0.0 device-name/a21snnxx device-brand/samsung device-model/a21s android-installer/com.android.vending fire-android/30 fire-installations/17.0.0 fire-fcm/22.0.0 android-platform/ kotlin/1.9.23 android-target-sdk/34"
	androidUserAgent = "Dalvik/2.1.0 (Linux; U; Android 11; SM-A217F Build/RP1A.200720.012)"
)

const (
	// registerTimeout - time limit for each request of the registration
	registerTimeout = 20 * time.Second

	// registerTries - the last request is refused now and then for no
	// reason that it names, and accepted a moment later
	registerTries = 5
	registerPause = 2 * time.Second

	// maxAnswerSize - the answers are a few kilobytes at most
	maxAnswerSize = 1 << 20
)

// firebase registers a device at the Firebase service.
type firebase struct {
	APIKey string

	// The addresses of the service. Empty means the real ones.
	InstallationsURL string
	CheckinURL       string
	RegisterURL      string

	// pause - time between two tries of the last request. Zero means
	// registerPause.
	pause time.Duration
}

var firebaseHTTP = &http.Client{Timeout: registerTimeout}

func registerWithFirebase(apiKey string) (session.Push, error) {
	return (&firebase{APIKey: apiKey}).Register()
}

// Register makes a new device and returns what is needed to connect as
// that device, and the token that the Nanit service needs to send
// notifications to it. It takes three requests:
//
//  1. an installation of the app, which gives a proof that the next
//     requests come from an app that the service knows
//  2. a new device with its secret (check-in)
//  3. a token for notifications to the app on that device
func (f *firebase) Register() (session.Push, error) {
	installation, err := f.install()
	if err != nil {
		return session.Push{}, fmt.Errorf("installation: %w", err)
	}

	androidID, securityToken, err := f.checkin()
	if err != nil {
		return session.Push{}, fmt.Errorf("check-in: %w", err)
	}

	pause := f.pause
	if pause == 0 {
		pause = registerPause
	}

	var token string

	for try := 1; ; try++ {
		token, err = f.token(installation, androidID, securityToken)
		if err == nil {
			break
		}

		if try == registerTries {
			return session.Push{}, fmt.Errorf("token: %w", err)
		}

		time.Sleep(pause)
	}

	return session.Push{AndroidID: androidID, SecurityToken: securityToken, Token: token}, nil
}

// send sends a request and returns the body of an answer with status 200.
func send(req *http.Request) ([]byte, error) {
	res, err := firebaseHTTP.Do(req)
	if err != nil {
		return nil, err
	}

	defer res.Body.Close()

	body, err := io.ReadAll(io.LimitReader(res.Body, maxAnswerSize))
	if err != nil {
		return nil, err
	}

	if res.StatusCode != http.StatusOK {
		// The body says why, for example that the API key is not valid
		return nil, fmt.Errorf("status %v: %.300s", res.StatusCode, body)
	}

	return body, nil
}

// newInstallationID returns a new ID for an installation of an app, in
// the form that Firebase gives it: 22 characters, of which the first one
// stands for the bits 0111.
func newInstallationID() (string, error) {
	random := make([]byte, 17)
	if _, err := rand.Read(random); err != nil {
		return "", err
	}

	random[0] = 0b01110000 | random[0]&0b00001111

	return base64.URLEncoding.EncodeToString(random)[:22], nil
}

func (f *firebase) install() (authToken string, err error) {
	id, err := newInstallationID()
	if err != nil {
		return "", err
	}

	body, err := json.Marshal(map[string]string{
		"fid":         id,
		"appId":       firebaseAppID,
		"authVersion": "FIS_v2",
		"sdkVersion":  "a:17.0.0",
	})
	if err != nil {
		return "", err
	}

	address := f.InstallationsURL
	if address == "" {
		address = "https://firebaseinstallations.googleapis.com/v1/projects/" + firebaseProjectID + "/installations"
	}

	req, err := http.NewRequest("POST", address, bytes.NewReader(body))
	if err != nil {
		return "", err
	}

	req.Header.Set("Accept", "application/json")
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("x-goog-api-key", f.APIKey)
	req.Header.Set("X-Android-Package", androidPackage)
	req.Header.Set("X-Android-Cert", androidCertSHA1)
	req.Header.Set("x-firebase-client", firebaseClient)
	req.Header.Set("x-firebase-client-log-type", "3")
	req.Header.Set("User-Agent", androidUserAgent)

	answer, err := send(req)
	if err != nil {
		return "", err
	}

	var installation struct {
		AuthToken struct {
			Token string `json:"token"`
		} `json:"authToken"`
	}
	if err := json.Unmarshal(answer, &installation); err != nil {
		return "", err
	}

	if installation.AuthToken.Token == "" {
		return "", errors.New("the answer holds no token")
	}

	return installation.AuthToken.Token, nil
}

func (f *firebase) checkin() (androidID uint64, securityToken uint64, err error) {
	address := f.CheckinURL
	if address == "" {
		address = "https://android.clients.google.com/checkin"
	}

	req, err := http.NewRequest("POST", address, bytes.NewReader(encodeCheckinRequest()))
	if err != nil {
		return 0, 0, err
	}

	req.Header.Set("Content-Type", "application/x-protobuf")
	// An empty value: no User-Agent header is sent
	req.Header.Set("User-Agent", "")

	answer, err := send(req)
	if err != nil {
		return 0, 0, err
	}

	return decodeCheckinResponse(answer)
}

func (f *firebase) token(installation string, androidID uint64, securityToken uint64) (string, error) {
	device := strconv.FormatUint(androidID, 10)

	form := url.Values{}
	form.Set("X-subtype", firebaseSenderID)
	form.Set("sender", firebaseSenderID)
	form.Set("device", device)
	form.Set("app", androidPackage)
	form.Set("cert", androidCertSHA1)
	form.Set("app_ver", "1")
	form.Set("X-app_ver", "1")
	form.Set("X-app_ver_name", "1")
	form.Set("target_ver", "31")
	form.Set("X-osv", "29")
	form.Set("X-cliv", "fiid-21.1.1")
	form.Set("X-gmsv", "220217001")
	form.Set("X-scope", "*")
	form.Set("X-gms_app_id", firebaseAppID)
	form.Set("X-Goog-Firebase-Installations-Auth", installation)
	form.Set("X-Firebase-Client", firebaseClient)
	form.Set("X-Firebase-Client-Log-Type", "1")

	address := f.RegisterURL
	if address == "" {
		address = "https://android.clients.google.com/c2dm/register3"
	}

	req, err := http.NewRequest("POST", address, strings.NewReader(form.Encode()))
	if err != nil {
		return "", err
	}

	req.Header.Set("Authorization", "AidLogin "+device+":"+strconv.FormatUint(securityToken, 10))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("User-Agent", "")

	answer, err := send(req)
	if err != nil {
		return "", err
	}

	// The answer is one line: token=... or Error=...
	values, err := url.ParseQuery(strings.TrimSpace(string(answer)))
	if err != nil {
		return "", err
	}

	if reason := values.Get("Error"); reason != "" {
		return "", fmt.Errorf("the service refuses the registration: %v", reason)
	}

	if values.Get("token") == "" {
		return "", errors.New("the answer holds no token")
	}

	return values.Get("token"), nil
}
