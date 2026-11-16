package app

import (
	"github.com/indiefan/home_assistant_nanit/pkg/mqtt"
	"time"
)

// Opts - application run options
type Opts struct {
	NanitCredentials NanitCredentials
	SessionFile      string
	DataDirectories  DataDirectories
	HTTPEnabled      bool
	MQTT             *mqtt.Opts
	RTMP             *RTMPOpts
	EventPolling     EventPollingOpts
	Websocket        WebsocketOpts
}

// WebsocketOpts - options for the websocket connection to the camera.
// A zero value means the default of the client package.
type WebsocketOpts struct {
	// PingInterval - time between two websocket pings
	PingInterval time.Duration

	// IdleTimeout - a connection on which nothing has arrived for this
	// long is given up and made again. Negative turns this off.
	IdleTimeout time.Duration
}

// NanitCredentials - user credentials for Nanit account
type NanitCredentials struct {
	Email        string
	Password     string
	RefreshToken string
}

// DataDirectories - dictionary of dir paths
type DataDirectories struct {
	BaseDir  string
	VideoDir string
	LogDir   string
}

// RTMPOpts - options for RTMP streaming
type RTMPOpts struct {
	// IP:Port of the interface on which we should listen
	ListenAddr string

	// IP:Port under which can Cam reach the RTMP server
	PublicAddr string

	// ReadTimeout - a stream from which nothing has arrived for this
	// long has ended, and a new one is asked for. 0 means the default
	// of the receiver, and a negative value means no limit.
	ReadTimeout time.Duration
}

type EventPollingOpts struct {
	Enabled         bool
	PollingInterval time.Duration
	MessageTimeout  time.Duration
}
