package main

import (
	"os"
	"os/signal"
	"syscall"
	"regexp"
	"time"

	"github.com/rs/zerolog/log"
	"github.com/indiefan/home_assistant_nanit/pkg/app"
	"github.com/indiefan/home_assistant_nanit/pkg/client"
	"github.com/indiefan/home_assistant_nanit/pkg/mqtt"
	"github.com/indiefan/home_assistant_nanit/pkg/rtmpserver"
	"github.com/indiefan/home_assistant_nanit/pkg/utils"
)

// websocketOpts reads the settings of the websocket connection to the
// camera.
func websocketOpts() app.WebsocketOpts {
	opts := app.WebsocketOpts{
		PingInterval: utils.EnvVarSeconds("NANIT_WS_PING_INTERVAL", client.DefaultPingInterval),
	}

	if opts.PingInterval <= 0 {
		log.Fatal().Msg("Invalid NANIT_WS_PING_INTERVAL. It must be 1 or more.")
	}

	// Without the setting, the idle limit stays at zero, and the client
	// works out a default that fits the ping interval
	if _, found := os.LookupEnv("NANIT_WS_IDLE_TIMEOUT"); !found {
		return opts
	}

	opts.IdleTimeout = utils.EnvVarSeconds("NANIT_WS_IDLE_TIMEOUT", 0)

	if opts.IdleTimeout == 0 {
		// 0 turns the idle limit off. In the options, a negative value
		// says so, because 0 stands for the default there.
		opts.IdleTimeout = -1
	} else if opts.IdleTimeout < 2*opts.PingInterval {
		// With less than two intervals, one answer to a ping that is
		// late would end a connection that works
		log.Fatal().Msgf("NANIT_WS_IDLE_TIMEOUT (%v) must be at least two times NANIT_WS_PING_INTERVAL (%v), or 0 to turn the limit off", opts.IdleTimeout, opts.PingInterval)
	}

	return opts
}

// defaultShutdownTimeout - time that the clean up gets after a signal to
// stop. A container is killed 10 seconds after it was told to stop,
// unless it is set up otherwise, so the program must be gone before that.
//
// A request to the Nanit API that is running is waited for, and it can
// take longer than this. Where the container gets more time to stop,
// NANIT_SHUTDOWN_TIMEOUT can give the clean up more time too.
const defaultShutdownTimeout = 8 * time.Second

func main() {
	initLogger()
	logAppVersion()
	utils.LoadDotEnvFile()
	setLogLevel()

	opts := app.Opts{
		NanitCredentials: app.NanitCredentials{
			Email:        utils.EnvVarStr("NANIT_EMAIL", ""),
			Password:     utils.EnvVarStr("NANIT_PASSWORD", ""),
			RefreshToken: utils.EnvVarStr("NANIT_REFRESH_TOKEN", ""),
		},
		SessionFile:     utils.EnvVarStr("NANIT_SESSION_FILE", "/data/session.json"),
		DataDirectories: ensureDataDirectories(),
		HTTPEnabled:     false,
		EventPolling: app.EventPollingOpts{
			// Event message polling disabled by default
			Enabled: utils.EnvVarBool("NANIT_EVENTS_POLLING", false),
			// 30 second default polling interval
			PollingInterval: utils.EnvVarSeconds("NANIT_EVENTS_POLLING_INTERVAL", 30*time.Second),
			// 300 second (5 min) default message timeout (unseen messages are ignored once they are this old)
			MessageTimeout: utils.EnvVarSeconds("NANIT_EVENTS_MESSAGE_TIMEOUT", 300*time.Second),
		},
		Websocket: websocketOpts(),
	}

	if utils.EnvVarBool("NANIT_RTMP_ENABLED", true) {
		publicAddr := utils.EnvVarReqStr("NANIT_RTMP_ADDR")
		m := regexp.MustCompile("(:[0-9]+)$").FindStringSubmatch(publicAddr)
		if len(m) != 2 {
			log.Fatal().Msg("Invalid NANIT_RTMP_ADDR. Unable to parse port.")
		}

		opts.RTMP = &app.RTMPOpts{
			ListenAddr:  m[1],
			PublicAddr:  publicAddr,
			ReadTimeout: utils.EnvVarSeconds("NANIT_RTMP_READ_TIMEOUT", rtmpserver.DefaultReadTimeout),
		}

		if opts.RTMP.ReadTimeout < 0 {
			log.Fatal().Msg("Invalid NANIT_RTMP_READ_TIMEOUT. It must be 0 or more.")
		}
	}

	if utils.EnvVarBool("NANIT_MQTT_ENABLED", false) {
		opts.MQTT = &mqtt.Opts{
			BrokerURL:   utils.EnvVarReqStr("NANIT_MQTT_BROKER_URL"),
			ClientID:    utils.EnvVarStr("NANIT_MQTT_CLIENT_ID", "nanit"),
			Username:    utils.EnvVarStr("NANIT_MQTT_USERNAME", ""),
			Password:    utils.EnvVarStr("NANIT_MQTT_PASSWORD", ""),
			TopicPrefix: utils.EnvVarStr("NANIT_MQTT_PREFIX", "nanit"),
		}
	}

	if opts.EventPolling.Enabled {
		log.Info().Msgf("Event polling enabled with an interval of %v", opts.EventPolling.PollingInterval)
	}

	shutdownTimeout := utils.EnvVarSeconds("NANIT_SHUTDOWN_TIMEOUT", defaultShutdownTimeout)
	if shutdownTimeout <= 0 {
		log.Fatal().Msg("Invalid NANIT_SHUTDOWN_TIMEOUT. It must be 1 or more.")
	}

	// SIGTERM is what a container gets when it is stopped. Without a
	// handler for it, the program was ended without any clean up, and
	// the camera kept the place for its stream.
	interrupt := make(chan os.Signal, 2)
	signal.Notify(interrupt, os.Interrupt, syscall.SIGTERM)

	instance := app.NewApp(opts)

	runner := utils.RunWithGracefulCancel(instance.Run)

	received := <-interrupt
	log.Warn().Stringer("signal", received).Msg("Received a signal to stop, terminating")

	waitForCleanup := make(chan struct{}, 1)

	go func() {
		runner.Cancel()
		close(waitForCleanup)
	}()

	select {
	case <-interrupt:
		log.Fatal().Msg("Received another signal to stop, forcing termination without clean up")
	case <-time.After(shutdownTimeout):
		log.Fatal().Str("waited", shutdownTimeout.String()).Msg("The clean up did not finish in time, forcing termination")
	case <-waitForCleanup:
		log.Info().Msg("Clean exit")
		return
	}
}
