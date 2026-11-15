package main

import (
	"os"
	"os/signal"
	"regexp"
	"time"

	"github.com/rs/zerolog/log"
	"github.com/indiefan/home_assistant_nanit/pkg/app"
	"github.com/indiefan/home_assistant_nanit/pkg/client"
	"github.com/indiefan/home_assistant_nanit/pkg/mqtt"
	"github.com/indiefan/home_assistant_nanit/pkg/utils"
)

// websocketOpts reads the settings of the websocket connection to the
// camera.
func websocketOpts() app.WebsocketOpts {
	opts := app.WebsocketOpts{
		PingInterval: utils.EnvVarSeconds("NANIT_WS_PING_INTERVAL", client.DefaultPingInterval),
		IdleTimeout:  utils.EnvVarSeconds("NANIT_WS_IDLE_TIMEOUT", client.DefaultIdleTimeout),
	}

	if opts.PingInterval <= 0 {
		log.Fatal().Msg("Invalid NANIT_WS_PING_INTERVAL. It must be 1 or more.")
	}

	if opts.IdleTimeout == 0 {
		// 0 turns the idle limit off
		opts.IdleTimeout = -1
	} else if opts.IdleTimeout <= opts.PingInterval {
		log.Fatal().Msg("Invalid NANIT_WS_IDLE_TIMEOUT. It must be longer than NANIT_WS_PING_INTERVAL, or 0 to turn it off.")
	}

	return opts
}

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
			ListenAddr: m[1],
			PublicAddr: publicAddr,
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

	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, os.Interrupt)

	instance := app.NewApp(opts)

	runner := utils.RunWithGracefulCancel(instance.Run)

	<-interrupt
	log.Warn().Msg("Received interrupt signal, terminating")

	waitForCleanup := make(chan struct{}, 1)

	go func() {
		runner.Cancel()
		close(waitForCleanup)
	}()

	select {
	case <-interrupt:
		log.Fatal().Msg("Received another interrupt signal, forcing termination without clean up")
	case <-waitForCleanup:
		log.Info().Msg("Clean exit")
		return
	}
}
