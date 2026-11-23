package mqtt

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	MQTT "github.com/eclipse/paho.mqtt.golang"
	"github.com/indiefan/home_assistant_nanit/pkg/activity"
	"github.com/indiefan/home_assistant_nanit/pkg/baby"
	"github.com/indiefan/home_assistant_nanit/pkg/push"
	"github.com/indiefan/home_assistant_nanit/pkg/utils"
	"github.com/rs/zerolog/log"
)

// publishTimeout - time that a message gets to leave for the broker
const publishTimeout = 5 * time.Second

type SendLightCommandHandler func(nightLightState bool)
type SendStandbyCommandHandler func(standbyState bool)

// Connection - MQTT context
type Connection struct {
	Opts         Opts
	StateManager *baby.StateManager

	// NotificationBabies - the babies whose push notifications are
	// published. Home Assistant is told about an entity for each of them.
	// Set it before Run.
	NotificationBabies []baby.Baby

	// client is made with the connection and never replaced, so that
	// PublishNotification can use it from another goroutine than Run
	client                    MQTT.Client
	sendLightCommandHandler   SendLightCommandHandler
	sendStandbyCommandHandler SendStandbyCommandHandler
}

// NewConnection - constructor
func NewConnection(opts Opts) *Connection {
	clientOpts := MQTT.NewClientOptions()
	clientOpts.AddBroker(opts.BrokerURL)
	clientOpts.SetClientID(opts.TopicPrefix)
	clientOpts.SetUsername(opts.Username)
	clientOpts.SetPassword(opts.Password)
	clientOpts.SetCleanSession(false)

	return &Connection{
		Opts:   opts,
		client: MQTT.NewClient(clientOpts),
	}
}

// Run - runs the mqtt connection handler
func (conn *Connection) Run(manager *baby.StateManager, ctx utils.GracefulContext) {
	conn.StateManager = manager

	utils.RunWithPerseverance(func(attempt utils.AttemptContext) {
		runMqtt(conn, attempt)
	}, ctx, utils.PerseverenceOpts{
		RunnerID:       "mqtt",
		ResetThreshold: 2 * time.Second,
		Cooldown: []time.Duration{
			2 * time.Second,
			10 * time.Second,
			1 * time.Minute,
		},
	})
}

func (conn *Connection) RegisterLightHandler(sendLightCommandHandler SendLightCommandHandler) {
	conn.sendLightCommandHandler = sendLightCommandHandler
}

func (conn *Connection) subscribeToLightCommand() {
	commandTopic := fmt.Sprintf("%v/babies/+/night_light/switch", conn.Opts.TopicPrefix)
	log.Debug().
		Str("topic", commandTopic).
		Msg("Subscribing to command topic")

	lightMessageHandler := func(mqttConn MQTT.Client, msg MQTT.Message) {
		// Extract baby UID and command from topic
		parts := strings.Split(msg.Topic(), "/")
		if len(parts) < 4 {
			log.Error().Str("topic", msg.Topic()).Msg("Invalid command topic format")
			return
		}

		babyUID := parts[2]
		command := parts[4]

		// Validate baby UID
		baby.EnsureValidBabyUID(babyUID)

		// Handle different commands
		switch command {
		case "switch":
			enabled := string(msg.Payload()) == "true"
			log.Debug().
				Str("baby", babyUID).
				Bool("enabled", enabled).
				Str("payload", string(msg.Payload())).
				Msg("Received light command")

			conn.sendLightCommandHandler(enabled)
		default:
			log.Warn().Str("command", command).Msg("Unknown command received")
		}
	}

	if token := conn.client.Subscribe(commandTopic, 0, lightMessageHandler); token.Wait() && token.Error() != nil {
		log.Error().Err(token.Error()).Str("topic", commandTopic).Msg("Failed to subscribe to command topic")
	}
}

func (conn *Connection) RegisterStandyHandler(sendStandbyCommandHandler SendStandbyCommandHandler) {
	conn.sendStandbyCommandHandler = sendStandbyCommandHandler
}

func (conn *Connection) subscribeToStandbyCommand() {
	commandTopic := fmt.Sprintf("%v/babies/+/standby/switch", conn.Opts.TopicPrefix)
	log.Debug().
		Str("topic", commandTopic).
		Msg("Subscribing to command topic")

	standbyMessageHandler := func(mqttConn MQTT.Client, msg MQTT.Message) {
		// Extract baby UID and command from topic
		parts := strings.Split(msg.Topic(), "/")
		if len(parts) < 4 {
			log.Error().Str("topic", msg.Topic()).Msg("Invalid command topic format")
			return
		}

		babyUID := parts[2]
		command := parts[4]

		// Validate baby UID
		baby.EnsureValidBabyUID(babyUID)

		// Handle different commands
		switch command {
		case "switch":
			enabled := string(msg.Payload()) == "true"
			log.Debug().
				Str("baby", babyUID).
				Bool("enabled", enabled).
				Str("payload", string(msg.Payload())).
				Msg("Received standby command")

			conn.sendStandbyCommandHandler(enabled)
		default:
			log.Warn().Str("command", command).Msg("Unknown command received")
		}
	}

	if token := conn.client.Subscribe(commandTopic, 0, standbyMessageHandler); token.Wait() && token.Error() != nil {
		log.Error().Err(token.Error()).Str("topic", commandTopic).Msg("Failed to subscribe to command topic")
	}
}

func runMqtt(conn *Connection, attempt utils.AttemptContext) {

	if token := conn.client.Connect(); token.Wait() && token.Error() != nil {
		log.Error().Str("broker_url", conn.Opts.BrokerURL).Err(token.Error()).Msg("Unable to connect to MQTT broker")
		attempt.Fail(token.Error())
		return
	}

	log.Info().Str("broker_url", conn.Opts.BrokerURL).Msg("Successfully connected to MQTT broker")

	conn.announceNotifications()

	unsubscribe := conn.StateManager.Subscribe(func(babyUID string, state baby.State) {
		publish := func(key string, value interface{}) {
			topic := fmt.Sprintf("%v/babies/%v/%v", conn.Opts.TopicPrefix, babyUID, key)
			log.Trace().Str("topic", topic).Interface("value", value).Msg("MQTT publish")

			token := conn.client.Publish(topic, 0, false, fmt.Sprintf("%v", value))
			if token.Wait(); token.Error() != nil {
				log.Error().Err(token.Error()).Msgf("Unable to publish %v update", key)
			}
		}

		for key, value := range state.AsMap(false) {
			publish(key, value)
		}

		if state.StreamState != nil && *state.StreamState != baby.StreamState_Unknown {
			publish("is_stream_alive", *state.StreamState == baby.StreamState_Alive)
		}
	})

	// Subscribe to accept light mqtt messages
	conn.subscribeToLightCommand()
	conn.subscribeToStandbyCommand()

	// Wait until interrupt signal is received
	<-attempt.Done()

	log.Debug().Msg("Closing MQTT connection on interrupt")
	unsubscribe()
	conn.client.Disconnect(250)
}

// notificationTopic - the topic for the push notifications about a baby
func (conn *Connection) notificationTopic(babyUID string) string {
	return fmt.Sprintf("%v/babies/%v/notification", conn.Opts.TopicPrefix, babyUID)
}

// PublishNotification publishes a push notification of the Nanit service
// about one of NotificationBabies. A notification that is published while
// there is no connection to the broker is lost.
func (conn *Connection) PublishNotification(notification push.Notification) {
	known := false
	for _, b := range conn.NotificationBabies {
		known = known || b.UID == notification.BabyUID
	}

	if !known {
		// The topic and the entity are those of a baby
		log.Warn().Str("type", notification.Type).Str("baby_uid", notification.BabyUID).Msg("Push notification is not about a baby of this account, not published")
		return
	}

	payload, err := notificationPayload(notification)
	if err != nil {
		log.Error().Err(err).Str("type", notification.Type).Msg("Unable to publish a push notification")
		return
	}

	token := conn.client.Publish(conn.notificationTopic(notification.BabyUID), 0, false, payload)
	if !token.WaitTimeout(publishTimeout) {
		log.Error().Str("type", notification.Type).Msg("Unable to publish a push notification: the broker did not take it in time")
	} else if token.Error() != nil {
		log.Error().Err(token.Error()).Str("type", notification.Type).Msg("Unable to publish a push notification")
	}
}

// notificationPayload builds the message for a push notification: a JSON
// object with everything that the notification carries, and event_type,
// which is what an event entity of Home Assistant reads.
func notificationPayload(notification push.Notification) ([]byte, error) {
	payload := make(map[string]interface{}, len(notification.Fields)+1)
	for key, value := range notification.Fields {
		payload[key] = value
	}

	payload["event_type"] = notification.Kind()

	return json.Marshal(payload)
}

// announceNotifications tells Home Assistant about one event entity for
// each of NotificationBabies. Home Assistant reads this from a topic that
// it watches (MQTT discovery), and the broker keeps the message for a
// Home Assistant that starts later.
func (conn *Connection) announceNotifications() {
	for _, b := range conn.NotificationBabies {
		topic, payload, err := notificationDiscovery(b, conn.notificationTopic(b.UID))
		if err != nil {
			log.Error().Err(err).Str("baby_uid", b.UID).Msg("Unable to announce the notifications to Home Assistant")
			continue
		}

		token := conn.client.Publish(topic, 1, true, payload)
		if !token.WaitTimeout(publishTimeout) {
			log.Error().Str("topic", topic).Msg("Unable to announce the notifications to Home Assistant: the broker did not take the message in time")
		} else if token.Error() != nil {
			log.Error().Err(token.Error()).Str("topic", topic).Msg("Unable to announce the notifications to Home Assistant")
		}
	}
}

// notificationDiscovery returns the topic and the message that make Home
// Assistant create the event entity for the notifications about a baby.
func notificationDiscovery(b baby.Baby, stateTopic string) (topic string, payload []byte, err error) {
	id := "nanit_" + b.UID

	name := "Nanit"
	if b.Name != "" {
		name = "Nanit " + b.Name
	}

	payload, err = json.Marshal(map[string]interface{}{
		"name":        "Notification",
		"unique_id":   id + "_notification",
		"state_topic": stateTopic,
		"event_types": push.Kinds(),
		"icon":        "mdi:bell-ring",
		"device": map[string]interface{}{
			"identifiers":  []string{id},
			"name":         name,
			"manufacturer": "Nanit",
		},
	})

	return fmt.Sprintf("homeassistant/event/%v/notification/config", id), payload, err
}

// PublishActivity publishes an event of the activity list of the Nanit
// service to the topic for the events of the baby. It returns an error
// when the broker has not confirmed the message, so that the caller can
// publish it again later. The receiver can therefore get an event more
// than once, and must go by its uid.
func (conn *Connection) PublishActivity(babyUID string, event activity.Event, backfill bool) error {
	payload, err := activityPayload(event, backfill)
	if err != nil {
		return err
	}

	topic := fmt.Sprintf("%v/babies/%v/event", conn.Opts.TopicPrefix, babyUID)

	token := conn.client.Publish(topic, 1, false, payload)
	if !token.WaitTimeout(publishTimeout) {
		return errors.New("the broker did not confirm the message in time")
	}

	return token.Error()
}

// activityPayload builds the message for an event of the activity list.
func activityPayload(event activity.Event, backfill bool) ([]byte, error) {
	return json.Marshal(map[string]interface{}{
		"uid":        event.UID,
		"key":        event.Key,
		"detail":     event.InternalKey,
		"title":      event.Text(),
		"time":       event.Time,
		"created_at": event.CreatedAt,
		"clip":       event.Clip,
		"backfill":   backfill,
	})
}
