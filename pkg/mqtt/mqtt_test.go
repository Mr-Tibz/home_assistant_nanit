package mqtt

import (
	"encoding/json"
	"testing"

	"github.com/indiefan/home_assistant_nanit/pkg/activity"
	"github.com/indiefan/home_assistant_nanit/pkg/baby"
	"github.com/indiefan/home_assistant_nanit/pkg/push"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNotificationPayload(t *testing.T) {
	payload, err := notificationPayload(push.Notification{
		Type:    "CAMERA_CRY_DETECTION",
		BabyUID: "abc123",
		Fields:  map[string]interface{}{"type": "CAMERA_CRY_DETECTION", "baby_uid": "abc123", "time": float64(1763856000), "id": float64(42)},
	})

	require.NoError(t, err)
	assert.JSONEq(t, `{"event_type":"cry","type":"CAMERA_CRY_DETECTION","baby_uid":"abc123","time":1763856000,"id":42}`, string(payload))
}

// Home Assistant takes an event only when its type is in the list that
// the entity was announced with.
func TestNotificationPayloadOfAKindWithoutAName(t *testing.T) {
	payload, err := notificationPayload(push.Notification{Type: "SOMETHING_NEW", Fields: map[string]interface{}{"type": "SOMETHING_NEW"}})

	require.NoError(t, err)
	assert.JSONEq(t, `{"event_type":"other","type":"SOMETHING_NEW"}`, string(payload))
}

func TestNotificationDiscovery(t *testing.T) {
	topic, payload, err := notificationDiscovery(baby.Baby{UID: "abc123", Name: "Robin"}, "nanit/babies/abc123/notification")

	require.NoError(t, err)
	assert.Equal(t, "homeassistant/event/nanit_abc123/notification/config", topic)

	var config map[string]interface{}
	require.NoError(t, json.Unmarshal(payload, &config))

	assert.Equal(t, "Notification", config["name"])
	assert.Equal(t, "nanit_abc123_notification", config["unique_id"])
	assert.Equal(t, "nanit/babies/abc123/notification", config["state_topic"])
	assert.Equal(t, []interface{}{"sound", "motion", "cry", "temperature", "humidity", "other"}, config["event_types"])
	assert.Equal(t, map[string]interface{}{"identifiers": []interface{}{"nanit_abc123"}, "name": "Nanit Robin", "manufacturer": "Nanit"}, config["device"])
}

func TestNotificationOfAnUnknownBabyIsNotPublished(t *testing.T) {
	conn := NewConnection(Opts{BrokerURL: "tcp://127.0.0.1:1", TopicPrefix: "nanit"})
	conn.NotificationBabies = []baby.Baby{{UID: "abc123"}}

	// No connection to a broker: a publish would report an error, and
	// must not be tried for a baby that is not known
	conn.PublishNotification(push.Notification{Type: "SOUND", BabyUID: "../other"})
	conn.PublishNotification(push.Notification{Type: "SOUND"})
}

func TestActivityPayload(t *testing.T) {
	payload, err := activityPayload(activity.Event{
		UID: "c1b96daf28", Key: "VISIT", InternalKey: "VISIT_WOKE_UP", Title: "Parent approaching",
		Time: 1791772138.97, CreatedAt: "2026-10-12T02:34:28Z", Clip: true,
	}, true)

	require.NoError(t, err)
	assert.JSONEq(t, `{"uid":"c1b96daf28","key":"VISIT","detail":"VISIT_WOKE_UP","title":"Parent approaching","time":1791772138.97,"created_at":"2026-10-12T02:34:28Z","clip":true,"backfill":true}`, string(payload))
}

// Motion and sound come from the service with their kind as the title
func TestActivityPayloadOfMotion(t *testing.T) {
	payload, err := activityPayload(activity.Event{UID: "7fa5814758", Key: "MOTION", InternalKey: "MOTION", Title: "MOTION", Time: 1791772000}, false)

	require.NoError(t, err)
	assert.JSONEq(t, `{"uid":"7fa5814758","key":"MOTION","detail":"MOTION","title":"Motion","time":1791772000,"created_at":"","clip":false,"backfill":false}`, string(payload))
}

func TestPublishActivityReportsThatThereIsNoBroker(t *testing.T) {
	conn := NewConnection(Opts{BrokerURL: "tcp://127.0.0.1:1", TopicPrefix: "nanit"})

	assert.Error(t, conn.PublishActivity("abc123", activity.Event{UID: "7fa5814758", Key: "MOTION"}, false))
}
