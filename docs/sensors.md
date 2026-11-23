# Sensors

App exposes cam sensors by publishing the updates to MQTT. See `NANIT_MQTT_*` variables in the [.env.sample](../.env.sample) file for configuration.

It will push any sensor updates to following topics:

- `nanit/babies/{baby_uid}/temperature` - temperature in degrees celsius (float)
- `nanit/babies/{baby_uid}/humidity` - humidity in percent (float)
- `nanit/babies/{baby_uid}/is_night` - flag if cam is in the night mode (bool)

You can configure these in your [HASS setup](./home-assistant.md).

In case you run into trouble and need to see what is going on, you can try using [MQTT Explorer](http://mqtt-explorer.com/).

## Push notifications

With `NANIT_PUSH_ENABLED` (see [.env.sample](../.env.sample)), the app receives the push notifications that the Nanit service sends to the phones with the Nanit app, and publishes each one within seconds:

- `nanit/babies/{baby_uid}/notification` - one message for each notification, a JSON object

```json
{"event_type": "cry", "type": "CAMERA_CRY_DETECTION", "baby_uid": "abc123", "time": 1763856000, "id": 42}
```

`type` is the kind of the notification, as the Nanit service names it. `event_type` is a short name from a fixed list:

| `event_type` | `type` |
| --- | --- |
| `sound` | `SOUND` |
| `motion` | `MOTION` |
| `cry` | `CAMERA_CRY_DETECTION` |
| `temperature` | `TEMPERATURE` |
| `humidity` | `HUMIDITY` |
| `other` | every other kind |

The other fields are what the notification carries, passed on as they arrived.

A sound or motion notification also sets the time of the last sound or motion:

- `nanit/babies/{baby_uid}/sound_timestamp` - time of the last sound notification (Unix time, seconds)
- `nanit/babies/{baby_uid}/motion_timestamp` - time of the last motion notification (Unix time, seconds)

### In Home Assistant

Home Assistant needs no configuration for the notifications. The app announces one event entity for each baby (MQTT discovery): the device `Nanit {name of the baby}` with the entity `Notification`. The state of the entity is the time of the last notification, and its attributes are the fields of the message above. An automation can start from it:

```yaml
triggers:
  - trigger: state
    entity_id: event.nanit_robin_notification
conditions:
  - condition: state
    entity_id: event.nanit_robin_notification
    attribute: event_type
    state: cry
```

### What to know

- The app gets what the Nanit account is set to get. A kind of notification that is turned off in the Nanit app does not arrive, and some kinds need a paid plan of Nanit.
- The app registers one device at the Firebase service of Google and tells the Nanit service to send to it, once. The registration is kept in the session file. To the Nanit service it is one more phone with the Nanit app. It takes none of the places that the camera has for streams.
- A notification that is older than 5 minutes when it arrives is dropped. After a time without a connection, the Firebase service delivers what it has kept.
- A notification is published once, and is lost when there is no connection to the MQTT broker at that moment. After a restart of the app, the notifications of the last minutes can arrive a second time.
- A notification that names no baby of the account goes to the log only.
- When the Firebase service refuses the device (the log says so), stop the app, remove `"push"` from the session file and start it again. The app then registers a new device.
- Without MQTT, the notifications go to the log only.
