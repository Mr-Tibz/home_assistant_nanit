package push

import (
	"errors"
	"strconv"

	"google.golang.org/protobuf/encoding/protowire"
)

// The messages that this program exchanges with the Firebase service, in
// the protocol buffers format. Only the fields that are used are written
// and read here, by their numbers in the definitions that Chromium
// publishes (mcs.proto, checkin.proto and android_checkin.proto in
// google_apis/gcm/protocol). Generated code for the whole of those
// definitions would be many times longer than this file.

// keyValue - one entry of the data of a message
type keyValue struct {
	Key   string
	Value string
}

// dataMessage - what is read of a message that the service delivers
// (DataMessageStanza)
type dataMessage struct {
	// From - the sender, which is the number of a Firebase project
	From string

	// PersistentID - the ID under which the service keeps the message
	// until it is told that the message has arrived
	PersistentID string

	// Data - the content, as the app that sent it has filled it
	Data []keyValue
}

// loginRefusal - the error in the answer to a login (ErrorInfo)
type loginRefusal struct {
	Code    int64
	Message string
}

// eachField calls visit for every field of a message. A field of text,
// bytes or another message arrives in data, a number in number.
func eachField(message []byte, visit func(field protowire.Number, data []byte, number uint64) error) error {
	for len(message) > 0 {
		field, kind, n := protowire.ConsumeTag(message)
		if n < 0 {
			return protowire.ParseError(n)
		}
		message = message[n:]

		var data []byte
		var number uint64

		switch kind {
		case protowire.VarintType:
			number, n = protowire.ConsumeVarint(message)
		case protowire.Fixed64Type:
			number, n = protowire.ConsumeFixed64(message)
		case protowire.BytesType:
			data, n = protowire.ConsumeBytes(message)
		default:
			// A kind of field that no message here has. It is skipped.
			n = protowire.ConsumeFieldValue(field, kind, message)
			if n < 0 {
				return protowire.ParseError(n)
			}
			message = message[n:]
			continue
		}

		if n < 0 {
			return protowire.ParseError(n)
		}
		message = message[n:]

		if err := visit(field, data, number); err != nil {
			return err
		}
	}

	return nil
}

func appendString(message []byte, field protowire.Number, value string) []byte {
	return protowire.AppendString(protowire.AppendTag(message, field, protowire.BytesType), value)
}

func appendMessage(message []byte, field protowire.Number, value []byte) []byte {
	return protowire.AppendBytes(protowire.AppendTag(message, field, protowire.BytesType), value)
}

func appendNumber(message []byte, field protowire.Number, value uint64) []byte {
	return protowire.AppendVarint(protowire.AppendTag(message, field, protowire.VarintType), value)
}

func appendFixed64(message []byte, field protowire.Number, value uint64) []byte {
	return protowire.AppendFixed64(protowire.AppendTag(message, field, protowire.Fixed64Type), value)
}

// encodeLoginRequest builds the login of a device (LoginRequest), with
// the values that a Chrome browser sends, as other programs that receive
// push notifications do. receivedIDs are the messages that have arrived:
// the service then does not send them again.
func encodeLoginRequest(androidID uint64, securityToken uint64, receivedIDs []string) []byte {
	device := strconv.FormatUint(androidID, 10)

	var m []byte
	m = appendString(m, 1, "chrome-63.0.3234.0")                                  // id
	m = appendString(m, 2, "mcs.android.com")                                     // domain
	m = appendString(m, 3, device)                                                // user
	m = appendString(m, 4, device)                                                // resource
	m = appendString(m, 5, strconv.FormatUint(securityToken, 10))                 // auth_token
	m = appendString(m, 6, "android-"+strconv.FormatUint(androidID, 16))          // device_id
	m = appendMessage(m, 8, appendString(appendString(nil, 1, "new_vc"), 2, "1")) // setting

	for _, id := range receivedIDs {
		m = appendString(m, 10, id) // received_persistent_id
	}

	m = appendNumber(m, 12, 0) // adaptive_heartbeat: false
	m = appendNumber(m, 14, 1) // use_rmq2: true
	m = appendNumber(m, 16, 2) // auth_service: ANDROID_ID
	m = appendNumber(m, 17, 1) // network_type

	return m
}

// decodeLoginResponse reads the answer to a login (LoginResponse). It
// returns the error of the answer, or nil when the login is accepted.
func decodeLoginResponse(message []byte) (*loginRefusal, error) {
	var refusal *loginRefusal

	err := eachField(message, func(field protowire.Number, data []byte, _ uint64) error {
		if field != 3 || data == nil { // error
			return nil
		}

		refusal = &loginRefusal{}

		return eachField(data, func(field protowire.Number, data []byte, number uint64) error {
			switch field {
			case 1: // code
				refusal.Code = int64(number)
			case 2: // message
				refusal.Message = string(data)
			}
			return nil
		})
	})

	return refusal, err
}

// decodeDataMessage reads a message that the service delivers.
func decodeDataMessage(message []byte) (dataMessage, error) {
	var result dataMessage

	err := eachField(message, func(field protowire.Number, data []byte, _ uint64) error {
		switch field {
		case 3: // from
			result.From = string(data)
		case 9: // persistent_id
			result.PersistentID = string(data)
		case 7: // app_data
			var entry keyValue

			err := eachField(data, func(field protowire.Number, data []byte, _ uint64) error {
				switch field {
				case 1:
					entry.Key = string(data)
				case 2:
					entry.Value = string(data)
				}
				return nil
			})
			if err != nil {
				return err
			}

			result.Data = append(result.Data, entry)
		}
		return nil
	})

	return result, err
}

// decodeStreamError reads the error with which the service ends a
// connection (StreamErrorStanza), as far as it can be read.
func decodeStreamError(message []byte) (kind string, text string) {
	_ = eachField(message, func(field protowire.Number, data []byte, _ uint64) error {
		switch field {
		case 1:
			kind = string(data)
		case 2:
			text = string(data)
		}
		return nil
	})

	return kind, text
}

// encodeCheckinRequest builds the request for a new device
// (AndroidCheckinRequest), with the values that a Chrome browser sends.
func encodeCheckinRequest() []byte {
	var build []byte
	build = appendNumber(build, 1, 2)             // platform
	build = appendString(build, 2, "63.0.3234.0") // chrome_version
	build = appendNumber(build, 3, 1)             // channel: stable

	var checkin []byte
	checkin = appendNumber(checkin, 12, 3)      // type: Chrome browser
	checkin = appendMessage(checkin, 13, build) // chrome_build

	var m []byte
	m = appendNumber(m, 2, 0)        // id: none yet
	m = appendMessage(m, 4, checkin) // checkin
	m = appendFixed64(m, 13, 0)      // security_token: none yet
	m = appendNumber(m, 14, 3)       // version
	m = appendNumber(m, 22, 0)       // user_serial_number

	return m
}

// decodeCheckinResponse reads the device that the service has made
// (AndroidCheckinResponse).
func decodeCheckinResponse(message []byte) (androidID uint64, securityToken uint64, err error) {
	err = eachField(message, func(field protowire.Number, _ []byte, number uint64) error {
		switch field {
		case 7:
			androidID = number
		case 8:
			securityToken = number
		}
		return nil
	})
	if err != nil {
		return 0, 0, err
	}

	if androidID == 0 || securityToken == 0 {
		return 0, 0, errors.New("the answer names no device")
	}

	return androidID, securityToken, nil
}
