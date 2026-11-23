package push

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protowire"
)

func TestDecodeDataMessage(t *testing.T) {
	message, err := decodeDataMessage(encodeDataMessage("id-1", "notification", `{"type":"SOUND"}`))

	require.NoError(t, err)
	assert.Equal(t, dataMessage{
		From:         "25705829844",
		PersistentID: "id-1",
		Data:         []keyValue{{"ignored", "x"}, {"notification", `{"type":"SOUND"}`}},
	}, message)
}

// What arrives from the network can be anything. Nothing may panic.
func TestDecodeRefusesAMessageThatIsCutOff(t *testing.T) {
	whole := encodeDataMessage("id-1", "notification", `{"type":"SOUND"}`)

	for length := 0; length < len(whole); length++ {
		// Every cut either gives a message with fewer fields or an error
		_, _ = decodeDataMessage(whole[:length])
		_, _ = decodeLoginResponse(whole[:length])
		_, _, _ = decodeCheckinResponse(whole[:length])
		_, _ = decodeStreamError(whole[:length])
	}

	_, err := decodeDataMessage([]byte{0xff, 0xff, 0xff})
	assert.Error(t, err)

	// A field for the data that holds a message that is cut off
	_, err = decodeDataMessage(appendMessage(nil, 7, []byte{0x0a, 0x05, 'k'}))
	assert.Error(t, err)
}

func TestDecodeLoginResponse(t *testing.T) {
	refusal, err := decodeLoginResponse(encodeLoginResponse(nil))
	require.NoError(t, err)
	assert.Nil(t, refusal)

	refusal, err = decodeLoginResponse(encodeLoginResponse(&loginRefusal{Code: 401, Message: "unauthorized"}))
	require.NoError(t, err)
	assert.Equal(t, &loginRefusal{Code: 401, Message: "unauthorized"}, refusal)
}

func TestDecodeCheckinResponse(t *testing.T) {
	androidID, securityToken, err := decodeCheckinResponse(appendFixed64(appendFixed64(appendNumber(nil, 1, 1), 7, 18446744073709551615), 8, 42))

	require.NoError(t, err)
	assert.Equal(t, uint64(18446744073709551615), androidID, "the whole range of the number is kept")
	assert.Equal(t, uint64(42), securityToken)
}

func TestEachFieldSkipsKindsThatNoMessageHas(t *testing.T) {
	message := protowire.AppendFixed32(protowire.AppendTag(nil, 1, protowire.Fixed32Type), 7)
	message = appendString(message, 2, "text")

	var fields []protowire.Number
	require.NoError(t, eachField(message, func(field protowire.Number, data []byte, _ uint64) error {
		fields = append(fields, field)
		assert.Equal(t, "text", string(data))
		return nil
	}))

	assert.Equal(t, []protowire.Number{2}, fields)
}
