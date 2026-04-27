package utils

import (
	"encoding/json"
	"fmt"

	"github.com/TicketsBot-cloud/gdl/objects/channel/message"
	"github.com/TicketsBot-cloud/logarchiver/pkg/model"
	v1 "github.com/TicketsBot-cloud/logarchiver/pkg/model/v1"
	v2 "github.com/TicketsBot-cloud/logarchiver/pkg/model/v2"
)

// Decode parses a raw transcript payload into a v2.Transcript, converting from v1 if necessary.
func Decode(data []byte) (v2.Transcript, error) {
	version := model.GetVersion(data)
	switch version {
	case model.V1:
		var messages []message.Message
		if err := json.Unmarshal(data, &messages); err != nil {
			return v2.Transcript{}, err
		}
		return v1.ConvertToV2(messages), nil
	case model.V2:
		var t v2.Transcript
		if err := json.Unmarshal(data, &t); err != nil {
			return v2.Transcript{}, err
		}
		return t, nil
	default:
		return v2.Transcript{}, fmt.Errorf("unknown transcript version %d", version)
	}
}
