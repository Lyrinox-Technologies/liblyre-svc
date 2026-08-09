package liblyresvc

import "testing"

func TestServiceMessagePayloadMatchesLyreWireFormat(t *testing.T) {
	payload := &serviceMessagePayload{
		MessageID: "message-1", FromService: "caller", ToService: "crypto.encrypt",
		Endpoint: "", Payload: []byte(`{"text":"hello"}`), ReplyTo: "",
	}
	encoded, err := payload.Marshal()
	if err != nil {
		t.Fatalf("marshal service message: %v", err)
	}
	var decoded serviceMessagePayload
	if err := decoded.Unmarshal(encoded); err != nil {
		t.Fatalf("unmarshal service message: %v", err)
	}
	if decoded.MessageID != payload.MessageID || decoded.FromService != payload.FromService || decoded.ToService != payload.ToService || decoded.Endpoint != payload.Endpoint || string(decoded.Payload) != string(payload.Payload) || decoded.ReplyTo != payload.ReplyTo {
		t.Fatalf("service message round trip mismatch: %#v", decoded)
	}
}
