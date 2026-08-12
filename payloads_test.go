package liblyresvc

import (
	"bytes"
	"github.com/LyrinoxTechnologies/ridged-proto/rdgproto"
	"testing"
)

func TestServiceAuthPublisherFieldsAreSerialized(t *testing.T) {
	payload := &serviceAuthPayload{ServiceID: "test", Secret: "secret", Endpoints: []string{"echo"}, Name: "test", Type: "utility", Capabilities: []capabilityPayload{}, PublisherUserID: "publisher-user", PublisherPrivateKey: "private-key"}
	data, err := payload.Marshal()
	if err != nil {
		t.Fatal(err)
	}
	decoded := &serviceAuthPayload{}
	r := bytes.NewReader(data)
	for i := 0; i < 7; i++ {
		if _, err := rdgproto.ReadString(r); err != nil {
			t.Fatal(err)
		}
	}
	decoded.PublisherUserID, err = rdgproto.ReadString(r)
	if err != nil {
		t.Fatal(err)
	}
	decoded.PublisherPrivateKey, err = rdgproto.ReadString(r)
	if err != nil {
		t.Fatal(err)
	}
	if decoded.PublisherUserID != payload.PublisherUserID || decoded.PublisherPrivateKey != payload.PublisherPrivateKey {
		t.Fatalf("publisher fields lost: user=%q key=%q", decoded.PublisherUserID, decoded.PublisherPrivateKey)
	}
}
