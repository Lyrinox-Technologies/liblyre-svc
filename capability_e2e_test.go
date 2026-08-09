package liblyresvc

import (
	"os"
	"testing"
	"time"
)

// TestCapabilityCallE2E exercises a real Lyre capability route when explicitly
// enabled. It is opt-in because it registers two temporary public services.
func TestCapabilityCallE2E(t *testing.T) {
	serverURL := os.Getenv("LYRE_E2E_SERVER_URL")
	if serverURL == "" {
		t.Skip("set LYRE_E2E_SERVER_URL to run against a Lyre deployment")
	}
	provider, err := New(Config{
		ServiceID:    "capability-provider-e2e",
		ServiceName:  "Capability E2E Provider",
		Secret:       "e2e-provider-secret-must-be-at-least-24-bytes",
		ServerURL:    serverURL,
		Endpoints:    []string{"internal.reverse"},
		Capabilities: []Capability{{Name: "test.reverse", Version: 1, Endpoint: "internal.reverse"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	provider.Handle("internal.reverse", func(req *Request) *Response {
		return req.Success(map[string]interface{}{"value": req.Payload["value"], "provider": "capability-provider-e2e"})
	})
	if err := provider.Connect(); err != nil {
		t.Fatal(err)
	}
	defer provider.Close()
	go func() { _ = provider.Run() }()

	caller, err := New(Config{
		ServiceID:   "capability-caller-e2e",
		ServiceName: "Capability E2E Caller",
		Secret:      "e2e-caller-secret-must-be-at-least-24-bytes",
		ServerURL:   serverURL,
		Endpoints:   []string{"internal.noop"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := caller.Connect(); err != nil {
		t.Fatal(err)
	}
	defer caller.Close()
	go func() { _ = caller.Run() }()

	time.Sleep(100 * time.Millisecond)
	response, err := caller.CallCapability("test.reverse", map[string]interface{}{"value": "lyre"}, 10*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if !response.Success || response.Payload["value"] != "lyre" || response.Payload["provider"] != "capability-provider-e2e" {
		t.Fatalf("unexpected capability response: %#v", response)
	}
}
