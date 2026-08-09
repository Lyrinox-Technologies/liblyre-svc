package liblyresvc

import (
	"testing"
	"time"
)

func TestConfig_Defaults(t *testing.T) {
	cfg := Config{
		ServiceID: "test-service",
		Secret:    "test-secret",
		ServerURL: "ws://localhost:36623/ws",
	}

	svc, err := New(cfg)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	if svc.config.ServiceType != "backend" {
		t.Errorf("ServiceType = %q, want %q", svc.config.ServiceType, "backend")
	}
	if svc.config.ServiceName != "test-service" {
		t.Errorf("ServiceName = %q, want %q", svc.config.ServiceName, "test-service")
	}
	if svc.config.HeartbeatInterval != 30*time.Second {
		t.Errorf("HeartbeatInterval = %v, want %v", svc.config.HeartbeatInterval, 30*time.Second)
	}
	if svc.config.ReconnectDelay != 5*time.Second {
		t.Errorf("ReconnectDelay = %v, want %v", svc.config.ReconnectDelay, 5*time.Second)
	}
	if svc.logger == nil {
		t.Error("Logger should not be nil")
	}
}

func TestConfig_ValidationErrors(t *testing.T) {
	tests := []struct {
		name    string
		cfg     Config
		wantErr string
	}{
		{
			name:    "missing ServiceID",
			cfg:     Config{Secret: "secret", ServerURL: "ws://localhost/ws"},
			wantErr: "serviceID is required",
		},
		{
			name:    "missing Secret",
			cfg:     Config{ServiceID: "svc", ServerURL: "ws://localhost/ws"},
			wantErr: "secret is required",
		},
		{
			name:    "missing ServerURL",
			cfg:     Config{ServiceID: "svc", Secret: "secret"},
			wantErr: "serverURL is required",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := New(tt.cfg)
			if err == nil {
				t.Fatal("New() expected error, got nil")
			}
			if err.Error() != tt.wantErr {
				t.Errorf("New() error = %q, want %q", err.Error(), tt.wantErr)
			}
		})
	}
}

func TestConfig_CustomValues(t *testing.T) {
	cfg := Config{
		ServiceID:         "my-service",
		ServiceName:       "My Service",
		ServiceType:       "webapp",
		Description:       "Test description",
		Secret:            "super-secret",
		ServerURL:         "wss://example.com/ws",
		Endpoints:         []string{"ep1", "ep2"},
		HeartbeatInterval: 60 * time.Second,
		ReconnectDelay:    10 * time.Second,
	}

	svc, err := New(cfg)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	if svc.config.ServiceName != "My Service" {
		t.Errorf("ServiceName = %q, want %q", svc.config.ServiceName, "My Service")
	}
	if svc.config.ServiceType != "webapp" {
		t.Errorf("ServiceType = %q, want %q", svc.config.ServiceType, "webapp")
	}
	if svc.config.HeartbeatInterval != 60*time.Second {
		t.Errorf("HeartbeatInterval = %v, want %v", svc.config.HeartbeatInterval, 60*time.Second)
	}
}

func TestService_Handle(t *testing.T) {
	svc, _ := New(Config{
		ServiceID: "test",
		Secret:    "secret",
		ServerURL: "ws://localhost/ws",
	})

	handler := func(req *Request) *Response {
		return req.Success(nil)
	}

	svc.Handle("test-endpoint", handler)

	if _, ok := svc.handlers["test-endpoint"]; !ok {
		t.Error("Handler not registered")
	}
}

func TestPrincipalFromPayloadRecognizesAgent(t *testing.T) {
	principal := principalFromPayload(map[string]interface{}{
		"_principal_type": "agent",
		"_agent_id":       "agent-123",
		"_agent_name":     "release-bot",
	})
	if principal.Type != "agent" || principal.ID != "agent-123" || principal.AgentName != "release-bot" {
		t.Fatalf("unexpected agent principal: %#v", principal)
	}
}

func TestPrincipalFromPayloadRecognizesUser(t *testing.T) {
	principal := principalFromPayload(map[string]interface{}{
		"_principal_type": "user",
		"_user_id":        "user-123",
		"_username":       "lyrinox",
		"_email":          "test@example.com",
	})
	if principal.Type != "user" || principal.ID != "user-123" || principal.Username != "lyrinox" {
		t.Fatalf("unexpected user principal: %#v", principal)
	}
}

func TestService_HandleWildcard(t *testing.T) {
	svc, _ := New(Config{
		ServiceID: "test",
		Secret:    "secret",
		ServerURL: "ws://localhost/ws",
	})

	svc.Handle("*", func(req *Request) *Response {
		return req.Success(nil)
	})

	if _, ok := svc.handlers["*"]; !ok {
		t.Error("Wildcard handler not registered")
	}
}

func TestRequest_Success(t *testing.T) {
	req := &Request{MessageID: "123"}
	resp := req.Success(map[string]interface{}{"key": "value"})

	if !resp.Success {
		t.Error("Success = false, want true")
	}
	if resp.Payload["key"] != "value" {
		t.Errorf("Payload[key] = %v, want %v", resp.Payload["key"], "value")
	}
	if resp.Error != "" {
		t.Errorf("Error = %q, want empty", resp.Error)
	}
}

func TestRequest_Error(t *testing.T) {
	req := &Request{MessageID: "123"}
	resp := req.Error("something went wrong")

	if resp.Success {
		t.Error("Success = true, want false")
	}
	if resp.Error != "something went wrong" {
		t.Errorf("Error = %q, want %q", resp.Error, "something went wrong")
	}
}

func TestRequest_Errorf(t *testing.T) {
	req := &Request{MessageID: "123"}
	resp := req.Errorf("error: %d - %s", 42, "test")

	if resp.Success {
		t.Error("Success = true, want false")
	}
	if resp.Error != "error: 42 - test" {
		t.Errorf("Error = %q, want %q", resp.Error, "error: 42 - test")
	}
}

func TestGenerateSecret(t *testing.T) {
	secret1, err := GenerateSecret()
	if err != nil {
		t.Fatalf("GenerateSecret() error = %v", err)
	}

	if len(secret1) != 64 {
		t.Errorf("Secret length = %d, want 64", len(secret1))
	}

	secret2, err := GenerateSecret()
	if err != nil {
		t.Fatalf("GenerateSecret() error = %v", err)
	}

	if secret1 == secret2 {
		t.Error("Generated secrets should be unique")
	}
}

func TestHashSecret(t *testing.T) {
	secret := "my-secret-password"
	hash, err := HashSecret(secret)
	if err != nil {
		t.Fatalf("HashSecret() error = %v", err)
	}

	if hash[:4] != "$2a$" && hash[:4] != "$2b$" {
		t.Errorf("Hash doesnt look like bcrypt: %s", hash[:10])
	}

	hash2, _ := HashSecret(secret)
	if hash == hash2 {
		t.Error("Same secret should produce different hashes (bcrypt uses salt)")
	}
}

func TestConfigureCommand(t *testing.T) {
	svc, _ := New(Config{
		ServiceID:   "my-service",
		ServiceName: "My Service",
		ServiceType: "backend",
		Description: "Test service",
		Secret:      "secret123",
		ServerURL:   "ws://localhost/ws",
		Endpoints:   []string{"action.one", "action.two"},
	})

	cmd := svc.ConfigureCommand()

	expected := []string{
		"--id",
		"my-service",
		"--name",
		"My Service",
		"--type",
		"backend",
		"--secret",
		"secret123",
		"--description",
		"Test service",
		"--endpoint",
		"action.one",
		"action.two",
	}

	for _, part := range expected {
		if !contains(cmd, part) {
			t.Errorf("Command missing: %s, Got: %s", part, cmd)
		}
	}
}

func TestConfigureCommandHashed(t *testing.T) {
	svc, _ := New(Config{
		ServiceID:   "my-service",
		ServiceName: "My Service",
		ServiceType: "backend",
		Secret:      "secret123",
		ServerURL:   "ws://localhost/ws",
		Endpoints:   []string{"action.one"},
	})

	cmd, err := svc.ConfigureCommandHashed()
	if err != nil {
		t.Fatalf("ConfigureCommandHashed() error = %v", err)
	}

	if !contains(cmd, "--no-hash") {
		t.Error("Command should contain --no-hash")
	}

	if contains(cmd, "secret123") {
		t.Error("Command should not contain plain text secret")
	}
}

func TestServiceConfigYAML(t *testing.T) {
	svc, _ := New(Config{
		ServiceID:   "my-service",
		ServiceName: "My Service",
		ServiceType: "backend",
		Description: "Test service",
		Secret:      "secret123",
		ServerURL:   "ws://localhost/ws",
		Endpoints:   []string{"action.one", "action.two"},
	})

	yaml := svc.ServiceConfigYAML()

	expected := []string{
		"id:",
		"my-service",
		"name:",
		"My Service",
		"type:",
		"backend",
		"description:",
		"Test service",
		"action.one",
		"action.two",
	}

	for _, part := range expected {
		if !contains(yaml, part) {
			t.Errorf("YAML missing: %s, Got: %s", part, yaml)
		}
	}

	if contains(yaml, "secret123") {
		t.Error("YAML should not contain plain text secret")
	}
}

func TestService_IsConnected(t *testing.T) {
	svc, _ := New(Config{
		ServiceID: "test",
		Secret:    "secret",
		ServerURL: "ws://localhost/ws",
	})

	if svc.IsConnected() {
		t.Error("IsConnected() = true, want false (not connected yet)")
	}
}

func TestService_RunNotConnected(t *testing.T) {
	svc, _ := New(Config{
		ServiceID: "test",
		Secret:    "secret",
		ServerURL: "ws://localhost/ws",
	})

	err := svc.Run()
	if err == nil {
		t.Error("Run() should fail when not connected")
	}
}

func TestService_CloseNotConnected(t *testing.T) {
	svc, _ := New(Config{
		ServiceID: "test",
		Secret:    "secret",
		ServerURL: "ws://localhost/ws",
	})

	err := svc.Close()
	if err != nil {
		t.Errorf("Close() error = %v, want nil", err)
	}
}

func contains(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
