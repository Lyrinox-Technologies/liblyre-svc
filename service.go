// Package liblyresvc provides a simple, ergonomic library for building Lyre services.
//
// Example usage:
//
//svc, err := liblyresvc.New(liblyresvc.Config{
//    ServiceID:   "my-service",
//    ServiceName: "My Service",
//    Secret:      "shared-secret",
//    ServerURL:   "ws://localhost:36623/ws",
//    Endpoints:   []string{"action.one", "action.two"},
//})
//if err != nil { panic(err) }
//
//svc.Handle("action.one", func(req *liblyresvc.Request) *liblyresvc.Response {
//    return req.Success(map[string]interface{}{"result": "ok"})
//})
//
//if err := svc.Connect(); err != nil { panic(err) }
//defer svc.Close()
//
//svc.Run() // Blocks until shutdown
package liblyresvc

import (
"crypto/rand"
"encoding/hex"
"encoding/json"
"errors"
"fmt"
"log"
"net/url"
"sync"
"time"

"github.com/LyrinoxTechnologies/ridged-proto/rdgproto"
"github.com/gorilla/websocket"
"golang.org/x/crypto/bcrypt"
)

// Message type constants (must match Lyre-Server protocol/messages.go)
const (
MsgTypeServiceAuth         byte = 240
MsgTypeServiceAuthResponse byte = 241
MsgTypeServiceHeartbeat    byte = 242
MsgTypeServiceMessage      byte = 243
MsgTypeServiceResponse     byte = 244
MsgTypeClientToService     byte = 247
)

// Config holds service configuration.
type Config struct {
// ServiceID is the unique identifier for this service (must match config.yaml)
ServiceID string

// ServiceName is a human-readable name for the service
ServiceName string

// ServiceType is the type of service: "backend", "cli", "webapp"
ServiceType string

// Description is a brief description of the service
Description string

// Secret is the shared secret for authenticating with Lyre-Server
Secret string

// ServerURL is the WebSocket URL of the Lyre-Server (e.g., "ws://localhost:36623/ws")
ServerURL string

// Endpoints lists the endpoints/actions this service provides
Endpoints []string

// Capabilities are public contracts mapped to this service's private handlers.
Capabilities []Capability

// HeartbeatInterval is how often to send heartbeats (default: 30s)
HeartbeatInterval time.Duration

// ReconnectDelay is the delay between reconnection attempts (default: 5s)
ReconnectDelay time.Duration

// Logger is an optional logger (default: log.Default())
Logger *log.Logger
}

// Request represents an incoming request from a client or another service.
type Request struct {
// MessageID is the unique ID for this request (used for responses)
MessageID string

// FromService is the source service ID (empty if from client)
FromService string

// FromUser is the source user ID (if from client)
FromUser string

// Principal is the authoritative Lyre identity attached to this request.
// Services must use it rather than any identity fields supplied by callers.
Principal Principal

// Endpoint is the endpoint/action being called
Endpoint string

// Payload is the request payload as a map
Payload map[string]interface{}

// RawPayload is the raw JSON bytes of the payload
RawPayload []byte

// svc is a reference to the service for sending responses
svc *Service
}

// Principal identifies the authenticated caller without exposing passwords,
// MFA data, session tokens, or other authentication secrets.
type Principal struct {
	Type      string
	ID        string
	Username  string
	Email     string
	AgentName string
}

type Capability struct {
	Name string
	Version uint32
	Endpoint string
	InputSchema string
	OutputSchema string
	Priority int
}

// Response represents a response to send back.
type Response struct {
Success bool
Payload map[string]interface{}
Error   string
}

// Success creates a successful response with the given payload.
func (r *Request) Success(payload map[string]interface{}) *Response {
return &Response{
Success: true,
Payload: payload,
}
}

// Error creates an error response with the given message.
func (r *Request) Error(message string) *Response {
return &Response{
Success: false,
Error:   message,
}
}

// Errorf creates an error response with a formatted message.
func (r *Request) Errorf(format string, args ...interface{}) *Response {
return &Response{
Success: false,
Error:   fmt.Sprintf(format, args...),
}
}

// HandlerFunc is the signature for endpoint handlers.
type HandlerFunc func(req *Request) *Response

// Service represents a Lyre service instance.
type Service struct {
config    Config
conn      *websocket.Conn
proto     *rdgproto.Protocol
handlers  map[string]HandlerFunc
logger    *log.Logger

// For synchronous auth
authResp chan *serviceAuthResponsePayload
	pendingCalls map[string]chan *Response

mu          sync.RWMutex
connected   bool
running     bool
stopChan    chan struct{}
wg          sync.WaitGroup
}

// New creates a new service instance.
func New(cfg Config) (*Service, error) {
if cfg.ServiceID == "" {
return nil, errors.New("serviceID is required")
}
if cfg.Secret == "" {
return nil, errors.New("secret is required")
}
if cfg.ServerURL == "" {
return nil, errors.New("serverURL is required")
}

// Set defaults
if cfg.ServiceType == "" {
cfg.ServiceType = "backend"
}
if cfg.ServiceName == "" {
cfg.ServiceName = cfg.ServiceID
}
if cfg.HeartbeatInterval == 0 {
cfg.HeartbeatInterval = 30 * time.Second
}
if cfg.ReconnectDelay == 0 {
cfg.ReconnectDelay = 5 * time.Second
}
if cfg.Logger == nil {
cfg.Logger = log.Default()
}

return &Service{
config:   cfg,
handlers: make(map[string]HandlerFunc),
logger:   cfg.Logger,
stopChan: make(chan struct{}),
authResp: make(chan *serviceAuthResponsePayload, 1),
	pendingCalls: make(map[string]chan *Response),
}, nil
}

// Handle registers a handler for an endpoint.
func (s *Service) Handle(endpoint string, handler HandlerFunc) {
s.mu.Lock()
defer s.mu.Unlock()
s.handlers[endpoint] = handler
}

// Connect establishes a connection to the Lyre-Server and authenticates.
func (s *Service) Connect() error {
s.mu.Lock()
defer s.mu.Unlock()

if s.connected {
return errors.New("already connected")
}

// Parse and validate URL
u, err := url.Parse(s.config.ServerURL)
if err != nil {
return fmt.Errorf("invalid server URL: %w", err)
}

// Connect WebSocket
s.logger.Printf("[%s] Connecting to %s", s.config.ServiceID, s.config.ServerURL)
conn, _, err := websocket.DefaultDialer.Dial(u.String(), nil)
if err != nil {
return fmt.Errorf("failed to connect: %w", err)
}
s.conn = conn

// Create rdgproto protocol (low-level)
wsConn := newWSConn(conn)
s.proto = rdgproto.NewProtocol(wsConn, nil)

// Authenticate synchronously
if err := s.authenticate(); err != nil {
conn.Close()
return fmt.Errorf("authentication failed: %w", err)
}

s.connected = true
s.logger.Printf("[%s] Connected and authenticated", s.config.ServiceID)

return nil
}

// authenticate sends the service auth request and waits for response.
func (s *Service) authenticate() error {
// Build auth payload
authPayload := &serviceAuthPayload{
ServiceID: s.config.ServiceID,
Secret:    s.config.Secret,
Endpoints: s.config.Endpoints,
Name:      s.config.ServiceName,
Type:      s.config.ServiceType,
Description: s.config.Description,
	Capabilities: capabilitiesForWire(s.config.Capabilities),
}

data, err := authPayload.Marshal()
if err != nil {
return err
}

// Send auth request
if _, err := s.proto.SendRaw(MsgTypeServiceAuth, data); err != nil {
return err
}

// Wait for response
msg, payload, err := s.proto.ReceiveMessage()
if err != nil {
return err
}

if msg.Type != MsgTypeServiceAuthResponse {
return fmt.Errorf("unexpected response type: %d", msg.Type)
}

// Payload is []byte for unregistered types
payloadBytes, ok := payload.([]byte)
if !ok {
return fmt.Errorf("unexpected payload type: %T", payload)
}

var resp serviceAuthResponsePayload
if err := resp.Unmarshal(payloadBytes); err != nil {
return err
}

if !resp.Success {
return fmt.Errorf("auth failed: %s", resp.Message)
}

return nil
}

// Run starts the service message loop. Blocks until Close() is called.
func (s *Service) Run() error {
s.mu.Lock()
if !s.connected {
s.mu.Unlock()
return errors.New("not connected")
}
s.running = true
s.mu.Unlock()

// Start heartbeat goroutine
s.wg.Add(1)
go s.heartbeatLoop()

// Message loop
for {
select {
case <-s.stopChan:
return nil
default:
}

msg, payload, err := s.proto.ReceiveMessage()
if err != nil {
s.mu.RLock()
running := s.running
s.mu.RUnlock()
if !running {
return nil
}
s.logger.Printf("[%s] Error receiving message: %v", s.config.ServiceID, err)
return err
}

s.handleMessage(msg, payload)
}
}

// handleMessage processes an incoming message.
func (s *Service) handleMessage(msg *rdgproto.Message, payload interface{}) {
switch msg.Type {
case MsgTypeServiceMessage, MsgTypeClientToService:
s.handleServiceMessage(msg, payload)
case MsgTypeServiceResponse:
	s.handleServiceResponse(payload)
	default:
	s.logger.Printf("[%s] Unknown message type: %d", s.config.ServiceID, msg.Type)
	}
	}

// CallCapability invokes a public Lyre capability without knowing its provider
// or the provider's private endpoint. Run must be active to receive the result.
func (s *Service) CallCapability(capability string, payload map[string]interface{}, timeout time.Duration) (*Response, error) {
	if capability == "" { return nil, errors.New("capability is required") }
	if timeout <= 0 { timeout = 30 * time.Second }
	idBytes := make([]byte, 16)
	if _, err := rand.Read(idBytes); err != nil { return nil, err }
	id := hex.EncodeToString(idBytes)
	body, err := json.Marshal(payload)
	if err != nil { return nil, err }
	result := make(chan *Response, 1)
	s.mu.Lock()
	if !s.running { s.mu.Unlock(); return nil, errors.New("service Run must be active before calling a capability") }
	s.pendingCalls[id] = result
	s.mu.Unlock()
	data, err := (&serviceMessagePayload{MessageID: id, ToService: capability, Payload: body}).Marshal()
	if err == nil { _, err = s.proto.SendRaw(MsgTypeServiceMessage, data) }
	if err != nil { s.mu.Lock(); delete(s.pendingCalls, id); s.mu.Unlock(); return nil, err }
	select { case response := <-result: return response, nil; case <-time.After(timeout): s.mu.Lock(); delete(s.pendingCalls, id); s.mu.Unlock(); return nil, errors.New("capability call timed out") }
}

func (s *Service) handleServiceResponse(payload interface{}) {
	bytes, ok := payload.([]byte); if !ok { return }
	response := &serviceResponsePayload{}
	if response.Unmarshal(bytes) != nil { return }
	s.mu.Lock(); pending := s.pendingCalls[response.MessageID]; delete(s.pendingCalls, response.MessageID); s.mu.Unlock()
	if pending == nil { return }
	result := &Response{Success: response.Success, Error: response.Error}
	_ = json.Unmarshal(response.Payload, &result.Payload)
	pending <- result
	}

// handleServiceMessage processes a service or client-to-service message.
func (s *Service) handleServiceMessage(msg *rdgproto.Message, payload interface{}) {
// Payload is []byte for unregistered types
payloadBytes, ok := payload.([]byte)
if !ok {
s.logger.Printf("[%s] Invalid payload type: %T", s.config.ServiceID, payload)
return
}

var svcMsg serviceMessagePayload
if err := svcMsg.Unmarshal(payloadBytes); err != nil {
s.logger.Printf("[%s] Failed to unmarshal message: %v", s.config.ServiceID, err)
return
}

// Parse JSON payload
var reqPayload map[string]interface{}
if len(svcMsg.Payload) > 0 {
json.Unmarshal(svcMsg.Payload, &reqPayload)
}

// Build request
req := &Request{
MessageID:   svcMsg.MessageID,
FromService: svcMsg.FromService,
FromUser:    svcMsg.FromUser,
Principal:  principalFromPayload(reqPayload),
Endpoint:    svcMsg.Endpoint,
Payload:     reqPayload,
RawPayload:  svcMsg.Payload,
svc:         s,
}

// Find handler
s.mu.RLock()
handler, ok := s.handlers[svcMsg.Endpoint]
if !ok {
// Check for wildcard handler
handler, ok = s.handlers["*"]
}
s.mu.RUnlock()

var resp *Response
if !ok {
resp = &Response{
Success: false,
Error:   fmt.Sprintf("unknown endpoint: %s", svcMsg.Endpoint),
}
} else {
// Call handler
resp = handler(req)
}

// Send response
s.sendResponse(req.MessageID, resp)
}

func principalFromPayload(payload map[string]interface{}) Principal {
	principalType, _ := payload["_principal_type"].(string)
	if principalType == "agent" {
		agentID, _ := payload["_agent_id"].(string)
		agentName, _ := payload["_agent_name"].(string)
		return Principal{Type: "agent", ID: agentID, AgentName: agentName}
	}
	if principalType == "user" {
		userID, _ := payload["_user_id"].(string)
		username, _ := payload["_username"].(string)
		email, _ := payload["_email"].(string)
		return Principal{Type: "user", ID: userID, Username: username, Email: email}
	}
	return Principal{}
}

// sendResponse sends a response back to the caller.
func (s *Service) sendResponse(messageID string, resp *Response) {
payloadBytes, _ := json.Marshal(resp.Payload)

respPayload := &serviceResponsePayload{
MessageID: messageID,
Success:   resp.Success,
Payload:   payloadBytes,
Error:     resp.Error,
}

data, err := respPayload.Marshal()
if err != nil {
s.logger.Printf("[%s] Failed to marshal response: %v", s.config.ServiceID, err)
return
}

if _, err := s.proto.SendRaw(MsgTypeServiceResponse, data); err != nil {
s.logger.Printf("[%s] Failed to send response: %v", s.config.ServiceID, err)
}
}

// heartbeatLoop sends periodic heartbeats.
func (s *Service) heartbeatLoop() {
defer s.wg.Done()

ticker := time.NewTicker(s.config.HeartbeatInterval)
defer ticker.Stop()

for {
select {
case <-s.stopChan:
return
case <-ticker.C:
s.sendHeartbeat()
}
}
}

// sendHeartbeat sends a heartbeat message.
func (s *Service) sendHeartbeat() {
payload := &serviceHeartbeatPayload{
ServiceID: s.config.ServiceID,
Timestamp: time.Now().Unix(),
}

data, err := payload.Marshal()
if err != nil {
return
}

s.proto.SendRaw(MsgTypeServiceHeartbeat, data)
}

// Close shuts down the service connection.
func (s *Service) Close() error {
s.mu.Lock()
defer s.mu.Unlock()

if !s.connected {
return nil
}

s.running = false
close(s.stopChan)
s.wg.Wait()

s.connected = false
if s.conn != nil {
return s.conn.Close()
}
return nil
}

// IsConnected returns whether the service is connected.
func (s *Service) IsConnected() bool {
s.mu.RLock()
defer s.mu.RUnlock()
return s.connected
}

// GenerateSecret generates a secure random secret for service authentication.
func GenerateSecret() (string, error) {
bytes := make([]byte, 32)
if _, err := rand.Read(bytes); err != nil {
return "", err
}
return hex.EncodeToString(bytes), nil
}

// HashSecret creates a bcrypt hash of a secret for use in config.yaml.
func HashSecret(secret string) (string, error) {
hash, err := bcrypt.GenerateFromPassword([]byte(secret), bcrypt.DefaultCost)
if err != nil {
return "", err
}
return string(hash), nil
}

// ConfigureCommand returns the full command to run lyre-service-configure
// to register this service with a Lyre-Server.
func (s *Service) ConfigureCommand() string {
cmd := fmt.Sprintf("lyre-service-configure add --id %q --name %q --type %q --secret %q",
s.config.ServiceID,
s.config.ServiceName,
s.config.ServiceType,
s.config.Secret,
)

if s.config.Description != "" {
cmd += fmt.Sprintf(" --description %q", s.config.Description)
}

for _, ep := range s.config.Endpoints {
cmd += fmt.Sprintf(" --endpoint %q", ep)
}

return cmd
}

// ConfigureCommandHashed returns the configure command with a hashed secret.
// This is recommended for production use.
func (s *Service) ConfigureCommandHashed() (string, error) {
hashedSecret, err := HashSecret(s.config.Secret)
if err != nil {
return "", err
}

cmd := fmt.Sprintf("lyre-service-configure add --id %q --name %q --type %q --secret %q --no-hash",
s.config.ServiceID,
s.config.ServiceName,
s.config.ServiceType,
hashedSecret,
)

if s.config.Description != "" {
cmd += fmt.Sprintf(" --description %q", s.config.Description)
}

for _, ep := range s.config.Endpoints {
cmd += fmt.Sprintf(" --endpoint %q", ep)
}

return cmd, nil
}

// ServiceConfigYAML returns the YAML snippet to add to config.yaml for this service.
func (s *Service) ServiceConfigYAML() string {
hashedSecret, _ := HashSecret(s.config.Secret)

yaml := fmt.Sprintf(`  - id: %q
    name: %q
    type: %q
    description: %q
    secret: %q
    endpoints:`,
s.config.ServiceID,
s.config.ServiceName,
s.config.ServiceType,
s.config.Description,
hashedSecret,
)

for _, ep := range s.config.Endpoints {
yaml += fmt.Sprintf("\n      - %q", ep)
}

return yaml
}
