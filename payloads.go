package liblyresvc

import (
"bytes"
"encoding/json"

"github.com/LyrinoxTechnologies/ridged-proto/rdgproto"
)

// serviceAuthPayload mirrors protocol.ServiceAuthPayload
type serviceAuthPayload struct {
ServiceID string
Secret    string
Endpoints []string
Name        string
Type        string
Description string
Capabilities []capabilityPayload
}

type capabilityPayload struct { Capability string `json:"capability"`; Version uint32 `json:"version"`; Endpoint string `json:"endpoint"`; InputSchema string `json:"input_schema,omitempty"`; OutputSchema string `json:"output_schema,omitempty"`; Priority int `json:"priority,omitempty"` }

func capabilitiesForWire(capabilities []Capability) []capabilityPayload { out := make([]capabilityPayload, 0, len(capabilities)); for _, c := range capabilities { out = append(out, capabilityPayload{c.Name, c.Version, c.Endpoint, c.InputSchema, c.OutputSchema, c.Priority}) }; return out }

func (p *serviceAuthPayload) Marshal() ([]byte, error) {
buf := new(bytes.Buffer)
rdgproto.WriteString(buf, p.ServiceID)
rdgproto.WriteString(buf, p.Secret)
// Encode endpoints as JSON array
endpointsStr := "["
for i, ep := range p.Endpoints {
if i > 0 {
endpointsStr += ","
}
endpointsStr += `"` + ep + `"`
}
endpointsStr += "]"
rdgproto.WriteString(buf, endpointsStr)
	rdgproto.WriteString(buf, p.Name)
	rdgproto.WriteString(buf, p.Type)
rdgproto.WriteString(buf, p.Description)
	capabilities, _ := json.Marshal(p.Capabilities)
	rdgproto.WriteString(buf, string(capabilities))
return buf.Bytes(), nil
}

// serviceAuthResponsePayload mirrors protocol.ServiceAuthResponsePayload
type serviceAuthResponsePayload struct {
Success   bool
ServiceID string
Message   string
}

func (p *serviceAuthResponsePayload) Unmarshal(data []byte) error {
r := bytes.NewReader(data)
var err error
p.Success, err = rdgproto.ReadBool(r)
if err != nil {
return err
}
p.ServiceID, err = rdgproto.ReadString(r)
if err != nil {
return err
}
p.Message, err = rdgproto.ReadString(r)
return err
}

// serviceHeartbeatPayload mirrors protocol.ServiceHeartbeatPayload
type serviceHeartbeatPayload struct {
ServiceID string
Timestamp int64
}

func (p *serviceHeartbeatPayload) Marshal() ([]byte, error) {
buf := new(bytes.Buffer)
rdgproto.WriteString(buf, p.ServiceID)
rdgproto.WriteUint64(buf, uint64(p.Timestamp))
return buf.Bytes(), nil
}

// serviceMessagePayload mirrors protocol.ServiceMessagePayload
type serviceMessagePayload struct {
MessageID   string
FromService string
FromUser    string
ToService   string
Endpoint    string
Payload     []byte
ReplyTo     string
}

func (p *serviceMessagePayload) Marshal() ([]byte, error) {
	buf := new(bytes.Buffer)
	rdgproto.WriteString(buf, p.MessageID)
	rdgproto.WriteString(buf, p.FromService)
	rdgproto.WriteString(buf, p.ToService)
	rdgproto.WriteString(buf, p.Endpoint)
	rdgproto.WriteBytes(buf, p.Payload)
	rdgproto.WriteString(buf, p.ReplyTo)
	return buf.Bytes(), nil
}

func (p *serviceMessagePayload) Unmarshal(data []byte) error {
r := bytes.NewReader(data)
var err error
p.MessageID, err = rdgproto.ReadString(r)
if err != nil {
return err
}
p.FromService, err = rdgproto.ReadString(r)
if err != nil {
return err
}
p.FromUser, err = rdgproto.ReadString(r)
if err != nil {
return err
}
p.ToService, err = rdgproto.ReadString(r)
if err != nil {
return err
}
p.Endpoint, err = rdgproto.ReadString(r)
if err != nil {
return err
}
p.Payload, err = rdgproto.ReadBytes(r)
if err != nil {
return err
}
p.ReplyTo, err = rdgproto.ReadString(r)
return err
}

// serviceResponsePayload mirrors protocol.ServiceResponsePayload
type serviceResponsePayload struct {
MessageID string
Success   bool
Payload   []byte
Error     string
}

func (p *serviceResponsePayload) Unmarshal(data []byte) error {
	r := bytes.NewReader(data)
	var err error
	if p.MessageID, err = rdgproto.ReadString(r); err != nil { return err }
	if p.Success, err = rdgproto.ReadBool(r); err != nil { return err }
	if p.Payload, err = rdgproto.ReadBytes(r); err != nil { return err }
	p.Error, err = rdgproto.ReadString(r)
	return err
}

func (p *serviceResponsePayload) Marshal() ([]byte, error) {
buf := new(bytes.Buffer)
rdgproto.WriteString(buf, p.MessageID)
rdgproto.WriteBool(buf, p.Success)
rdgproto.WriteBytes(buf, p.Payload)
rdgproto.WriteString(buf, p.Error)
return buf.Bytes(), nil
}
