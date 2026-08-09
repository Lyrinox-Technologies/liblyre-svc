# liblyre-svc - Lyre Service Library

A simple, ergonomic Go library for building services that integrate with Lyre-Server.

## Installation

```bash
go get github.com/LyrinoxTechnologies/liblyre-svc
```

## Quick Start

```go
package main

import (
    "fmt"
    "log"

    "github.com/LyrinoxTechnologies/liblyre-svc"
)

func main() {
    // Create service
    svc, err := liblyresvc.New(liblyresvc.Config{
        ServiceID:   "my-service",
        ServiceName: "My Service",
        ServiceType: "backend",
        Description: "Example service",
        Secret:      "my-shared-secret",
        ServerURL:   "ws://localhost:36623/ws",
        Endpoints:   []string{"echo", "greet"},
    })
    if err != nil {
        log.Fatal(err)
    }

    // Register handlers
    svc.Handle("echo", func(req *liblyresvc.Request) *liblyresvc.Response {
        return req.Success(req.Payload)
    })

    svc.Handle("greet", func(req *liblyresvc.Request) *liblyresvc.Response {
        name, _ := req.Payload["name"].(string)
        if name == "" {
            return req.Error("name is required")
        }
        return req.Success(map[string]interface{}{
            "greeting": fmt.Sprintf("Hello, %s!", name),
        })
    })

    // Print the configure command for the admin
    fmt.Println("Run this to configure Lyre-Server:")
    fmt.Println(svc.ConfigureCommand())

    // Connect and run
    if err := svc.Connect(); err != nil {
        log.Fatal(err)
    }
    defer svc.Close()

    log.Println("Service running...")
    if err := svc.Run(); err != nil {
        log.Fatal(err)
    }
}
```

## Registering a Service

Public services self-register on their first authenticated connection. Use a
unique ID, a generated 32-byte secret, and only non-sensitive endpoints. The
Lyre operator must approve any service that needs identity, authentication, or
other elevated infrastructure access.

### Option 1: Use lyre-service-configure CLI

The library provides a helper method to generate the configuration command:

```go
svc, _ := liblyresvc.New(config)

// Get the command to run (plain text secret - for development)
fmt.Println(svc.ConfigureCommand())
// Output: lyre-service-configure add --id "my-service" --name "My Service" --type "backend" --secret "my-secret" --endpoint "echo" --endpoint "greet"

// Get the command with hashed secret (recommended for production)
cmd, _ := svc.ConfigureCommandHashed()
fmt.Println(cmd)
```

### Option 2: Get YAML snippet

```go
fmt.Println(svc.ServiceConfigYAML())
// Output:
//   - id: "my-service"
//     name: "My Service"
//     type: "backend"
//     description: "Example service"
//     secret: "$2a$10$..."
//     endpoints:
//       - "echo"
//       - "greet"
```

## Publishing and Calling Capabilities

Expose a stable capability name while keeping the provider's handler endpoint
private to the implementation. Other Lyre services call the capability name;
Lyre routes the response back to the caller. A capability/version has one
provider; a different public service cannot replace an established contract.

```go
svc, _ := liblyresvc.New(liblyresvc.Config{
    ServiceID:   "crypto-service",
    Secret:      mustSecret(),
    ServerURL:   "wss://lyre.example/lyre/service/ws",
    Endpoints:   []string{"internal.encrypt"},
    Capabilities: []liblyresvc.Capability{{
        Name: "crypto.encrypt", Version: 1, Endpoint: "internal.encrypt",
    }},
})

svc.Handle("internal.encrypt", func(req *liblyresvc.Request) *liblyresvc.Response {
    return req.Success(map[string]interface{}{"ciphertext": "..."})
})

// Connect both services and start Run in a goroutine before making calls.
response, err := svc.CallCapability("crypto.encrypt", map[string]interface{}{
    "plaintext": "hello",
}, 10*time.Second)
```

Capability names are provider-neutral, lowercase dotted identifiers. Version
`1` is currently the supported routing version. Do not use capability endpoints
for identity, authentication, session, credential, MFA, administration, or
Lyre control-plane operations.

## Handling Requests

### Caller Principals

Handlers receive `req.Principal`, an authoritative Lyre identity. Check
`req.Principal.Type` before privileged work: `user` includes the user identity;
`agent` includes only the independent agent ID and name. Services should treat
agent requests as non-human and keep sensitive operations human-only.

```go
svc.Handle("endpoint-name", func(req *liblyresvc.Request) *liblyresvc.Response {
    // Access request data
    userID := req.FromUser      // User ID if from client
    serviceID := req.FromService // Service ID if from another service
    msgID := req.MessageID      // Unique message ID
    
    // Access payload fields
    name := req.Payload["name"].(string)
    
    // Return success
    return req.Success(map[string]interface{}{
        "result": "value",
    })
    
    // Or return error
    return req.Error("something went wrong")
    return req.Errorf("invalid value: %v", value)
})
```

## Wildcard Handler

Handle all unmatched endpoints:

```go
svc.Handle("*", func(req *liblyresvc.Request) *liblyresvc.Response {
    log.Printf("Unknown endpoint: %s", req.Endpoint)
    return req.Errorf("unknown endpoint: %s", req.Endpoint)
})
```

## Security

### Generating Secrets

```go
// Generate a cryptographically secure secret
secret, err := liblyresvc.GenerateSecret()
// secret = "a1b2c3d4e5f6..." (64 hex characters)

// Hash it for use in config.yaml
hash, err := liblyresvc.HashSecret(secret)
// hash = "$2a$10$..."
```

### Production Recommendations

1. **Use bcrypt-hashed secrets in config.yaml** - The `ConfigureCommandHashed()` method does this automatically
2. **Use TLS** - Connect via `wss://` in production
3. **Rotate secrets periodically** - Update both the service and config.yaml

## Configuration Options

```go
liblyresvc.Config{
    // Required
    ServiceID:   "my-service",    // Must match config.yaml
    Secret:      "shared-secret", // Must match config.yaml
    ServerURL:   "ws://host:port/ws",

    // Optional
    ServiceName: "My Service",
    ServiceType: "backend",       // "backend", "cli", "webapp"
    Description: "Service description",
    Endpoints:   []string{"ep1", "ep2"},
    
    // Tuning
    HeartbeatInterval: 30 * time.Second,
    ReconnectDelay:    5 * time.Second,
    Logger:            customLogger,
}
```

## Error Handling

```go
svc.Handle("risky", func(req *liblyresvc.Request) *liblyresvc.Response {
    result, err := doSomethingRisky()
    if err != nil {
        return req.Errorf("operation failed: %v", err)
    }
    return req.Success(result)
})
```

## Graceful Shutdown

```go
// Handle signals
sigChan := make(chan os.Signal, 1)
signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

go func() {
    <-sigChan
    log.Println("Shutting down...")
    svc.Close()
}()

svc.Run()
```
