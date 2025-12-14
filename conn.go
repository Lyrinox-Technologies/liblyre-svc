package liblyresvc

import (
"io"
"sync"

"github.com/gorilla/websocket"
)

// wsConn wraps a WebSocket connection to implement rdgproto.Connection.
type wsConn struct {
conn   *websocket.Conn
mu     sync.Mutex
reader io.Reader
}

func newWSConn(conn *websocket.Conn) *wsConn {
return &wsConn{conn: conn}
}

func (c *wsConn) Read(p []byte) (int, error) {
if c.reader == nil {
_, reader, err := c.conn.NextReader()
if err != nil {
return 0, err
}
c.reader = reader
}

n, err := c.reader.Read(p)
if err == io.EOF {
c.reader = nil
if n > 0 {
return n, nil
}
return c.Read(p)
}
return n, err
}

func (c *wsConn) Write(p []byte) (int, error) {
c.mu.Lock()
defer c.mu.Unlock()
err := c.conn.WriteMessage(websocket.BinaryMessage, p)
if err != nil {
return 0, err
}
return len(p), nil
}

func (c *wsConn) Close() error {
return c.conn.Close()
}
