package immich

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/coder/websocket"
)

// SocketListener connects to the Immich Socket.IO gateway and acts as a
// doorbell: any recognised lifecycle event signals the sync-stream consumer
// to run immediately rather than waiting for the next tick.
//
// The sidecar is deliberately dumb on the socket path — event contents are
// ignored.  All bus messages are produced exclusively by the sync stream
// (publish-then-ack) so there is no double-publishing and no ID mismatch.
type SocketListener struct {
	baseURL string
	apiKey  string
	wake    chan<- struct{}
}

// NewSocketListener creates a listener.  wake is sent to whenever a known
// Immich lifecycle event arrives.
func NewSocketListener(baseURL, apiKey string, wake chan<- struct{}) *SocketListener {
	return &SocketListener{
		baseURL: baseURL,
		apiKey:  apiKey,
		wake:    wake,
	}
}

// Run connects to the Socket.IO endpoint and loops until ctx is cancelled,
// reconnecting with exponential back-off on error.
func (l *SocketListener) Run(ctx context.Context) {
	backoff := time.Second
	for {
		if err := l.connect(ctx); err != nil {
			if ctx.Err() != nil {
				return
			}
			slog.Warn("socket.io disconnected, reconnecting", "err", err, "backoff", backoff)
			select {
			case <-ctx.Done():
				return
			case <-time.After(backoff):
			}
			if backoff < 60*time.Second {
				backoff *= 2
			}
			continue
		}
		backoff = time.Second
	}
}

// connect establishes one WebSocket session.  It returns when the connection
// closes or the context is cancelled.
func (l *SocketListener) connect(ctx context.Context) error {
	// Build WebSocket URL: http→ws, https→wss.
	wsURL := l.baseURL
	if strings.HasPrefix(wsURL, "http://") {
		wsURL = "ws://" + wsURL[7:]
	} else if strings.HasPrefix(wsURL, "https://") {
		wsURL = "wss://" + wsURL[8:]
	}
	wsURL += "/api/socket.io/?EIO=4&transport=websocket"

	headers := http.Header{}
	headers.Set("x-api-key", l.apiKey)

	conn, _, err := websocket.Dial(ctx, wsURL, &websocket.DialOptions{HTTPHeader: headers})
	if err != nil {
		return fmt.Errorf("websocket dial: %w", err)
	}
	defer conn.CloseNow()

	slog.Info("socket.io connected", "url", wsURL)

	// Engine.IO / Socket.IO framing loop.
	for {
		_, msg, err := conn.Read(ctx)
		if err != nil {
			return fmt.Errorf("ws read: %w", err)
		}
		if err := l.handleFrame(ctx, conn, string(msg)); err != nil {
			slog.Warn("socket.io frame error", "err", err)
		}
	}
}

// handleFrame processes a single Engine.IO frame.
func (l *SocketListener) handleFrame(ctx context.Context, conn *websocket.Conn, msg string) error {
	if len(msg) == 0 {
		return nil
	}

	// Engine.IO packet type is the first byte.
	eioType := msg[0]
	payload := msg[1:]

	switch eioType {
	case '0': // open — server hello, reply with Socket.IO connect
		slog.Debug("engine.io open", "data", payload)
		return conn.Write(ctx, websocket.MessageText, []byte("40"))
	case '2': // ping — respond with pong
		return conn.Write(ctx, websocket.MessageText, []byte("3"))
	case '4': // message
		return l.handleSocketIOMessage(payload)
	default:
		return nil
	}
}

// handleSocketIOMessage processes a Socket.IO packet (Engine.IO type 4).
// Socket.IO packet type is the first byte of payload.
func (l *SocketListener) handleSocketIOMessage(payload string) error {
	if len(payload) == 0 {
		return nil
	}
	sioType := payload[0]
	if sioType != '2' { // 2 = EVENT
		return nil
	}

	// Payload after type byte may include a namespace prefix like "/,".
	// Strip optional namespace; find the JSON array start.
	data := payload[1:]
	idx := strings.Index(data, "[")
	if idx < 0 {
		return nil
	}
	data = data[idx:]

	// Decode as [eventName, ...args]
	var parts []json.RawMessage
	if err := json.Unmarshal([]byte(data), &parts); err != nil {
		return fmt.Errorf("decode socket.io event: %w", err)
	}
	if len(parts) < 1 {
		return nil
	}
	var name string
	if err := json.Unmarshal(parts[0], &name); err != nil {
		return nil
	}

	return l.dispatchSocketEvent(name)
}

// knownSocketEvents is the set of Immich lifecycle event names that should
// trigger a sync-stream pass.
var knownSocketEvents = map[string]bool{
	"on_upload_success": true,
	"on_asset_update":   true,
	"on_asset_delete":   true,
	"on_asset_trash":    true,
	"on_asset_restore":  true,
	"on_album_update":   true,
}

// dispatchSocketEvent signals the sync-stream consumer if the event name is known.
func (l *SocketListener) dispatchSocketEvent(name string) error {
	if !knownSocketEvents[name] {
		slog.Debug("socket.io: ignoring unknown event", "name", name)
		return nil
	}
	slog.Debug("socket.io: doorbell", "name", name)
	select {
	case l.wake <- struct{}{}:
	default:
	}
	return nil
}
