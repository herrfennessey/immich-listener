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
	"github.com/herrfennessey/immich-listener/internal/events"
)

// SocketListener connects to the Immich Socket.IO gateway, decodes lifecycle
// events, and calls publish for each recognised event.  The connection is
// automatically re-established on error.
//
// Architecture note: Socket.IO events are fast but lossy.  The listener only
// publishes them as a low-latency complement to the reliable sync stream.  When
// a socket event arrives it also signals wake to trigger an immediate sync-stream
// pass so any gaps are filled.
type SocketListener struct {
	baseURL string
	apiKey  string
	publish func(context.Context, events.Event) error
	wake    chan<- struct{}
}

// NewSocketListener creates a listener.  wake is sent to whenever any socket
// event arrives so the sync-stream consumer can run immediately.
func NewSocketListener(baseURL, apiKey string, wake chan<- struct{}, publish func(context.Context, events.Event) error) *SocketListener {
	return &SocketListener{
		baseURL: baseURL,
		apiKey:  apiKey,
		publish: publish,
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
		// Send Socket.IO namespace connect packet.
		return conn.Write(ctx, websocket.MessageText, []byte("40"))
	case '2': // ping — respond with pong
		return conn.Write(ctx, websocket.MessageText, []byte("3"))
	case '4': // message
		return l.handleSocketIOMessage(ctx, payload)
	default:
		return nil
	}
}

// handleSocketIOMessage processes a Socket.IO packet (Engine.IO type 4).
// Socket.IO packet type is the first byte of payload.
func (l *SocketListener) handleSocketIOMessage(ctx context.Context, payload string) error {
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

	var arg json.RawMessage
	if len(parts) > 1 {
		arg = parts[1]
	}

	return l.dispatchSocketEvent(ctx, name, arg)
}

// dispatchSocketEvent maps an Immich socket event name to a canonical event.
func (l *SocketListener) dispatchSocketEvent(ctx context.Context, name string, arg json.RawMessage) error {
	var ev events.Event
	switch name {
	case "on_upload_success":
		id := extractID(arg)
		ev = events.Event{Type: events.AssetCreated, Source: "socket", AssetID: id}
	case "on_asset_update":
		id := extractID(arg)
		ev = events.Event{Type: events.AssetUpdated, Source: "socket", AssetID: id}
	case "on_asset_delete", "on_asset_trash":
		id := extractID(arg)
		ev = events.Event{Type: events.AssetDeleted, Source: "socket", AssetID: id}
	case "on_asset_restore":
		id := extractID(arg)
		ev = events.Event{Type: events.AssetCreated, Source: "socket", AssetID: id}
	case "on_album_update":
		id := extractID(arg)
		ev = events.Event{Type: events.AlbumUpdated, Source: "socket", AlbumID: id}
	default:
		slog.Debug("socket.io: ignoring unknown event", "name", name)
		return nil
	}

	// Trigger an immediate sync-stream pass for every known event.
	select {
	case l.wake <- struct{}{}:
	default:
	}

	if err := l.publish(ctx, ev); err != nil {
		slog.Warn("publish socket event", "name", name, "err", err)
	}
	return nil
}

// extractID tries to read an "id" field from a JSON object argument.
func extractID(raw json.RawMessage) string {
	if raw == nil {
		return ""
	}
	var obj struct {
		ID string `json:"id"`
	}
	_ = json.Unmarshal(raw, &obj)
	return obj.ID
}
