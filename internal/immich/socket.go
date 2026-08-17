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

// SocketListener connects to the Immich Socket.IO gateway. A known event sends a
// signal on the wake channel. The signal makes the sync consumer run at once
// instead of at the next tick.
//
// The listener does not read the event content. Only the sync stream produces
// events. This prevents duplicate events and ID mismatches.
type SocketListener struct {
	baseURL string
	apiKey  string
	wake    chan<- struct{}
}

// NewSocketListener creates a listener. wake receives a signal for each known
// Immich event.
func NewSocketListener(baseURL, apiKey string, wake chan<- struct{}) *SocketListener {
	return &SocketListener{
		baseURL: baseURL,
		apiKey:  apiKey,
		wake:    wake,
	}
}

// Run connects to the Socket.IO endpoint until ctx ends. After an error, Run
// reconnects with exponential back-off. A successful connection resets the
// back-off, so a session that stayed up for a long time reconnects fast.
func (l *SocketListener) Run(ctx context.Context) {
	backoff := time.Second
	for {
		err := l.connect(ctx, func() { backoff = time.Second })
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
	}
}

// connect opens one WebSocket session. connect returns when the connection
// closes or when ctx ends. connect calls onConnect once, after the dial
// succeeds.
func (l *SocketListener) connect(ctx context.Context, onConnect func()) error {
	// Change the URL scheme: http to ws, https to wss.
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
	onConnect()

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

// knownSocketEvents lists the Immich event names that start a sync pass.
var knownSocketEvents = map[string]bool{
	"on_upload_success": true,
	"on_asset_update":   true,
	"on_asset_delete":   true,
	"on_asset_trash":    true,
	"on_asset_restore":  true,
	"on_album_update":   true,
}

// dispatchSocketEvent sends a wake signal for a known event name.
func (l *SocketListener) dispatchSocketEvent(name string) error {
	if !knownSocketEvents[name] {
		slog.Debug("socket.io: ignore unknown event", "name", name)
		return nil
	}
	slog.Debug("socket.io: wake", "name", name)
	select {
	case l.wake <- struct{}{}:
	default:
	}
	return nil
}
