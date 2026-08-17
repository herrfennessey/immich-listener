package immich

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/herrfennessey/immich-listener/internal/events"
)

// wsServer creates a test WebSocket server that sends the given frames and then
// closes the connection.
func wsServer(t *testing.T, frames []string) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{InsecureSkipVerify: true})
		if err != nil {
			t.Logf("ws accept error: %v", err)
			return
		}
		defer conn.CloseNow()
		for _, frame := range frames {
			if err := conn.Write(r.Context(), websocket.MessageText, []byte(frame)); err != nil {
				return
			}
		}
		// Keep open briefly so client can process.
		time.Sleep(200 * time.Millisecond)
	}))
}

func TestHandleFrame_OpenSendsSocketIOConnect(t *testing.T) {
	// sentCh receives the first message the client sends after the open frame.
	sentCh := make(chan string, 1)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, _ := websocket.Accept(w, r, &websocket.AcceptOptions{InsecureSkipVerify: true})
		defer conn.CloseNow()
		// Send EIO open frame.
		_ = conn.Write(r.Context(), websocket.MessageText,
			[]byte(`0{"sid":"abc","upgrades":[],"pingInterval":25000,"pingTimeout":20000}`))
		// Read client response.
		_, msg, err := conn.Read(r.Context())
		if err == nil {
			sentCh <- string(msg)
		}
		time.Sleep(100 * time.Millisecond)
	}))
	defer srv.Close()

	wake := make(chan struct{}, 1)
	listener := NewSocketListener(
		"http://"+srv.Listener.Addr().String(), "key", wake,
		func(_ context.Context, _ events.Event) error { return nil },
	)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	// connect returns when server closes; ignore error.
	_ = listener.connect(ctx)

	select {
	case got := <-sentCh:
		if got != "40" {
			t.Errorf("expected Socket.IO connect packet '40', got %q", got)
		}
	default:
		t.Error("expected Socket.IO connect packet, got nothing")
	}
}

func TestHandleFrame_PingRepliedWithPong(t *testing.T) {
	// responseCh receives the first message the client sends after the ping.
	responseCh := make(chan string, 1)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, _ := websocket.Accept(w, r, &websocket.AcceptOptions{InsecureSkipVerify: true})
		defer conn.CloseNow()
		_ = conn.Write(r.Context(), websocket.MessageText, []byte("2"))
		_, msg, err := conn.Read(r.Context())
		if err == nil {
			responseCh <- string(msg)
		}
		time.Sleep(100 * time.Millisecond)
	}))
	defer srv.Close()

	wake := make(chan struct{}, 1)
	listener := NewSocketListener(
		"http://"+srv.Listener.Addr().String(), "key", wake,
		func(_ context.Context, _ events.Event) error { return nil },
	)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_ = listener.connect(ctx)

	select {
	case got := <-responseCh:
		if got != "3" {
			t.Errorf("expected pong '3', got %q", got)
		}
	default:
		t.Error("expected pong, got nothing")
	}
}

func TestHandleSocketIOMessage_UploadSuccess(t *testing.T) {
	payload := buildEvent("on_upload_success", map[string]string{"id": "asset-123"})
	srv := wsServer(t, []string{
		`0{"sid":"x","upgrades":[],"pingInterval":25000,"pingTimeout":20000}`,
		"4" + payload,
	})
	defer srv.Close()

	var published []events.Event
	wake := make(chan struct{}, 1)
	listener := NewSocketListener(
		"http://"+srv.Listener.Addr().String(), "key", wake,
		func(_ context.Context, ev events.Event) error {
			published = append(published, ev)
			return nil
		},
	)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_ = listener.connect(ctx)

	if len(published) == 0 {
		t.Fatal("no event published")
	}
	ev := published[0]
	if ev.Type != events.AssetCreated {
		t.Errorf("type = %q, want AssetCreated", ev.Type)
	}
	if ev.AssetID != "asset-123" {
		t.Errorf("assetId = %q, want asset-123", ev.AssetID)
	}
	if ev.Source != "socket" {
		t.Errorf("source = %q, want socket", ev.Source)
	}

	// wake channel should have been signalled.
	select {
	case <-wake:
	default:
		t.Error("wake channel not signalled")
	}
}

func TestHandleSocketIOMessage_AssetDelete(t *testing.T) {
	payload := buildEvent("on_asset_delete", map[string]string{"id": "del-1"})
	srv := wsServer(t, []string{"4" + payload})
	defer srv.Close()

	var published []events.Event
	wake := make(chan struct{}, 1)
	listener := NewSocketListener(
		"http://"+srv.Listener.Addr().String(), "key", wake,
		func(_ context.Context, ev events.Event) error {
			published = append(published, ev)
			return nil
		},
	)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_ = listener.connect(ctx)

	if len(published) == 0 {
		t.Fatal("no event published")
	}
	if published[0].Type != events.AssetDeleted {
		t.Errorf("type = %q, want AssetDeleted", published[0].Type)
	}
}

func TestHandleSocketIOMessage_UnknownEvent(t *testing.T) {
	payload := buildEvent("on_some_unknown_event", nil)
	srv := wsServer(t, []string{"4" + payload})
	defer srv.Close()

	var published []events.Event
	wake := make(chan struct{}, 1)
	listener := NewSocketListener(
		"http://"+srv.Listener.Addr().String(), "key", wake,
		func(_ context.Context, ev events.Event) error {
			published = append(published, ev)
			return nil
		},
	)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_ = listener.connect(ctx)

	if len(published) != 0 {
		t.Errorf("expected no published events for unknown socket event, got %d", len(published))
	}
}

func TestSocketListener_ReconnectsOnError(t *testing.T) {
	calls := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		// Reject the WebSocket upgrade to force an error.
		http.Error(w, "not a websocket", http.StatusBadRequest)
	}))
	defer srv.Close()

	wake := make(chan struct{}, 1)
	listener := NewSocketListener(
		"http://"+srv.Listener.Addr().String(), "key", wake,
		func(_ context.Context, _ events.Event) error { return nil },
	)
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	listener.Run(ctx)

	// Should have attempted at least one connection.
	if calls == 0 {
		t.Error("expected at least one connection attempt")
	}
}

// buildEvent encodes a Socket.IO event packet for the given name and args.
func buildEvent(name string, args any) string {
	var parts []any
	parts = append(parts, name)
	if args != nil {
		parts = append(parts, args)
	}
	data, _ := json.Marshal(parts)
	return "2" + string(data) // SIO type 2 = EVENT
}
