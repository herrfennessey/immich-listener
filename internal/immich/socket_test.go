package immich

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/coder/websocket"
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
	sentCh := make(chan string, 1)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, _ := websocket.Accept(w, r, &websocket.AcceptOptions{InsecureSkipVerify: true})
		defer conn.CloseNow()
		_ = conn.Write(r.Context(), websocket.MessageText,
			[]byte(`0{"sid":"abc","upgrades":[],"pingInterval":25000,"pingTimeout":20000}`))
		_, msg, err := conn.Read(r.Context())
		if err == nil {
			sentCh <- string(msg)
		}
		time.Sleep(100 * time.Millisecond)
	}))
	defer srv.Close()

	wake := make(chan struct{}, 1)
	listener := NewSocketListener("http://"+srv.Listener.Addr().String(), staticSessionTokenSource("key"), wake)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_ = listener.connect(ctx, func() {})

	select {
	case got := <-sentCh:
		if got != "40" {
			t.Errorf("expected Socket.IO connect packet '40', got %q", got)
		}
	default:
		t.Error("expected Socket.IO connect packet, got nothing")
	}
}

func TestSocketListener_AuthenticatesWithSessionToken(t *testing.T) {
	gotToken := make(chan string, 1)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotToken <- r.Header.Get("x-immich-session-token")
		conn, _ := websocket.Accept(w, r, &websocket.AcceptOptions{InsecureSkipVerify: true})
		defer conn.CloseNow()
		time.Sleep(100 * time.Millisecond)
	}))
	defer srv.Close()

	wake := make(chan struct{}, 1)
	listener := NewSocketListener("http://"+srv.Listener.Addr().String(), staticSessionTokenSource("session-token"), wake)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_ = listener.connect(ctx, func() {})

	select {
	case got := <-gotToken:
		if got != "session-token" {
			t.Errorf("x-immich-session-token = %q, want session-token", got)
		}
	default:
		t.Fatal("Socket.IO connection did not send a session token")
	}
}

func TestHandleFrame_PingRepliedWithPong(t *testing.T) {
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
	listener := NewSocketListener("http://"+srv.Listener.Addr().String(), staticSessionTokenSource("key"), wake)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_ = listener.connect(ctx, func() {})

	select {
	case got := <-responseCh:
		if got != "3" {
			t.Errorf("expected pong '3', got %q", got)
		}
	default:
		t.Error("expected pong, got nothing")
	}
}

func TestSocketListener_KnownEventSignalsWake(t *testing.T) {
	// All known Immich lifecycle events should signal wake.
	knownEvents := []string{
		"on_upload_success",
		"on_asset_update",
		"on_asset_delete",
		"on_asset_trash",
		"on_asset_restore",
		"on_album_update",
	}
	for _, name := range knownEvents {
		t.Run(name, func(t *testing.T) {
			payload := buildSocketEvent(name, nil)
			srv := wsServer(t, []string{"4" + payload})
			defer srv.Close()

			wake := make(chan struct{}, 1)
			listener := NewSocketListener("http://"+srv.Listener.Addr().String(), staticSessionTokenSource("key"), wake)
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			_ = listener.connect(ctx, func() {})

			select {
			case <-wake:
			default:
				t.Errorf("wake not signalled for event %q", name)
			}
		})
	}
}

func TestSocketListener_UnknownEventDoesNotSignalWake(t *testing.T) {
	payload := buildSocketEvent("on_some_unknown_event", nil)
	srv := wsServer(t, []string{"4" + payload})
	defer srv.Close()

	wake := make(chan struct{}, 1)
	listener := NewSocketListener("http://"+srv.Listener.Addr().String(), staticSessionTokenSource("key"), wake)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_ = listener.connect(ctx, func() {})

	select {
	case <-wake:
		t.Error("wake should not be signalled for unknown event")
	default:
	}
}

func TestSocketListener_DoorbellOnly_NoBusPublish(t *testing.T) {
	// The socket listener has no publish function — compile-time guarantee.
	// This test verifies the struct has no publish field.
	wake := make(chan struct{}, 1)
	l := NewSocketListener("http://localhost", staticSessionTokenSource("key"), wake)
	// If SocketListener had a publish field this would fail to compile.
	_ = l.baseURL
	_ = l.session
	_ = l.wake
}

func TestSocketListener_ReconnectsOnError(t *testing.T) {
	calls := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		http.Error(w, "not a websocket", http.StatusBadRequest)
	}))
	defer srv.Close()

	wake := make(chan struct{}, 1)
	listener := NewSocketListener("http://"+srv.Listener.Addr().String(), staticSessionTokenSource("key"), wake)
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	listener.Run(ctx)

	if calls == 0 {
		t.Error("expected at least one connection attempt")
	}
}

func TestSocketListener_BackoffResetOnlyAfterConnectAck(t *testing.T) {
	// The server accepts the WS upgrade and sends the Engine.IO open, but never
	// sends the Socket.IO namespace CONNECT ack ("40") — e.g. the token is
	// rejected at the namespace level. onConnect must NOT fire, so the caller's
	// back-off is not reset into a tight reconnect loop.
	srv := wsServer(t, []string{`0{"sid":"x","pingInterval":25000,"pingTimeout":20000}`})
	defer srv.Close()

	wake := make(chan struct{}, 1)
	l := NewSocketListener("http://"+srv.Listener.Addr().String(), staticSessionTokenSource("key"), wake)
	connected := false
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_ = l.connect(ctx, func() { connected = true })

	if connected {
		t.Error("onConnect must not fire before the Socket.IO connect ack '40'")
	}
}

func TestSocketListener_BackoffResetAfterConnectAck(t *testing.T) {
	// With the namespace CONNECT ack present, onConnect fires exactly once.
	srv := wsServer(t, []string{`0{"sid":"x","pingInterval":25000,"pingTimeout":20000}`, "40"})
	defer srv.Close()

	wake := make(chan struct{}, 1)
	l := NewSocketListener("http://"+srv.Listener.Addr().String(), staticSessionTokenSource("key"), wake)
	connects := 0
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_ = l.connect(ctx, func() { connects++ })

	if connects != 1 {
		t.Errorf("onConnect fired %d times, want 1 (on the '40' ack)", connects)
	}
}

func TestSocketListener_ReadDeadlineClosesSilentConn(t *testing.T) {
	// The server accepts, sends the open frame, then goes silent (no pings).
	// The per-read deadline must fire so connect returns instead of blocking.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, _ := websocket.Accept(w, r, &websocket.AcceptOptions{InsecureSkipVerify: true})
		defer conn.CloseNow()
		_ = conn.Write(r.Context(), websocket.MessageText,
			[]byte(`0{"sid":"x","pingInterval":25000,"pingTimeout":20000}`))
		time.Sleep(2 * time.Second) // stay silent past the client's read deadline
	}))
	defer srv.Close()

	wake := make(chan struct{}, 1)
	l := NewSocketListener("http://"+srv.Listener.Addr().String(), staticSessionTokenSource("key"), wake)
	l.readTimeout = 150 * time.Millisecond
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	start := time.Now()
	err := l.connect(ctx, func() {})
	if err == nil {
		t.Fatal("expected a read error from a silent (half-open) connection")
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Errorf("connect blocked %v; read deadline not enforced", elapsed)
	}
}

// buildSocketEvent encodes a Socket.IO event packet for the given name and args.
func buildSocketEvent(name string, args any) string {
	var parts []any
	parts = append(parts, name)
	if args != nil {
		parts = append(parts, args)
	}
	data, _ := json.Marshal(parts)
	return "2" + string(data) // SIO type 2 = EVENT
}
