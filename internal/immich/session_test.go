package immich

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
)

func TestSessionClient_LogsInAndReusesToken(t *testing.T) {
	var loginCalls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/auth/login" {
			http.NotFound(w, r)
			return
		}
		loginCalls.Add(1)
		var credentials loginCredentials
		if err := json.NewDecoder(r.Body).Decode(&credentials); err != nil {
			t.Fatalf("decode credentials: %v", err)
		}
		if credentials.Email != "listener@example.com" || credentials.Password != "correct-horse" {
			t.Fatalf("credentials = %+v", credentials)
		}
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte(`{"accessToken":"session-one"}`))
	}))
	defer srv.Close()

	client := NewSessionClient(srv.URL, "listener@example.com", "correct-horse", filepath.Join(t.TempDir(), "session-token"))
	first, err := client.Token(context.Background())
	if err != nil {
		t.Fatalf("first Token: %v", err)
	}
	second, err := client.Token(context.Background())
	if err != nil {
		t.Fatalf("second Token: %v", err)
	}
	if first != "session-one" || second != "session-one" {
		t.Fatalf("tokens = %q, %q", first, second)
	}
	if got := loginCalls.Load(); got != 1 {
		t.Errorf("login calls = %d, want 1", got)
	}
}

func TestSessionClient_RenewsFailedToken(t *testing.T) {
	var loginCalls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		loginCalls.Add(1)
		w.WriteHeader(http.StatusCreated)
		_, _ = fmt.Fprintf(w, `{"accessToken":"session-%d"}`, loginCalls.Load())
	}))
	defer srv.Close()

	client := NewSessionClient(srv.URL, "listener@example.com", "correct-horse", filepath.Join(t.TempDir(), "session-token"))
	first, err := client.Token(context.Background())
	if err != nil {
		t.Fatalf("first Token: %v", err)
	}
	second, err := client.Renew(context.Background(), first)
	if err != nil {
		t.Fatalf("Renew: %v", err)
	}
	if first != "session-1" || second != "session-2" {
		t.Fatalf("tokens = %q, %q", first, second)
	}
}

func TestSessionClient_ReusesPersistedToken(t *testing.T) {
	tokenFile := filepath.Join(t.TempDir(), "session-token")
	if err := os.WriteFile(tokenFile, []byte(`{"email":"listener@example.com","token":"persisted-session"}`), 0o600); err != nil {
		t.Fatalf("write persisted token: %v", err)
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Fatal("login should not occur when a persisted token exists")
	}))
	defer srv.Close()

	client := NewSessionClient(srv.URL, "listener@example.com", "correct-horse", tokenFile)
	token, err := client.Token(context.Background())
	if err != nil {
		t.Fatalf("Token: %v", err)
	}
	if token != "persisted-session" {
		t.Errorf("token = %q, want persisted-session", token)
	}
}

func TestSessionClient_DiscardsTokenForDifferentEmail(t *testing.T) {
	tokenFile := filepath.Join(t.TempDir(), "session-token")
	if err := os.WriteFile(tokenFile, []byte(`{"email":"former@example.com","token":"former-token"}`), 0o600); err != nil {
		t.Fatalf("write persisted token: %v", err)
	}
	var loginCalls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		loginCalls.Add(1)
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte(`{"accessToken":"current-token"}`))
	}))
	defer srv.Close()

	client := NewSessionClient(srv.URL, "current@example.com", "correct-horse", tokenFile)
	token, err := client.Token(context.Background())
	if err != nil {
		t.Fatalf("Token: %v", err)
	}
	if token != "current-token" {
		t.Errorf("token = %q, want current-token", token)
	}
	if got := loginCalls.Load(); got != 1 {
		t.Errorf("login calls = %d, want 1", got)
	}
}
