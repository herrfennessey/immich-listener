package config

import (
	"os"
	"testing"
	"time"
)

func TestLoad_MissingAPIKey(t *testing.T) {
	os.Unsetenv("IMMICH_API_KEY")
	_, err := Load()
	if err == nil {
		t.Fatal("expected error when IMMICH_API_KEY is missing")
	}
}

func TestLoad_Defaults(t *testing.T) {
	t.Setenv("IMMICH_API_KEY", "test-key")
	os.Unsetenv("IMMICH_BASE_URL")
	os.Unsetenv("NATS_URL")
	os.Unsetenv("NATS_STREAM_NAME")
	os.Unsetenv("NATS_SUBJECT_PREFIX")
	os.Unsetenv("SYNC_INTERVAL")
	os.Unsetenv("SOCKETIO_ENABLED")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.ImmichBaseURL != "http://immich-server:2283" {
		t.Errorf("ImmichBaseURL = %q, want default", cfg.ImmichBaseURL)
	}
	if cfg.NATSURL != "nats://localhost:4222" {
		t.Errorf("NATSURL = %q, want default", cfg.NATSURL)
	}
	if cfg.NATSStreamName != "IMMICH" {
		t.Errorf("NATSStreamName = %q, want IMMICH", cfg.NATSStreamName)
	}
	if cfg.NATSSubjectPrefix != "immich" {
		t.Errorf("NATSSubjectPrefix = %q, want immich", cfg.NATSSubjectPrefix)
	}
	if cfg.SyncInterval != 30*time.Second {
		t.Errorf("SyncInterval = %v, want 30s", cfg.SyncInterval)
	}
	if !cfg.SocketIOEnabled {
		t.Error("SocketIOEnabled should default to true")
	}
}

func TestLoad_CustomValues(t *testing.T) {
	t.Setenv("IMMICH_API_KEY", "my-key")
	t.Setenv("IMMICH_BASE_URL", "http://immich:8080")
	t.Setenv("NATS_URL", "nats://nats:4222")
	t.Setenv("SYNC_INTERVAL", "1m")
	t.Setenv("SOCKETIO_ENABLED", "false")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.ImmichBaseURL != "http://immich:8080" {
		t.Errorf("ImmichBaseURL = %q", cfg.ImmichBaseURL)
	}
	if cfg.ImmichAPIKey != "my-key" {
		t.Errorf("ImmichAPIKey = %q", cfg.ImmichAPIKey)
	}
	if cfg.SyncInterval != time.Minute {
		t.Errorf("SyncInterval = %v", cfg.SyncInterval)
	}
	if cfg.SocketIOEnabled {
		t.Error("SocketIOEnabled should be false")
	}
}
