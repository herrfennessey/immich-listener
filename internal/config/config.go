// Package config loads sidecar configuration from environment variables.
package config

import (
	"fmt"
	"os"
	"strconv"
	"time"
)

// Config holds all runtime configuration for the sidecar.
type Config struct {
	// ImmichBaseURL is the base URL of the Immich server, e.g. "http://immich:2283".
	ImmichBaseURL string
	// ImmichAPIKey is the Immich owner API key used for authentication.
	ImmichAPIKey string

	// NATSUrl is the NATS server URL, e.g. "nats://nats:4222".
	NATSUrl string
	// NATSStreamName is the JetStream stream name to publish to.
	NATSStreamName string
	// NATSSubjectPrefix is prepended to every published subject, defaults to "immich".
	NATSSubjectPrefix string

	// MembershipBucket is the JetStream KV bucket holding the asset→albums index.
	MembershipBucket string

	// SyncInterval is how long the sidecar waits between sync stream passes when idle.
	// The spec calls for a 10-minute backstop; the default here is 30s.
	SyncInterval time.Duration

	// SocketIOEnabled controls whether the Socket.IO real-time listener is started.
	SocketIOEnabled bool
}

// Load reads configuration from environment variables and returns a validated Config.
func Load() (*Config, error) {
	c := &Config{
		ImmichBaseURL:     getEnv("IMMICH_BASE_URL", "http://immich-server:2283"),
		ImmichAPIKey:      os.Getenv("IMMICH_API_KEY"),
		NATSUrl:           getEnv("NATS_URL", "nats://localhost:4222"),
		NATSStreamName:    getEnv("NATS_STREAM_NAME", "IMMICH"),
		NATSSubjectPrefix: getEnv("NATS_SUBJECT_PREFIX", "immich"),
		MembershipBucket:  getEnv("MEMBERSHIP_BUCKET", "immich_asset_albums"),
		SyncInterval:      parseDuration(os.Getenv("SYNC_INTERVAL"), 30*time.Second),
		SocketIOEnabled:   parseBool(os.Getenv("SOCKETIO_ENABLED"), true),
	}

	if c.ImmichAPIKey == "" {
		return nil, fmt.Errorf("IMMICH_API_KEY is required")
	}
	return c, nil
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func parseDuration(s string, fallback time.Duration) time.Duration {
	if s == "" {
		return fallback
	}
	d, err := time.ParseDuration(s)
	if err != nil {
		return fallback
	}
	return d
}

func parseBool(s string, fallback bool) bool {
	if s == "" {
		return fallback
	}
	b, err := strconv.ParseBool(s)
	if err != nil {
		return fallback
	}
	return b
}
