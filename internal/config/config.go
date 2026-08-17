// Package config loads the sidecar configuration from environment variables.
package config

import (
	"fmt"
	"time"

	"github.com/Netflix/go-env"
)

// Config holds the runtime configuration.
type Config struct {
	// ImmichBaseURL is the Immich server base URL.
	ImmichBaseURL string `env:"IMMICH_BASE_URL,default=http://immich-server:2283"`
	// ImmichAPIKey is the Immich owner API key.
	ImmichAPIKey string `env:"IMMICH_API_KEY,required=true"`

	// NATSURL is the NATS server URL.
	NATSURL string `env:"NATS_URL,default=nats://localhost:4222"`
	// NATSStreamName is the JetStream stream name.
	NATSStreamName string `env:"NATS_STREAM_NAME,default=IMMICH"`
	// NATSSubjectPrefix is the prefix for every subject.
	NATSSubjectPrefix string `env:"NATS_SUBJECT_PREFIX,default=immich"`

	// SyncInterval is the idle wait between sync passes.
	SyncInterval time.Duration `env:"SYNC_INTERVAL,default=30s"`

	// SocketIOEnabled starts the Socket.IO listener when true.
	SocketIOEnabled bool `env:"SOCKETIO_ENABLED,default=true"`
}

// Load reads the environment into a Config and returns it.
func Load() (*Config, error) {
	var c Config
	if _, err := env.UnmarshalFromEnviron(&c); err != nil {
		return nil, fmt.Errorf("load config: %w", err)
	}
	return &c, nil
}
