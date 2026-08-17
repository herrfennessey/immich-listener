// Package config loads the sidecar configuration from environment variables.
package config

import (
	"fmt"
	"time"

	"github.com/Netflix/go-env"
	"github.com/go-playground/validator/v10"
)

// Config holds the runtime configuration.
type Config struct {
	// ImmichBaseURL is the Immich server base URL.
	ImmichBaseURL string `env:"IMMICH_BASE_URL,default=http://immich-server:2283" validate:"required,url"`
	// ImmichAPIKey is the Immich owner API key.
	ImmichAPIKey string `env:"IMMICH_API_KEY" validate:"required"`

	// NATSURL is the NATS server URL.
	NATSURL string `env:"NATS_URL,default=nats://localhost:4222" validate:"required"`
	// NATSStreamName is the JetStream stream name.
	NATSStreamName string `env:"NATS_STREAM_NAME,default=IMMICH" validate:"required"`
	// NATSSubjectPrefix is the prefix for every subject.
	NATSSubjectPrefix string `env:"NATS_SUBJECT_PREFIX,default=immich" validate:"required"`

	// SyncInterval is the idle wait between sync passes. It must be positive.
	// A zero or negative value makes the sync loop run without pause.
	SyncInterval time.Duration `env:"SYNC_INTERVAL,default=30s" validate:"gt=0"`

	// SocketIOEnabled starts the Socket.IO listener when true.
	SocketIOEnabled bool `env:"SOCKETIO_ENABLED,default=true"`
}

// Load reads the environment into a Config and validates it. Load returns an
// error for a missing or empty required value, or a non-positive SyncInterval.
func Load() (*Config, error) {
	var c Config
	if _, err := env.UnmarshalFromEnviron(&c); err != nil {
		return nil, fmt.Errorf("load config: %w", err)
	}
	if err := validator.New().Struct(&c); err != nil {
		return nil, fmt.Errorf("invalid config: %w", err)
	}
	return &c, nil
}
