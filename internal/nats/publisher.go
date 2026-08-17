// Package nats wraps the NATS JetStream client used by the sidecar.
package nats

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/herrfennessey/immich-listener/internal/events"
	natsclient "github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// Publisher publishes events to a NATS JetStream stream.
type Publisher struct {
	nc     *natsclient.Conn
	js     jetstream.JetStream
	stream string
	prefix string
}

// NewPublisher connects to NATS and ensures the stream exists, then returns a Publisher.
func NewPublisher(ctx context.Context, url, streamName, subjectPrefix string) (*Publisher, error) {
	nc, err := natsclient.Connect(url,
		natsclient.RetryOnFailedConnect(true),
		natsclient.MaxReconnects(-1),
		natsclient.ReconnectWait(2*time.Second),
	)
	if err != nil {
		return nil, fmt.Errorf("nats connect: %w", err)
	}

	js, err := jetstream.New(nc)
	if err != nil {
		nc.Close()
		return nil, fmt.Errorf("jetstream context: %w", err)
	}

	// Ensure the stream exists; update config if it already exists.
	_, err = js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:        streamName,
		Subjects:    []string{subjectPrefix + ".>"},
		Storage:     jetstream.FileStorage,
		Retention:   jetstream.LimitsPolicy,
		MaxAge:      7 * 24 * time.Hour,
		Replicas:    1,
		Description: "Immich lifecycle events",
	})
	if err != nil {
		nc.Close()
		return nil, fmt.Errorf("create/update stream %q: %w", streamName, err)
	}

	slog.Info("connected to NATS JetStream", "stream", streamName, "subjects", subjectPrefix+".>")

	return &Publisher{nc: nc, js: js, stream: streamName, prefix: subjectPrefix}, nil
}

// Publish marshals the event and publishes it to JetStream.
func (p *Publisher) Publish(ctx context.Context, ev events.Event) error {
	data, err := json.Marshal(ev)
	if err != nil {
		return fmt.Errorf("marshal event: %w", err)
	}
	subject := ev.Subject(p.prefix)
	_, err = p.js.Publish(ctx, subject, data)
	if err != nil {
		return fmt.Errorf("publish to %q: %w", subject, err)
	}
	slog.Debug("published event", "subject", subject, "assetId", ev.AssetID, "albumId", ev.AlbumID)
	return nil
}

// Close drains and closes the NATS connection.
func (p *Publisher) Close() {
	_ = p.nc.Drain()
}
