// Package nats is a Publisher adapter for NATS JetStream.
//
// It implements core.Publisher. It is one possible queue. To use a different
// queue, write a new adapter that implements core.Publisher.
package nats

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/herrfennessey/immich-listener/internal/core"
	"github.com/herrfennessey/immich-listener/internal/events"
	natsclient "github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// Publisher sends events to a NATS JetStream stream.
type Publisher struct {
	nc     *natsclient.Conn
	js     jetstream.JetStream
	stream string
	prefix string
}

// compile-time check that Publisher satisfies the port.
var _ core.Publisher = (*Publisher)(nil)

// NewPublisher connects to NATS and makes sure the stream exists.
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

	// Create the stream, or update it if it exists.
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

// Publish sends the event and waits for the JetStream acknowledgement.
func (p *Publisher) Publish(ctx context.Context, ev events.Event) error {
	data, err := json.Marshal(ev)
	if err != nil {
		return fmt.Errorf("marshal event: %w", err)
	}
	subject := ev.Subject(p.prefix)
	if _, err := p.js.Publish(ctx, subject, data); err != nil {
		return fmt.Errorf("publish to %q: %w", subject, err)
	}
	slog.Debug("published event", "subject", subject, "assetId", ev.AssetID, "albumId", ev.AlbumID)
	return nil
}

// Close drains and closes the NATS connection.
func (p *Publisher) Close() {
	_ = p.nc.Drain()
}
