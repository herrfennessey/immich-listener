// Command sidecar is the Immich event-to-NATS bridge.
//
// It connects to the Immich Socket.IO gateway for low-latency event delivery
// and continuously polls /api/sync/stream for durable, checkpointed delivery.
// All events are published to a NATS JetStream stream as JSON.
//
// Configuration is entirely via environment variables; see internal/config.
package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/herrfennessey/immich-listener/internal/config"
	"github.com/herrfennessey/immich-listener/internal/events"
	immichpkg "github.com/herrfennessey/immich-listener/internal/immich"
	natspkg "github.com/herrfennessey/immich-listener/internal/nats"
)

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	})))

	cfg, err := config.Load()
	if err != nil {
		slog.Error("config error", "err", err)
		os.Exit(1)
	}

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	pub, err := natspkg.NewPublisher(ctx, cfg.NATSUrl, cfg.NATSStreamName, cfg.NATSSubjectPrefix)
	if err != nil {
		slog.Error("nats publisher error", "err", err)
		os.Exit(1)
	}
	defer pub.Close()

	publish := func(ctx context.Context, ev events.Event) error {
		return pub.Publish(ctx, ev)
	}

	// wake is used by the socket listener to poke the sync stream consumer
	// into running immediately rather than waiting for the next tick.
	wake := make(chan struct{}, 1)

	sync := immichpkg.NewSyncStreamConsumer(
		cfg.ImmichBaseURL, cfg.ImmichAPIKey, cfg.CheckpointFile, publish)

	if cfg.SocketIOEnabled {
		socketListener := immichpkg.NewSocketListener(
			cfg.ImmichBaseURL, cfg.ImmichAPIKey, wake, publish)
		go socketListener.Run(ctx)
	}

	slog.Info("sidecar started",
		"immich", cfg.ImmichBaseURL,
		"nats", cfg.NATSUrl,
		"stream", cfg.NATSStreamName,
		"socketio", cfg.SocketIOEnabled,
	)

	sync.Run(ctx, wake, cfg.SyncInterval)
	slog.Info("sidecar stopped")
}
