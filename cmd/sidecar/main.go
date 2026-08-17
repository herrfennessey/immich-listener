// Command sidecar connects Immich events to a message queue.
//
// It holds a Socket.IO connection for low-latency signals and polls
// /api/sync/stream for durable, checkpointed delivery. It sends every change to
// a Publisher adapter, then advances the Immich cursor.
//
// The queue is a port (see internal/core). This binary uses the NATS adapter.
// To use a different queue, add an adapter and wire it here.
//
// The environment holds the configuration. See internal/config.
package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	natsadapter "github.com/herrfennessey/immich-listener/internal/adapters/nats"
	"github.com/herrfennessey/immich-listener/internal/config"
	immichpkg "github.com/herrfennessey/immich-listener/internal/immich"
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

	// Outbound adapter: the message queue. Replace this to use another queue.
	publisher, err := natsadapter.NewPublisher(ctx, cfg.NATSURL, cfg.NATSStreamName, cfg.NATSSubjectPrefix)
	if err != nil {
		slog.Error("nats publisher error", "err", err)
		os.Exit(1)
	}
	defer publisher.Close()

	// AlbumResolver reads album membership from the Immich API.
	albums := immichpkg.NewAlbumClient(cfg.ImmichBaseURL, cfg.ImmichAPIKey)

	// wake lets the socket listener start a sync pass at once.
	wake := make(chan struct{}, 1)

	sync := immichpkg.NewSyncStreamConsumer(cfg.ImmichBaseURL, cfg.ImmichAPIKey, publisher, albums)

	if cfg.SocketIOEnabled {
		socketListener := immichpkg.NewSocketListener(cfg.ImmichBaseURL, cfg.ImmichAPIKey, wake)
		go socketListener.Run(ctx)
	}

	slog.Info("sidecar started",
		"immich", cfg.ImmichBaseURL,
		"nats", cfg.NATSURL,
		"stream", cfg.NATSStreamName,
		"socketio", cfg.SocketIOEnabled,
	)

	sync.Run(ctx, wake, cfg.SyncInterval)
	slog.Info("sidecar stopped")
}
