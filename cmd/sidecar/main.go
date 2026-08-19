// Command sidecar connects Immich events to a message queue.
//
// It holds a Socket.IO connection for low-latency signals and polls
// /api/sync/stream for durable, checkpointed delivery. It sends every change to
// a Publisher adapter, then advances the Immich cursor.
//
// The messaging system is a port (see internal/core). This binary uses the NATS
// adapter. To use a different messaging system, add an adapter and wire it here.
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

	// Outbound adapter: the messaging system. Replace this to use another one.
	publisher, err := natsadapter.NewPublisher(ctx, cfg.NATSURL, cfg.NATSStreamName, cfg.NATSSubjectPrefix)
	if err != nil {
		slog.Error("nats publisher error", "err", err)
		os.Exit(1)
	}
	defer publisher.Close()

	// Immich's checkpointed sync API requires a user session; it deliberately
	// rejects API keys. Verify the credentials before starting the run loop so a
	// configuration error is immediately visible.
	session := immichpkg.NewSessionClient(cfg.ImmichBaseURL, cfg.ImmichEmail, cfg.ImmichPassword, cfg.ImmichSessionTokenFile)
	if _, err := session.Token(ctx); err != nil {
		slog.Error("immich session login error", "err", err)
		os.Exit(1)
	}

	// AlbumResolver reads album membership from the Immich API.
	albums := immichpkg.NewAlbumClient(cfg.ImmichBaseURL, session)

	// wake lets the socket listener start a sync pass at once.
	wake := make(chan struct{}, 1)

	sync := immichpkg.NewSyncStreamConsumer(cfg.ImmichBaseURL, session, publisher, albums)

	if cfg.SocketIOEnabled {
		socketListener := immichpkg.NewSocketListener(cfg.ImmichBaseURL, session, wake)
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
