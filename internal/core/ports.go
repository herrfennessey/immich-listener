// Package core defines the ports between the sidecar and its adapters.
//
// The sidecar reads changes from Immich and sends events to a message queue.
// The queue is a port. Each queue integration is an adapter that implements the
// Publisher interface. To use a different queue, add an adapter. Do not change
// the sidecar core.
package core

import (
	"context"

	"github.com/herrfennessey/immich-listener/internal/events"
)

// Publisher sends one event to a durable message queue. Adapters implement this
// interface. The sidecar uses one Publisher at a time.
type Publisher interface {
	// Publish sends the event and returns only after the queue stores it.
	// A non-nil error means the queue did not store the event.
	Publish(ctx context.Context, event events.Event) error
}

// AlbumResolver returns the IDs of the albums that contain an asset.
type AlbumResolver interface {
	// Albums returns the album IDs for the asset. An asset in no album returns
	// an empty slice.
	Albums(ctx context.Context, assetID string) ([]string, error)
}
