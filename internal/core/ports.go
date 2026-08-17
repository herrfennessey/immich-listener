// Package core defines the ports between the sidecar and its adapters.
//
// The sidecar reads changes from Immich and sends events to a messaging system.
// The messaging system is a port. Each integration is an adapter that
// implements the Publisher interface. To use a different messaging system, add
// an adapter. Do not change the sidecar core.
package core

import (
	"context"

	"github.com/herrfennessey/immich-listener/internal/events"
)

// Publisher sends one event to a durable messaging system. Adapters implement
// this interface. The sidecar uses one Publisher at a time.
type Publisher interface {
	// Publish sends the event and returns only after the messaging system
	// stores it. A non-nil error means the system did not store the event.
	Publish(ctx context.Context, event events.Event) error
}

// AlbumResolver returns the IDs of the albums that contain an asset.
type AlbumResolver interface {
	// Albums returns the album IDs for the asset. An asset in no album returns
	// an empty slice.
	Albums(ctx context.Context, assetID string) ([]string, error)
}
