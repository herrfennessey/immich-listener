// Package events defines the canonical event types published to NATS.
package events

// Type enumerates the event types the sidecar publishes.
type Type string

const (
	// Asset events sourced from sync stream and Socket.IO.
	AssetCreated Type = "asset.created"
	AssetUpdated Type = "asset.updated"
	AssetDeleted Type = "asset.deleted"

	// Album events sourced from sync stream.
	AlbumUpdated          Type = "album.updated"
	AlbumDeleted          Type = "album.deleted"
	AlbumAssetAdded       Type = "album.asset.added"
	AlbumAssetRemoved     Type = "album.asset.removed"
)

// Event is the envelope published on every NATS subject.
// The payload is intentionally minimal — carry identity, not data.
// Downstream consumers re-fetch state from Immich using the IDs provided.
type Event struct {
	// Type identifies the kind of change.
	Type Type `json:"type"`
	// Source indicates where the event originated: "socket" or "sync".
	Source string `json:"source"`
	// AssetID is set for asset events.
	AssetID string `json:"assetId,omitempty"`
	// AlbumID is set for album events.
	AlbumID string `json:"albumId,omitempty"`
}

// Subject returns the NATS subject for this event, given the configured prefix.
// Example: prefix="immich", Type=AssetCreated → "immich.asset.created"
func (e Event) Subject(prefix string) string {
	return prefix + "." + string(e.Type)
}
