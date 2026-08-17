// Package events defines the canonical event types published to NATS.
package events

// Type enumerates the event types the sidecar publishes.
type Type string

const (
	// AssetUpserted covers asset create and update (AssetV2 from sync stream).
	AssetUpserted Type = "asset.upserted"
	// AssetTrashed is emitted when an asset is moved to the Immich trash.
	AssetTrashed Type = "asset.trashed"
	// AssetDeleted is emitted when an asset is permanently deleted (AssetDeleteV1).
	AssetDeleted Type = "asset.deleted"

	// AlbumChanged covers album metadata create/update (AlbumV2).
	AlbumChanged Type = "album.changed"
	// AlbumDeleted is emitted when an album is permanently deleted (AlbumDeleteV1).
	AlbumDeleted Type = "album.deleted"
	// AlbumMembership is emitted when assets are added to or removed from an album.
	AlbumMembership Type = "album.membership"
)

// Event is the envelope published on every NATS subject.
// The payload is intentionally minimal — carry identity, not data.
// Downstream consumers re-fetch state from Immich using the IDs provided.
type Event struct {
	// Type identifies the kind of change.
	Type Type `json:"type"`
	// AssetID is set for asset events.
	AssetID string `json:"assetId,omitempty"`
	// AlbumID is set for album events and for AlbumMembership events.
	AlbumID string `json:"albumId,omitempty"`
	// AlbumIDs is the full list of albums the asset belongs to, resolved from
	// the AlbumToAsset deltas seen in the same sync batch.  Set on AssetUpserted.
	AlbumIDs []string `json:"albumIds,omitempty"`
	// Removed is true for AlbumMembership events that represent a removal.
	Removed bool `json:"removed,omitempty"`
}

// Subject returns the NATS subject for this event, given the configured prefix.
// Example: prefix="immich", Type=AssetUpserted → "immich.asset.upserted"
func (e Event) Subject(prefix string) string {
	return prefix + "." + string(e.Type)
}
