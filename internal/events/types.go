// Package events defines the canonical event types published to NATS.
package events

// Type enumerates the event types the sidecar publishes.
type Type string

const (
	// AssetUpserted covers asset create and update (an AssetV2 row with no
	// deletedAt, or an AssetExifV1 metadata change).
	AssetUpserted Type = "asset.upserted"
	// AssetTrashed is emitted when an asset is moved to the Immich trash
	// (an AssetV2 row carrying a non-null deletedAt).
	AssetTrashed Type = "asset.trashed"
	// AssetDeleted is emitted when an asset is permanently deleted (AssetDeleteV1).
	AssetDeleted Type = "asset.deleted"

	// AlbumChanged covers album metadata create/update (AlbumV2).
	AlbumChanged Type = "album.changed"
	// AlbumDeleted is emitted when an album is permanently deleted (AlbumDeleteV1).
	AlbumDeleted Type = "album.deleted"
	// AlbumMembership is emitted when an asset is added to or removed from an
	// album (AlbumToAssetV1 / AlbumToAssetDeleteV1).
	AlbumMembership Type = "album.membership"
)

// Event is the message published on a subject. The payload holds IDs and a few
// fields. A downstream reads full state from Immich with the IDs.
type Event struct {
	// Type is the kind of change.
	Type Type `json:"type"`

	// AssetID is set for asset events and AlbumMembership events.
	AssetID string `json:"assetId,omitempty"`
	// AlbumID is set for album events and AlbumMembership events.
	AlbumID string `json:"albumId,omitempty"`
	// AlbumIDs is the full set of albums that contain the asset. It is set on
	// AssetUpserted and AssetTrashed.
	AlbumIDs []string `json:"albumIds,omitempty"`

	// OwnerID, Checksum, and AssetType are set on AssetUpserted.
	OwnerID   string `json:"ownerId,omitempty"`
	Checksum  string `json:"checksum,omitempty"`
	AssetType string `json:"assetType,omitempty"`

	// Name and Description are set on AlbumChanged.
	Name        string `json:"name,omitempty"`
	Description string `json:"description,omitempty"`

	// Present is set on AlbumMembership. true means the asset was added to the
	// album. false means the asset was removed.
	Present *bool `json:"present,omitempty"`
}

// Subject returns the NATS subject for this event, given the configured prefix.
// Example: prefix="immich", Type=AssetUpserted → "immich.asset.upserted"
func (e Event) Subject(prefix string) string {
	return prefix + "." + string(e.Type)
}
