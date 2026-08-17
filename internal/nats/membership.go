package nats

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"

	"github.com/nats-io/nats.go/jetstream"
)

// MembershipStore persists the assetID → set of albumIDs mapping in a JetStream
// KV bucket.
//
// It exists because Immich only emits an AlbumToAssetV1 delta when membership
// *changes*: an ordinary edit to an asset already in an album arrives as a bare
// AssetV2 row with no membership delta in the batch. Resolving albumIds from the
// batch alone would therefore omit them, and a downstream gallery listener could
// not tell which album manifests to rebuild. The KV map is seeded from the
// AlbumToAsset backfill on first sync and maintained by every membership delta
// thereafter, so an asset upsert can always be enriched with its full album set.
//
// The sidecar's sync loop is strictly single-flight, so this is a single-writer
// store and a plain read-modify-write is sufficient.
type MembershipStore struct {
	kv jetstream.KeyValue
}

// NewMembershipStore creates or opens the KV bucket used for asset→albums membership.
func NewMembershipStore(ctx context.Context, js jetstream.JetStream, bucket string) (*MembershipStore, error) {
	kv, err := js.CreateOrUpdateKeyValue(ctx, jetstream.KeyValueConfig{
		Bucket:      bucket,
		Description: "Immich asset → album membership index",
		History:     1,
	})
	if err != nil {
		return nil, fmt.Errorf("create/open KV bucket %q: %w", bucket, err)
	}
	return &MembershipStore{kv: kv}, nil
}

// Add records that assetID belongs to albumID. Idempotent.
func (m *MembershipStore) Add(ctx context.Context, assetID, albumID string) error {
	return m.mutate(ctx, assetID, func(set map[string]struct{}) { set[albumID] = struct{}{} })
}

// Remove records that assetID no longer belongs to albumID. Idempotent.
func (m *MembershipStore) Remove(ctx context.Context, assetID, albumID string) error {
	return m.mutate(ctx, assetID, func(set map[string]struct{}) { delete(set, albumID) })
}

// Albums returns the sorted set of albumIDs the asset currently belongs to.
// An asset with no recorded membership returns an empty slice, not an error.
func (m *MembershipStore) Albums(ctx context.Context, assetID string) ([]string, error) {
	return m.load(ctx, assetID)
}

func (m *MembershipStore) mutate(ctx context.Context, assetID string, fn func(map[string]struct{})) error {
	albums, err := m.load(ctx, assetID)
	if err != nil {
		return err
	}
	set := make(map[string]struct{}, len(albums))
	for _, a := range albums {
		set[a] = struct{}{}
	}
	fn(set)

	key := membershipKey(assetID)
	if len(set) == 0 {
		// No memberships left: drop the key rather than storing an empty list, so
		// deleted assets don't accumulate in the bucket.
		if err := m.kv.Delete(ctx, key); err != nil && !errors.Is(err, jetstream.ErrKeyNotFound) {
			return fmt.Errorf("membership delete %s: %w", assetID, err)
		}
		return nil
	}

	out := make([]string, 0, len(set))
	for a := range set {
		out = append(out, a)
	}
	sort.Strings(out)

	data, err := json.Marshal(out)
	if err != nil {
		return err
	}
	if _, err := m.kv.Put(ctx, key, data); err != nil {
		return fmt.Errorf("membership put %s: %w", assetID, err)
	}
	return nil
}

// load returns the current album set for an asset. A missing key yields (nil, nil).
func (m *MembershipStore) load(ctx context.Context, assetID string) ([]string, error) {
	entry, err := m.kv.Get(ctx, membershipKey(assetID))
	if errors.Is(err, jetstream.ErrKeyNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("membership get %s: %w", assetID, err)
	}
	var albums []string
	if err := json.Unmarshal(entry.Value(), &albums); err != nil {
		return nil, fmt.Errorf("membership decode %s: %w", assetID, err)
	}
	return albums, nil
}

// membershipKey maps an asset ID to a KV key. Asset IDs are UUIDs, which are
// already valid KV key characters.
func membershipKey(assetID string) string { return assetID }
