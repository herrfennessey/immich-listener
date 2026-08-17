// Package immich reads changes from the Immich server API.
package immich

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/herrfennessey/immich-listener/internal/core"
	"github.com/herrfennessey/immich-listener/internal/events"
)

// syncRequestTypes lists the SyncRequestType values the sidecar subscribes to.
// These are request type names (plural). They differ from the entity type names
// (singular) in each response row. One request type returns several entity
// types. AssetsV2 returns both AssetV2 and AssetDeleteV1 rows.
var syncRequestTypes = []string{
	"AssetsV2",        // AssetV2 (upsert/trash) and AssetDeleteV1 (permanent delete)
	"AssetExifsV1",    // AssetExifV1 (metadata edits)
	"AlbumsV2",        // AlbumV2 (metadata) and AlbumDeleteV1
	"AlbumToAssetsV1", // AlbumToAssetV1 and AlbumToAssetDeleteV1 (membership)
}

// SyncStreamConsumer reads POST /api/sync/stream, converts each delta to an
// event, sends it to the Publisher, and then advances the Immich cursor with
// POST /api/sync/ack. If a publish fails, the consumer does not ack, so Immich
// sends the same batch again.
//
// The sidecar keeps no local cursor. Immich stores the cursor in the
// session_sync_checkpoint table for the session of the API key.
type SyncStreamConsumer struct {
	baseURL    string
	apiKey     string
	httpClient *http.Client
	publisher  core.Publisher
	albums     core.AlbumResolver
}

// NewSyncStreamConsumer creates a consumer. publisher receives every event.
// albums returns the album IDs for an asset upsert.
func NewSyncStreamConsumer(baseURL, apiKey string, publisher core.Publisher, albums core.AlbumResolver) *SyncStreamConsumer {
	return &SyncStreamConsumer{
		baseURL:    baseURL,
		apiKey:     apiKey,
		httpClient: &http.Client{Timeout: 5 * time.Minute},
		publisher:  publisher,
		albums:     albums,
	}
}

// Run reads the sync stream on each tick or wake signal. Run stops when ctx ends.
func (s *SyncStreamConsumer) Run(ctx context.Context, wake <-chan struct{}, interval time.Duration) {
	tick := time.NewTimer(0)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		case <-wake:
		}
		// Discard extra wake signals that arrived during the last pass.
		for len(wake) > 0 {
			<-wake
		}
		if err := s.runOnce(ctx); err != nil {
			slog.Warn("sync stream error", "err", err)
		}
		tick.Reset(interval)
	}
}

// runOnce runs one sync pass.
//
// The consumer publishes every event first, then advances the cursor with
// POST /api/sync/ack. If a publish fails, runOnce returns an error and does not
// ack. Immich then sends the same batch again.
func (s *SyncStreamConsumer) runOnce(ctx context.Context) error {
	reqBody, err := json.Marshal(syncStreamRequest{Types: syncRequestTypes})
	if err != nil {
		return err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		s.baseURL+"/api/sync/stream", bytes.NewReader(reqBody))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/jsonlines+json")
	req.Header.Set("x-api-key", s.apiKey)

	resp, err := s.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("POST /api/sync/stream: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return fmt.Errorf("sync stream returned %d: %s", resp.StatusCode, body)
	}

	// A line that does not decode is an error. A skip followed by an ack would
	// move the cursor past a change that the sidecar did not publish. An error
	// keeps the cursor and replays the batch.
	rows, err := parseSyncStream(resp.Body)
	if err != nil {
		return fmt.Errorf("parse sync stream: %w", err)
	}
	if len(rows) == 0 {
		return nil
	}

	// Publish every event. Stop and return on the first publish error, without
	// an ack, so Immich replays the batch.
	albumCache := map[string][]string{}
	published := 0
	for _, row := range rows {
		ev, ok := syncRowToEvent(row)
		if !ok {
			continue
		}
		if ev.Type == events.AssetUpserted {
			albumIDs, err := s.resolveAlbums(ctx, ev.AssetID, albumCache)
			if err != nil {
				return fmt.Errorf("resolve albums for %s: %w", ev.AssetID, err)
			}
			ev.AlbumIDs = albumIDs
		}
		if err := s.publisher.Publish(ctx, ev); err != nil {
			return fmt.Errorf("publish %s: %w", row.Type, err)
		}
		published++
	}

	// Send the last ack per entity type. Immich stores one checkpoint per type,
	// so the last value for each type is the correct resume position.
	acks := collectAcks(rows)
	if published > 0 {
		slog.Info("sync batch published", "events", published, "acks", len(acks))
	}
	if len(acks) > 0 {
		if err := s.ack(ctx, acks); err != nil {
			// The events are already published. A failed ack replays the batch,
			// and a duplicate is safe because the downstream is idempotent.
			slog.Warn("sync ack failed", "err", err)
		}
	}
	return nil
}

// resolveAlbums returns the album IDs for an asset. It caches the result for the
// current batch so a repeated asset needs only one API call.
func (s *SyncStreamConsumer) resolveAlbums(ctx context.Context, assetID string, cache map[string][]string) ([]string, error) {
	if ids, ok := cache[assetID]; ok {
		return ids, nil
	}
	ids, err := s.albums.Albums(ctx, assetID)
	if err != nil {
		return nil, err
	}
	cache[assetID] = ids
	return ids, nil
}

// ack advances the Immich cursor with POST /api/sync/ack.
func (s *SyncStreamConsumer) ack(ctx context.Context, acks []string) error {
	body, err := json.Marshal(syncAckSetRequest{Acks: acks})
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		s.baseURL+"/api/sync/ack", bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("x-api-key", s.apiKey)

	resp, err := s.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("POST /api/sync/ack: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNoContent {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
		return fmt.Errorf("sync ack returned %d: %s", resp.StatusCode, b)
	}
	return nil
}

// parseSyncStream reads all JSON lines from r. A line that does not decode is an
// error, so the caller can keep the cursor and replay the batch.
func parseSyncStream(r io.Reader) ([]syncRow, error) {
	var rows []syncRow
	scanner := bufio.NewScanner(r)
	scanner.Buffer(make([]byte, 0, 64*1024), 4*1024*1024)
	for scanner.Scan() {
		line := scanner.Bytes()
		if len(bytes.TrimSpace(line)) == 0 {
			continue
		}
		var row syncRow
		if err := json.Unmarshal(line, &row); err != nil {
			return nil, fmt.Errorf("unparseable sync line %q: %w", string(line), err)
		}
		rows = append(rows, row)
	}
	return rows, scanner.Err()
}

// collectAcks returns the last ack string for each entity type. It drops
// SyncResetV1 acks. An ack for SyncResetV1 would reset the Immich sync progress.
func collectAcks(rows []syncRow) []string {
	byType := make(map[string]string)
	var order []string
	for _, row := range rows {
		if row.Ack == "" {
			continue
		}
		typ := row.Ack
		if i := strings.IndexByte(typ, '|'); i >= 0 {
			typ = typ[:i]
		}
		if typ == entitySyncReset {
			continue
		}
		if _, seen := byType[typ]; !seen {
			order = append(order, typ)
		}
		byType[typ] = row.Ack
	}
	acks := make([]string, 0, len(order))
	for _, typ := range order {
		acks = append(acks, byType[typ])
	}
	return acks
}

// Immich entity (response) type names.
const (
	entityAssetV2            = "AssetV2"
	entityAssetDelete        = "AssetDeleteV1"
	entityAssetExif          = "AssetExifV1"
	entityAlbumV2            = "AlbumV2"
	entityAlbumDelete        = "AlbumDeleteV1"
	entityAlbumToAsset       = "AlbumToAssetV1"
	entityAlbumToAssetDelete = "AlbumToAssetDeleteV1"
	entitySyncReset          = "SyncResetV1"
)

// syncStreamRequest is the body for POST /api/sync/stream (SyncStreamDto).
type syncStreamRequest struct {
	Types []string `json:"types"`
	Reset bool     `json:"reset,omitempty"`
}

// syncAckSetRequest is the body for POST /api/sync/ack (SyncAckSetDto).
type syncAckSetRequest struct {
	Acks []string `json:"acks"`
}

// syncRow is one JSON line from the sync stream: {type, data, ack}.
type syncRow struct {
	// Type is the entity type, for example "AssetV2" or "AlbumDeleteV1".
	Type string `json:"type"`
	// Ack is the resume token for the row ("<type>|<updateId>|<extraId>").
	Ack string `json:"ack"`
	// Data holds the payload. The consumer decodes only the fields it uses.
	Data syncData `json:"data"`
}

// syncData holds the payload fields across the entity types the consumer maps.
// The set of populated fields depends on Type.
type syncData struct {
	ID      string `json:"id,omitempty"`      // AssetV2, AlbumV2
	AssetID string `json:"assetId,omitempty"` // AssetDeleteV1, AssetExifV1, AlbumToAsset*
	AlbumID string `json:"albumId,omitempty"` // AlbumDeleteV1, AlbumToAsset*

	DeletedAt *string `json:"deletedAt,omitempty"` // AssetV2: set means the asset is in the trash
	OwnerID   string  `json:"ownerId,omitempty"`   // AssetV2
	Checksum  string  `json:"checksum,omitempty"`  // AssetV2
	AssetType string  `json:"type,omitempty"`      // AssetV2

	Name        string `json:"name,omitempty"`        // AlbumV2
	Description string `json:"description,omitempty"` // AlbumV2
}

func boolPtr(b bool) *bool { return &b }

// syncRowToEvent maps a sync row to an event. It returns (event, true) for a row
// type the downstream uses. The caller fills AlbumIDs on an asset upsert.
func syncRowToEvent(row syncRow) (events.Event, bool) {
	switch row.Type {
	case entityAssetV2:
		if row.Data.DeletedAt != nil && *row.Data.DeletedAt != "" {
			return events.Event{Type: events.AssetTrashed, AssetID: row.Data.ID}, true
		}
		return events.Event{
			Type:      events.AssetUpserted,
			AssetID:   row.Data.ID,
			OwnerID:   row.Data.OwnerID,
			Checksum:  row.Data.Checksum,
			AssetType: row.Data.AssetType,
		}, true
	case entityAssetExif:
		// A metadata edit changes what the gallery shows.
		return events.Event{Type: events.AssetUpserted, AssetID: row.Data.AssetID}, true
	case entityAssetDelete:
		return events.Event{Type: events.AssetDeleted, AssetID: row.Data.AssetID}, true
	case entityAlbumV2:
		return events.Event{
			Type:        events.AlbumChanged,
			AlbumID:     row.Data.ID,
			Name:        row.Data.Name,
			Description: row.Data.Description,
		}, true
	case entityAlbumDelete:
		return events.Event{Type: events.AlbumDeleted, AlbumID: row.Data.AlbumID}, true
	case entityAlbumToAsset:
		return events.Event{
			Type:    events.AlbumMembership,
			AlbumID: row.Data.AlbumID,
			AssetID: row.Data.AssetID,
			Present: boolPtr(true),
		}, true
	case entityAlbumToAssetDelete:
		return events.Event{
			Type:    events.AlbumMembership,
			AlbumID: row.Data.AlbumID,
			AssetID: row.Data.AssetID,
			Present: boolPtr(false),
		}, true
	default:
		// AlbumUserV1, SyncAckV1, SyncCompleteV1, and SyncResetV1 are not
		// published. Their acks still advance the cursor.
		return events.Event{}, false
	}
}
