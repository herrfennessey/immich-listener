// Package immich implements consumers for the Immich server API.
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

	"github.com/herrfennessey/immich-listener/internal/events"
)

// syncRequestTypes is the set of SyncRequestType values the sidecar subscribes
// to on POST /api/sync/stream.
//
// These are Immich's *request* type names (plural), which are distinct from the
// *entity* type names (singular) that come back on each response row.  A single
// request type yields several entity types — e.g. AssetsV2 produces both AssetV2
// and AssetDeleteV1 rows.
var syncRequestTypes = []string{
	"AssetsV2",        // AssetV2 (upsert/trash) + AssetDeleteV1 (permanent delete)
	"AssetExifsV1",    // AssetExifV1 (metadata edits)
	"AlbumsV2",        // AlbumV2 (metadata) + AlbumDeleteV1
	"AlbumToAssetsV1", // AlbumToAssetV1 / AlbumToAssetDeleteV1 (membership)
}

// SyncStreamConsumer reads from POST /api/sync/stream, converts deltas to Events,
// publishes them to NATS JetStream, and only then advances the server-side cursor
// via POST /api/sync/ack.  If any publish fails the cursor is NOT advanced, so the
// next pass replays the same batch.
//
// The sidecar holds no local cursor: the checkpoint lives in Immich's
// session_sync_checkpoint table, keyed on the session behind the API key.
type SyncStreamConsumer struct {
	baseURL    string
	apiKey     string
	httpClient *http.Client
	publish    func(context.Context, events.Event) error
}

// NewSyncStreamConsumer creates a consumer.  publish is called for every event
// parsed and must confirm durable delivery (NATS JetStream ack) before returning nil.
func NewSyncStreamConsumer(baseURL, apiKey string, publish func(context.Context, events.Event) error) *SyncStreamConsumer {
	return &SyncStreamConsumer{
		baseURL:    baseURL,
		apiKey:     apiKey,
		httpClient: &http.Client{Timeout: 5 * time.Minute},
		publish:    publish,
	}
}

// Run reads the sync stream on every tick or wake signal until ctx is cancelled.
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
		// Drain any additional wake signals that queued while we were working.
		for len(wake) > 0 {
			<-wake
		}
		if err := s.runOnce(ctx); err != nil {
			slog.Warn("sync stream error", "err", err)
		}
		tick.Reset(interval)
	}
}

// runOnce performs a single sync stream pass.
//
// The correctness invariant is: publish every event durably to NATS *first*,
// then advance the Immich cursor via POST /api/sync/ack.  If any publish fails
// we return an error without acking, so Immich replays the same batch next pass.
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

	// Read the whole batch first so we can resolve albumIds[] for asset events.
	rows, err := parseSyncStream(resp.Body)
	if err != nil {
		return fmt.Errorf("parse sync stream: %w", err)
	}
	if len(rows) == 0 {
		return nil
	}

	// Build assetId → []albumId from the AlbumToAsset deltas in this batch.
	assetAlbums := buildAssetAlbumsIndex(rows)

	// Publish all events.  On the first publish failure, stop and return without
	// acking so Immich replays the whole batch on the next pass.
	published := 0
	for _, row := range rows {
		ev, ok := syncRowToEvent(row, assetAlbums)
		if !ok {
			continue
		}
		if err := s.publish(ctx, ev); err != nil {
			return fmt.Errorf("publish %s: %w", row.Type, err)
		}
		published++
	}

	// Collect the final ack token per entity type. Immich upserts one checkpoint
	// per type keyed on the leading segment of the ack string, so last-write-wins
	// across the ordered stream yields the correct resume position for each type.
	acks := collectAcks(rows)
	if published > 0 {
		slog.Info("sync batch published", "events", published, "acks", len(acks))
	}

	if len(acks) > 0 {
		if err := s.ack(ctx, acks); err != nil {
			// Events are already durably published; a failed ack only means the
			// batch replays next pass, and duplicates are no-ops downstream
			// (at-least-once). Log and move on rather than fail the pass.
			slog.Warn("sync ack failed", "err", err)
		}
	}
	return nil
}

// ack advances the server-side cursor via POST /api/sync/ack.
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

// parseSyncStream reads all JSON-lines from r and returns the decoded rows.
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
			slog.Warn("sync stream: unparseable line", "line", string(line), "err", err)
			continue
		}
		rows = append(rows, row)
	}
	return rows, scanner.Err()
}

// buildAssetAlbumsIndex maps assetId → []albumId from AlbumToAssetV1 rows in the
// batch. This is an in-batch delta only, not the asset's full album set.
func buildAssetAlbumsIndex(rows []syncRow) map[string][]string {
	idx := make(map[string][]string)
	for _, row := range rows {
		if row.Type != entityAlbumToAsset {
			continue
		}
		if row.Data.AssetID == "" || row.Data.AlbumID == "" {
			continue
		}
		idx[row.Data.AssetID] = append(idx[row.Data.AssetID], row.Data.AlbumID)
	}
	return idx
}

// collectAcks returns the final ack string per entity type across the batch.
// SyncResetV1 acks are dropped: echoing one back to /api/sync/ack would reset the
// server's sync progress.
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

// sync stream wire types -------------------------------------------------------

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

// syncStreamRequest is the JSON body sent to POST /api/sync/stream (SyncStreamDto).
type syncStreamRequest struct {
	Types []string `json:"types"`
	Reset bool     `json:"reset,omitempty"`
}

// syncAckSetRequest is the JSON body sent to POST /api/sync/ack (SyncAckSetDto).
type syncAckSetRequest struct {
	Acks []string `json:"acks"`
}

// syncRow is one JSON line from the sync stream response: {type, data, ack}.
type syncRow struct {
	// Type is the Immich entity type, e.g. "AssetV2", "AlbumDeleteV1".
	Type string `json:"type"`
	// Ack is the opaque, pipe-delimited resume token for this row
	// ("<type>|<updateId>|<extraId>"), echoed back to /api/sync/ack.
	Ack string `json:"ack"`
	// Data carries the per-type payload; only the fields the sidecar uses are decoded.
	Data syncData `json:"data"`
}

// syncData holds the union of payload fields across the entity types we map.
// Which fields are populated depends on Type.
type syncData struct {
	// AssetV2 / AlbumV2 identity.
	ID string `json:"id,omitempty"`
	// AssetDeleteV1 / AssetExifV1 / AlbumToAsset* asset reference.
	AssetID string `json:"assetId,omitempty"`
	// AlbumDeleteV1 / AlbumToAsset* album reference.
	AlbumID string `json:"albumId,omitempty"`

	// AssetV2 enrichment; deletedAt distinguishes trash from upsert.
	DeletedAt *string `json:"deletedAt,omitempty"`
	OwnerID   string  `json:"ownerId,omitempty"`
	Checksum  string  `json:"checksum,omitempty"`
	AssetType string  `json:"type,omitempty"`

	// AlbumV2 enrichment.
	Name        string `json:"name,omitempty"`
	Description string `json:"description,omitempty"`
}

func boolPtr(b bool) *bool { return &b }

// syncRowToEvent maps an Immich sync row to a canonical sidecar event.
// assetAlbums is the asset→albums index built from the same batch.
// Returns (event, true) if the row type is one downstream cares about.
func syncRowToEvent(row syncRow, assetAlbums map[string][]string) (events.Event, bool) {
	switch row.Type {
	case entityAssetV2:
		if row.Data.DeletedAt != nil && *row.Data.DeletedAt != "" {
			return events.Event{Type: events.AssetTrashed, AssetID: row.Data.ID}, true
		}
		return events.Event{
			Type:      events.AssetUpserted,
			AssetID:   row.Data.ID,
			AlbumIDs:  assetAlbums[row.Data.ID],
			OwnerID:   row.Data.OwnerID,
			Checksum:  row.Data.Checksum,
			AssetType: row.Data.AssetType,
		}, true
	case entityAssetExif:
		// A metadata-only edit still changes what the gallery renders.
		return events.Event{
			Type:     events.AssetUpserted,
			AssetID:  row.Data.AssetID,
			AlbumIDs: assetAlbums[row.Data.AssetID],
		}, true
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
		// AssetExifV1 handled above; AlbumUserV1, SyncAckV1, SyncCompleteV1,
		// SyncResetV1, etc. are not published (but their acks still advance the cursor).
		return events.Event{}, false
	}
}
