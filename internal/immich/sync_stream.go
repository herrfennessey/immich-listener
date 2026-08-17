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
	"time"

	"github.com/herrfennessey/immich-listener/internal/events"
)

// syncDeltaTypes is the set of delta types the sidecar subscribes to.
// Immich will only stream rows of these types; others are silently skipped server-side.
var syncDeltaTypes = []string{
	"AssetV2",
	"AssetDeleteV1",
	"AlbumV2",
	"AlbumDeleteV1",
	"AlbumToAssetV1",
	"AlbumToAssetDeleteV1",
	"AssetExifV1", // subscribed but ignored by the mapper; required by Immich to advance cursor
}

// SyncStreamConsumer reads from POST /api/sync/stream, converts deltas to Events,
// publishes them to NATS JetStream, and only then calls POST /api/sync/ack to advance
// the server-side cursor.  If any publish fails the cursor is NOT advanced so that
// the next pass replays the same batch.
type SyncStreamConsumer struct {
	baseURL    string
	apiKey     string
	httpClient *http.Client
	publish    func(context.Context, events.Event) error
}

// NewSyncStreamConsumer creates a consumer.  publish is called for every event parsed
// and must confirm durable delivery (e.g. NATS JetStream ack) before returning nil.
func NewSyncStreamConsumer(baseURL, apiKey string, publish func(context.Context, events.Event) error) *SyncStreamConsumer {
	return &SyncStreamConsumer{
		baseURL:    baseURL,
		apiKey:     apiKey,
		httpClient: &http.Client{Timeout: 5 * time.Minute},
		publish:    publish,
	}
}

// Run starts a continuous loop that reads the sync stream on every tick.
// It blocks until ctx is cancelled.
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
// then advance the Immich server-side cursor via POST /api/sync/ack.
// If any publish fails we return an error without acking, so Immich replays
// the same batch on the next pass.
func (s *SyncStreamConsumer) runOnce(ctx context.Context) error {
	reqBody, err := json.Marshal(syncRequest{Types: syncDeltaTypes})
	if err != nil {
		return err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		s.baseURL+"/api/sync/stream", bytes.NewReader(reqBody))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/jsonlines+json, application/json")
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

	// Parse the full batch first so we can resolve albumIds[] for asset events.
	rows, err := parseSyncStream(resp.Body)
	if err != nil {
		return fmt.Errorf("parse sync stream: %w", err)
	}
	if len(rows) == 0 {
		return nil
	}

	// Collect the ack token — it's the last row with a non-empty Checkpoint field.
	var ackToken string
	for i := len(rows) - 1; i >= 0; i-- {
		if rows[i].Checkpoint != "" {
			ackToken = rows[i].Checkpoint
			break
		}
	}

	// Build asset→albumIds index from AlbumToAsset deltas in this batch.
	assetAlbums := buildAssetAlbumsIndex(rows)

	// Publish all events.  On the first publish failure we stop and return the
	// error without acking Immich; the next pass will replay the whole batch.
	eventsPublished := 0
	for _, row := range rows {
		if row.Checkpoint != "" {
			continue // metadata row
		}
		ev, ok := syncRowToEvent(row, assetAlbums)
		if !ok {
			continue
		}
		if err := s.publish(ctx, ev); err != nil {
			return fmt.Errorf("publish %s %s: %w", row.Type, row.Data.ID, err)
		}
		eventsPublished++
	}

	if eventsPublished > 0 {
		slog.Info("sync batch published", "events", eventsPublished)
	}

	// Only advance the server-side cursor after all events are durably on the bus.
	if ackToken != "" {
		if err := s.ack(ctx, ackToken); err != nil {
			// Log but do not return: events are already published; best effort to
			// advance the cursor. The worst case is a duplicate batch on the next pass,
			// which downstream consumers must tolerate (at-least-once semantics).
			slog.Warn("sync ack failed", "err", err)
		}
	}
	return nil
}

// ack calls POST /api/sync/ack to advance the server-side cursor.
func (s *SyncStreamConsumer) ack(ctx context.Context, checkpoint string) error {
	body, err := json.Marshal(syncAckRequest{Checkpoints: []syncCheckpoint{{Type: "v1", Checkpoint: checkpoint}}})
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

// buildAssetAlbumsIndex builds a map of assetId → []albumId from AlbumToAsset rows.
func buildAssetAlbumsIndex(rows []syncRow) map[string][]string {
	idx := make(map[string][]string)
	for _, row := range rows {
		if row.Type != "AlbumToAssetV1" {
			continue
		}
		if row.Data.AssetID == "" || row.Data.AlbumID == "" {
			continue
		}
		idx[row.Data.AssetID] = append(idx[row.Data.AssetID], row.Data.AlbumID)
	}
	return idx
}

// sync stream wire types -------------------------------------------------------

// syncRequest is the JSON body sent to POST /api/sync/stream.
type syncRequest struct {
	// Types is the list of delta types to subscribe to.
	Types []string `json:"types"`
}

// syncAckRequest is the JSON body sent to POST /api/sync/ack.
type syncAckRequest struct {
	Checkpoints []syncCheckpoint `json:"checkpoints"`
}

type syncCheckpoint struct {
	Type       string `json:"type"`
	Checkpoint string `json:"checkpoint"`
}

// syncRow represents one JSON line from the sync stream response.
type syncRow struct {
	// Checkpoint is set on the final line of a batch (the ack token).
	Checkpoint string `json:"checkpoint,omitempty"`
	// Type is the Immich delta type, e.g. "AssetV2", "AlbumDeleteV1".
	Type string `json:"type,omitempty"`
	// Data carries the per-type payload fields.
	Data syncData `json:"data,omitempty"`
}

type syncData struct {
	ID      string `json:"id,omitempty"`
	AlbumID string `json:"albumId,omitempty"`
	AssetID string `json:"assetId,omitempty"`
	// IsTrashed is set on AssetV2 rows when the asset is in the trash.
	IsTrashed bool `json:"isTrashed,omitempty"`
}

// syncRowToEvent maps an Immich sync row to a canonical sidecar event.
// assetAlbums is the asset→albums index built from the same batch.
// Returns (event, true) if the row type is known and should be published.
func syncRowToEvent(row syncRow, assetAlbums map[string][]string) (events.Event, bool) {
	switch row.Type {
	case "AssetV2":
		if row.Data.IsTrashed {
			return events.Event{Type: events.AssetTrashed, AssetID: row.Data.ID}, true
		}
		albumIDs := assetAlbums[row.Data.ID]
		return events.Event{Type: events.AssetUpserted, AssetID: row.Data.ID, AlbumIDs: albumIDs}, true
	case "AssetDeleteV1":
		return events.Event{Type: events.AssetDeleted, AssetID: row.Data.ID}, true
	case "AlbumV2":
		return events.Event{Type: events.AlbumChanged, AlbumID: row.Data.ID}, true
	case "AlbumDeleteV1":
		return events.Event{Type: events.AlbumDeleted, AlbumID: row.Data.ID}, true
	case "AlbumToAssetV1":
		return events.Event{
			Type:    events.AlbumMembership,
			AlbumID: row.Data.AlbumID,
			AssetID: row.Data.AssetID,
		}, true
	case "AlbumToAssetDeleteV1":
		return events.Event{
			Type:    events.AlbumMembership,
			AlbumID: row.Data.AlbumID,
			AssetID: row.Data.AssetID,
			Removed: true,
		}, true
	default:
		// AssetExifV1, AlbumUserV1, etc. — subscribed for cursor advancement but
		// not interesting to downstream consumers.
		return events.Event{}, false
	}
}
