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
	"os"
	"path/filepath"
	"time"

	"github.com/herrfennessey/immich-listener/internal/events"
)

// SyncStreamConsumer reads from POST /api/sync/stream, converts deltas to Events,
// and calls the provided publish function for each one.  The checkpoint is persisted
// to disk between calls so restarts continue from where they left off.
type SyncStreamConsumer struct {
	baseURL        string
	apiKey         string
	checkpointFile string
	httpClient     *http.Client
	publish        func(context.Context, events.Event) error
}

// NewSyncStreamConsumer creates a consumer.  publish is called for every event parsed.
func NewSyncStreamConsumer(baseURL, apiKey, checkpointFile string, publish func(context.Context, events.Event) error) *SyncStreamConsumer {
	return &SyncStreamConsumer{
		baseURL:        baseURL,
		apiKey:         apiKey,
		checkpointFile: checkpointFile,
		httpClient:     &http.Client{Timeout: 5 * time.Minute},
		publish:        publish,
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
		if err := s.runOnce(ctx); err != nil {
			slog.Warn("sync stream error", "err", err)
		}
		tick.Reset(interval)
	}
}

// runOnce performs a single sync stream pass.
func (s *SyncStreamConsumer) runOnce(ctx context.Context) error {
	checkpoint, err := s.loadCheckpoint()
	if err != nil {
		return fmt.Errorf("load checkpoint: %w", err)
	}

	reqBody, err := json.Marshal(syncRequest{Checkpoint: checkpoint})
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

	var newCheckpoint string
	scanner := bufio.NewScanner(resp.Body)
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
		if row.Checkpoint != "" {
			newCheckpoint = row.Checkpoint
			continue
		}
		ev, ok := syncRowToEvent(row)
		if !ok {
			continue
		}
		if err := s.publish(ctx, ev); err != nil {
			slog.Warn("publish error", "err", err, "type", ev.Type)
		}
	}
	if err := scanner.Err(); err != nil {
		return fmt.Errorf("scan sync stream: %w", err)
	}
	if newCheckpoint != "" {
		if err := s.saveCheckpoint(newCheckpoint); err != nil {
			slog.Warn("save checkpoint", "err", err)
		}
	}
	return nil
}

// checkpoint persistence -------------------------------------------------------

type checkpointFile struct {
	Checkpoint string `json:"checkpoint"`
}

func (s *SyncStreamConsumer) loadCheckpoint() (string, error) {
	data, err := os.ReadFile(s.checkpointFile)
	if os.IsNotExist(err) {
		return "", nil
	}
	if err != nil {
		return "", err
	}
	var cf checkpointFile
	if err := json.Unmarshal(data, &cf); err != nil {
		return "", err
	}
	return cf.Checkpoint, nil
}

func (s *SyncStreamConsumer) saveCheckpoint(cp string) error {
	data, err := json.Marshal(checkpointFile{Checkpoint: cp})
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(s.checkpointFile), 0o700); err != nil {
		return err
	}
	tmp := s.checkpointFile + ".tmp"
	if err := os.WriteFile(tmp, data, 0o600); err != nil {
		return err
	}
	return os.Rename(tmp, s.checkpointFile)
}

// sync stream wire types -------------------------------------------------------

// syncRequest is the JSON body sent to POST /api/sync/stream.
type syncRequest struct {
	// Checkpoint is the opaque token from the previous response; empty on first call.
	Checkpoint string `json:"checkpoint,omitempty"`
}

// syncRow represents one JSON line from the sync stream response.
// Only the fields the sidecar cares about are decoded.
type syncRow struct {
	// Checkpoint is set on the last line of a batch.
	Checkpoint string `json:"checkpoint,omitempty"`
	// Type is the Immich delta type, e.g. "AssetV2", "AlbumDeleteV1".
	Type string `json:"type,omitempty"`

	// AssetID / AlbumID appear in the data object for each respective type.
	Data syncData `json:"data,omitempty"`
}

type syncData struct {
	ID      string `json:"id,omitempty"`
	AlbumID string `json:"albumId,omitempty"`
	AssetID string `json:"assetId,omitempty"`
}

// syncRowToEvent maps an Immich sync row to a canonical sidecar event.
// Returns (event, true) if the row type is known and should be published.
func syncRowToEvent(row syncRow) (events.Event, bool) {
	switch row.Type {
	case "AssetV2":
		return events.Event{Type: events.AssetCreated, Source: "sync", AssetID: row.Data.ID}, true
	case "AssetDeleteV1":
		return events.Event{Type: events.AssetDeleted, Source: "sync", AssetID: row.Data.ID}, true
	case "AlbumV2":
		return events.Event{Type: events.AlbumUpdated, Source: "sync", AlbumID: row.Data.ID}, true
	case "AlbumDeleteV1":
		return events.Event{Type: events.AlbumDeleted, Source: "sync", AlbumID: row.Data.ID}, true
	case "AlbumToAssetV1":
		return events.Event{
			Type:    events.AlbumAssetAdded,
			Source:  "sync",
			AlbumID: row.Data.AlbumID,
			AssetID: row.Data.AssetID,
		}, true
	case "AlbumToAssetDeleteV1":
		return events.Event{
			Type:    events.AlbumAssetRemoved,
			Source:  "sync",
			AlbumID: row.Data.AlbumID,
			AssetID: row.Data.AssetID,
		}, true
	default:
		// AssetExifV1, AlbumUserV1, etc. — not interesting to downstream consumers.
		return events.Event{}, false
	}
}
