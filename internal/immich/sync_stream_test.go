package immich

import (
	"bufio"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/herrfennessey/immich-listener/internal/events"
)

func TestSyncStreamConsumer_RunOnce(t *testing.T) {
	// Build a fake sync stream response with some deltas + checkpoint line.
	rows := []any{
		syncRow{Type: "AssetV2", Data: syncData{ID: "asset-1"}},
		syncRow{Type: "AssetDeleteV1", Data: syncData{ID: "asset-2"}},
		syncRow{Type: "AlbumV2", Data: syncData{ID: "album-1"}},
		syncRow{Type: "AlbumDeleteV1", Data: syncData{ID: "album-2"}},
		syncRow{Type: "AlbumToAssetV1", Data: syncData{AlbumID: "album-3", AssetID: "asset-3"}},
		syncRow{Type: "AlbumToAssetDeleteV1", Data: syncData{AlbumID: "album-4", AssetID: "asset-4"}},
		syncRow{Type: "AssetExifV1", Data: syncData{ID: "asset-5"}}, // should be ignored
		syncRow{Checkpoint: "tok-abc"},
	}

	var sb strings.Builder
	enc := json.NewEncoder(&sb)
	for _, r := range rows {
		_ = enc.Encode(r)
	}
	body := sb.String()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/api/sync/stream" {
			http.NotFound(w, r)
			return
		}
		if r.Header.Get("x-api-key") != "test-key" {
			http.Error(w, "forbidden", http.StatusForbidden)
			return
		}
		w.Header().Set("Content-Type", "application/jsonlines+json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(body))
	}))
	defer srv.Close()

	dir := t.TempDir()
	cpFile := filepath.Join(dir, "checkpoint.json")

	var published []events.Event
	publish := func(_ context.Context, ev events.Event) error {
		published = append(published, ev)
		return nil
	}

	consumer := NewSyncStreamConsumer(srv.URL, "test-key", cpFile, publish)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := consumer.runOnce(ctx); err != nil {
		t.Fatalf("runOnce error: %v", err)
	}

	// Verify published events.
	want := []events.Event{
		{Type: events.AssetCreated, Source: "sync", AssetID: "asset-1"},
		{Type: events.AssetDeleted, Source: "sync", AssetID: "asset-2"},
		{Type: events.AlbumUpdated, Source: "sync", AlbumID: "album-1"},
		{Type: events.AlbumDeleted, Source: "sync", AlbumID: "album-2"},
		{Type: events.AlbumAssetAdded, Source: "sync", AlbumID: "album-3", AssetID: "asset-3"},
		{Type: events.AlbumAssetRemoved, Source: "sync", AlbumID: "album-4", AssetID: "asset-4"},
	}
	if len(published) != len(want) {
		t.Fatalf("published %d events, want %d; got: %+v", len(published), len(want), published)
	}
	for i, ev := range published {
		if ev != want[i] {
			t.Errorf("event[%d] = %+v, want %+v", i, ev, want[i])
		}
	}

	// Verify checkpoint was persisted.
	data, err := os.ReadFile(cpFile)
	if err != nil {
		t.Fatalf("checkpoint file not created: %v", err)
	}
	var cf checkpointFile
	if err := json.Unmarshal(data, &cf); err != nil {
		t.Fatalf("bad checkpoint file: %v", err)
	}
	if cf.Checkpoint != "tok-abc" {
		t.Errorf("checkpoint = %q, want tok-abc", cf.Checkpoint)
	}
}

func TestSyncStreamConsumer_CheckpointSentInRequest(t *testing.T) {
	var receivedCP string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req syncRequest
		_ = json.NewDecoder(r.Body).Decode(&req)
		receivedCP = req.Checkpoint
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	dir := t.TempDir()
	cpFile := filepath.Join(dir, "checkpoint.json")
	// Pre-seed a checkpoint.
	_ = os.WriteFile(cpFile, []byte(`{"checkpoint":"prev-token"}`), 0o600)

	consumer := NewSyncStreamConsumer(srv.URL, "key", cpFile, func(_ context.Context, _ events.Event) error { return nil })
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = consumer.runOnce(ctx)

	if receivedCP != "prev-token" {
		t.Errorf("checkpoint sent = %q, want prev-token", receivedCP)
	}
}

func TestSyncStreamConsumer_HTTPError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "internal error", http.StatusInternalServerError)
	}))
	defer srv.Close()

	consumer := NewSyncStreamConsumer(srv.URL, "key", t.TempDir()+"/cp.json",
		func(_ context.Context, _ events.Event) error { return nil })
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := consumer.runOnce(ctx); err == nil {
		t.Fatal("expected error on HTTP 500")
	}
}

func TestSyncStreamConsumer_InvalidJSON(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("not-json\n{\"type\":\"AssetV2\",\"data\":{\"id\":\"a1\"}}\n"))
	}))
	defer srv.Close()

	var published []events.Event
	consumer := NewSyncStreamConsumer(srv.URL, "key", t.TempDir()+"/cp.json",
		func(_ context.Context, ev events.Event) error {
			published = append(published, ev)
			return nil
		})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	// Should not error; bad lines are skipped.
	if err := consumer.runOnce(ctx); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(published) != 1 {
		t.Errorf("published %d events, want 1", len(published))
	}
}

func TestCheckpointPersistence(t *testing.T) {
	dir := t.TempDir()
	cpFile := filepath.Join(dir, "nested", "checkpoint.json")

	consumer := &SyncStreamConsumer{checkpointFile: cpFile}

	// Load non-existent file.
	cp, err := consumer.loadCheckpoint()
	if err != nil {
		t.Fatalf("load missing checkpoint: %v", err)
	}
	if cp != "" {
		t.Errorf("expected empty checkpoint, got %q", cp)
	}

	// Save and reload.
	if err := consumer.saveCheckpoint("my-token"); err != nil {
		t.Fatalf("save checkpoint: %v", err)
	}
	cp, err = consumer.loadCheckpoint()
	if err != nil {
		t.Fatalf("reload checkpoint: %v", err)
	}
	if cp != "my-token" {
		t.Errorf("checkpoint = %q, want my-token", cp)
	}
}

func TestSyncRowToEvent(t *testing.T) {
	tests := []struct {
		row     syncRow
		wantOK  bool
		wantTyp events.Type
	}{
		{syncRow{Type: "AssetV2", Data: syncData{ID: "a"}}, true, events.AssetCreated},
		{syncRow{Type: "AssetDeleteV1", Data: syncData{ID: "a"}}, true, events.AssetDeleted},
		{syncRow{Type: "AlbumV2", Data: syncData{ID: "al"}}, true, events.AlbumUpdated},
		{syncRow{Type: "AlbumDeleteV1", Data: syncData{ID: "al"}}, true, events.AlbumDeleted},
		{syncRow{Type: "AlbumToAssetV1"}, true, events.AlbumAssetAdded},
		{syncRow{Type: "AlbumToAssetDeleteV1"}, true, events.AlbumAssetRemoved},
		{syncRow{Type: "AssetExifV1"}, false, ""},
		{syncRow{Type: "AlbumUserV1"}, false, ""},
		{syncRow{Type: "unknown"}, false, ""},
	}
	for _, tt := range tests {
		ev, ok := syncRowToEvent(tt.row)
		if ok != tt.wantOK {
			t.Errorf("syncRowToEvent(%q) ok=%v, want %v", tt.row.Type, ok, tt.wantOK)
			continue
		}
		if ok && ev.Type != tt.wantTyp {
			t.Errorf("syncRowToEvent(%q) type=%q, want %q", tt.row.Type, ev.Type, tt.wantTyp)
		}
	}
}

// validateBodySize ensures the scanner can handle lines up to the default limit.
func TestScanner_LargeBodyIsRead(t *testing.T) {
	bigLine := strings.Repeat("x", bufio.MaxScanTokenSize/2)
	row := syncRow{Type: "AssetV2", Data: syncData{ID: bigLine}}
	var sb strings.Builder
	enc := json.NewEncoder(&sb)
	_ = enc.Encode(row)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(sb.String()))
	}))
	defer srv.Close()

	var published []events.Event
	consumer := NewSyncStreamConsumer(srv.URL, "key", t.TempDir()+"/cp.json",
		func(_ context.Context, ev events.Event) error {
			published = append(published, ev)
			return nil
		})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := consumer.runOnce(ctx); err != nil {
		t.Fatalf("runOnce error: %v", err)
	}
	if len(published) != 1 {
		t.Errorf("published %d events, want 1", len(published))
	}
}
