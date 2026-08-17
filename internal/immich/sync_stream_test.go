package immich

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/herrfennessey/immich-listener/internal/events"
)

// makeStreamBody encodes a slice of syncRow values as newline-delimited JSON.
func makeStreamBody(rows []any) string {
	var sb strings.Builder
	enc := json.NewEncoder(&sb)
	for _, r := range rows {
		_ = enc.Encode(r)
	}
	return sb.String()
}

// streamAndAckServer returns a test server that serves the sync stream at /api/sync/stream
// and records ack calls at /api/sync/ack.
func streamAndAckServer(t *testing.T, streamBody string) (*httptest.Server, *[]syncAckRequest) {
	t.Helper()
	var mu sync.Mutex
	var acks []syncAckRequest

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/api/sync/stream":
			if r.Header.Get("x-api-key") != "test-key" {
				http.Error(w, "forbidden", http.StatusForbidden)
				return
			}
			w.Header().Set("Content-Type", "application/jsonlines+json")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(streamBody))
		case r.Method == http.MethodPost && r.URL.Path == "/api/sync/ack":
			var ack syncAckRequest
			_ = json.NewDecoder(r.Body).Decode(&ack)
			mu.Lock()
			acks = append(acks, ack)
			mu.Unlock()
			w.WriteHeader(http.StatusNoContent)
		default:
			http.NotFound(w, r)
		}
	}))
	return srv, &acks
}

func TestSyncStreamConsumer_RunOnce(t *testing.T) {
	// Fixtures match the real Immich wire format:
	//   entity rows:  { "type": "...", "ids": [...], "data": {...} }
	//   SyncAckV1:    { "type": "SyncAckV1", "ids": ["<token>", "COMPLETE_ID"] }
	//   SyncCompleteV1: { "type": "SyncCompleteV1" }
	rows := []any{
		syncRow{Type: "AssetV2", IDs: []string{"asset-1"}, Data: syncData{ID: "asset-1"}},
		syncRow{Type: "AssetV2", IDs: []string{"asset-trashed"}, Data: syncData{ID: "asset-trashed", IsTrashed: true}},
		syncRow{Type: "AssetDeleteV1", IDs: []string{"asset-2"}, Data: syncData{ID: "asset-2"}},
		syncRow{Type: "AlbumV2", IDs: []string{"album-1"}, Data: syncData{ID: "album-1"}},
		syncRow{Type: "AlbumDeleteV1", IDs: []string{"album-2"}, Data: syncData{ID: "album-2"}},
		syncRow{Type: "AlbumToAssetV1", Data: syncData{AlbumID: "album-3", AssetID: "asset-3"}},
		syncRow{Type: "AlbumToAssetDeleteV1", Data: syncData{AlbumID: "album-4", AssetID: "asset-4"}},
		syncRow{Type: "AssetExifV1", IDs: []string{"asset-5"}, Data: syncData{ID: "asset-5"}}, // subscribed but not published
		syncRow{Type: "SyncAckV1", IDs: []string{"tok-abc"}},
		syncRow{Type: "SyncCompleteV1"},
	}

	srv, acks := streamAndAckServer(t, makeStreamBody(rows))
	defer srv.Close()

	var published []events.Event
	consumer := NewSyncStreamConsumer(srv.URL, "test-key",
		func(_ context.Context, ev events.Event) error {
			published = append(published, ev)
			return nil
		})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := consumer.runOnce(ctx); err != nil {
		t.Fatalf("runOnce error: %v", err)
	}

	// Verify published events (AlbumToAsset rows contribute albumIds to AssetV2).
	// asset-3 appears in AlbumToAssetV1 → asset-3 upserted should carry albumIds.
	wantTypes := []events.Type{
		events.AssetUpserted,  // asset-1
		events.AssetTrashed,   // asset-trashed
		events.AssetDeleted,   // asset-2
		events.AlbumChanged,   // album-1
		events.AlbumDeleted,   // album-2
		events.AlbumMembership, // album-3/asset-3 added
		events.AlbumMembership, // album-4/asset-4 removed
	}
	if len(published) != len(wantTypes) {
		t.Fatalf("published %d events, want %d; got: %+v", len(published), len(wantTypes), published)
	}
	for i, typ := range wantTypes {
		if published[i].Type != typ {
			t.Errorf("event[%d].Type = %q, want %q", i, published[i].Type, typ)
		}
	}

	// AlbumMembership removal should have Removed=true.
	if !published[6].Removed {
		t.Error("AlbumToAssetDeleteV1 event should have Removed=true")
	}

	// Ack must have been called exactly once with the batch token.
	if len(*acks) != 1 {
		t.Fatalf("ack called %d times, want 1", len(*acks))
	}
	if len((*acks)[0].Acks) != 1 || (*acks)[0].Acks[0] != "tok-abc" {
		t.Errorf("ack payload = %+v, want acks=[tok-abc]", (*acks)[0])
	}
}

func TestSyncStreamConsumer_TypesArraySent(t *testing.T) {
	var receivedTypes []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/sync/stream" {
			var req syncRequest
			_ = json.NewDecoder(r.Body).Decode(&req)
			receivedTypes = req.Types
			w.WriteHeader(http.StatusOK)
		}
	}))
	defer srv.Close()

	consumer := NewSyncStreamConsumer(srv.URL, "key",
		func(_ context.Context, _ events.Event) error { return nil })
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = consumer.runOnce(ctx)

	if len(receivedTypes) == 0 {
		t.Fatal("no types sent in sync stream request")
	}
	// Verify request uses SyncRequestType values (plural), not SyncEntityType values.
	wantTypes := map[string]bool{
		"AssetsV2":        true,
		"AssetExifsV1":    true,
		"AlbumsV2":        true,
		"AlbumToAssetsV1": true,
	}
	for _, typ := range receivedTypes {
		delete(wantTypes, typ)
	}
	for missing := range wantTypes {
		t.Errorf("missing required SyncRequestType in request: %s", missing)
	}
	// Entity types (singular) must NOT appear in the request body.
	badTypes := []string{"AssetV2", "AlbumV2", "AssetDeleteV1", "AlbumDeleteV1", "AlbumToAssetV1", "AlbumToAssetDeleteV1"}
	for _, typ := range receivedTypes {
		for _, bad := range badTypes {
			if typ == bad {
				t.Errorf("request must not contain SyncEntityType %q (use SyncRequestType instead)", typ)
			}
		}
	}
}

func TestSyncStreamConsumer_PublishFailPreventsAck(t *testing.T) {
	rows := []any{
		syncRow{Type: "AssetV2", IDs: []string{"a1"}, Data: syncData{ID: "a1"}},
		syncRow{Type: "SyncAckV1", IDs: []string{"tok-xyz"}},
		syncRow{Type: "SyncCompleteV1"},
	}
	srv, acks := streamAndAckServer(t, makeStreamBody(rows))
	defer srv.Close()

	consumer := NewSyncStreamConsumer(srv.URL, "key",
		func(_ context.Context, _ events.Event) error {
			return fmt.Errorf("nats unavailable")
		})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err := consumer.runOnce(ctx)
	if err == nil {
		t.Fatal("expected error when publish fails")
	}
	if len(*acks) != 0 {
		t.Errorf("ack should NOT be called when publish fails; called %d times", len(*acks))
	}
}

func TestSyncStreamConsumer_NoAckWhenNoCheckpoint(t *testing.T) {
	// Empty response — no checkpoint line — ack should NOT be called.
	var ackCalled bool
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/sync/stream":
			w.WriteHeader(http.StatusOK)
		case "/api/sync/ack":
			ackCalled = true
			w.WriteHeader(http.StatusNoContent)
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	consumer := NewSyncStreamConsumer(srv.URL, "key",
		func(_ context.Context, _ events.Event) error { return nil })
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := consumer.runOnce(ctx); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ackCalled {
		t.Error("ack should not be called on empty batch")
	}
}

func TestSyncStreamConsumer_HTTPError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "internal error", http.StatusInternalServerError)
	}))
	defer srv.Close()

	consumer := NewSyncStreamConsumer(srv.URL, "key",
		func(_ context.Context, _ events.Event) error { return nil })
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := consumer.runOnce(ctx); err == nil {
		t.Fatal("expected error on HTTP 500")
	}
}

func TestSyncStreamConsumer_InvalidJSON(t *testing.T) {
	body := "not-json\n" + `{"type":"AssetV2","ids":["a1"],"data":{"id":"a1"}}` + "\n"
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(body))
	}))
	defer srv.Close()

	var published []events.Event
	consumer := NewSyncStreamConsumer(srv.URL, "key",
		func(_ context.Context, ev events.Event) error {
			published = append(published, ev)
			return nil
		})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := consumer.runOnce(ctx); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(published) != 1 {
		t.Errorf("published %d events, want 1", len(published))
	}
}

func TestSyncRowToEvent_AllTypes(t *testing.T) {
	emptyIdx := map[string][]string{}
	tests := []struct {
		row     syncRow
		wantOK  bool
		wantTyp events.Type
	}{
		{syncRow{Type: "AssetV2", Data: syncData{ID: "a"}}, true, events.AssetUpserted},
		{syncRow{Type: "AssetV2", Data: syncData{ID: "a", IsTrashed: true}}, true, events.AssetTrashed},
		{syncRow{Type: "AssetDeleteV1", Data: syncData{ID: "a"}}, true, events.AssetDeleted},
		{syncRow{Type: "AlbumV2", Data: syncData{ID: "al"}}, true, events.AlbumChanged},
		{syncRow{Type: "AlbumDeleteV1", Data: syncData{ID: "al"}}, true, events.AlbumDeleted},
		{syncRow{Type: "AlbumToAssetV1"}, true, events.AlbumMembership},
		{syncRow{Type: "AlbumToAssetDeleteV1"}, true, events.AlbumMembership},
		{syncRow{Type: "AssetExifV1"}, false, ""},
		{syncRow{Type: "AlbumUserV1"}, false, ""},
		{syncRow{Type: "unknown"}, false, ""},
	}
	for _, tt := range tests {
		ev, ok := syncRowToEvent(tt.row, emptyIdx)
		if ok != tt.wantOK {
			t.Errorf("syncRowToEvent(%q) ok=%v, want %v", tt.row.Type, ok, tt.wantOK)
			continue
		}
		if ok && ev.Type != tt.wantTyp {
			t.Errorf("syncRowToEvent(%q) type=%q, want %q", tt.row.Type, ev.Type, tt.wantTyp)
		}
	}
}

func TestSyncRowToEvent_AlbumIDs(t *testing.T) {
	idx := map[string][]string{"asset-1": {"album-a", "album-b"}}
	ev, ok := syncRowToEvent(syncRow{Type: "AssetV2", Data: syncData{ID: "asset-1"}}, idx)
	if !ok {
		t.Fatal("expected ok=true")
	}
	if len(ev.AlbumIDs) != 2 {
		t.Errorf("AlbumIDs = %v, want [album-a album-b]", ev.AlbumIDs)
	}
}

func TestBuildAssetAlbumsIndex(t *testing.T) {
	rows := []syncRow{
		{Type: "AlbumToAssetV1", Data: syncData{AlbumID: "al1", AssetID: "a1"}},
		{Type: "AlbumToAssetV1", Data: syncData{AlbumID: "al2", AssetID: "a1"}},
		{Type: "AlbumToAssetV1", Data: syncData{AlbumID: "al3", AssetID: "a2"}},
		{Type: "AssetV2", Data: syncData{ID: "a3"}}, // should be ignored
	}
	idx := buildAssetAlbumsIndex(rows)
	if len(idx["a1"]) != 2 {
		t.Errorf("a1 albums = %v, want 2", idx["a1"])
	}
	if len(idx["a2"]) != 1 {
		t.Errorf("a2 albums = %v, want 1", idx["a2"])
	}
	if _, ok := idx["a3"]; ok {
		t.Error("a3 should not be in index")
	}
}

