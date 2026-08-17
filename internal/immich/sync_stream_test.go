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

func strPtr(s string) *string { return &s }

// makeStreamBody encodes rows as newline-delimited JSON (the sync stream wire format).
func makeStreamBody(rows []any) string {
	var sb strings.Builder
	enc := json.NewEncoder(&sb)
	for _, r := range rows {
		_ = enc.Encode(r)
	}
	return sb.String()
}

// streamAndAckServer serves the sync stream at /api/sync/stream and records ack
// calls at /api/sync/ack.
func streamAndAckServer(t *testing.T, streamBody string) (*httptest.Server, *[]syncAckSetRequest) {
	t.Helper()
	var mu sync.Mutex
	var acks []syncAckSetRequest

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
			var ack syncAckSetRequest
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
	// Rows use the real Immich wire shape: {type, ack, data}. The ack is the
	// pipe-delimited resume token "<type>|<updateId>".
	rows := []any{
		syncRow{Type: "AssetV2", Ack: "AssetV2|100", Data: syncData{ID: "asset-1", OwnerID: "owner-1", Checksum: "chk", AssetType: "IMAGE"}},
		syncRow{Type: "AssetV2", Ack: "AssetV2|101", Data: syncData{ID: "asset-trashed", DeletedAt: strPtr("2026-08-17T00:00:00Z")}},
		syncRow{Type: "AssetDeleteV1", Ack: "AssetDeleteV1|5", Data: syncData{AssetID: "asset-2"}},
		syncRow{Type: "AssetExifV1", Ack: "AssetExifV1|9", Data: syncData{AssetID: "asset-3"}},
		syncRow{Type: "AlbumV2", Ack: "AlbumV2|3", Data: syncData{ID: "album-1", Name: "Trip", Description: "desc"}},
		syncRow{Type: "AlbumDeleteV1", Ack: "AlbumDeleteV1|2", Data: syncData{AlbumID: "album-2"}},
		syncRow{Type: "AlbumToAssetV1", Ack: "AlbumToAssetV1|7", Data: syncData{AlbumID: "album-3", AssetID: "asset-1"}},
		syncRow{Type: "AlbumToAssetDeleteV1", Ack: "AlbumToAssetDeleteV1|8", Data: syncData{AlbumID: "album-4", AssetID: "asset-4"}},
		// Control rows: SyncResetV1's ack must never be echoed back; SyncCompleteV1
		// carries no data and is not published.
		syncRow{Type: "SyncResetV1", Ack: "SyncResetV1|0"},
		syncRow{Type: "SyncCompleteV1", Ack: "SyncCompleteV1|999"},
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

	wantTypes := []events.Type{
		events.AssetUpserted,   // asset-1
		events.AssetTrashed,    // asset-trashed (deletedAt set)
		events.AssetDeleted,    // asset-2
		events.AssetUpserted,   // asset-3 (from AssetExifV1)
		events.AlbumChanged,    // album-1
		events.AlbumDeleted,    // album-2
		events.AlbumMembership, // album-3 / asset-1 added
		events.AlbumMembership, // album-4 / asset-4 removed
	}
	if len(published) != len(wantTypes) {
		t.Fatalf("published %d events, want %d; got: %+v", len(published), len(wantTypes), published)
	}
	for i, typ := range wantTypes {
		if published[i].Type != typ {
			t.Errorf("event[%d].Type = %q, want %q", i, published[i].Type, typ)
		}
	}

	// asset-1 upsert should carry albumIds resolved from the AlbumToAssetV1 row,
	// plus the enrichment fields from AssetV2 data.
	up := published[0]
	if len(up.AlbumIDs) != 1 || up.AlbumIDs[0] != "album-3" {
		t.Errorf("asset-1 AlbumIDs = %v, want [album-3]", up.AlbumIDs)
	}
	if up.OwnerID != "owner-1" || up.Checksum != "chk" || up.AssetType != "IMAGE" {
		t.Errorf("asset-1 enrichment = %+v", up)
	}

	// Delete events must carry the id from the correct per-type data field.
	if published[2].AssetID != "asset-2" {
		t.Errorf("AssetDeleteV1 AssetID = %q, want asset-2", published[2].AssetID)
	}
	if published[5].AlbumID != "album-2" {
		t.Errorf("AlbumDeleteV1 AlbumID = %q, want album-2", published[5].AlbumID)
	}

	// Membership present flags.
	if published[6].Present == nil || !*published[6].Present {
		t.Error("AlbumToAssetV1 event should have present=true")
	}
	if published[7].Present == nil || *published[7].Present {
		t.Error("AlbumToAssetDeleteV1 event should have present=false")
	}

	// Ack called exactly once, carrying the final ack per type, AssetV2 collapsed
	// to its last row (101), SyncResetV1 excluded.
	if len(*acks) != 1 {
		t.Fatalf("ack called %d times, want 1", len(*acks))
	}
	got := map[string]bool{}
	for _, a := range (*acks)[0].Acks {
		got[a] = true
	}
	wantAcks := []string{
		"AssetV2|101", "AssetDeleteV1|5", "AssetExifV1|9", "AlbumV2|3",
		"AlbumDeleteV1|2", "AlbumToAssetV1|7", "AlbumToAssetDeleteV1|8", "SyncCompleteV1|999",
	}
	if len((*acks)[0].Acks) != len(wantAcks) {
		t.Fatalf("acks = %v, want %d entries", (*acks)[0].Acks, len(wantAcks))
	}
	for _, a := range wantAcks {
		if !got[a] {
			t.Errorf("missing ack %q in %v", a, (*acks)[0].Acks)
		}
	}
	if got["AssetV2|100"] {
		t.Error("AssetV2|100 should have been collapsed to the later AssetV2|101")
	}
	if got["SyncResetV1|0"] {
		t.Error("SyncResetV1 ack must never be echoed back")
	}
}

func TestSyncStreamConsumer_TypesArraySent(t *testing.T) {
	var receivedTypes []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/sync/stream" {
			var req syncStreamRequest
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
	// These must be SyncRequestType (plural) values, not entity types.
	wantTypes := map[string]bool{
		"AssetsV2": true, "AssetExifsV1": true,
		"AlbumsV2": true, "AlbumToAssetsV1": true,
	}
	for _, typ := range receivedTypes {
		delete(wantTypes, typ)
		if strings.HasSuffix(typ, "DeleteV1") {
			t.Errorf("request type %q looks like an entity type, not a SyncRequestType", typ)
		}
	}
	for missing := range wantTypes {
		t.Errorf("missing required request type: %s", missing)
	}
}

func TestSyncStreamConsumer_PublishFailPreventsAck(t *testing.T) {
	rows := []any{
		syncRow{Type: "AssetV2", Ack: "AssetV2|1", Data: syncData{ID: "a1"}},
	}
	srv, acks := streamAndAckServer(t, makeStreamBody(rows))
	defer srv.Close()

	consumer := NewSyncStreamConsumer(srv.URL, "test-key",
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

func TestSyncStreamConsumer_NoAckWhenEmpty(t *testing.T) {
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
	body := "not-json\n" + `{"type":"AssetV2","ack":"AssetV2|1","data":{"id":"a1"}}` + "\n"
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
		{syncRow{Type: "AssetV2", Data: syncData{ID: "a", DeletedAt: strPtr("2026-01-01T00:00:00Z")}}, true, events.AssetTrashed},
		{syncRow{Type: "AssetExifV1", Data: syncData{AssetID: "a"}}, true, events.AssetUpserted},
		{syncRow{Type: "AssetDeleteV1", Data: syncData{AssetID: "a"}}, true, events.AssetDeleted},
		{syncRow{Type: "AlbumV2", Data: syncData{ID: "al"}}, true, events.AlbumChanged},
		{syncRow{Type: "AlbumDeleteV1", Data: syncData{AlbumID: "al"}}, true, events.AlbumDeleted},
		{syncRow{Type: "AlbumToAssetV1"}, true, events.AlbumMembership},
		{syncRow{Type: "AlbumToAssetDeleteV1"}, true, events.AlbumMembership},
		{syncRow{Type: "AlbumUserV1"}, false, ""},
		{syncRow{Type: "SyncCompleteV1"}, false, ""},
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

func TestSyncRowToEvent_DeleteUsesCorrectID(t *testing.T) {
	emptyIdx := map[string][]string{}

	ev, _ := syncRowToEvent(syncRow{Type: "AssetDeleteV1", Data: syncData{AssetID: "asset-x"}}, emptyIdx)
	if ev.AssetID != "asset-x" {
		t.Errorf("AssetDeleteV1 AssetID = %q, want asset-x (from data.assetId)", ev.AssetID)
	}
	ev, _ = syncRowToEvent(syncRow{Type: "AlbumDeleteV1", Data: syncData{AlbumID: "album-x"}}, emptyIdx)
	if ev.AlbumID != "album-x" {
		t.Errorf("AlbumDeleteV1 AlbumID = %q, want album-x (from data.albumId)", ev.AlbumID)
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

func TestCollectAcks(t *testing.T) {
	rows := []syncRow{
		{Type: "AssetV2", Ack: "AssetV2|1"},
		{Type: "AssetV2", Ack: "AssetV2|2"},         // last wins
		{Type: "SyncAckV1", Ack: "AssetV2|3"},       // backfill marker, keyed by real type
		{Type: "AlbumV2", Ack: "AlbumV2|9"},
		{Type: "SyncResetV1", Ack: "SyncResetV1|0"}, // must be dropped
		{Type: "AssetV2"},                           // no ack, ignored
	}
	acks := collectAcks(rows)
	got := strings.Join(acks, ",")
	// AssetV2 collapses to its last ack (3, from the SyncAckV1 marker); AlbumV2 kept;
	// SyncResetV1 dropped. Order is first-seen-type.
	if got != "AssetV2|3,AlbumV2|9" {
		t.Errorf("collectAcks = %q, want %q", got, "AssetV2|3,AlbumV2|9")
	}
}
