package immich

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestAlbumClient_Albums(t *testing.T) {
	var gotAssetID, gotAPIKey string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/albums" {
			http.NotFound(w, r)
			return
		}
		gotAssetID = r.URL.Query().Get("assetId")
		gotAPIKey = r.Header.Get("x-api-key")
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`[{"id":"album-1","albumName":"Trip"},{"id":"album-2","albumName":"Pets"}]`))
	}))
	defer srv.Close()

	c := NewAlbumClient(srv.URL, "test-key")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	ids, err := c.Albums(ctx, "asset-9")
	if err != nil {
		t.Fatalf("Albums error: %v", err)
	}
	if gotAssetID != "asset-9" {
		t.Errorf("assetId query = %q, want asset-9", gotAssetID)
	}
	if gotAPIKey != "test-key" {
		t.Errorf("x-api-key = %q, want test-key", gotAPIKey)
	}
	if len(ids) != 2 || ids[0] != "album-1" || ids[1] != "album-2" {
		t.Errorf("ids = %v, want [album-1 album-2]", ids)
	}
}

func TestAlbumClient_AlbumsEmpty(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`[]`))
	}))
	defer srv.Close()

	c := NewAlbumClient(srv.URL, "k")
	ids, err := c.Albums(context.Background(), "asset-1")
	if err != nil {
		t.Fatalf("Albums error: %v", err)
	}
	if len(ids) != 0 {
		t.Errorf("ids = %v, want empty", ids)
	}
}

func TestAlbumClient_AlbumsHTTPError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
	}))
	defer srv.Close()

	c := NewAlbumClient(srv.URL, "k")
	if _, err := c.Albums(context.Background(), "asset-1"); err == nil {
		t.Fatal("expected error on HTTP 401")
	}
}
