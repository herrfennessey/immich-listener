package events

import "testing"

func TestEvent_Subject(t *testing.T) {
	tests := []struct {
		ev      Event
		prefix  string
		want    string
	}{
		{Event{Type: AssetCreated}, "immich", "immich.asset.created"},
		{Event{Type: AssetUpdated}, "immich", "immich.asset.updated"},
		{Event{Type: AssetDeleted}, "immich", "immich.asset.deleted"},
		{Event{Type: AlbumUpdated}, "immich", "immich.album.updated"},
		{Event{Type: AlbumDeleted}, "immich", "immich.album.deleted"},
		{Event{Type: AlbumAssetAdded}, "immich", "immich.album.asset.added"},
		{Event{Type: AlbumAssetRemoved}, "immich", "immich.album.asset.removed"},
		{Event{Type: AssetCreated}, "home", "home.asset.created"},
	}
	for _, tt := range tests {
		got := tt.ev.Subject(tt.prefix)
		if got != tt.want {
			t.Errorf("Subject(%q) = %q, want %q", tt.prefix, got, tt.want)
		}
	}
}
