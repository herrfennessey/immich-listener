package events

import "testing"

func TestEvent_Subject(t *testing.T) {
	tests := []struct {
		ev     Event
		prefix string
		want   string
	}{
		{Event{Type: AssetUpserted}, "immich", "immich.asset.upserted"},
		{Event{Type: AssetTrashed}, "immich", "immich.asset.trashed"},
		{Event{Type: AssetDeleted}, "immich", "immich.asset.deleted"},
		{Event{Type: AlbumChanged}, "immich", "immich.album.changed"},
		{Event{Type: AlbumDeleted}, "immich", "immich.album.deleted"},
		{Event{Type: AlbumMembership}, "immich", "immich.album.membership"},
		{Event{Type: AssetUpserted}, "home", "home.asset.upserted"},
	}
	for _, tt := range tests {
		got := tt.ev.Subject(tt.prefix)
		if got != tt.want {
			t.Errorf("Subject(%q) = %q, want %q", tt.prefix, got, tt.want)
		}
	}
}
