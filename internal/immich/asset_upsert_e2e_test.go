//go:build integration

package immich

import (
	"context"
	"testing"
	"time"

	"github.com/herrfennessey/immich-listener/internal/events"
)

func TestAssetUpsertPublishesEvent(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	immich := newE2EImmich(t, e2e)
	nats := newE2ENATS(t, e2e)
	startE2ESidecar(t, e2e)

	assetID := immich.UploadImage(t, ctx, "listener-e2e.png", e2eImage(t))
	nats.AwaitEvent(t, events.AssetUpserted, func(event events.Event) bool {
		return event.AssetID == assetID
	})
}
