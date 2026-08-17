# immich-listener

An event-driven sidecar that bridges [Immich](https://immich.app/) lifecycle events to a local [NATS JetStream](https://docs.nats.io/nats-concepts/jetstream) bus.

## How it works

The sidecar runs two loops in parallel:

1. **Socket.IO doorbell** — connects to Immich's WebSocket gateway and watches for `on_upload_success`, `on_asset_update`, `on_asset_delete`, `on_asset_trash`, `on_asset_restore`, and `on_album_update`. Each event immediately triggers the sync loop below.

2. **Sync stream** — calls `POST /api/sync/stream` in a checkpointed loop, reads the JSON-lines delta batch, publishes every event to NATS JetStream, and only then calls `POST /api/sync/ack` to advance the server-side cursor. If any publish fails the cursor is **not** advanced and the batch replays on the next pass (at-least-once delivery).

### NATS subjects

| Subject | When emitted |
|---------|-------------|
| `immich.asset.upserted` | Asset created or updated (`AssetV2`). Includes `albumIds[]` resolved from the same batch. |
| `immich.asset.trashed`  | Asset moved to trash (`AssetV2` with `isTrashed=true`). |
| `immich.asset.deleted`  | Asset permanently deleted (`AssetDeleteV1`). |
| `immich.album.changed`  | Album metadata created or updated (`AlbumV2`). |
| `immich.album.deleted`  | Album deleted (`AlbumDeleteV1`). |
| `immich.album.membership` | Asset added to or removed from an album (`AlbumToAssetV1` / `AlbumToAssetDeleteV1`). `removed: true` on removals. |

The subject prefix (`immich`) is configurable via `NATS_SUBJECT_PREFIX`.

### Event envelope

```json
{
  "type":     "asset.upserted",
  "assetId":  "...",
  "albumIds": ["...", "..."],
  "albumId":  "",
  "removed":  false
}
```

Fields not relevant to an event type are omitted.

## Running with Docker Compose

```sh
# 1. Copy the example env file and fill in your Immich API key.
cp .env.example .env
$EDITOR .env

# 2. Start NATS and the sidecar.
docker compose up -d

# 3. Tail sidecar logs.
docker compose logs -f sidecar
```

## Environment variables

| Variable | Default | Description |
|----------|---------|-------------|
| `IMMICH_API_KEY` | **required** | Immich owner API key. |
| `IMMICH_BASE_URL` | `http://immich-server:2283` | Immich server base URL. |
| `NATS_URL` | `nats://localhost:4222` | NATS server URL. |
| `NATS_STREAM_NAME` | `IMMICH` | JetStream stream name. |
| `NATS_SUBJECT_PREFIX` | `immich` | Subject prefix for all published events. |
| `SYNC_INTERVAL` | `30s` | How long to wait between sync passes when idle. |
| `SOCKETIO_ENABLED` | `true` | Set to `false` to disable the Socket.IO doorbell. |

## Running tests

```sh
go test -race ./...
```

## Building the binary

```sh
go build -o immich-sidecar ./cmd/sidecar
```
