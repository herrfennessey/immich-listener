# immich-listener

A sidecar that sends [Immich](https://immich.app/) events to a message queue. It ships a [NATS JetStream](https://docs.nats.io/nats-concepts/jetstream) adapter. The queue is pluggable — see [Architecture](#architecture).

## How it works

The sidecar runs two loops in parallel:

1. **Socket.IO listener** — connects to Immich's WebSocket gateway and watches for `on_upload_success`, `on_asset_update`, `on_asset_delete`, `on_asset_trash`, `on_asset_restore`, and `on_album_update`. A known event starts a sync pass at once. The listener does not read the event content.

2. **Sync stream** — calls `POST /api/sync/stream` in a checkpointed loop, reads the JSON-lines delta batch, publishes every event, then calls `POST /api/sync/ack` to advance the server-side cursor. If a publish fails, the cursor does not advance and the batch replays on the next pass (at-least-once delivery).

On an asset upsert, the sidecar reads the asset's albums from `GET /api/albums?assetId=<id>` and sets `albumIds[]`. This lets a downstream know which albums to rebuild, even for an edit to an asset that was already in an album.

## Architecture

The sidecar uses ports and adapters (hexagonal). The core reads Immich changes and calls the `Publisher` port (`internal/core`). NATS is one adapter (`internal/adapters/nats`). The core does not know which queue is in use.

To use a different queue:

1. Add a package under `internal/adapters/` that implements `core.Publisher` (one method: `Publish(ctx, events.Event) error`).
2. Wire it in `cmd/sidecar/main.go` in place of the NATS adapter.
3. Add the queue service to `docker-compose.yml`. Commented examples for Redis, RabbitMQ, and Redpanda (Kafka API) are included.

The event schema is stable across queues (`internal/events`), so a change of queue does not change what downstreams receive.

### Subjects

The table uses the default `immich` prefix (`NATS_SUBJECT_PREFIX`).

| Subject | When emitted |
|---------|-------------|
| `immich.asset.upserted` | Asset created or updated (`AssetV2` with no `deletedAt`, or an `AssetExifV1` metadata edit). Carries `ownerId`, `checksum`, `assetType`, and the asset's full `albumIds[]` (read from the Immich API). |
| `immich.asset.trashed`  | Asset moved to trash (`AssetV2` with a non-null `deletedAt`). Carries `albumIds[]` so a downstream can clear the asset from those albums. |
| `immich.asset.deleted`  | Asset permanently deleted (`AssetDeleteV1`). |
| `immich.album.changed`  | Album metadata created or updated (`AlbumV2`). Carries `name` and `description`. |
| `immich.album.deleted`  | Album deleted (`AlbumDeleteV1`). |
| `immich.album.membership` | Asset added to or removed from an album (`AlbumToAssetV1` / `AlbumToAssetDeleteV1`). `present: true` on add, `present: false` on removal. |

### Event envelope

Asset upserted example (fields not relevant to the event type are omitted):
```json
{
  "type":      "asset.upserted",
  "assetId":   "...",
  "ownerId":   "...",
  "checksum":  "...",
  "assetType": "IMAGE",
  "albumIds":  ["...", "..."]
}
```

Album membership removal example:
```json
{
  "type":    "album.membership",
  "albumId": "...",
  "assetId": "...",
  "present": false
}
```

## Connecting to Immich

This project is intentionally **its own Compose project** — it does not live inside
Immich's `docker-compose.yml`. That keeps the two independently upgradeable, but it
means their containers start on **different Docker networks**, and by default the
sidecar cannot resolve `immich-server`. The two projects have to be joined on a
shared network.

Docker names each Compose project's default network `<project>_default`. Immich's
project is usually the directory its compose file sits in, so the network is
typically **`immich_default`**. Confirm the exact name:

```sh
docker network ls
# NETWORK ID     NAME              DRIVER    SCOPE
# ...            immich_default    bridge    local     ← the one Immich created
```

Then point this project at it via `IMMICH_NETWORK` in `.env`:

```sh
IMMICH_NETWORK=immich_default
```

`docker-compose.yml` declares that network as **external** (Compose will attach to
it rather than create it) and puts the sidecar on both it and this project's own
network — so the sidecar reaches `nats` locally and `immich-server` across the
Immich network:

```yaml
services:
  sidecar:
    networks: [default, immich]   # nats over default, immich-server over immich
networks:
  immich:
    external: true
    name: ${IMMICH_NETWORK:-immich_default}
```

Verify the network reaches Immich. The sidecar image is distroless and has no
shell, so run the check from a throwaway container on the same network:

```sh
docker run --rm --network "${IMMICH_NETWORK:-immich_default}" curlimages/curl \
  -s http://immich-server:2283/api/server/ping
# {"res":"pong"}
```

**Alternatives, if the default-network approach doesn't fit your setup:**

- **A dedicated shared network you own.** Create one (`docker network create immich-shared`),
  attach Immich's `immich-server` to it (add it under that service's `networks:` in
  Immich's compose), and set `IMMICH_NETWORK=immich-shared`. This avoids depending on
  Immich's implicit network name.
- **Reach Immich over the host/LAN instead of a shared network.** Drop the `immich`
  network from `docker-compose.yml` and set `IMMICH_BASE_URL` to a routable address
  (e.g. `http://<nuc-host>:2283`, or `http://host.docker.internal:2283` where
  supported). Simpler, but the traffic leaves the Docker bridge.

## Running with Docker Compose

```sh
# 1. Copy the example env file, fill in your Immich API key, and set IMMICH_NETWORK
#    (see "Connecting to Immich" above).
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
| `IMMICH_NETWORK` | `immich_default` | Name of Immich's Docker network to attach to (see "Connecting to Immich"). Compose-only. |
| `IMMICH_BASE_URL` | `http://immich-server:2283` | Immich server base URL. |
| `NATS_URL` | `nats://localhost:4222` | NATS server URL. |
| `NATS_STREAM_NAME` | `IMMICH` | JetStream stream name. |
| `NATS_SUBJECT_PREFIX` | `immich` | Subject prefix for all published events. |
| `SYNC_INTERVAL` | `30s` | Idle wait between sync passes. |
| `SOCKETIO_ENABLED` | `true` | Set to `false` to stop the Socket.IO listener. |

## Running tests

```sh
go test -race ./...
```

### Real Immich integration test

Docker is required for the real-stack test:

```sh
go test -tags=integration ./internal/immich -run TestRealImmichAssetUpsert -v
```

Testcontainers starts a disposable, pinned Immich v3.1.0 Compose stack with
PostgreSQL, Valkey, and NATS JetStream, then uploads an embedded fixture image
and verifies the sidecar emits `immich.asset.upserted`. Containers and volumes
are removed automatically when the test finishes.

## Building the binary

```sh
go build -o immich-sidecar ./cmd/sidecar
```
