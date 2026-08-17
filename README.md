# immich-listener

An event-driven sidecar that bridges [Immich](https://immich.app/) lifecycle events to a local [NATS JetStream](https://docs.nats.io/nats-concepts/jetstream) bus.

## How it works

The sidecar runs two loops in parallel:

1. **Socket.IO doorbell** — connects to Immich's WebSocket gateway and watches for `on_upload_success`, `on_asset_update`, `on_asset_delete`, `on_asset_trash`, `on_asset_restore`, and `on_album_update`. Each event immediately triggers the sync loop below.

2. **Sync stream** — calls `POST /api/sync/stream` in a checkpointed loop, reads the JSON-lines delta batch, publishes every event to NATS JetStream, and only then calls `POST /api/sync/ack` to advance the server-side cursor. If any publish fails the cursor is **not** advanced and the batch replays on the next pass (at-least-once delivery).

### NATS subjects

| Subject | When emitted |
|---------|-------------|
| `immich.asset.upserted` | Asset created or updated (`AssetV2` with no `deletedAt`, or an `AssetExifV1` metadata edit). Carries `ownerId`, `checksum`, `assetType`, and the asset's full `albumIds[]` (resolved from a persistent JetStream KV index, so edits to assets already in an album still report their albums). |
| `immich.asset.trashed`  | Asset moved to trash (`AssetV2` with a non-null `deletedAt`). |
| `immich.asset.deleted`  | Asset permanently deleted (`AssetDeleteV1`). |
| `immich.album.changed`  | Album metadata created or updated (`AlbumV2`). Carries `name` and `description`. |
| `immich.album.deleted`  | Album deleted (`AlbumDeleteV1`). |
| `immich.album.membership` | Asset added to or removed from an album (`AlbumToAssetV1` / `AlbumToAssetDeleteV1`). `present: true` on add, `present: false` on removal. |

The subject prefix (`immich`) is configurable via `NATS_SUBJECT_PREFIX`.

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

Verify the sidecar can see Immich after `up`:

```sh
docker compose exec sidecar wget -qO- http://immich-server:2283/api/server/ping
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
| `MEMBERSHIP_BUCKET` | `immich_asset_albums` | JetStream KV bucket for the asset→albums index. |
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
