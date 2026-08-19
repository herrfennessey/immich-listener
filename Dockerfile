FROM golang:1.25.9-alpine AS builder
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -o /immich-sidecar ./cmd/sidecar
RUN mkdir -p /data && touch /data/.keep && chown -R 65532:65532 /data

FROM gcr.io/distroless/static:nonroot
COPY --from=builder /immich-sidecar /immich-sidecar
COPY --from=builder --chown=65532:65532 /data/ /data/
ENTRYPOINT ["/immich-sidecar"]
