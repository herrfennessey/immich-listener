FROM golang:1.24-alpine AS builder
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -o /immich-sidecar ./cmd/sidecar

FROM gcr.io/distroless/static:nonroot
COPY --from=builder /immich-sidecar /immich-sidecar
ENTRYPOINT ["/immich-sidecar"]
