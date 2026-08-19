//go:build integration

package immich

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"io"
	"mime/multipart"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/herrfennessey/immich-listener/internal/events"
	natsclient "github.com/nats-io/nats.go"
	"github.com/testcontainers/testcontainers-go/modules/compose"
	"github.com/testcontainers/testcontainers-go/wait"
)

func TestImmichComposeFile(t *testing.T) {
	want, err := filepath.Abs(filepath.Join("testdata", "docker-compose.yml"))
	if err != nil {
		t.Fatalf("resolve fixture path: %v", err)
	}
	if got := immichComposeFile(t); got != want {
		t.Errorf("Compose file = %q, want %q", got, want)
	}
}

func TestRealImmichAssetUpsert(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	stack, err := compose.NewDockerComposeWith(
		compose.StackIdentifier("immich-listener-e2e"),
		compose.WithStackFiles(immichComposeFile(t)),
	)
	if err != nil {
		t.Fatalf("create Compose stack: %v", err)
	}
	t.Cleanup(func() {
		if err := stack.Down(context.Background(), compose.RemoveOrphans(true), compose.RemoveVolumes(true)); err != nil {
			t.Errorf("stop Compose stack: %v", err)
		}
	})

	err = stack.
		WaitForService("immich-server", wait.ForHTTP("/api/server/ping").WithPort("2283/tcp").WithStartupTimeout(4*time.Minute)).
		WaitForService("nats", wait.ForListeningPort("4222/tcp")).
		Up(ctx, compose.Wait(true))
	if err != nil {
		t.Fatalf("start Compose stack: %v", err)
	}

	immichURL := serviceURL(t, ctx, stack, "immich-server", "2283/tcp")
	natsURL := "nats://" + serviceAddress(t, ctx, stack, "nats", "4222/tcp")
	client := newRealImmichClient(t, immichURL)

	nc, err := natsclient.Connect(natsURL)
	if err != nil {
		t.Fatalf("subscribe to NATS: %v", err)
	}
	defer nc.Close()
	sub, err := nc.SubscribeSync("immich.asset.upserted")
	if err != nil {
		t.Fatalf("subscribe to asset events: %v", err)
	}
	if err := nc.Flush(); err != nil {
		t.Fatalf("flush NATS subscription: %v", err)
	}

	stopSidecar := startSidecar(t, immichURL, natsURL, client.email, client.password, filepath.Join(t.TempDir(), "session-token"))
	defer stopSidecar()

	assetID := client.uploadImage(t, ctx, "listener-e2e.png", onePixelPNG(t))

	message, err := sub.NextMsg(30 * time.Second)
	if err != nil {
		t.Fatalf("read asset-upsert event: %v", err)
	}
	if message.Subject != "immich.asset.upserted" {
		t.Errorf("event subject = %q, want immich.asset.upserted", message.Subject)
	}
	var event events.Event
	if err := json.Unmarshal(message.Data, &event); err != nil {
		t.Fatalf("decode asset-upsert event: %v", err)
	}
	if event.Type != events.AssetUpserted {
		t.Errorf("event type = %q, want %q", event.Type, events.AssetUpserted)
	}
	if event.AssetID != assetID {
		t.Errorf("event assetId = %q, want uploaded asset %q", event.AssetID, assetID)
	}
}

func immichComposeFile(t *testing.T) string {
	t.Helper()
	composeFile, err := filepath.Abs(filepath.Join("testdata", "docker-compose.yml"))
	if err != nil {
		t.Fatalf("resolve Compose fixture path: %v", err)
	}
	if _, err := os.Stat(composeFile); err != nil {
		t.Fatalf("stat Compose fixture: %v", err)
	}
	return composeFile
}

type realImmichClient struct {
	baseURL      string
	sessionToken string
	email        string
	password     string
}

func newRealImmichClient(t *testing.T, baseURL string) *realImmichClient {
	t.Helper()
	const email = "listener-e2e@example.com"
	const password = "listener-e2e-password"
	postJSON(t, baseURL+"/api/auth/admin-sign-up", map[string]string{
		"email": email, "password": password, "name": "Listener E2E",
	}, http.StatusCreated)
	response := postJSON(t, baseURL+"/api/auth/login", map[string]string{
		"email": email, "password": password,
	}, http.StatusCreated)
	var login struct {
		AccessToken string `json:"accessToken"`
	}
	if err := json.Unmarshal(response, &login); err != nil {
		t.Fatalf("decode Immich login: %v", err)
	}
	if login.AccessToken == "" {
		t.Fatal("Immich login returned an empty session token")
	}
	return &realImmichClient{baseURL: baseURL, sessionToken: login.AccessToken, email: email, password: password}
}

func startSidecar(t *testing.T, immichURL, natsURL, email, password, tokenFile string) func() {
	t.Helper()
	repoRoot, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatalf("resolve repository root: %v", err)
	}
	binary := filepath.Join(t.TempDir(), "immich-listener")
	build := exec.Command("go", "build", "-o", binary, "./cmd/sidecar")
	build.Dir = repoRoot
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build sidecar: %v: %s", err, output)
	}

	ctx, cancel := context.WithCancel(context.Background())
	command := exec.CommandContext(ctx, binary)
	command.Env = append(os.Environ(),
		"IMMICH_BASE_URL="+immichURL,
		"IMMICH_EMAIL="+email,
		"IMMICH_PASSWORD="+password,
		"IMMICH_SESSION_TOKEN_FILE="+tokenFile,
		"NATS_URL="+natsURL,
		"SYNC_INTERVAL=1s",
		"SOCKETIO_ENABLED=true",
	)
	var output bytes.Buffer
	command.Stdout = &output
	command.Stderr = &output
	if err := command.Start(); err != nil {
		cancel()
		t.Fatalf("start sidecar: %v", err)
	}

	return func() {
		cancel()
		if err := command.Wait(); err != nil && ctx.Err() == nil {
			t.Errorf("sidecar exited unexpectedly: %v: %s", err, output.String())
		}
	}
}

func (c *realImmichClient) uploadImage(t *testing.T, ctx context.Context, filename string, image []byte) string {
	t.Helper()
	var body bytes.Buffer
	form := multipart.NewWriter(&body)
	file, err := form.CreateFormFile("assetData", filename)
	if err != nil {
		t.Fatalf("create image upload field: %v", err)
	}
	if _, err := file.Write(image); err != nil {
		t.Fatalf("write image upload field: %v", err)
	}
	for key, value := range map[string]string{
		"fileCreatedAt":  "2026-08-17T11:00:00.000Z",
		"fileModifiedAt": "2026-08-17T11:00:00.000Z",
	} {
		if err := form.WriteField(key, value); err != nil {
			t.Fatalf("write upload field %s: %v", key, err)
		}
	}
	if err := form.Close(); err != nil {
		t.Fatalf("close upload multipart body: %v", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/api/assets", &body)
	if err != nil {
		t.Fatalf("create asset upload request: %v", err)
	}
	req.Header.Set("Content-Type", form.FormDataContentType())
	req.Header.Set("x-immich-session-token", c.sessionToken)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("upload image to Immich: %v", err)
	}
	defer resp.Body.Close()
	response, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read asset upload response: %v", err)
	}
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("upload image status = %d, want %d: %s", resp.StatusCode, http.StatusCreated, response)
	}
	var asset struct {
		ID string `json:"id"`
	}
	if err := json.Unmarshal(response, &asset); err != nil {
		t.Fatalf("decode asset upload response: %v", err)
	}
	if asset.ID == "" {
		t.Fatalf("asset upload returned an empty id: %s", response)
	}
	return asset.ID
}

func postJSON(t *testing.T, url string, value any, wantStatus int) []byte {
	t.Helper()
	body, err := json.Marshal(value)
	if err != nil {
		t.Fatalf("marshal JSON request: %v", err)
	}
	resp, err := http.Post(url, "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("POST %s: %v", url, err)
	}
	defer resp.Body.Close()
	response, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read POST %s response: %v", url, err)
	}
	if resp.StatusCode != wantStatus {
		t.Fatalf("POST %s status = %d, want %d: %s", url, resp.StatusCode, wantStatus, response)
	}
	return response
}

func serviceURL(t *testing.T, ctx context.Context, stack compose.ComposeStack, service, port string) string {
	t.Helper()
	return "http://" + serviceAddress(t, ctx, stack, service, port)
}

func serviceAddress(t *testing.T, ctx context.Context, stack compose.ComposeStack, service, port string) string {
	t.Helper()
	container, err := stack.ServiceContainer(ctx, service)
	if err != nil {
		t.Fatalf("get %s container: %v", service, err)
	}
	host, err := container.Host(ctx)
	if err != nil {
		t.Fatalf("get %s host: %v", service, err)
	}
	mappedPort, err := container.MappedPort(ctx, port)
	if err != nil {
		t.Fatalf("get %s mapped port: %v", service, err)
	}
	return net.JoinHostPort(host, mappedPort.Port())
}

func onePixelPNG(t *testing.T) []byte {
	t.Helper()
	image, err := base64.StdEncoding.DecodeString("iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVQIHWP4z8DwHwAFgAI/ScL6sAAAAABJRU5ErkJggg==")
	if err != nil {
		t.Fatalf("decode embedded PNG fixture: %v", err)
	}
	return image
}
