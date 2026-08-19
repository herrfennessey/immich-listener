//go:build integration

package immich

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go/modules/compose"
	"github.com/testcontainers/testcontainers-go/wait"
)

const (
	e2eEmail    = "listener-e2e@example.com"
	e2ePassword = "listener-e2e-password"
)

var e2e *e2eSuite

// TestMain owns the expensive shared fixture. Individual flows remain isolated
// through their own Immich login sessions and sidecar token files.
func TestMain(m *testing.M) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	var err error
	e2e, err = newE2ESuite(ctx)
	if err != nil {
		fmt.Fprintln(os.Stderr, "start E2E suite:", err)
		os.Exit(1)
	}

	code := m.Run()
	if err := e2e.Close(); err != nil {
		fmt.Fprintln(os.Stderr, "stop E2E suite:", err)
		code = 1
	}
	os.Exit(code)
}

type e2eSuite struct {
	stack       compose.ComposeStack
	immichURL   string
	natsURL     string
	sidecarPath string
}

func newE2ESuite(ctx context.Context) (*e2eSuite, error) {
	stack, err := compose.NewDockerComposeWith(
		compose.StackIdentifier("immich-listener-e2e"),
		compose.WithStackFiles(immichComposeFile()),
	)
	if err != nil {
		return nil, fmt.Errorf("create Compose stack: %w", err)
	}
	closeStack := func() { _ = stack.Down(context.Background(), compose.RemoveOrphans(true), compose.RemoveVolumes(true)) }

	err = stack.
		WaitForService("immich-server", wait.ForHTTP("/api/server/ping").WithPort("2283/tcp").WithStartupTimeout(4*time.Minute)).
		WaitForService("nats", wait.ForListeningPort("4222/tcp")).
		Up(ctx, compose.Wait(true))
	if err != nil {
		closeStack()
		return nil, fmt.Errorf("start Compose stack: %w", err)
	}

	suite := &e2eSuite{
		stack: stack,
	}
	immichAddress, err := serviceAddress(ctx, stack, "immich-server", "2283/tcp")
	if err != nil {
		closeStack()
		return nil, err
	}
	natsAddress, err := serviceAddress(ctx, stack, "nats", "4222/tcp")
	if err != nil {
		closeStack()
		return nil, err
	}
	suite.immichURL = "http://" + immichAddress
	suite.natsURL = "nats://" + natsAddress
	if err := suite.bootstrapUser(ctx); err != nil {
		closeStack()
		return nil, err
	}
	if err := suite.buildSidecar(); err != nil {
		closeStack()
		return nil, err
	}
	return suite, nil
}

func (s *e2eSuite) Close() error {
	return s.stack.Down(context.Background(), compose.RemoveOrphans(true), compose.RemoveVolumes(true))
}

func (s *e2eSuite) bootstrapUser(ctx context.Context) error {
	_, err := postE2EJSON(ctx, s.immichURL+"/api/auth/admin-sign-up", map[string]string{
		"email": e2eEmail, "password": e2ePassword, "name": "Listener E2E",
	}, http.StatusCreated)
	if err != nil {
		return fmt.Errorf("create E2E admin: %w", err)
	}
	return nil
}

func (s *e2eSuite) buildSidecar() error {
	repoRoot, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		return fmt.Errorf("resolve repository root: %w", err)
	}
	binary := filepath.Join(os.TempDir(), "immich-listener-e2e")
	command := exec.Command("go", "build", "-o", binary, "./cmd/sidecar")
	command.Dir = repoRoot
	if output, err := command.CombinedOutput(); err != nil {
		return fmt.Errorf("build sidecar: %w: %s", err, output)
	}
	s.sidecarPath = binary
	return nil
}

func immichComposeFile() string {
	composeFile, err := filepath.Abs(filepath.Join("testdata", "docker-compose.yml"))
	if err != nil {
		panic(fmt.Sprintf("resolve Compose fixture: %v", err))
	}
	return composeFile
}

func serviceAddress(ctx context.Context, stack compose.ComposeStack, service, port string) (string, error) {
	container, err := stack.ServiceContainer(ctx, service)
	if err != nil {
		return "", fmt.Errorf("get %s container: %w", service, err)
	}
	host, err := container.Host(ctx)
	if err != nil {
		return "", fmt.Errorf("get %s host: %w", service, err)
	}
	mappedPort, err := container.MappedPort(ctx, port)
	if err != nil {
		return "", fmt.Errorf("get %s mapped port: %w", service, err)
	}
	return net.JoinHostPort(host, mappedPort.Port()), nil
}
