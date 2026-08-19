//go:build integration

package immich

import (
	"bytes"
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

type e2eSidecar struct {
	cancel context.CancelFunc
	command *exec.Cmd
	output  bytes.Buffer
}

func startE2ESidecar(t *testing.T, suite *e2eSuite) *e2eSidecar {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	sidecar := &e2eSidecar{cancel: cancel}
	sidecar.command = exec.CommandContext(ctx, suite.sidecarPath)
	sidecar.command.Env = append(os.Environ(),
		"IMMICH_BASE_URL="+suite.immichURL,
		"IMMICH_EMAIL="+e2eEmail,
		"IMMICH_PASSWORD="+e2ePassword,
		"IMMICH_SESSION_TOKEN_FILE="+filepath.Join(t.TempDir(), "session-token"),
		"NATS_URL="+suite.natsURL,
		"SYNC_INTERVAL=1s",
		"SOCKETIO_ENABLED=true",
	)
	sidecar.command.Stdout = &sidecar.output
	sidecar.command.Stderr = &sidecar.output
	if err := sidecar.command.Start(); err != nil {
		cancel()
		t.Fatalf("start sidecar: %v", err)
	}
	t.Cleanup(func() { sidecar.Stop(t) })
	return sidecar
}

func (s *e2eSidecar) Stop(t *testing.T) {
	t.Helper()
	s.cancel()
	if err := s.command.Wait(); err != nil && s.command.ProcessState.ExitCode() != -1 {
		t.Errorf("sidecar exited unexpectedly: %v: %s", err, s.output.String())
	}
}
