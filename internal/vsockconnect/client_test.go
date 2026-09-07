package vsockconnect_test

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/liquidmetal-dev/battery/internal/vsockconnect"
)

func TestPingSuccess(t *testing.T) {
	dir := t.TempDir()
	binPath := writeFakeVsockConnect(t, dir)
	argsFile := filepath.Join(dir, "args")
	t.Setenv("ARGS_FILE", argsFile)
	t.Setenv("FAKE_PING_EXIT", "0")

	client := vsockconnect.New(binPath)
	err := client.Ping(context.Background(), "/run/flintlock/some.vsock", 1024)
	if err != nil {
		t.Fatalf("Ping returned error: %v", err)
	}

	args := readArgsFile(t, argsFile)
	if !strings.Contains(args, "ping") ||
		!strings.Contains(args, "--uds /run/flintlock/some.vsock") ||
		!strings.Contains(args, "--port 1024") {
		t.Fatalf("unexpected args recorded: %q", args)
	}
}

func TestPingFailure(t *testing.T) {
	dir := t.TempDir()
	binPath := writeFakeVsockConnect(t, dir)
	t.Setenv("ARGS_FILE", filepath.Join(dir, "args"))
	t.Setenv("FAKE_PING_EXIT", "1")

	client := vsockconnect.New(binPath)
	err := client.Ping(context.Background(), "/run/flintlock/some.vsock", 1024)
	if err == nil {
		t.Fatal("expected an error when the fake binary exits non-zero")
	}
}

func TestExecStreamsOutputAndExitCode(t *testing.T) {
	dir := t.TempDir()
	binPath := writeFakeVsockConnect(t, dir)
	argsFile := filepath.Join(dir, "args")
	t.Setenv("ARGS_FILE", argsFile)
	t.Setenv("FAKE_STDOUT", "hello stdout")
	t.Setenv("FAKE_STDERR", "hello stderr")
	t.Setenv("FAKE_EXEC_EXIT", "7")

	client := vsockconnect.New(binPath)
	execution, err := client.Exec(context.Background(), "/run/flintlock/some.vsock", 1024, []string{"echo", "hi"})
	if err != nil {
		t.Fatalf("Exec returned error: %v", err)
	}

	stdout, err := io.ReadAll(execution.Stdout())
	if err != nil {
		t.Fatalf("reading stdout: %v", err)
	}
	stderr, err := io.ReadAll(execution.Stderr())
	if err != nil {
		t.Fatalf("reading stderr: %v", err)
	}

	exitCode, err := execution.Wait()
	if err != nil {
		t.Fatalf("Wait returned error: %v", err)
	}

	if string(stdout) != "hello stdout" {
		t.Fatalf("unexpected stdout: %q", stdout)
	}
	if string(stderr) != "hello stderr" {
		t.Fatalf("unexpected stderr: %q", stderr)
	}
	if exitCode != 7 {
		t.Fatalf("expected exit code 7, got %d", exitCode)
	}

	args := readArgsFile(t, argsFile)
	if !strings.Contains(args, "exec") ||
		!strings.Contains(args, "--uds /run/flintlock/some.vsock") ||
		!strings.Contains(args, "--port 1024") ||
		!strings.Contains(args, "-- echo hi") {
		t.Fatalf("unexpected args recorded: %q", args)
	}
}

func TestPingRespectsContextTimeout(t *testing.T) {
	dir := t.TempDir()
	// A binary that never exits, to prove Ping's context cancellation kills it rather than
	// hanging forever.
	script := "#!/bin/sh\nsleep 5\n"
	binPath := filepath.Join(dir, "vsock-connect")
	if err := os.WriteFile(binPath, []byte(script), 0o755); err != nil {
		t.Fatalf("writing fake vsock-connect: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	client := vsockconnect.New(binPath)
	start := time.Now()
	err := client.Ping(ctx, "/run/flintlock/some.vsock", 1024)
	elapsed := time.Since(start)

	if err == nil {
		t.Fatal("expected an error when the context times out")
	}
	if elapsed > 2*time.Second {
		t.Fatalf("Ping did not respect context timeout, took %s", elapsed)
	}
}

func readArgsFile(t *testing.T, path string) string {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("reading args file: %v", err)
	}
	return strings.TrimSpace(string(data))
}
