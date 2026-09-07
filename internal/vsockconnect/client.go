// Package vsockconnect wraps the vsock-connect CLI, the only supported way to reach a
// flintlock-managed guest-agent over its vsock device (the wire protocol itself isn't importable
// as a Go package). It shells out to vsock-connect ping/exec and isolates all os/exec details
// behind the Runner interface.
package vsockconnect

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os/exec"
	"strconv"
)

// Runner performs vsock-connect ping/exec calls against a guest-agent.
type Runner interface {
	// Ping checks liveness of the guest-agent listening on the vsock device at vsockPath.
	Ping(ctx context.Context, vsockPath string, port int) error
	// Exec runs cmd via the guest-agent's exec, returning an Execution that streams output live.
	Exec(ctx context.Context, vsockPath string, port int, cmd []string) (Execution, error)
}

// Execution represents an in-flight (or completed) vsock-connect exec invocation.
type Execution interface {
	Stdout() io.Reader
	Stderr() io.Reader
	// Wait blocks until the underlying command exits and returns its exit code.
	Wait() (int, error)
}

// cmdExecution is the Execution implementation backed by a real subprocess.
type cmdExecution struct {
	stdout io.Reader
	stderr io.Reader
	cmd    *exec.Cmd
}

func (e *cmdExecution) Stdout() io.Reader { return e.stdout }
func (e *cmdExecution) Stderr() io.Reader { return e.stderr }

func (e *cmdExecution) Wait() (int, error) {
	err := e.cmd.Wait()
	if err == nil {
		return e.cmd.ProcessState.ExitCode(), nil
	}

	var exitErr *exec.ExitError
	if errors.As(err, &exitErr) {
		return exitErr.ExitCode(), nil
	}
	return -1, err
}

// Client is the real Runner implementation, shelling out to the vsock-connect binary.
type Client struct {
	// BinaryPath is the path to (or name of, if resolvable via $PATH) the vsock-connect binary.
	BinaryPath string
}

// New returns a Client that invokes the vsock-connect binary at binaryPath.
func New(binaryPath string) *Client {
	return &Client{BinaryPath: binaryPath}
}

// Ping runs `vsock-connect ping --uds <vsockPath> --port <port>`.
func (c *Client) Ping(ctx context.Context, vsockPath string, port int) error {
	cmd := exec.CommandContext(ctx, c.BinaryPath, "ping", "--uds", vsockPath, "--port", strconv.Itoa(port))
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("vsock-connect ping: %w", err)
	}
	return nil
}

// Exec runs `vsock-connect exec --uds <vsockPath> --port <port> -- <cmd...>`, without a shell.
func (c *Client) Exec(ctx context.Context, vsockPath string, port int, cmdArgs []string) (Execution, error) {
	args := append([]string{"exec", "--uds", vsockPath, "--port", strconv.Itoa(port), "--"}, cmdArgs...)
	cmd := exec.CommandContext(ctx, c.BinaryPath, args...)

	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, fmt.Errorf("vsock-connect exec: stdout pipe: %w", err)
	}
	stderr, err := cmd.StderrPipe()
	if err != nil {
		return nil, fmt.Errorf("vsock-connect exec: stderr pipe: %w", err)
	}

	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("vsock-connect exec: starting: %w", err)
	}

	return &cmdExecution{stdout: stdout, stderr: stderr, cmd: cmd}, nil
}
