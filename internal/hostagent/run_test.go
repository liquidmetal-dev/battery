package hostagent_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"

	poolmgrv1alpha1 "github.com/liquidmetal-dev/battery/api/proto/poolmgr/v1alpha1"
	"github.com/liquidmetal-dev/battery/internal/hostagent"
	"github.com/liquidmetal-dev/battery/internal/vsockconnect"
)

func TestRunStreamsOutputAndExitCode(t *testing.T) {
	runner := &fakeRunner{
		execFn: func(_ context.Context, _ string, _ int, _ []string) (vsockconnect.Execution, error) {
			return &fakeExecution{
				stdout:   strings.NewReader("hello stdout"),
				stderr:   strings.NewReader("hello stderr"),
				exitCode: 3,
			}, nil
		},
	}
	server := hostagent.NewServer(runner, 1024, time.Millisecond)
	stream := newFakeRunStream(context.Background())

	err := server.Run(&poolmgrv1alpha1.RunRequest{
		VsockPath: "/run/flintlock/a.vsock",
		Cmd:       []string{"echo", "hi"},
		Timeout:   durationpb.New(time.Second),
	}, stream)
	if err != nil {
		t.Fatalf("Run returned error: %v", err)
	}

	var gotStdout, gotStderr []byte
	var gotExitCode int32
	var sawExitCode bool
	for _, msg := range stream.messages() {
		switch out := msg.Output.(type) {
		case *poolmgrv1alpha1.RunResponse_StdoutChunk:
			gotStdout = append(gotStdout, out.StdoutChunk...)
		case *poolmgrv1alpha1.RunResponse_StderrChunk:
			gotStderr = append(gotStderr, out.StderrChunk...)
		case *poolmgrv1alpha1.RunResponse_ExitCode:
			gotExitCode = out.ExitCode
			sawExitCode = true
		}
	}

	if string(gotStdout) != "hello stdout" {
		t.Fatalf("unexpected stdout: %q", gotStdout)
	}
	if string(gotStderr) != "hello stderr" {
		t.Fatalf("unexpected stderr: %q", gotStderr)
	}
	if !sawExitCode {
		t.Fatal("expected an exit code message")
	}
	if gotExitCode != 3 {
		t.Fatalf("expected exit code 3, got %d", gotExitCode)
	}
	last := stream.messages()[len(stream.messages())-1]
	if _, ok := last.Output.(*poolmgrv1alpha1.RunResponse_ExitCode); !ok {
		t.Fatalf("expected the last message to be the exit code, got %T", last.Output)
	}
}

func TestRunPropagatesExecStartError(t *testing.T) {
	runner := &fakeRunner{
		execFn: func(_ context.Context, _ string, _ int, _ []string) (vsockconnect.Execution, error) {
			return nil, errors.New("boom")
		},
	}
	server := hostagent.NewServer(runner, 1024, time.Millisecond)
	stream := newFakeRunStream(context.Background())

	err := server.Run(&poolmgrv1alpha1.RunRequest{
		VsockPath: "/run/flintlock/a.vsock",
		Cmd:       []string{"echo", "hi"},
		Timeout:   durationpb.New(time.Second),
	}, stream)
	if err == nil {
		t.Fatal("expected an error when exec fails to start")
	}
}

func TestRunTimesOut(t *testing.T) {
	blockForever := make(chan struct{})
	t.Cleanup(func() { close(blockForever) })

	runner := &fakeRunner{
		execFn: func(_ context.Context, _ string, _ int, _ []string) (vsockconnect.Execution, error) {
			return &fakeExecution{
				stdout: strings.NewReader(""),
				stderr: strings.NewReader(""),
				waitFn: func() (int, error) {
					<-blockForever
					return 0, nil
				},
			}, nil
		},
	}
	server := hostagent.NewServer(runner, 1024, time.Millisecond)
	stream := newFakeRunStream(context.Background())

	err := server.Run(&poolmgrv1alpha1.RunRequest{
		VsockPath: "/run/flintlock/a.vsock",
		Cmd:       []string{"sleep", "100"},
		Timeout:   durationpb.New(20 * time.Millisecond),
	}, stream)
	if err == nil {
		t.Fatal("expected an error when the command never completes")
	}
	if got := status.Code(err); got != codes.DeadlineExceeded {
		t.Fatalf("expected codes.DeadlineExceeded, got %v", got)
	}
}

func TestRunRejectsEmptyVsockPath(t *testing.T) {
	runner := &fakeRunner{}
	server := hostagent.NewServer(runner, 1024, time.Millisecond)
	stream := newFakeRunStream(context.Background())

	err := server.Run(&poolmgrv1alpha1.RunRequest{
		VsockPath: "",
		Cmd:       []string{"echo", "hi"},
	}, stream)
	if err == nil {
		t.Fatal("expected an error for an empty vsock_path")
	}
	if got := status.Code(err); got != codes.InvalidArgument {
		t.Fatalf("expected codes.InvalidArgument, got %v", got)
	}
}
