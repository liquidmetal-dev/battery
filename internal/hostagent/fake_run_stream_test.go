package hostagent_test

import (
	"context"
	"sync"

	"google.golang.org/grpc/metadata"

	poolmgrv1alpha1 "github.com/liquidmetal-dev/battery/api/proto/poolmgr/v1alpha1"
)

// fakeRunStream is a minimal grpc.ServerStreamingServer[poolmgrv1alpha1.RunResponse] double that
// records every message sent to it, without needing a real gRPC connection.
type fakeRunStream struct {
	ctx context.Context

	// sendErr, when set, is returned by every call to Send instead of recording the message.
	sendErr error

	mu   sync.Mutex
	sent []*poolmgrv1alpha1.RunResponse
}

func newFakeRunStream(ctx context.Context) *fakeRunStream {
	return &fakeRunStream{ctx: ctx}
}

func (f *fakeRunStream) Send(resp *poolmgrv1alpha1.RunResponse) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.sendErr != nil {
		return f.sendErr
	}
	f.sent = append(f.sent, resp)
	return nil
}

func (f *fakeRunStream) messages() []*poolmgrv1alpha1.RunResponse {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]*poolmgrv1alpha1.RunResponse, len(f.sent))
	copy(out, f.sent)
	return out
}

func (f *fakeRunStream) Context() context.Context     { return f.ctx }
func (f *fakeRunStream) SetHeader(metadata.MD) error  { return nil }
func (f *fakeRunStream) SendHeader(metadata.MD) error { return nil }
func (f *fakeRunStream) SetTrailer(metadata.MD)       {}
func (f *fakeRunStream) SendMsg(_ any) error          { return nil }
func (f *fakeRunStream) RecvMsg(_ any) error          { return nil }
