package flintlockclient_test

import (
	"context"
	"errors"
	"net"
	"sync"
	"testing"

	microvmv1alpha1 "github.com/liquidmetal-dev/flintlock/api/services/microvm/v1alpha1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/liquidmetal-dev/battery/internal/flintlockclient"
)

// serverInfoServer is a fake flintlock MicroVM service whose ServerInfo
// reports a settable version (or error), and counts how often it's called.
type serverInfoServer struct {
	microvmv1alpha1.UnimplementedMicroVMServer

	mu      sync.Mutex
	version string
	err     error
	calls   int
}

func (f *serverInfoServer) ServerInfo(context.Context, *emptypb.Empty) (*microvmv1alpha1.ServerInfoResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	if f.err != nil {
		return nil, f.err
	}
	return &microvmv1alpha1.ServerInfoResponse{Version: &microvmv1alpha1.VersionInfo{Version: f.version}}, nil
}

func (f *serverInfoServer) set(version string, err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.version, f.err = version, err
}

func (f *serverInfoServer) callCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls
}

// startServerInfoServer serves fake on a loopback listener and returns its
// address.
func startServerInfoServer(t *testing.T, fake *serverInfoServer) string {
	t.Helper()

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	srv := grpc.NewServer()
	microvmv1alpha1.RegisterMicroVMServer(srv, fake)
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)

	return lis.Addr().String()
}

// startServerInfoPool serves fake on a loopback listener and returns a
// single-host Pool ("host-a") dialled to it.
func startServerInfoPool(t *testing.T, fake *serverInfoServer) *flintlockclient.Pool {
	t.Helper()
	return newPool(t, insecureHost("host-a", startServerInfoServer(t, fake)))
}

func TestCheckVersion(t *testing.T) {
	tests := []struct {
		name            string
		version         string
		err             error
		wantUnsupported bool
		wantOtherErr    bool
	}{
		{name: "minimum version", version: "v0.15.2"},
		{name: "no v prefix", version: "0.15.3"},
		{name: "newer minor", version: "v0.16.0"},
		{name: "git describe build of minimum", version: "v0.15.2-3-gabc1234"},
		{name: "older patch", version: "v0.15.1", wantUnsupported: true},
		{name: "older minor", version: "v0.14.1", wantUnsupported: true},
		{name: "unparseable version", version: "undefined", wantUnsupported: true},
		{name: "empty version", version: "", wantUnsupported: true},
		{name: "ServerInfo unimplemented (pre-v0.15.0)", err: status.Error(codes.Unimplemented, "unknown method"), wantUnsupported: true},
		{name: "host unavailable", err: status.Error(codes.Unavailable, "down"), wantOtherErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fake := &serverInfoServer{}
			fake.set(tt.version, tt.err)
			pool := startServerInfoPool(t, fake)

			err := pool.CheckVersion(context.Background(), "host-a")
			switch {
			case tt.wantUnsupported:
				if !errors.Is(err, flintlockclient.ErrUnsupportedVersion) {
					t.Fatalf("CheckVersion() = %v, want ErrUnsupportedVersion", err)
				}
			case tt.wantOtherErr:
				if err == nil || errors.Is(err, flintlockclient.ErrUnsupportedVersion) {
					t.Fatalf("CheckVersion() = %v, want a non-version error", err)
				}
			default:
				if err != nil {
					t.Fatalf("CheckVersion() = %v, want nil", err)
				}
			}
		})
	}
}

func TestCheckVersion_CachesOnlySupportedHosts(t *testing.T) {
	fake := &serverInfoServer{}
	fake.set("v0.15.1", nil)
	pool := startServerInfoPool(t, fake)
	ctx := context.Background()

	if err := pool.CheckVersion(ctx, "host-a"); !errors.Is(err, flintlockclient.ErrUnsupportedVersion) {
		t.Fatalf("CheckVersion() on old host = %v, want ErrUnsupportedVersion", err)
	}

	// The host is upgraded: a rejected host must be asked again, not cached.
	fake.set("v0.15.2", nil)
	if err := pool.CheckVersion(ctx, "host-a"); err != nil {
		t.Fatalf("CheckVersion() after upgrade = %v, want nil", err)
	}
	if got := fake.callCount(); got != 2 {
		t.Fatalf("ServerInfo calls = %d, want 2", got)
	}

	// Once supported, the result is cached.
	if err := pool.CheckVersion(ctx, "host-a"); err != nil {
		t.Fatalf("CheckVersion() cached = %v, want nil", err)
	}
	if got := fake.callCount(); got != 2 {
		t.Fatalf("ServerInfo calls = %d, want still 2 (cached)", got)
	}
}

func TestCheckVersion_UnknownHost(t *testing.T) {
	pool := startServerInfoPool(t, &serverInfoServer{})
	if err := pool.CheckVersion(context.Background(), "nope"); !errors.Is(err, flintlockclient.ErrUnknownHost) {
		t.Fatalf("CheckVersion() = %v, want ErrUnknownHost", err)
	}
}

func TestFlintlockVersion(t *testing.T) {
	fake := &serverInfoServer{}
	fake.set("v0.15.1", nil)
	pool := startServerInfoPool(t, fake)
	ctx := context.Background()

	if v, ok := pool.FlintlockVersion("host-a"); ok {
		t.Fatalf("FlintlockVersion() before any check = %q, true; want false", v)
	}

	// A version too old to pass is still reported.
	if err := pool.CheckVersion(ctx, "host-a"); !errors.Is(err, flintlockclient.ErrUnsupportedVersion) {
		t.Fatalf("CheckVersion() on old host = %v, want ErrUnsupportedVersion", err)
	}
	if v, ok := pool.FlintlockVersion("host-a"); !ok || v != "v0.15.1" {
		t.Fatalf("FlintlockVersion() = %q, %v; want v0.15.1, true", v, ok)
	}

	fake.set("v0.16.0", nil)
	if err := pool.CheckVersion(ctx, "host-a"); err != nil {
		t.Fatalf("CheckVersion() after upgrade = %v, want nil", err)
	}
	if v, ok := pool.FlintlockVersion("host-a"); !ok || v != "v0.16.0" {
		t.Fatalf("FlintlockVersion() = %q, %v; want v0.16.0, true", v, ok)
	}

	// A failed RPC leaves the last reported version in place.
	fake.set("", status.Error(codes.Unavailable, "down"))
	if v, ok := pool.FlintlockVersion("host-a"); !ok || v != "v0.16.0" {
		t.Fatalf("FlintlockVersion() after outage = %q, %v; want v0.16.0, true", v, ok)
	}

	if _, ok := pool.FlintlockVersion("nope"); ok {
		t.Fatalf("FlintlockVersion(unknown) = true, want false")
	}
}

func TestCheckVersion_ClearedByUpdateAndRemove(t *testing.T) {
	fake := &serverInfoServer{}
	fake.set("v0.15.2", nil)
	pool := startServerInfoPool(t, fake)
	ctx := context.Background()

	if err := pool.CheckVersion(ctx, "host-a"); err != nil {
		t.Fatalf("CheckVersion() = %v, want nil", err)
	}
	addr, err := pool.Address("host-a")
	if err != nil {
		t.Fatalf("Address: %v", err)
	}

	// Update swaps in a connection that hasn't been checked, so the next
	// check asks the host again.
	c, err := flintlockclient.Dial(insecureHost("host-a", addr))
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	if err := pool.Update(c); err != nil {
		t.Fatalf("Update: %v", err)
	}
	if v, ok := pool.FlintlockVersion("host-a"); ok {
		t.Fatalf("FlintlockVersion() after Update = %q, true; want false", v)
	}
	if err := pool.CheckVersion(ctx, "host-a"); err != nil {
		t.Fatalf("CheckVersion() after Update = %v, want nil", err)
	}
	if got := fake.callCount(); got != 2 {
		t.Fatalf("ServerInfo calls = %d, want 2 (re-checked after Update)", got)
	}

	// A host removed and added back starts over too.
	if err := pool.Remove("host-a"); err != nil {
		t.Fatalf("Remove: %v", err)
	}
	if _, ok := pool.FlintlockVersion("host-a"); ok {
		t.Fatalf("FlintlockVersion() after Remove = true, want false")
	}
	c, err = flintlockclient.Dial(insecureHost("host-a", addr))
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	if err := pool.Add(c); err != nil {
		t.Fatalf("Add: %v", err)
	}
	if err := pool.CheckVersion(ctx, "host-a"); err != nil {
		t.Fatalf("CheckVersion() after re-Add = %v, want nil", err)
	}
	if got := fake.callCount(); got != 3 {
		t.Fatalf("ServerInfo calls = %d, want 3 (re-checked after Remove and Add)", got)
	}
}

// TestConn_CheckVersionBeforeAdd covers checking a candidate host before it
// joins the pool: the version it saw carries over into the pool.
func TestConn_CheckVersionBeforeAdd(t *testing.T) {
	fake := &serverInfoServer{}
	fake.set("v0.15.2", nil)
	addr := startServerInfoServer(t, fake)

	c, err := flintlockclient.Dial(insecureHost("host-a", addr))
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	if err := c.CheckVersion(context.Background()); err != nil {
		t.Fatalf("Conn.CheckVersion() = %v, want nil", err)
	}

	pool, err := flintlockclient.New(nil)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = pool.Close() })
	if err := pool.Add(c); err != nil {
		t.Fatalf("Add: %v", err)
	}

	if v, ok := pool.FlintlockVersion("host-a"); !ok || v != "v0.15.2" {
		t.Fatalf("FlintlockVersion() = %q, %v; want v0.15.2, true", v, ok)
	}
	if err := pool.CheckVersion(context.Background(), "host-a"); err != nil {
		t.Fatalf("CheckVersion() = %v, want nil", err)
	}
	if got := fake.callCount(); got != 1 {
		t.Fatalf("ServerInfo calls = %d, want 1 (cached from the candidate check)", got)
	}
}
