package flintlockclient

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"golang.org/x/mod/semver"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
)

// MinFlintlockVersion is the oldest flintlockd the pool manager provisions
// on. Before v0.15.2 flintlockd put each VM's guest-agent socket under
// <state-dir>/<namespace>/<name>/<uid>/, so a long namespace or pool name
// pushed it past the 107-byte Unix socket path limit and every VM failed
// with "connect: invalid argument" well after CreateMicroVM succeeded
// (https://github.com/liquidmetal-dev/battery/issues/94). v0.15.2 keys the
// socket path by uid alone.
const MinFlintlockVersion = "v0.15.2"

// ErrUnsupportedVersion is returned by Pool.CheckVersion when a host's
// flintlockd is older than MinFlintlockVersion, or its version can't be
// determined.
var ErrUnsupportedVersion = errors.New("flintlockclient: unsupported flintlock version")

// CheckVersion reports whether the named host runs a flintlockd at least as
// new as MinFlintlockVersion, returning an ErrUnsupportedVersion-wrapping
// error if not. Any other RPC failure (e.g. the host being unreachable) is
// returned as-is. A host that passes is remembered and not asked again; one
// that fails is asked again next time, so upgrading it takes effect without
// restarting the pool manager.
func (p *Pool) CheckVersion(ctx context.Context, hostName string) error {
	p.versionMu.Lock()
	ok := p.versionOK[hostName]
	p.versionMu.Unlock()
	if ok {
		return nil
	}

	client, err := p.Client(hostName)
	if err != nil {
		return err
	}

	resp, err := client.ServerInfo(ctx, &emptypb.Empty{})
	if status.Code(err) == codes.Unimplemented {
		// ServerInfo was added in v0.15.0.
		return fmt.Errorf("%w: host %q is older than v0.15.0, need %s or newer", ErrUnsupportedVersion, hostName, MinFlintlockVersion)
	}
	if err != nil {
		return fmt.Errorf("flintlockclient: host %q: ServerInfo: %w", hostName, err)
	}

	version := resp.GetVersion().GetVersion()
	core, valid := versionCore(version)
	if !valid {
		return fmt.Errorf("%w: host %q reports version %q, which can't be compared with the minimum %s (is flintlockd built without version information?)",
			ErrUnsupportedVersion, hostName, version, MinFlintlockVersion)
	}
	if semver.Compare(core, MinFlintlockVersion) < 0 {
		return fmt.Errorf("%w: host %q runs flintlock %s, need %s or newer", ErrUnsupportedVersion, hostName, version, MinFlintlockVersion)
	}

	p.versionMu.Lock()
	p.versionOK[hostName] = true
	p.versionMu.Unlock()
	return nil
}

// versionCore reduces a flintlock version string to its vMAJOR.MINOR.PATCH
// core, so that builds between releases (git describe output such as
// "v0.15.2-3-gabc1234") compare as the release they're built on rather than
// as a semver prerelease of it.
func versionCore(version string) (string, bool) {
	v := version
	if !strings.HasPrefix(v, "v") {
		v = "v" + v
	}
	if i := strings.IndexAny(v, "-+"); i >= 0 {
		v = v[:i]
	}
	if !semver.IsValid(v) {
		return "", false
	}
	return v, true
}
