package flintlockclient

import (
	"context"
	"errors"
	"fmt"

	"google.golang.org/protobuf/types/known/emptypb"
)

// ErrBalloonUnsupported is returned when a host's flintlockd does not know
// MicroVMSpec.balloon. A flintlockd from before liquidmetal-dev/flintlock#1415
// accepts a spec with a balloon and drops the field, so the VM boots with no
// balloon and never returns the memory it frees.
var ErrBalloonUnsupported = errors.New("flintlockclient: flintlockd does not support the balloon")

// MarkBalloonUnsupported records that the named host's flintlockd dropped
// the balloon from a CreateMicroVM, at the version the host last reported.
// CheckBalloon then refuses the host until it reports another version. It
// does nothing if no such host is in the pool.
func (p *Pool) MarkBalloonUnsupported(hostName string) {
	c, err := p.conn(hostName)
	if err != nil {
		return
	}
	c.versionMu.Lock()
	defer c.versionMu.Unlock()
	// A host with no version yet is marked with a placeholder, so that the
	// next CheckBalloon asks it again.
	c.balloonUnsupported = c.version
	if c.balloonUnsupported == "" {
		c.balloonUnsupported = "unknown"
	}
}

// CheckBalloon returns an ErrBalloonUnsupported-wrapping error if the named
// host was marked by MarkBalloonUnsupported and still runs the version it
// ran then. It asks a marked host for its version each time, so an upgrade
// takes effect without restarting the pool manager. A host that was never
// marked costs no RPC. It returns ErrUnknownHost if no such host is in the
// pool.
func (p *Pool) CheckBalloon(ctx context.Context, hostName string) error {
	c, err := p.conn(hostName)
	if err != nil {
		return err
	}

	c.versionMu.Lock()
	marked := c.balloonUnsupported
	c.versionMu.Unlock()
	if marked == "" {
		return nil
	}

	resp, err := c.microVM.ServerInfo(ctx, &emptypb.Empty{})
	if err != nil {
		return fmt.Errorf("flintlockclient: host %q: ServerInfo: %w", c.name, err)
	}
	version := resp.GetVersion().GetVersion()

	c.versionMu.Lock()
	defer c.versionMu.Unlock()
	c.version = version
	if version != marked {
		c.balloonUnsupported = ""
		return nil
	}
	return fmt.Errorf("%w: host %q runs flintlock %s, which drops MicroVMSpec.balloon; upgrade it to a flintlockd with liquidmetal-dev/flintlock#1415",
		ErrBalloonUnsupported, c.name, version)
}
