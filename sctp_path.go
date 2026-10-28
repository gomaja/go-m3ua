// Copyright 2018-2024 go-m3ua authors. All rights reserved.
// Use of this source code is governed by a MIT-style license that can be
// found in the LICENSE file.

package m3ua

import (
	"errors"
	"fmt"
	"net/netip"
	"syscall"

	"github.com/gomaja/go-sctp"
)

// SetSCTPPrimaryAddr selects one peer address for default transmissions
// (RFC 9260 Section 11.1.6; RFC 6458 Section 8.1.9). The address must be a
// current peer address. Validation and socket errors are returned by go-sctp.
func (c *Association) SetSCTPPrimaryAddr(addr netip.Addr) error {
	if c.sctpConn == nil {
		return ErrAssociationClosed
	}
	return c.sctpConn.SetPrimaryAddr(addr)
}

// SCTPPathParams reads one path's parameters, or the association defaults for
// the zero netip.Addr (RFC 6458 Section 8.1.12).
func (c *Association) SCTPPathParams(addr netip.Addr) (*sctp.PathParams, error) {
	if c.sctpConn == nil {
		return nil, ErrAssociationClosed
	}
	return c.sctpConn.PathParams(addr)
}

// SetSCTPPathParams changes one path's parameters. The zero netip.Addr applies
// to the association and all current and future paths (RFC 6458 Section
// 8.1.12). Nil fields leave settings unchanged; go-sctp validates the values.
func (c *Association) SetSCTPPathParams(addr netip.Addr, p *sctp.PathParams) error {
	if c.sctpConn == nil {
		return ErrAssociationClosed
	}
	return c.sctpConn.SetPathParams(addr, p)
}

// SCTPPathThresholds reads one path's failure thresholds, or the association
// defaults for the zero netip.Addr (RFC 7829 Section 7.2).
func (c *Association) SCTPPathThresholds(addr netip.Addr) (*sctp.PathThresholds, error) {
	if c.sctpConn == nil {
		return nil, ErrAssociationClosed
	}
	return c.sctpConn.PathThresholds(addr)
}

// SetSCTPPathThresholds changes one path's failure thresholds. The zero
// netip.Addr applies to the association and all current and future paths
// (RFC 7829 Section 7.2). Nil fields leave thresholds unchanged; go-sctp
// validates the values.
func (c *Association) SetSCTPPathThresholds(addr netip.Addr, t *sctp.PathThresholds) error {
	if c.sctpConn == nil {
		return ErrAssociationClosed
	}
	return c.sctpConn.SetPathThresholds(addr, t)
}

func (c *Association) setUpPaths() error {
	if p := c.cfg.PathParams; p != nil {
		if err := c.SetSCTPPathParams(netip.Addr{}, p); err != nil {
			return fmt.Errorf("failed to set SCTP path parameters: %w", err)
		}
	}
	if t := c.cfg.PathThresholds; t != nil {
		if err := c.SetSCTPPathThresholds(netip.Addr{}, t); err != nil {
			return fmt.Errorf("failed to set SCTP path thresholds: %w", err)
		}
	}
	if e := c.cfg.PFExposure; e != sctp.PFExposeUnset {
		if err := c.sctpConn.SetPFExposure(e); err != nil {
			return fmt.Errorf("failed to set SCTP PF exposure: %w", err)
		}
	}
	return nil
}

func peerPathInfos(peers *sctp.Addr, read func(netip.Addr) (*sctp.PathInfo, error)) ([]sctp.PathInfo, error) {
	paths := make([]sctp.PathInfo, 0, len(peers.IPs))
	var removedErr error
	for _, ip := range peers.IPs {
		p, err := read(ip)
		switch {
		case errors.Is(err, syscall.EINVAL):
			// RFC 6458 Sections 9.3 and 8.2.2 are separate queries: an address
			// removed after enumeration no longer has path information.
			removedErr = err
			continue
		case errors.Is(err, syscall.EACCES):
			// Linux net/sctp/socket.c, sctp_getsockopt_peer_addr_info: EACCES
			// means SCTP_PF with PF exposure disabled (RFC 7829 Section 7.3).
			paths = append(paths, sctp.PathInfo{Addr: netip.AddrPortFrom(ip, peers.Port), State: sctp.PathPotentiallyFailed})
		case err != nil:
			return nil, err
		default:
			paths = append(paths, *p)
		}
	}
	// A live association has a peer path. If none survived enumeration, the
	// association may have ended; preserve the actual PathInfo error.
	if len(paths) == 0 && removedErr != nil {
		return nil, removedErr
	}
	return paths, nil
}

// The caller may reuse and change configuration after Dial/Accept snapshots it.
func snapshotPathParams(p *sctp.PathParams) *sctp.PathParams {
	if p == nil {
		return nil
	}
	return &sctp.PathParams{
		Heartbeat:         snapshotOption(p.Heartbeat),
		HeartbeatInterval: snapshotOption(p.HeartbeatInterval),
		PathMaxRetrans:    snapshotOption(p.PathMaxRetrans),
		PMTUD:             snapshotOption(p.PMTUD),
		PathMTU:           snapshotOption(p.PathMTU),
		DelayedSACK:       snapshotOption(p.DelayedSACK),
		SACKDelay:         snapshotOption(p.SACKDelay),
		IPv6FlowLabel:     snapshotOption(p.IPv6FlowLabel),
		DSCP:              snapshotOption(p.DSCP),
	}
}
func snapshotPathThresholds(t *sctp.PathThresholds) *sctp.PathThresholds {
	if t == nil {
		return nil
	}
	return &sctp.PathThresholds{
		PathMaxRetrans:    snapshotOption(t.PathMaxRetrans),
		PFThreshold:       snapshotOption(t.PFThreshold),
		PrimarySwitchover: snapshotOption(t.PrimarySwitchover),
	}
}
func snapshotOption[T any](p *T) *T {
	if p == nil {
		return nil
	}
	v := *p
	return &v
}
