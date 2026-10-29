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

// SCTPRTOInfo reads the association's RTO.Initial, RTO.Min and RTO.Max
// (RFC 6458 §8.1.1). Paths' current RTOs are reported by AssociationStatus.
func (c *Association) SCTPRTOInfo() (*sctp.RTOInfo, error) {
	if c.sctpConn == nil {
		return nil, ErrAssociationClosed
	}
	return c.sctpConn.RTOInfo()
}

// SetSCTPRTOInfo changes the association's RTO parameters (RFC 9260 §6.3.1
// rules C1, C6 and C7; §6.3.3 rule E2; RFC 6458 §8.1.1). Zero fields keep
// their current values; go-sctp returns validation and socket errors unchanged.
//
// On Linux the association's bounds change at once, but existing paths keep
// their current RTO until their next RTT measurement or retransmission timeout
// (net/sctp/transport.c, sctp_transport_update_rto; net/sctp/sm_sideeffect.c,
// sctp_do_8_2_transport_strike). A timeout doubles the current RTO and caps
// it at Max; it need not reach a newly raised Min. The new Initial reaches
// only paths added later
// or reset by a restart (sctp_assoc_add_peer, sctp_transport_reset). The
// T5-shutdown-guard stays at five times the RTO.Max used at association creation
// (net/sctp/associola.c, sctp_association_init; RFC 9260 §9.2).
// See https://github.com/sctp/lksctp-tools/issues/74. On an accepting end the
// first measurement is normally the first M3UA message this end sends, once
// acknowledged without retransmission, or a HEARTBEAT ACK (sctp_check_transmitted,
// sctp_cmd_transport_on).
func (c *Association) SetSCTPRTOInfo(r *sctp.RTOInfo) error {
	if c.sctpConn == nil {
		return ErrAssociationClosed
	}
	return c.sctpConn.SetRTOInfo(r)
}

// SCTPAssociationMaxRetrans reads Association.Max.Retrans, the association's
// failure threshold (RFC 9260 §8.1; RFC 6458 §8.1.2).
func (c *Association) SCTPAssociationMaxRetrans() (uint16, error) {
	if c.sctpConn == nil {
		return 0, ErrAssociationClosed
	}
	a, err := c.sctpConn.AssocInfo()
	if err != nil {
		return 0, err
	}
	return a.MaxRetrans, nil
}

// SetSCTPAssociationMaxRetrans changes Association.Max.Retrans at once;
// zero keeps the current value (RFC 9260 §8.1; RFC 6458 §8.1.2).
// The Note in RFC 9260 §8.2 advises keeping it at most the sum of all peer
// paths' Path.Max.Retrans, or every path could be inactive while the peer is
// still considered reachable. Linux returns EINVAL for a larger value on a
// multi-homed association (net/sctp/socket.c, sctp_setsockopt_associnfo), but
// does not recheck it when SetSCTPPathParams later lowers Path.Max.Retrans.
// go-sctp's errors pass through unchanged.
func (c *Association) SetSCTPAssociationMaxRetrans(n uint16) error {
	if c.sctpConn == nil {
		return ErrAssociationClosed
	}
	return c.sctpConn.SetAssocInfo(&sctp.AssocInfo{MaxRetrans: n})
}

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

func equalRTOInfo(a, b *sctp.RTOInfo) bool {
	var av, bv sctp.RTOInfo
	if a != nil {
		av = *a
	}
	if b != nil {
		bv = *b
	}
	return av.Initial == bv.Initial && av.Min == bv.Min && av.Max == bv.Max
}
