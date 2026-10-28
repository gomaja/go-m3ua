// Copyright 2018-2024 go-m3ua authors. All rights reserved.
// Use of this source code is governed by a MIT-style license that can be
// found in the LICENSE file.

package m3ua

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/netip"
	"reflect"
	"slices"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/gomaja/go-sctp"
)

// Synthetic readers use one buffered handoff slot and explicitly dispatch each
// event, as restart tests do. This keeps the application queue independently
// testable without starting a monitor or opening SCTP.
func newQueuedPathAssociation(t *testing.T) *Association {
	t.Helper()
	c := newIndicationAssociation(t)
	c.inboundChan = make(chan inbound, 1)
	return c
}

func dispatchPeerAddrMarker(t *testing.T, c *Association) {
	t.Helper()
	select {
	case event := <-c.inboundChan:
		if event.kind != inboundSCTPPeerAddrChange {
			t.Fatalf("queued kind = %v, want peer-address change", event.kind)
		}
		c.dispatchInbound(context.Background(), event)
	default:
		t.Fatal("peer-address change was not queued for ordered dispatch")
	}
}

func TestSCTPPathEventsSubscription(t *testing.T) {
	if got := associationEvents(); !slices.Equal(got, []sctp.EventType{sctp.EventAssocChange, sctp.EventPeerAddrChange}) {
		t.Fatalf("associationEvents = %v", got)
	}
}

func TestSCTPPathEventRouting(t *testing.T) {
	for _, listener := range []bool{false, true} {
		t.Run(fmt.Sprintf("listener=%v", listener), func(t *testing.T) {
			first, second := newQueuedPathAssociation(t), newQueuedPathAssociation(t)
			first.assocID.Store(7)
			second.assocID.Store(8)
			firstEvents, secondEvents := first.ManagementIndications(), second.ManagementIndications()
			w := &restartWatcher{}
			w.setRoute(first.associationForSCTPID)
			if listener {
				l := &Listener{conns: map[*Association]struct{}{first: {}, second: {}}}
				w.setRoute(l.associationForSCTPID)
			}
			event := &sctp.PeerAddrChange{Addr: netip.MustParseAddrPort("127.0.0.2:2905"), State: sctp.AddrUnreachable, AssocID: 7}
			if err := w.handle(event); err != nil {
				t.Fatalf("handler ended read: %v", err)
			}
			dispatchPeerAddrMarker(t, first)
			select {
			case ind := <-firstEvents:
				if ind.Kind.String() != "M-SCTP_STATUS" || !strings.Contains(ind.Description, "SCTP_ADDR_UNREACHABLE") || !strings.Contains(ind.Description, event.Addr.String()) {
					t.Fatalf("indication = %+v", ind)
				}
			default:
				t.Fatal("no path status indication routed to association 7")
			}
			select {
			case ind := <-secondEvents:
				t.Fatalf("event misrouted: %+v", ind)
			default:
			}
			event.AssocID = 99
			if err := w.handle(event); err != nil {
				t.Fatal(err)
			}
			if len(firstEvents) != 0 || len(secondEvents) != 0 || len(first.inboundChan) != 0 || len(second.inboundChan) != 0 {
				t.Fatal("unknown association event delivered")
			}
		})
	}
}

func TestSCTPPathEventLazySubscriptionAndOverflow(t *testing.T) {
	c := newQueuedPathAssociation(t)
	w := &restartWatcher{}
	w.setRoute(func(sctp.AssocID) *Association { return c })
	event := &sctp.PeerAddrChange{Addr: netip.MustParseAddrPort("127.0.0.2:2905"), State: sctp.AddrAvailable}
	for range 100 {
		if err := w.handle(event); err != nil {
			t.Fatal(err)
		}
		dispatchPeerAddrMarker(t, c)
	}
	if len(c.mgmtChan) != 0 || c.indicationOverflow.Load() {
		t.Fatal("unsubscribed indications queued or overflowed")
	}
	ch := c.ManagementIndications()
	for range cap(ch) {
		if err := w.handle(event); err != nil {
			t.Fatal(err)
		}
		dispatchPeerAddrMarker(t, c)
	}
	if len(ch) != cap(ch) || c.indicationOverflow.Load() {
		t.Fatal("queue did not hold exactly 64 indications")
	}
	if err := w.handle(event); err != nil {
		t.Fatalf("overflow ended the notification read: %v", err)
	}
	dispatchPeerAddrMarker(t, c)
	select {
	case <-c.Done():
	case <-time.After(time.Second):
		t.Fatal("overflow did not close association")
	}
	if !errors.Is(c.Err(), ErrIndicationQueueFull) {
		t.Fatalf("overflow cause = %v", c.Err())
	}
	for range ch {
	}
}

func TestSCTPPathEventOwnsCopyAndDescriptions(t *testing.T) {
	c := newQueuedPathAssociation(t)
	ch := c.ManagementIndications()
	w := &restartWatcher{}
	w.setRoute(func(sctp.AssocID) *Association { return c })
	for _, tt := range []struct {
		state sctp.AddrChangeState
		name  string
	}{
		{sctp.AddrAvailable, "SCTP_ADDR_AVAILABLE"}, {sctp.AddrUnreachable, "SCTP_ADDR_UNREACHABLE"},
		{sctp.AddrRemoved, "SCTP_ADDR_REMOVED"}, {sctp.AddrAdded, "SCTP_ADDR_ADDED"},
		{sctp.AddrMadePrimary, "SCTP_ADDR_MADE_PRIM"}, {sctp.AddrConfirmed, "SCTP_ADDR_CONFIRMED"},
		{sctp.AddrPotentiallyFailed, "SCTP_ADDR_POTENTIALLY_FAILED"}, {sctp.AddrChangeState(99), "unknown(99)"},
	} {
		event := &sctp.PeerAddrChange{Addr: netip.MustParseAddrPort("127.0.0.2:2905"), State: tt.state, AssocID: 7}
		want := *event
		if err := w.handle(event); err != nil {
			t.Fatal(err)
		}
		event.State = sctp.AddrChangeState(100)
		event.Addr = netip.AddrPort{}
		dispatchPeerAddrMarker(t, c)
		select {
		case ind := <-ch:
			if ind.Kind != ManagementSCTPStatus || ind.PeerAddrChange == event || *ind.PeerAddrChange != want || ind.Description != tt.name+": "+want.Addr.String() {
				t.Fatalf("indication = %+v, event = %+v", ind, ind.PeerAddrChange)
			}
			ind.PeerAddrChange.AssocID = 20
			if event.AssocID != 7 {
				t.Fatal("indication aliases original")
			}
		default:
			t.Fatal("missing event")
		}
	}
	if ManagementSCTPStatus.String() != "M-SCTP_STATUS" {
		t.Fatal("wrong status kind name")
	}
	if err := (&restartWatcher{}).handle(&sctp.PeerAddrChange{}); err != nil {
		t.Fatal(err)
	}
}

func TestSCTPPathMaxRetransConflict(t *testing.T) {
	one, two := uint16(1), uint16(2)
	for _, tt := range []struct {
		name    string
		p       *sctp.PathParams
		th      *sctp.PathThresholds
		invalid bool
	}{
		{"unset", nil, nil, false}, {"params", &sctp.PathParams{PathMaxRetrans: &one}, nil, false},
		{"thresholds", nil, &sctp.PathThresholds{PathMaxRetrans: &two}, false},
		{"nil fields", &sctp.PathParams{}, &sctp.PathThresholds{}, false},
		{"equal", &sctp.PathParams{PathMaxRetrans: &one}, &sctp.PathThresholds{PathMaxRetrans: &one}, false},
		{"conflict", &sctp.PathParams{PathMaxRetrans: &one}, &sctp.PathThresholds{PathMaxRetrans: &two}, true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			err := validateSCTPConfig(&SCTPConfig{PathParams: tt.p, PathThresholds: tt.th})
			if errors.Is(err, ErrInvalidSCTPConfig) != tt.invalid {
				t.Fatalf("validation = %v, invalid=%v", err, tt.invalid)
			}
		})
	}
}

func TestSCTPPathControlsWithoutAssociation(t *testing.T) {
	c := newIndicationAssociation(t)
	for _, tt := range []struct {
		name string
		call func() error
	}{
		{"primary", func() error { return c.SetSCTPPrimaryAddr(netip.Addr{}) }},
		{"get params", func() error { _, err := c.SCTPPathParams(netip.Addr{}); return err }},
		{"set params", func() error { return c.SetSCTPPathParams(netip.Addr{}, nil) }},
		{"get thresholds", func() error { _, err := c.SCTPPathThresholds(netip.Addr{}); return err }},
		{"set thresholds", func() error { return c.SetSCTPPathThresholds(netip.Addr{}, nil) }},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.call(); !errors.Is(err, ErrAssociationClosed) {
				t.Fatalf("error = %v", err)
			}
		})
	}
}

func TestSCTPPathInfoRacesAndErrors(t *testing.T) {
	peers := mcAddr(2905, "127.0.0.1", "127.0.0.2", "127.0.0.3")
	gone, hidden := peers.IPs[1], peers.IPs[2]
	paths, err := peerPathInfos(peers, func(ip netip.Addr) (*sctp.PathInfo, error) {
		if ip == gone {
			return nil, &net.OpError{Op: "get", Err: syscall.EINVAL}
		}
		if ip == hidden {
			return nil, &net.OpError{Op: "get", Err: syscall.EACCES}
		}
		return &sctp.PathInfo{Addr: netip.AddrPortFrom(ip, 2905), State: sctp.PathActive, Cwnd: 123, MTU: 1500}, nil
	})
	if err != nil || len(paths) != 2 {
		t.Fatalf("paths=%+v error=%v", paths, err)
	}
	if paths[0].Addr.Addr() != peers.IPs[0] || paths[0].Cwnd != 123 {
		t.Fatalf("active path=%+v", paths[0])
	}
	want := sctp.PathInfo{Addr: netip.AddrPortFrom(hidden, 2905), State: sctp.PathPotentiallyFailed}
	if paths[1] != want {
		t.Fatalf("hidden PF path=%+v want=%+v", paths[1], want)
	}
	for _, wantErr := range []error{syscall.EIO, syscall.EBADF, net.ErrClosed} {
		paths, err = peerPathInfos(peers, func(netip.Addr) (*sctp.PathInfo, error) { return nil, wantErr })
		if paths != nil || err != wantErr {
			t.Fatalf("failure=%+v %v want=%v", paths, err, wantErr)
		}
	}
}

func TestSCTPPathConfigSnapshotOwnsEveryOption(t *testing.T) {
	cfg := mcASPConfig(mcASPPointCode)
	cfg.PathParams = &sctp.PathParams{}
	cfg.PathThresholds = &sctp.PathThresholds{}
	// Give every optional scalar a distinct allocated value, then mutate every
	// original field after snapshotting as a caller reusing configuration can.
	for _, v := range []any{cfg.PathParams, cfg.PathThresholds} {
		fields := reflect.ValueOf(v).Elem()
		for i := range fields.NumField() {
			f := fields.Field(i)
			p := reflect.New(f.Type().Elem())
			if p.Elem().Kind() == reflect.Bool {
				p.Elem().SetBool(true)
			} else {
				p.Elem().Set(reflect.ValueOf(1).Convert(p.Elem().Type()))
			}
			f.Set(p)
		}
	}
	snapshot := snapshotAssociationConfig(cfg)
	for i, v := range []any{cfg.PathParams, cfg.PathThresholds} {
		fields := reflect.ValueOf(v).Elem()
		copies := []any{snapshot.PathParams, snapshot.PathThresholds}
		for j := range fields.NumField() {
			original := fields.Field(j)
			saved := reflect.ValueOf(copies[i]).Elem().Field(j)
			if original.Pointer() == saved.Pointer() {
				t.Fatalf("field %s aliases caller", fields.Type().Field(j).Name)
			}
			original.Elem().SetZero()
			if saved.Elem().IsZero() {
				t.Fatalf("field %s changed in snapshot", fields.Type().Field(j).Name)
			}
		}
	}
}
