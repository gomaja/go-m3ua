// Copyright 2018-2024 go-m3ua authors. All rights reserved.
// Use of this source code is governed by a MIT-style license that can be
// found in the LICENSE file.

package m3ua

import (
	"context"
	"net"
	"net/netip"
	"syscall"
	"testing"
	"time"

	"github.com/gomaja/go-sctp"
)

func TestSCTPPathEventFollowsEarlierRestart(t *testing.T) {
	c := newIndicationAssociation(t)
	c.assocID.Store(7)
	management := c.ManagementIndications()
	// One buffered slot lets the synthetic reader finish handing over the
	// later event while the real dispatcher waits for the restart's entry
	// action. A live association's unbuffered queue also orders these items.
	c.inboundChan = make(chan inbound, 1)
	w := &restartWatcher{}
	w.setRoute(c.associationForSCTPID)

	ctx, cancel := context.WithCancel(context.Background())
	finished := make(chan struct{})
	go func() {
		defer close(finished)
		c.dispatchLoop(ctx, c.inboundChan)
	}()
	t.Cleanup(func() {
		cancel()
		_ = c.Close()
		select {
		case <-finished:
		case <-time.After(3 * time.Second):
			t.Error("dispatcher did not stop")
		}
	})

	if err := w.handle(assocChangeEvent(sctp.AssocRestart, 7)); err != nil {
		t.Fatal(err)
	}
	var update restartStateUpdate
	select {
	case update = <-c.restartStateChan:
	case <-time.After(3 * time.Second):
		t.Fatal("dispatcher did not enter the earlier restart")
	}
	t.Cleanup(func() {
		select {
		case <-update.applied:
		default:
			close(update.applied)
		}
	})

	change := &sctp.PeerAddrChange{
		Addr:  netip.MustParseAddrPort("127.0.0.2:2905"),
		State: sctp.AddrUnreachable, AssocID: 7,
	}
	if err := w.handle(change); err != nil {
		t.Fatalf("peer-address notification ended the read: %v", err)
	}
	select {
	case ind := <-management:
		t.Fatalf("%v overtook the earlier restart while its dispatcher was held", ind.Kind)
	default:
	}

	close(update.applied)
	for _, want := range []ManagementIndicationKind{ManagementSCTPRestart, ManagementSCTPStatus} {
		select {
		case ind, open := <-management:
			if !open || ind.Kind != want {
				t.Fatalf("indication = %+v (open %v), want %v in reader order", ind, open, want)
			}
			if want == ManagementSCTPStatus && (ind.PeerAddrChange == nil || *ind.PeerAddrChange != *change) {
				t.Fatalf("path indication lost its notification: %+v", ind)
			}
		case <-time.After(3 * time.Second):
			t.Fatalf("missing %v indication", want)
		}
	}
}

func TestSCTPPathEventWithoutInboundQueueIsIgnored(t *testing.T) {
	c := newIndicationAssociation(t)
	c.inboundChan = nil
	management := c.ManagementIndications()
	w := &restartWatcher{}
	w.setRoute(func(sctp.AssocID) *Association { return c })
	if err := w.handle(&sctp.PeerAddrChange{}); err != nil {
		t.Fatal(err)
	}
	if len(management) != 0 {
		t.Fatal("notification was delivered without an ordered dispatch queue")
	}
}

func TestSCTPPathInfoAllPeersDisappeared(t *testing.T) {
	peers := mcAddr(2905, "127.0.0.1", "127.0.0.2")
	want := &net.OpError{Op: "get", Err: syscall.EINVAL}
	calls := 0
	paths, err := peerPathInfos(peers, func(netip.Addr) (*sctp.PathInfo, error) {
		calls++
		return nil, want
	})
	if calls != len(peers.IPs) || paths != nil || err != want {
		t.Fatalf("paths=%+v error=%v calls=%d; want original EINVAL after every peer disappeared", paths, err, calls)
	}
}
