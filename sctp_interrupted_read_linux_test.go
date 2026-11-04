// Copyright 2018-2024 go-m3ua authors. All rights reserved.
// Use of this source code is governed by a MIT-style license that can be
// found in the LICENSE file.

//go:build linux

package m3ua

import (
	"bytes"
	"context"
	"errors"
	"net/netip"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/gomaja/go-m3ua/messages"
	"github.com/gomaja/go-m3ua/messages/params"
	"github.com/gomaja/go-sctp"
)

// These two-host tests need isolated network namespaces. The partial-delivery
// case drops selected DATA at the receiver after the M3UA handshake; loopback
// cannot substitute for the two-host network. The harness supplies a shared
// directory for barriers and installs the receiver's loss rule before "loss".
func twoHostSCTP(t *testing.T) (context.Context, string, *sctp.Addr, *sctp.Addr, string) {
	t.Helper()
	role := os.Getenv("M3UA_TWOHOST_ROLE")
	if role == "" {
		t.Skip("requires two-host SCTP harness")
	}
	if role != "server" && role != "client" {
		t.Fatalf("invalid M3UA_TWOHOST_ROLE: %q", role)
	}
	local, err := netip.ParseAddr(os.Getenv("M3UA_TWOHOST_LOCAL"))
	if err != nil {
		t.Fatal(err)
	}
	remote, err := netip.ParseAddr(os.Getenv("M3UA_TWOHOST_REMOTE"))
	if err != nil {
		t.Fatal(err)
	}
	dir := os.Getenv("M3UA_TWOHOST_SYNC")
	if dir == "" {
		t.Fatal("M3UA_TWOHOST_SYNC is required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	t.Cleanup(cancel)
	return ctx, role, &sctp.Addr{IPs: []netip.Addr{local}, Port: 41030}, &sctp.Addr{IPs: []netip.Addr{remote}, Port: 41030}, dir
}

func twoHostMark(t *testing.T, dir, name string) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(dir, name), []byte(name+"\n"), 0644); err != nil {
		t.Fatal(err)
	}
}

func twoHostWait(t *testing.T, ctx context.Context, dir, name string) {
	t.Helper()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		if _, err := os.Stat(filepath.Join(dir, name)); err == nil {
			return
		} else if !errors.Is(err, os.ErrNotExist) {
			t.Fatal(err)
		}
		select {
		case <-ctx.Done():
			t.Fatalf("waiting for %s: %v", name, ctx.Err())
		case <-ticker.C:
		}
	}
}

func TestSCTPInterruptedReadEndsAssociation(t *testing.T) {
	ctx, role, local, remote, dir := twoHostSCTP(t)
	if role == "client" {
		interruptedReadPeer(t, ctx, local, remote, dir)
		return
	}
	endpoint, err := NewEndpoint(EndpointConfig{Role: RoleSGP})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = endpoint.Close() })
	cfg := mcSGPConfig()
	cfg.ReadBufferSize = 1 << 20
	cfg.SocketReceiveBuffer = 16384
	cfg.Subscribe = func(a *Association) {
		if err := a.sctpConn.SetReadBuffer(16384); err != nil {
			t.Error(err)
		}
		if err := a.sctpConn.SetPartialDeliveryPoint(4096); err != nil {
			t.Error(err)
		}
	}
	listener, err := endpoint.Listen("m3ua", local, NewListenerConfig(cfg))
	if err != nil {
		t.Fatal(err)
	}
	twoHostMark(t, dir, "listening")
	a, err := listener.Accept(ctx)
	if err != nil {
		t.Fatal(err)
	}
	management := a.ManagementIndications()
	first, err := a.ReadData(ctx)
	if err != nil || !bytes.Equal(first.ProtocolData.Data, []byte("before interruption")) {
		t.Fatalf("initial DATA = %v, %v", first, err)
	}
	twoHostMark(t, dir, "ready-for-loss")
	twoHostWait(t, ctx, dir, "loss")
	twoHostMark(t, dir, "reading")
	type readResult struct {
		message *DataMessage
		err     error
	}
	read := make(chan readResult, 1)
	go func() {
		message, err := a.ReadData(ctx)
		read <- readResult{message, err}
	}()
	select {
	case <-a.Done():
	case <-ctx.Done():
		t.Fatalf("interrupted read did not end association: %v", ctx.Err())
	}
	if err := a.Err(); !errors.Is(err, sctp.ErrMessageInterrupted) {
		t.Errorf("association cause = %v; want ErrMessageInterrupted", err)
	}
	select {
	case got := <-read:
		if got.message != nil || !errors.Is(got.err, ErrNotEstablished) {
			t.Errorf("DATA after interrupted message = %v, %v", got.message, got.err)
		}
	case <-ctx.Done():
		t.Fatal("application read did not wake")
	}
	foundRelease := false
	for {
		select {
		case indication, ok := <-management:
			if !ok {
				if !foundRelease {
					t.Error("missing SCTP release indication")
				}
				twoHostMark(t, dir, "received")
				return
			}
			if indication.Kind == ManagementSCTPRelease {
				foundRelease = true
				if !errors.Is(indication.Cause, sctp.ErrMessageInterrupted) {
					t.Errorf("release cause = %v; want ErrMessageInterrupted", indication.Cause)
				}
			}
		case <-ctx.Done():
			t.Fatal("release indications did not close")
		}
	}
}

func interruptedReadPeer(t *testing.T, ctx context.Context, local, remote *sctp.Addr, dir string) {
	t.Helper()
	twoHostWait(t, ctx, dir, "listening")
	enabled := true
	conn, err := (&sctp.Config{
		PartialReliability: &enabled,
		InitMsg:            sctp.InitMsg{OutStreams: 16, MaxInStreams: 16},
		WriteBuffer:        new(4 << 20),
		RTOInfo:            &sctp.RTOInfo{Initial: time.Second, Min: 500 * time.Millisecond, Max: 2 * time.Second},
	}).Dial(ctx, "sctp", local, remote)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	send := func(message messages.M3UA, stream uint16, pr *sctp.PrInfo) {
		t.Helper()
		b, err := message.MarshalBinary()
		if err != nil {
			t.Fatal(err)
		}
		if _, err := conn.SendMsg(b, sctp.SendOptions{Info: &sctp.SndInfo{Stream: stream, PPID: M3UAPPID}, PR: pr}); err != nil {
			t.Fatal(err)
		}
	}
	await := func(class, kind uint8) {
		t.Helper()
		if err := conn.SetReadDeadline(time.Now().Add(10 * time.Second)); err != nil {
			t.Fatal(err)
		}
		for {
			b, _, err := conn.ReadMsg(1 << 20)
			if err != nil {
				t.Fatal(err)
			}
			m, err := messages.Parse(b)
			if err == nil && m.MessageClass() == class && m.MessageType() == kind {
				return
			}
		}
	}
	send(messages.NewAspUp(nil, nil), 0, nil)
	await(messages.MsgClassASPSM, messages.MsgTypeAspUpAck)
	send(messages.NewAspActive(params.NewTrafficModeType(params.TrafficModeLoadshare), params.NewRoutingContext(1), nil), 0, nil)
	await(messages.MsgClassASPTM, messages.MsgTypeAspActiveAck)
	data := func(payload []byte) messages.M3UA {
		return messages.NewData(params.NewNetworkAppearance(0), params.NewRoutingContext(1), params.NewProtocolData(mcASPPointCode, mcSGPPointCode, params.ServiceIndSCCP, 0, 0, 1, payload), nil)
	}
	send(data([]byte("before interruption")), 1, nil)
	twoHostWait(t, ctx, dir, "reading")
	send(data(bytes.Repeat([]byte{'A'}, 60<<10)), 1, &sctp.PrInfo{Policy: sctp.PRRtx, Value: 0})
	time.Sleep(time.Second)
	// A later reliable message advances the sender beyond the abandoned TSNs.
	// The receiver must close, rather than deliver either message to M3UA.
	_, err = conn.SendMsg([]byte("following message"), sctp.SendOptions{Info: &sctp.SndInfo{Stream: 1, PPID: M3UAPPID}})
	if err != nil {
		t.Logf("following send after receiver teardown: %v", err)
	}
	twoHostWait(t, ctx, dir, "received")
}
