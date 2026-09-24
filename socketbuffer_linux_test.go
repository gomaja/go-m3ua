// Copyright 2018-2024 go-m3ua authors. All rights reserved.
// Use of this source code is governed by a MIT-style license that can be
// found in the LICENSE file.

package m3ua

import (
	"context"
	"errors"
	"os"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/gomaja/go-sctp"
)

// These tests read the sizes back from the kernel and the window from the far
// end of the association, because a size that is set but applied too late is
// still reported by getsockopt: only the peer's view shows whether the INIT or
// INIT ACK announced it.

// socketBufferKernel is what the running kernel does with an unset or a
// requested socket buffer size. The sysctls differ between hosts, and a
// campaign may have raised net.core.rmem_default above rmem_max, so the tests
// derive every expectation from these rather than from the stock defaults.
type socketBufferKernel struct {
	rmemMax, wmemMax int
	// defaultReceive and defaultSend are what a new SCTP socket starts with.
	defaultReceive, defaultSend int
}

func readSocketBufferKernel(t *testing.T) socketBufferKernel {
	t.Helper()
	kernel := socketBufferKernel{
		rmemMax: readCoreSysctl(t, "rmem_max"),
		wmemMax: readCoreSysctl(t, "wmem_max"),
	}
	fd, err := syscall.Socket(syscall.AF_INET, syscall.SOCK_STREAM, syscall.IPPROTO_SCTP)
	if err != nil {
		t.Skipf("skipping socket-backed test: %v", err)
	}
	defer func() { _ = syscall.Close(fd) }()
	if kernel.defaultReceive, err = syscall.GetsockoptInt(fd, syscall.SOL_SOCKET, syscall.SO_RCVBUF); err != nil {
		t.Fatal(err)
	}
	if kernel.defaultSend, err = syscall.GetsockoptInt(fd, syscall.SOL_SOCKET, syscall.SO_SNDBUF); err != nil {
		t.Fatal(err)
	}
	t.Logf("net.core.rmem_max=%d wmem_max=%d; a new SCTP socket starts at SO_RCVBUF=%d SO_SNDBUF=%d",
		kernel.rmemMax, kernel.wmemMax, kernel.defaultReceive, kernel.defaultSend)
	// An eighth of either cap must stay clear of the kernel's minimum buffer
	// and far above handshakeWindowSlack for the requests below to be told
	// apart.
	if kernel.rmemMax < 1<<16 || kernel.wmemMax < 1<<16 {
		t.Skip("skipping: net.core.rmem_max or wmem_max is below 64 KiB")
	}
	return kernel
}

func readCoreSysctl(t *testing.T, name string) int {
	t.Helper()
	b, err := os.ReadFile("/proc/sys/net/core/" + name)
	if err != nil {
		t.Skipf("skipping: cannot read net.core.%s: %v", name, err)
	}
	value, err := strconv.Atoi(strings.TrimSpace(string(b)))
	if err != nil {
		t.Fatalf("net.core.%s: %v", name, err)
	}
	return value
}

// receive and send are the sizes getsockopt reports for a request: unchanged
// for zero, otherwise capped at the sysctl and doubled, as socket(7) describes.
// A test that got twice the request above the cap would be looking at
// SO_RCVBUFFORCE, which the library must not use.
func (k socketBufferKernel) receive(requested int) int {
	if requested == 0 {
		return k.defaultReceive
	}
	return 2 * min(requested, k.rmemMax)
}

func (k socketBufferKernel) send(requested int) int {
	if requested == 0 {
		return k.defaultSend
	}
	return 2 * min(requested, k.wmemMax)
}

// socketBufferSizes is one side's request, in SCTPConfig terms.
type socketBufferSizes struct {
	name          string
	receive, send int
}

// socketBufferRequests covers an unset association, a receive-only request, two
// receive sizes a factor of two apart so the window visibly follows the
// request, and requests above the caps. The sizes stay below the caps and far
// apart relative to handshakeWindowSlack on any host.
func (k socketBufferKernel) socketBufferRequests() []socketBufferSizes {
	return []socketBufferSizes{
		{name: "unset"},
		{name: "receive only", receive: k.rmemMax / 8},
		{name: "both", receive: k.rmemMax / 4, send: k.wmemMax / 8},
		{name: "above the caps", receive: 2 * k.rmemMax, send: 2 * k.wmemMax},
	}
}

// handshakeWindowSlack bounds how far below the announced window the peer's
// view may sit once the M3UA handshake is over. The handshake's own messages
// are each well under 100 octets; a SACK sent while they were still queued
// reports the window less those octets, and Linux sends no window update for so
// small an increase. It is far smaller than the gap between any two requests.
const handshakeWindowSlack = 4096

// requireWindow checks the peer's view of the receive window against the
// window announced at setup, which Linux makes half the receiving socket's
// buffer.
func requireWindow(t *testing.T, observer *Association, receiverBuffer int) {
	t.Helper()
	status, err := observer.AssociationStatus()
	if err != nil {
		t.Fatal(err)
	}
	announced := uint32(receiverBuffer / 2)
	t.Logf("peer sees a receive window of %d octets; the socket buffer of %d announces %d",
		status.ReceiverWindow, receiverBuffer, announced)
	if status.ReceiverWindow > announced || status.ReceiverWindow+handshakeWindowSlack < announced {
		t.Fatalf("peer sees a receive window of %d octets; want the %d announced at setup, less at most %d",
			status.ReceiverWindow, announced, handshakeWindowSlack)
	}
}

func requireSocketBuffers(t *testing.T, association *Association, wantReceive, wantSend int) {
	t.Helper()
	receive, err := association.SocketReceiveBuffer()
	if err != nil {
		t.Fatal(err)
	}
	send, err := association.SocketSendBuffer()
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("SO_RCVBUF=%d SO_SNDBUF=%d", receive, send)
	if receive != wantReceive || send != wantSend {
		t.Fatalf("SO_RCVBUF=%d SO_SNDBUF=%d; want %d and %d", receive, send, wantReceive, wantSend)
	}
}

// socketBufferPeers dials one ASP into a fresh SGP listener and returns both
// ends once each has reached ASP-ACTIVE.
func socketBufferPeers(t *testing.T, listenerConfig *ListenerConfig, aspConfig *AssociationConfig) (asp, sgp *Association) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)

	ln, err := listenSGP("m3ua", mcAddr(0, "127.0.0.1"), listenerConfig)
	if err != nil {
		skipIfSCTPUnsupported(t, err)
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ln.Close() })

	type acceptResult struct {
		association *Association
		err         error
	}
	accepted := make(chan acceptResult, 1)
	go func() {
		association, err := ln.Accept(ctx)
		accepted <- acceptResult{association, err}
	}()
	asp, err = dialASP(ctx, "m3ua", mcAddr(0, "127.0.0.2"), ln.Addr().(*sctp.SCTPAddr), aspConfig)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	t.Cleanup(func() { _ = asp.Close() })
	select {
	case result := <-accepted:
		if result.err != nil {
			t.Fatalf("Accept: %v", result.err)
		}
		t.Cleanup(func() { _ = result.association.Close() })
		return asp, result.association
	case <-time.After(15 * time.Second):
		t.Fatal("Accept never returned")
		return nil, nil
	}
}

// A dialled association applies its sizes before connecting, so its INIT
// announces the requested receive window, and an unset one keeps the kernel's
// defaults for both buffers.
func TestDialSizesTheSocketBuffersBeforeConnecting(t *testing.T) {
	kernel := readSocketBufferKernel(t)
	for _, request := range kernel.socketBufferRequests() {
		t.Run(request.name, func(t *testing.T) {
			aspConfig := mcASPConfig(0xEE000001)
			aspConfig.SocketReceiveBuffer = request.receive
			aspConfig.SocketSendBuffer = request.send
			asp, sgp := socketBufferPeers(t, NewListenerConfig(mcSGPConfig()), aspConfig)

			requireSocketBuffers(t, asp, kernel.receive(request.receive), kernel.send(request.send))
			requireWindow(t, sgp, kernel.receive(request.receive))
		})
	}
}

// A Listener applies its default configuration's sizes to the listening socket
// before listen, so every accepted association inherits them and the INIT ACK
// announces the requested receive window.
func TestListenSizesTheSocketBuffersAcceptedAssociationsInherit(t *testing.T) {
	kernel := readSocketBufferKernel(t)
	for _, request := range kernel.socketBufferRequests() {
		t.Run(request.name, func(t *testing.T) {
			sgpConfig := mcSGPConfig()
			sgpConfig.SocketReceiveBuffer = request.receive
			sgpConfig.SocketSendBuffer = request.send
			asp, sgp := socketBufferPeers(t, NewListenerConfig(sgpConfig), mcASPConfig(0xEE000002))

			requireSocketBuffers(t, sgp, kernel.receive(request.receive), kernel.send(request.send))
			requireWindow(t, asp, kernel.receive(request.receive))
		})
	}
}

// SelectAssociationConfig runs after the INIT ACK has announced the listening
// socket's window. A selected receive size can therefore only be the Listener's
// own or unset; anything else refuses that one peer and leaves the Listener
// serving. A selected send size is announced to no one and applies exactly.
func TestSelectedSocketBuffersAfterAccept(t *testing.T) {
	kernel := readSocketBufferKernel(t)
	listenerReceive, listenerSend := kernel.rmemMax/8, kernel.wmemMax/8
	selectedSend := kernel.wmemMax / 4

	selected := func(receive, send int) *AssociationConfig {
		config := mcSGPConfig()
		config.SocketReceiveBuffer = receive
		config.SocketSendBuffer = send
		return config
	}
	listenerConfig := NewListenerConfig(selected(listenerReceive, listenerSend))
	listenerConfig.SelectAssociationConfig = func(info AcceptInfo) (*AssociationConfig, error) {
		switch {
		case acceptInfoHasRemoteIP(info, "127.0.0.2"):
			return selected(2*listenerReceive, 0), nil
		case acceptInfoHasRemoteIP(info, "127.0.0.3"):
			return selected(0, 0), nil
		case acceptInfoHasRemoteIP(info, "127.0.0.4"):
			return selected(listenerReceive, selectedSend), nil
		default:
			return nil, errors.New("unexpected peer")
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	ln, err := listenSGP("m3ua", mcAddr(0, "127.0.0.1"), listenerConfig)
	if err != nil {
		skipIfSCTPUnsupported(t, err)
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	listenerAddr := ln.Addr().(*sctp.SCTPAddr)

	// connection is what Dial and Accept made of one ASP.
	type connection struct {
		asp, sgp           *Association
		dialErr, acceptErr error
	}
	// connect dials one ASP from ip. Each ASP has its own identifier, since the
	// accepted ones are active at the same time.
	connect := func(t *testing.T, ip string, aspIdentifier uint32) connection {
		t.Helper()
		type acceptResult struct {
			association *Association
			err         error
		}
		accepted := make(chan acceptResult, 1)
		go func() {
			association, err := ln.Accept(ctx)
			accepted <- acceptResult{association, err}
		}()
		aspConfig := mcASPConfig(aspIdentifier)
		aspConfig.EstablishTimeout = 5 * time.Second
		asp, dialErr := dialASP(ctx, "m3ua", mcAddr(0, ip), listenerAddr, aspConfig)
		if asp != nil {
			t.Cleanup(func() { _ = asp.Close() })
		}
		select {
		case result := <-accepted:
			if result.association != nil {
				t.Cleanup(func() { _ = result.association.Close() })
			}
			return connection{asp: asp, sgp: result.association, dialErr: dialErr, acceptErr: result.err}
		case <-time.After(15 * time.Second):
			t.Fatalf("Accept for %s never returned", ip)
			return connection{}
		}
	}
	accept := func(t *testing.T, ip string, aspIdentifier uint32) connection {
		t.Helper()
		c := connect(t, ip, aspIdentifier)
		if c.acceptErr != nil {
			t.Fatalf("Accept: %v", c.acceptErr)
		}
		if c.dialErr != nil {
			t.Fatalf("Dial: %v", c.dialErr)
		}
		return c
	}

	t.Run("a different receive size is refused", func(t *testing.T) {
		c := connect(t, "127.0.0.2", 0xEE000010)
		if c.asp != nil {
			t.Fatal("the ASP established an association the selected configuration should have refused")
		}
		if !errors.Is(c.acceptErr, ErrInvalidSCTPConfig) {
			t.Fatalf("Accept error = %v, want ErrInvalidSCTPConfig", c.acceptErr)
		}
		var establishment *AssociationEstablishmentError
		if !errors.As(c.acceptErr, &establishment) {
			t.Fatalf("Accept error = %T, want *AssociationEstablishmentError so the accept loop carries on", c.acceptErr)
		}
	})
	t.Run("unset sizes inherit the listener's", func(t *testing.T) {
		c := accept(t, "127.0.0.3", 0xEE000011)
		requireSocketBuffers(t, c.sgp, kernel.receive(listenerReceive), kernel.send(listenerSend))
		requireWindow(t, c.asp, kernel.receive(listenerReceive))
	})
	t.Run("the listener's receive size and a different send size", func(t *testing.T) {
		c := accept(t, "127.0.0.4", 0xEE000012)
		requireSocketBuffers(t, c.sgp, kernel.receive(listenerReceive), kernel.send(selectedSend))
		requireWindow(t, c.asp, kernel.receive(listenerReceive))
	})
}
