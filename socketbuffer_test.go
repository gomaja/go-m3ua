// Copyright 2018-2024 go-m3ua authors. All rights reserved.
// Use of this source code is governed by a MIT-style license that can be
// found in the LICENSE file.

package m3ua

import (
	"context"
	"errors"
	"math"
	"strconv"
	"testing"
)

// socketBufferConfig is an SGP configuration with the given socket sizes.
func socketBufferConfig(receive, send int) *AssociationConfig {
	config := mcSGPConfig()
	config.SocketReceiveBuffer = receive
	config.SocketSendBuffer = send
	return config
}

// The kernel takes SO_RCVBUF and SO_SNDBUF as a C int, and
// syscall.SetsockoptInt converts to int32 without a check, so a size beyond
// math.MaxInt32 would have set an unrelated one. Negative sizes have no meaning.
func TestSocketBufferSizesAreValidated(t *testing.T) {
	type sizes struct {
		name          string
		receive, send int
		wantErr       bool
	}
	tests := []sizes{
		{name: "unset", receive: 0, send: 0},
		{name: "set", receive: 1 << 20, send: 1 << 20},
		{name: "largest the option holds", receive: math.MaxInt32, send: math.MaxInt32},
		{name: "negative receive", receive: -1, wantErr: true},
		{name: "negative send", send: -1, wantErr: true},
	}
	if strconv.IntSize == 64 {
		// Not a constant expression, so the test still compiles where int
		// is 32 bits and no int can exceed the option.
		limit := int64(math.MaxInt32)
		tooLarge := int(limit + 1)
		tests = append(tests,
			sizes{name: "receive beyond a C int", receive: tooLarge, wantErr: true},
			sizes{name: "send beyond a C int", send: tooLarge, wantErr: true},
		)
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := validateAssociationConfigForRole(RoleSGP, socketBufferConfig(test.receive, test.send))
			if test.wantErr && !errors.Is(err, ErrInvalidSCTPConfig) {
				t.Fatalf("validation error = %v, want ErrInvalidSCTPConfig", err)
			}
			if !test.wantErr && err != nil {
				t.Fatalf("validation error = %v, want nil", err)
			}
		})
	}
}

// Dial refuses an invalid size before it opens a socket, so the error is the
// configuration's even where SCTP is unavailable.
func TestDialRefusesAnInvalidSocketBufferSize(t *testing.T) {
	config := mcASPConfig(1)
	config.SocketReceiveBuffer = -1
	association, err := dialASP(context.Background(), "m3ua", nil, mcAddr(2905, "127.0.0.1"), config)
	if association != nil {
		_ = association.Close()
	}
	if !errors.Is(err, ErrInvalidSCTPConfig) {
		t.Fatalf("Dial error = %v, want ErrInvalidSCTPConfig", err)
	}
}

// The default configuration sizes the listening socket even when a selector
// chooses every accepted association's configuration, so Listen validates its
// sizes either way.
func TestListenRefusesAnInvalidSocketBufferSize(t *testing.T) {
	for _, withSelector := range []bool{false, true} {
		t.Run("selector "+strconv.FormatBool(withSelector), func(t *testing.T) {
			listenerConfig := NewListenerConfig(socketBufferConfig(0, -1))
			if withSelector {
				listenerConfig.SelectAssociationConfig = func(AcceptInfo) (*AssociationConfig, error) {
					return mcSGPConfig(), nil
				}
			}
			listener, err := listenSGP("m3ua", mcAddr(0, "127.0.0.1"), listenerConfig)
			if listener != nil {
				_ = listener.Close()
			}
			if !errors.Is(err, ErrInvalidSCTPConfig) {
				t.Fatalf("Listen error = %v, want ErrInvalidSCTPConfig", err)
			}
		})
	}
}

// A selected configuration may leave the receive size unset or repeat the
// Listener's, because the INIT ACK announced the Listener's window before the
// selector ran. The send size is announced to no one and may differ.
func TestSelectedReceiveBufferMustBeTheListeners(t *testing.T) {
	const listenerReceive, listenerSend = 1 << 20, 256 << 10
	tests := []struct {
		name                          string
		listenerReceive               int
		selectedReceive, selectedSend int
		wantErr                       bool
	}{
		{name: "unset", listenerReceive: listenerReceive},
		{name: "the listener's", listenerReceive: listenerReceive, selectedReceive: listenerReceive},
		{name: "a different send size", listenerReceive: listenerReceive, selectedSend: 2 * listenerSend},
		{name: "larger", listenerReceive: listenerReceive, selectedReceive: 2 * listenerReceive, wantErr: true},
		{name: "smaller", listenerReceive: listenerReceive, selectedReceive: listenerReceive / 2, wantErr: true},
		{name: "set where the listener kept the default", selectedReceive: listenerReceive, wantErr: true},
		{name: "negative send", listenerReceive: listenerReceive, selectedSend: -1, wantErr: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			listenerConfig := NewListenerConfig(socketBufferConfig(test.listenerReceive, listenerSend))
			listenerConfig.SelectAssociationConfig = func(AcceptInfo) (*AssociationConfig, error) {
				return socketBufferConfig(test.selectedReceive, test.selectedSend), nil
			}
			listener := newSGPListener(listenerConfig)
			selected, err := listener.resolveAcceptedAssociationConfig(RoleSGP, AcceptInfo{})
			if test.wantErr {
				if !errors.Is(err, ErrInvalidSCTPConfig) {
					t.Fatalf("resolve error = %v, want ErrInvalidSCTPConfig", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("resolve error = %v, want nil", err)
			}
			if selected.SocketSendBuffer != test.selectedSend {
				t.Fatalf("selected SocketSendBuffer = %d, want %d", selected.SocketSendBuffer, test.selectedSend)
			}
		})
	}
}

// An unset configuration installs no Control hook at all, so socket
// construction is exactly what it was before the sizes existed.
func TestSocketBufferControlOnlyWhenRequested(t *testing.T) {
	if socketBuffersFor(&SCTPConfig{}).control() != nil {
		t.Fatal("an unset configuration installed a Control hook")
	}
	if socketBuffersFor(nil).control() != nil {
		t.Fatal("a nil configuration installed a Control hook")
	}
	if socketBuffersFor(&SCTPConfig{SocketReceiveBuffer: 1}).control() == nil {
		t.Fatal("a receive size installed no Control hook")
	}
	if socketBuffersFor(&SCTPConfig{SocketSendBuffer: 1}).control() == nil {
		t.Fatal("a send size installed no Control hook")
	}
}
