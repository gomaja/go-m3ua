// Copyright 2018-2024 go-m3ua authors. All rights reserved.
// Use of this source code is governed by a MIT-style license that can be
// found in the LICENSE file.

package m3ua

import (
	"fmt"
	"math"
	"syscall"
)

// socketBuffers is the SO_RCVBUF and SO_SNDBUF request of one SCTPConfig, in
// bytes. Zero leaves that buffer at the size the socket starts with.
type socketBuffers struct {
	receive int
	send    int
}

func socketBuffersFor(config *SCTPConfig) socketBuffers {
	if config == nil {
		return socketBuffers{}
	}
	return socketBuffers{receive: config.SocketReceiveBuffer, send: config.SocketSendBuffer}
}

// control returns the SocketConfig.Control hook that applies b, or nil when b
// requests nothing, so an unset configuration leaves socket construction
// exactly as it was.
//
// The hook runs after the socket is created and before it binds, connects or
// listens, and that order is the point. Linux sets an association's receive
// window from the socket's buffer when the association is created, and the INIT
// or INIT ACK announces it (RFC 9260 Sections 3.3.2 and 3.3.3), so a size
// applied to the connected or accepted socket changes the buffer and not the
// window the peer was given. An accepted socket inherits the listening socket's
// sizes, so applying them before listen covers every association a Listener
// accepts.
func (b socketBuffers) control() func(network, address string, raw syscall.RawConn) error {
	if b == (socketBuffers{}) {
		return nil
	}
	return func(_, _ string, raw syscall.RawConn) error {
		var setErr error
		if err := raw.Control(func(fd uintptr) { setErr = setSocketBuffers(fd, b) }); err != nil {
			return err
		}
		return setErr
	}
}

// validateSCTPConfig refuses socket buffer sizes the kernel cannot be asked
// for.
//
// setsockopt takes SO_RCVBUF and SO_SNDBUF as a C int, and
// syscall.SetsockoptInt converts its argument to int32 unchecked, so a larger
// size would silently set an unrelated one.
func validateSCTPConfig(config *SCTPConfig) error {
	if config == nil {
		return nil
	}
	if err := validateSocketBufferSize("SocketReceiveBuffer", config.SocketReceiveBuffer); err != nil {
		return err
	}
	return validateSocketBufferSize("SocketSendBuffer", config.SocketSendBuffer)
}

func validateSocketBufferSize(name string, size int) error {
	if size < 0 {
		return fmt.Errorf("%w: negative %s %d", ErrInvalidSCTPConfig, name, size)
	}
	if int64(size) > math.MaxInt32 {
		return fmt.Errorf("%w: %s %d exceeds the socket option's %d", ErrInvalidSCTPConfig, name, size, math.MaxInt32)
	}
	return nil
}
