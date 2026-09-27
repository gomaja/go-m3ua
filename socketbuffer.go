// Copyright 2018-2024 go-m3ua authors. All rights reserved.
// Use of this source code is governed by a MIT-style license that can be
// found in the LICENSE file.

package m3ua

import (
	"fmt"
	"math"

	"github.com/gomaja/go-sctp"
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

// apply sizes the socket before bind, connect or listen. Linux announces the
// receive window derived from this buffer in INIT or INIT ACK (RFC 9260
// Sections 3.3.2 and 3.3.3). Accepted sockets inherit their listener's sizes.
func (b socketBuffers) apply(config *sctp.Config) {
	if b.receive != 0 {
		config.ReadBuffer = &b.receive
	}
	if b.send != 0 {
		config.WriteBuffer = &b.send
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
