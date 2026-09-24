// Copyright 2018-2024 go-m3ua authors. All rights reserved.
// Use of this source code is governed by a MIT-style license that can be
// found in the LICENSE file.

//go:build linux

package m3ua

import (
	"fmt"
	"syscall"
)

// setSocketBuffers applies b to a descriptor lent to a SocketConfig.Control
// hook. SO_RCVBUF and SO_SNDBUF are socket-level options, set at SOL_SOCKET
// (RFC 6458 Sections 8.1.6 and 8.1.7).
//
// SO_RCVBUFFORCE and SO_SNDBUFFORCE are deliberately not used. They exceed
// net.core.rmem_max and wmem_max for a process holding CAP_NET_ADMIN, which
// would make the size an association gets depend on the process's privileges
// rather than on the host's configured ceiling.
func setSocketBuffers(fd uintptr, b socketBuffers) error {
	if b.receive > 0 {
		if err := syscall.SetsockoptInt(int(fd), syscall.SOL_SOCKET, syscall.SO_RCVBUF, b.receive); err != nil {
			return fmt.Errorf("failed to set SO_RCVBUF: %w", err)
		}
	}
	if b.send > 0 {
		if err := syscall.SetsockoptInt(int(fd), syscall.SOL_SOCKET, syscall.SO_SNDBUF, b.send); err != nil {
			return fmt.Errorf("failed to set SO_SNDBUF: %w", err)
		}
	}
	return nil
}
