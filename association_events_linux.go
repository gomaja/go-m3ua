// Copyright 2018-2024 go-m3ua authors. All rights reserved.
// Use of this source code is governed by a MIT-style license that can be
// found in the LICENSE file.

//go:build linux

package m3ua

import (
	"syscall"
	"unsafe"

	"github.com/gomaja/go-sctp"
)

// kernelAssociationEvents sets the SCTP_EVENT subscription associationEvents
// makes (RFC 6458 Section 6.2.2) on an SCTP socket that is never bound or
// connected, so asking puts nothing on the wire. The option carries the
// dependency's sctp.Event, the layout it applies itself.
func kernelAssociationEvents() error {
	fd, err := syscall.Socket(syscall.AF_INET, syscall.SOCK_STREAM, syscall.IPPROTO_SCTP)
	if err != nil {
		fd, err = syscall.Socket(syscall.AF_INET6, syscall.SOCK_STREAM, syscall.IPPROTO_SCTP)
	}
	if err != nil {
		return err
	}
	defer func() { _ = syscall.Close(fd) }()
	event := sctp.Event{AssocID: sctp.SCTP_FUTURE_ASSOC, Type: uint16(sctp.SCTP_ASSOC_CHANGE), On: 1}
	option := unsafe.Slice((*byte)(unsafe.Pointer(&event)), unsafe.Sizeof(event))
	return syscall.SetsockoptString(fd, syscall.IPPROTO_SCTP, sctp.SCTP_EVENT, string(option))
}
