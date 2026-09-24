// Copyright 2018-2024 go-m3ua authors. All rights reserved.
// Use of this source code is governed by a MIT-style license that can be
// found in the LICENSE file.

//go:build linux

// Package sctpevents asks the kernel whether SCTP sockets can subscribe to
// association events. The library and its test fixtures ask the same question
// the same way, without either exporting it.
package sctpevents

import (
	"syscall"
	"unsafe"

	"github.com/gomaja/go-sctp"
)

// Probe sets the SCTP_EVENT subscription to SCTP_ASSOC_CHANGE (RFC 6458
// Section 6.2.2) on an SCTP socket that is never bound or connected, so asking
// puts nothing on the wire. The option carries the dependency's sctp.Event,
// the layout it applies itself. Linux added SCTP_EVENT in 5.0 and refuses it
// with ENOPROTOOPT before that; the answer is the kernel's, not the address
// family's, so the socket is IPv4 unless the kernel cannot open one, and IPv6
// then.
func Probe() error {
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
