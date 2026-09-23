//go:build linux && !386

package main

import (
	"syscall"
	"unsafe"
)

// delayedSACK reads SCTP_DELAYED_SACK from one of this process's own SCTP
// sockets: struct sctp_sack_info { sctp_assoc_t sack_assoc_id; uint32_t
// sack_delay; uint32_t sack_freq; }. Association 0 on a one-to-one socket is
// its own association.
func delayedSACK(fd int) (delay, frequency uint32, err error) {
	sack := new([3]uint32)
	length := uint32(unsafe.Sizeof(*sack))
	_, _, errno := syscall.Syscall6(syscall.SYS_GETSOCKOPT, uintptr(fd), syscall.IPPROTO_SCTP, sctpDelayedSACKOption,
		uintptr(unsafe.Pointer(sack)), uintptr(unsafe.Pointer(&length)), 0)
	if errno != 0 {
		return 0, 0, errno
	}
	return sack[1], sack[2], nil
}
