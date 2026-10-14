//go:build linux && 386

package main

import "errors"

// delayedSACK is unavailable on linux/386, where socket options go through
// socketcall and the syscall package has no raw getsockopt for a structure.
// The fixture's reference platforms are 64-bit; the manifest records the
// socket options as unreadable here.
func delayedSACK(int) (delay, frequency uint32, err error) {
	return 0, 0, errors.New("SCTP_DELAYED_SACK is not read on linux/386")
}
