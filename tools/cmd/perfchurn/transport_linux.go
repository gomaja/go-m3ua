//go:build linux

package main

import (
	"runtime"
	"syscall"
	"unsafe"
)

const (
	interfaceRoot = "/sys/class/net"

	// sctpNoDelayOption and sctpDelayedSACKOption are SCTP_NODELAY and
	// SCTP_DELAYED_SACK of <linux/sctp.h>.
	sctpNoDelayOption     = 3
	sctpDelayedSACKOption = 16

	// siocEthtool is SIOCETHTOOL of <linux/sockios.h>.
	siocEthtool = 0x8946
)

// platformSocketOptions reads the options the fixture configures, and the
// buffer sizes they depend on, from one of this process's own SCTP sockets.
// It only reads.
func platformSocketOptions(fd int) (socketOptions, error) {
	var options socketOptions
	var err error
	if options.SendBuffer, err = syscall.GetsockoptInt(fd, syscall.SOL_SOCKET, syscall.SO_SNDBUF); err != nil {
		return options, err
	}
	if options.ReceiveBuffer, err = syscall.GetsockoptInt(fd, syscall.SOL_SOCKET, syscall.SO_RCVBUF); err != nil {
		return options, err
	}
	if options.NoDelay, err = syscall.GetsockoptInt(fd, syscall.IPPROTO_SCTP, sctpNoDelayOption); err != nil {
		return options, err
	}
	if options.SACKDelay, options.SACKFrequency, err = delayedSACK(fd); err != nil {
		return options, err
	}
	return options, nil
}

// legacyOffloads are the ethtool get commands that answer one flag each
// (<linux/ethtool.h>): what `ethtool -k` shows for these features, without
// needing ethtool in the image.
var legacyOffloads = []struct {
	name    string
	command uint32
}{
	{"rx-checksumming", 0x14},
	{"tx-checksumming", 0x16},
	{"scatter-gather", 0x18},
	{"tcp-segmentation-offload", 0x1e},
	{"generic-segmentation-offload", 0x23},
	{"generic-receive-offload", 0x2b},
}

func ethtoolOffloads(name string) (map[string]bool, map[string]string) {
	offloads, failures := map[string]bool{}, map[string]string{}
	fd, err := syscall.Socket(syscall.AF_INET, syscall.SOCK_DGRAM, 0)
	if err != nil {
		failures["socket"] = err.Error()
		return offloads, failures
	}
	defer func() { _ = syscall.Close(fd) }()
	for _, offload := range legacyOffloads {
		// struct ethtool_value { __u32 cmd; __u32 data; }, pointed to by the
		// data member of a struct ifreq (16-byte name, 24-byte union). Both
		// are heap allocated, so the address handed to the kernel is stable.
		value := &[2]uint32{offload.command, 0}
		request := new([40]byte)
		copy(request[:15], name)
		*(*uintptr)(unsafe.Pointer(&request[16])) = uintptr(unsafe.Pointer(value))
		_, _, errno := syscall.Syscall(syscall.SYS_IOCTL, uintptr(fd), siocEthtool, uintptr(unsafe.Pointer(request)))
		runtime.KeepAlive(value)
		if errno != 0 {
			failures[offload.name] = errno.Error()
			continue
		}
		offloads[offload.name] = value[1] != 0
	}
	if len(failures) == 0 {
		failures = nil
	}
	return offloads, failures
}
