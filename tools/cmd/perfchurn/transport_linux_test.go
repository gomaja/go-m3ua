//go:build linux

package main

import (
	"syscall"
	"testing"
)

func TestSocketOptionsAreReadFromARealSCTPSocket(t *testing.T) {
	fd, err := syscall.Socket(syscall.AF_INET, syscall.SOCK_STREAM, syscall.IPPROTO_SCTP)
	if err != nil {
		t.Skipf("no SCTP socket in this kernel: %v", err)
	}
	defer func() { _ = syscall.Close(fd) }()
	if err := syscall.SetsockoptInt(fd, syscall.IPPROTO_SCTP, sctpNoDelayOption, 1); err != nil {
		t.Fatalf("set SCTP_NODELAY: %v", err)
	}
	options, err := platformSocketOptions(fd)
	if err != nil || options.NoDelay != 1 || options.SendBuffer <= 0 || options.ReceiveBuffer <= 0 || options.SACKFrequency == 0 {
		t.Fatalf("options %+v, %v", options, err)
	}
}

func TestLoopbackInterfaceIsRecorded(t *testing.T) {
	interfaces := readInterfaces("/sys/class/net", ethtoolOffloads)
	lo, ok := interfaces["lo"]
	if !ok || lo.MTU <= 0 {
		t.Fatalf("interfaces %+v", interfaces)
	}
	if len(lo.Offloads)+len(lo.OffloadErrors) == 0 {
		t.Fatalf("lo offloads neither read nor reported: %+v", lo)
	}
}
