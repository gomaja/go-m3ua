package main

import (
	"errors"
	"math"
	"os"
	"runtime"
	"strings"
	"syscall"
	"time"
	"unsafe"
)

type linuxMeasurementClock struct{}

// Linux clock_gettime(2), CLOCK_MONOTONIC; time_namespaces(7).
const linuxClockMonotonic = 1

func newMeasurementClock() (measurementClock, error) {
	clock := linuxMeasurementClock{}
	if _, err := clock.Domain(); err != nil {
		return nil, err
	}
	if _, err := clock.Now(); err != nil {
		return nil, err
	}
	return clock, nil
}

func linuxClockValue(call uintptr) (int64, error) {
	var value syscall.Timespec
	_, _, errno := syscall.Syscall(call, linuxClockMonotonic, uintptr(unsafe.Pointer(&value)), 0)
	runtime.KeepAlive(&value)
	if errno != 0 {
		return 0, errno
	}
	seconds, nanos := int64(value.Sec), int64(value.Nsec)
	if seconds < 0 || nanos < 0 || nanos >= int64(time.Second) || seconds > (math.MaxInt64-nanos)/int64(time.Second) {
		return 0, errors.New("invalid Linux monotonic clock value")
	}
	return seconds*int64(time.Second) + nanos, nil
}

func (linuxMeasurementClock) Now() (int64, error) {
	return linuxClockValue(syscall.SYS_CLOCK_GETTIME)
}

func (linuxMeasurementClock) Domain() (sharedClockDomain, error) {
	boot, err := os.ReadFile("/proc/sys/kernel/random/boot_id")
	if err != nil {
		return sharedClockDomain{}, err
	}
	namespace, err := monotonicClockIdentity()
	if err != nil {
		return sharedClockDomain{}, err
	}
	resolution, err := linuxClockValue(syscall.SYS_CLOCK_GETRES)
	if err != nil {
		return sharedClockDomain{}, err
	}
	domain := sharedClockDomain{Clock: "CLOCK_MONOTONIC", BootID: strings.TrimSpace(string(boot)), TimeNamespace: namespace, Resolution: resolution}
	if !domain.valid() || len(domain.BootID) != 36 {
		return sharedClockDomain{}, errors.New("unverifiable Linux monotonic clock domain")
	}
	return domain, nil
}

// monotonicClockIdentity names the CLOCK_MONOTONIC this process reads within
// its boot. time_namespaces(7) defines a time namespace's CLOCK_MONOTONIC as
// the boot's clock plus the namespace's monotonic offset, so two processes on
// one boot with the same offset read the same clock. The namespace inode is
// not the identity: container runtimes give each container its own time
// namespace, all with zero offsets. A kernel without time namespaces has no
// timens_offsets file and no offset.
func monotonicClockIdentity() (string, error) {
	offsets, err := os.ReadFile("/proc/self/timens_offsets")
	if errors.Is(err, os.ErrNotExist) {
		return monotonicOffsetIdentity(0, 0), nil
	}
	if err != nil {
		return "", err
	}
	return parseMonotonicOffset(string(offsets))
}
