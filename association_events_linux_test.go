// Copyright 2018-2024 go-m3ua authors. All rights reserved.
// Use of this source code is governed by a MIT-style license that can be
// found in the LICENSE file.

//go:build linux

package m3ua

import (
	"errors"
	"fmt"
	"os"
	"strings"
	"syscall"
	"testing"
)

// The probe asks the running kernel. From Linux 5.0 it accepts SCTP_EVENT,
// and sockets then subscribe; a kernel without SCTP at all is not this test's.
func TestKernelAssociationEventsProbe(t *testing.T) {
	err := kernelAssociationEvents()
	if errors.Is(err, syscall.EPROTONOSUPPORT) || errors.Is(err, syscall.ESOCKTNOSUPPORT) || errors.Is(err, syscall.EAFNOSUPPORT) {
		t.Skipf("no SCTP in this kernel: %v", err)
	}
	if errors.Is(err, syscall.ENOPROTOOPT) {
		// Only a kernel older than 5.0 may refuse the option; a newer one
		// refusing it means the probe asked for the wrong thing.
		release, readErr := os.ReadFile("/proc/sys/kernel/osrelease")
		if readErr != nil {
			t.Fatalf("probe refused SCTP_EVENT (%v) and the kernel release is unreadable: %v", err, readErr)
		}
		var major int
		if _, scanErr := fmt.Sscanf(string(release), "%d.", &major); scanErr != nil || major >= 5 {
			t.Fatalf("Linux %s refused SCTP_EVENT: %v", strings.TrimSpace(string(release)), err)
		}
		t.Skipf("Linux %s predates SCTP_EVENT: %v", strings.TrimSpace(string(release)), err)
	}
	if err != nil {
		t.Fatalf("probe: %v", err)
	}
	if !associationEventsSupported() {
		t.Fatal("the kernel accepts SCTP_EVENT but sockets do not subscribe")
	}
}
