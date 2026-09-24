// Copyright 2018-2024 go-m3ua authors. All rights reserved.
// Use of this source code is governed by a MIT-style license that can be
// found in the LICENSE file.

//go:build linux

package m3ua

import (
	"errors"
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
		t.Skipf("kernel older than Linux 5.0, without SCTP_EVENT: %v", err)
	}
	if err != nil {
		t.Fatalf("probe: %v", err)
	}
	if !associationEventsSupported() {
		t.Fatal("the kernel accepts SCTP_EVENT but sockets do not subscribe")
	}
}
