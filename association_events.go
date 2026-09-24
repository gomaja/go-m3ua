// Copyright 2018-2024 go-m3ua authors. All rights reserved.
// Use of this source code is governed by a MIT-style license that can be
// found in the LICENSE file.

package m3ua

import "github.com/gomaja/go-m3ua/internal/sctpevents"

// kernelAssociationEvents asks whether sockets can take the SCTP_EVENT
// subscription associationEvents makes. The question lives in an internal
// package so the perftraffic fixture asks it exactly as the library does.
func kernelAssociationEvents() error { return sctpevents.Probe() }
