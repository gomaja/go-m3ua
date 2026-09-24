// Copyright 2018-2024 go-m3ua authors. All rights reserved.
// Use of this source code is governed by a MIT-style license that can be
// found in the LICENSE file.

//go:build !linux

package m3ua

// kernelAssociationEvents has nothing to ask where the dependency supports no
// SCTP sockets: opening one fails with that reason itself.
func kernelAssociationEvents() error { return nil }
