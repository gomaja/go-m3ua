// Copyright 2018-2024 go-m3ua authors. All rights reserved.
// Use of this source code is governed by a MIT-style license that can be
// found in the LICENSE file.

//go:build !linux

// Package sctpevents asks the kernel whether SCTP sockets can subscribe to
// association events. The library and its test fixtures ask the same question
// the same way, without either exporting it.
package sctpevents

// Probe has nothing to ask where the dependency supports no SCTP sockets:
// opening one fails with that reason itself.
func Probe() error { return nil }
