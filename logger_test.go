// Copyright 2019-2024 go-m3ua authors. All rights reserved.
// Use of this source code is governed by a MIT-style license that can be
// found in the LICENSE file.

package m3ua

import (
	"bytes"
	"log"
	"strings"
	"testing"
	"time"
)

// Logger configuration and logging must remain independent: neither may wait
// for the other while an association is dispatching messages.
func TestEnableLoggingReturns(t *testing.T) {
	writer := &capturedLogWriter{writes: make(chan string, 1)}

	done := make(chan struct{})
	go func() {
		EnableLogging(log.New(writer, "", 0))
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("EnableLogging did not return")
	}

	t.Cleanup(func() { EnableLogging(nil) })

	logged := make(chan struct{})
	go func() {
		logf("probe %d", 1)
		close(logged)
	}()
	select {
	case <-logged:
	case <-time.After(2 * time.Second):
		t.Fatal("logf blocked after EnableLogging")
	}

	select {
	case message := <-writer.writes:
		if !strings.Contains(message, "probe 1") {
			t.Errorf("log = %q, want probe", message)
		}
	case <-time.After(2 * time.Second):
		t.Error("EnableLogging installed a logger but nothing was written to it")
	}
}

// The other three exported entry points must be equally safe to call, and
// DisableLogging must actually silence the package.
func TestLoggingEntryPointsDoNotBlock(t *testing.T) {
	t.Cleanup(func() { EnableLogging(nil) })

	var buf bytes.Buffer
	for _, tc := range []struct {
		name string
		call func()
	}{
		{"SetLogger", func() { SetLogger(log.New(&buf, "", 0)) }},
		{"SetLogger(nil)", func() { SetLogger(nil) }},
		{"EnableLogging", func() { EnableLogging(log.New(&buf, "", 0)) }},
		{"EnableLogging(nil)", func() { EnableLogging(nil) }},
		{"DisableLogging", func() { DisableLogging() }},
	} {
		done := make(chan struct{})
		go func() {
			tc.call()
			close(done)
		}()
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Fatalf("%s did not return", tc.name)
		}
	}

	// DisableLogging ran last, so nothing further may be written.
	buf.Reset()
	SetLogger(log.New(&buf, "", 0))
	DisableLogging()
	logf("must not appear")
	if got := buf.String(); got != "" {
		t.Errorf("DisableLogging left logging on: %q", got)
	}
}
