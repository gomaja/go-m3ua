// Copyright 2019-2024 go-m3ua authors. All rights reserved.
// Use of this source code is governed by a MIT-style license that can be
// found in the LICENSE file.

package m3ua

import (
	"context"
	"encoding/hex"
	"errors"
	"log"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gomaja/go-m3ua/messages"
	"github.com/gomaja/go-m3ua/messages/params"
)

type capturedLogWriter struct {
	writes chan string
}

func (writer *capturedLogWriter) Write(p []byte) (int, error) {
	message := string(append([]byte(nil), p...))
	writer.writes <- message
	return len(p), nil
}

type gatedLogWriter struct {
	entered chan struct{}
	release chan struct{}
	writes  chan string
	once    sync.Once
}

func (writer *gatedLogWriter) Write(p []byte) (int, error) {
	message := string(append([]byte(nil), p...))
	writer.once.Do(func() {
		close(writer.entered)
		<-writer.release
	})
	writer.writes <- message
	return len(p), nil
}

func logsThrough(t *testing.T, writes <-chan string, marker string) []string {
	t.Helper()

	timer := time.NewTimer(2 * time.Second)
	defer timer.Stop()

	var collected []string
	for {
		select {
		case message := <-writes:
			if strings.Contains(message, marker) {
				return collected
			}
			collected = append(collected, message)
		case <-timer.C:
			t.Fatalf("timed out waiting for log marker %q; collected %q", marker, collected)
		}
	}
}

func waitForLog(t *testing.T, writes <-chan string, marker string) string {
	t.Helper()

	select {
	case message := <-writes:
		if !strings.Contains(message, marker) {
			t.Fatalf("log = %q, want marker %q", message, marker)
		}
		return message
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out waiting for log marker %q", marker)
		return ""
	}
}

func newLoggingTestConn(state State, role Role) (*Association, *[]messages.M3UA) {
	var sent []messages.M3UA
	connection := &Association{
		muState:   new(sync.RWMutex),
		role:      role,
		state:     state,
		stateChan: make(chan State, 4),
		errChan:   make(chan error, 4),
		done:      make(chan struct{}),
		signalWriter: func(message messages.M3UA) (int, error) {
			sent = append(sent, message)
			return message.MarshalLen(), nil
		},
	}
	return connection, &sent
}

func TestMalformedInputLoggingIsBoundedAndRateLimited(t *testing.T) {
	writer := &capturedLogWriter{writes: make(chan string, 256)}
	SetLogger(log.New(writer, "", 0))
	t.Cleanup(func() { EnableLogging(nil) })

	connection := &Association{}
	raw := make([]byte, 4096)
	for index := range raw {
		raw[index] = byte(index)
	}
	raw[2] = 7
	raw[3] = 9
	copy(raw[40:], []byte{0xde, 0xad, 0xbe, 0xef})
	wantPrefix := hex.EncodeToString(raw[:40])
	parseErr := errors.New(strings.Repeat("x", 1024))

	const attempts = 128
	start := make(chan struct{})
	var callers sync.WaitGroup
	callers.Add(attempts)
	for range attempts {
		go func() {
			defer callers.Done()
			<-start
			connection.logMalformedInput(parseErr, raw)
		}()
	}
	close(start)
	callers.Wait()

	const marker = "malformed-log-barrier"
	logf(marker)
	logs := logsThrough(t, writer.writes, marker)

	var malformed []string
	for _, message := range logs {
		if strings.Contains(message, "failed to parse M3UA message") {
			malformed = append(malformed, message)
		}
	}
	if got := len(malformed); got != malformedLogBurst {
		t.Fatalf("malformed log count = %d, want burst bound %d; logs: %q", got, malformedLogBurst, logs)
	}
	for _, message := range malformed {
		for _, field := range []string{
			"length=4096",
			"class=7",
			"type=9",
			"first40=" + wantPrefix,
		} {
			if !strings.Contains(message, field) {
				t.Errorf("malformed log %q does not contain %q", message, field)
			}
		}
		if strings.Contains(message, "deadbeef") {
			t.Errorf("malformed log leaked bytes after the first 40 octets: %q", message)
		}
		if len(message) > maxMalformedLogLineLen {
			t.Errorf("malformed log length = %d, want at most %d: %q", len(message), maxMalformedLogLineLen, message)
		}
	}

	if got, want := connection.malformedLogs.suppressedCount(), uint64(attempts-malformedLogBurst); got != want {
		t.Errorf("suppressed malformed log count = %d, want %d", got, want)
	}

	connection.malformedLogs.mu.Lock()
	connection.malformedLogs.windowStart = connection.malformedLogs.windowStart.Add(-malformedLogWindow)
	connection.malformedLogs.mu.Unlock()
	connection.logMalformedInput(errors.New("next window"), raw)
	const summaryMarker = "malformed-summary-barrier"
	logf(summaryMarker)
	summaryLogs := logsThrough(t, writer.writes, summaryMarker)
	if got := len(summaryLogs); got != 1 {
		t.Fatalf("next-window log count = %d, want 1; logs: %q", got, summaryLogs)
	}
	wantSuppressed := "suppressed=" + strconv.Itoa(attempts-malformedLogBurst)
	if !strings.Contains(summaryLogs[0], wantSuppressed) {
		t.Errorf("next-window log %q does not report %q", summaryLogs[0], wantSuppressed)
	}
}

func TestAsyncLoggingSnapshotsInputAndDoesNotHoldConfigurationLock(t *testing.T) {
	writer := &gatedLogWriter{
		entered: make(chan struct{}),
		release: make(chan struct{}),
		writes:  make(chan string, 4),
	}
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(writer.release) }) }
	t.Cleanup(release)
	t.Cleanup(func() { EnableLogging(nil) })
	SetLogger(log.New(writer, "", 0))

	logReturned := make(chan struct{})
	go func() {
		logf("gate")
		close(logReturned)
	}()

	select {
	case <-writer.entered:
	case <-time.After(2 * time.Second):
		t.Fatal("logger writer was never entered")
	}
	select {
	case <-logReturned:
	case <-time.After(250 * time.Millisecond):
		release()
		<-logReturned
		t.Fatal("logf blocked on the application writer")
	}

	raw := make([]byte, 64)
	for index := range raw {
		raw[index] = byte(index + 1)
	}
	wantPrefix := hex.EncodeToString(raw[:40])
	connection := &Association{}
	connection.logMalformedInput(errors.New("bad message"), raw)
	for index := range raw {
		raw[index] = 0xff
	}

	replaced := make(chan struct{})
	go func() {
		SetLogger(log.New(&capturedLogWriter{writes: make(chan string, 1)}, "", 0))
		close(replaced)
	}()
	select {
	case <-replaced:
	case <-time.After(250 * time.Millisecond):
		release()
		<-replaced
		t.Fatal("SetLogger blocked behind an application writer")
	}

	release()
	waitForLog(t, writer.writes, "gate")
	malformed := waitForLog(t, writer.writes, "failed to parse M3UA message")
	if !strings.Contains(malformed, "first40="+wantPrefix) {
		t.Errorf("malformed log did not snapshot the original octets: %q", malformed)
	}
	if strings.Contains(malformed, strings.Repeat("ff", 40)) {
		t.Errorf("malformed log observed the caller's later mutation: %q", malformed)
	}
}

func TestLogQueueIsBoundedAndNeverBlocksCallers(t *testing.T) {
	flusher := &capturedLogWriter{writes: make(chan string, 1)}
	SetLogger(log.New(flusher, "", 0))
	logf("queue-flush")
	waitForLog(t, flusher.writes, "queue-flush")

	writer := &gatedLogWriter{
		entered: make(chan struct{}),
		release: make(chan struct{}),
		writes:  make(chan string, logQueueSize+1),
	}
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(writer.release) }) }
	t.Cleanup(release)
	t.Cleanup(func() { EnableLogging(nil) })
	SetLogger(log.New(writer, "", 0))

	logf("gate")
	select {
	case <-writer.entered:
	case <-time.After(2 * time.Second):
		t.Fatal("logger writer was never entered")
	}

	for index := range logQueueSize {
		if !tryLogf("queued-%d", index) {
			t.Fatalf("queue rejected record %d before its %d-record bound", index, logQueueSize)
		}
	}
	if tryLog("overflow") {
		t.Fatalf("queue accepted more than its %d-record bound", logQueueSize)
	}

	release()
	for index := 0; index < logQueueSize+1; index++ {
		select {
		case <-writer.writes:
		case <-time.After(2 * time.Second):
			t.Fatalf("logger worker drained %d of %d accepted records", index, logQueueSize+1)
		}
	}
}

func TestBlockedMalformedLoggerDoesNotBlockDispatchOrSiblingHeartbeat(t *testing.T) {
	writer := &gatedLogWriter{
		entered: make(chan struct{}),
		release: make(chan struct{}),
		writes:  make(chan string, 2),
	}
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(writer.release) }) }
	t.Cleanup(release)
	t.Cleanup(func() { EnableLogging(nil) })
	SetLogger(log.New(writer, "", 0))

	attacker, _ := newLoggingTestConn(StateASPActive, RoleSGP)
	sibling, sent := newLoggingTestConn(StateASPActive, RoleSGP)
	malformed := []byte{
		1, 0, messages.MsgClassTransfer, messages.MsgTypePayloadData,
		0, 0, 0, 8,
	}

	attackerDone := make(chan struct{})
	go func() {
		attacker.dispatchRaw(context.Background(), inbound{data: malformed, ppid: M3UAPPID})
		close(attackerDone)
	}()
	select {
	case <-writer.entered:
	case <-time.After(2 * time.Second):
		t.Fatal("malformed input never reached the logger writer")
	}
	select {
	case <-attackerDone:
	case <-time.After(250 * time.Millisecond):
		release()
		<-attackerDone
		t.Fatal("malformed-input dispatch blocked on the logger writer")
	}

	heartbeat, err := messages.NewHeartbeat(params.NewHeartbeatData([]byte("live"))).MarshalBinary()
	if err != nil {
		t.Fatalf("marshal Heartbeat: %v", err)
	}
	siblingDone := make(chan struct{})
	go func() {
		sibling.dispatchRaw(context.Background(), inbound{data: heartbeat, ppid: M3UAPPID})
		close(siblingDone)
	}()
	select {
	case <-siblingDone:
	case <-time.After(250 * time.Millisecond):
		release()
		<-siblingDone
		t.Fatal("a sibling association's Heartbeat blocked behind malformed-input logging")
	}
	if got := len(*sent); got != 1 {
		t.Fatalf("sibling sent %d messages, want one Heartbeat Ack", got)
	}
	response, err := (*sent)[0].MarshalBinary()
	if err != nil {
		t.Fatalf("marshal sibling response: %v", err)
	}
	if got := response[3]; got != messages.MsgTypeHeartbeatAck {
		t.Errorf("sibling wire response type = %d, want Heartbeat Ack (%d)", got, messages.MsgTypeHeartbeatAck)
	}

	release()
	select {
	case <-writer.writes:
	case <-time.After(2 * time.Second):
		t.Fatal("logger worker did not leave the released writer")
	}
}
