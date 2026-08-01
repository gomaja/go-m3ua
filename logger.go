// Copyright 2019-2024 go-m3ua authors. All rights reserved.
// Use of this source code is governed by a MIT-style license that can be
// found in the LICENSE file.

package m3ua

import (
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

const (
	logQueueSize            = 64
	malformedLogBurst       = 4
	malformedLogWindow      = time.Minute
	malformedLogPrefixLen   = 40
	maxMalformedLogErrorLen = 256
	maxMalformedLogLineLen  = 512
)

type queuedLog struct {
	destination *log.Logger
	message     string
}

type malformedLogLimiter struct {
	mu          sync.Mutex
	windowStart time.Time
	emitted     uint32
	suppressed  uint64
}

var (
	logger   atomic.Pointer[log.Logger]
	logQueue = make(chan queuedLog, logQueueSize)
)

func init() {
	logger.Store(log.New(os.Stderr, "", log.LstdFlags))
	go writeLogs()
}

// SetLogger replaces the standard logger with arbitrary *log.Logger.
//
// This package prints just informational logs from goroutines working background
// that might help developers test the program but can be ignored safely. More
// important ones that needs any action by caller would be returned as errors.
// Logging is asynchronous and best-effort: a slow Writer never blocks protocol
// handling, and records are discarded when the bounded internal queue is full.
func SetLogger(l *log.Logger) {
	if l == nil {
		log.Println("Don't pass nil to SetLogger: use DisableLogging instead.")
	}

	setLogger(l)
}

// EnableLogging enables the logging from the package.
// If l is nil, it uses default logger provided by the package.
// Logging is enabled by default.
// The asynchronous, best-effort behaviour described by SetLogger applies.
//
// See also: SetLogger.
func EnableLogging(l *log.Logger) {
	setLogger(l)
}

// DisableLogging disables the logging from the package.
// Logging is enabled by default.
// Records already queued may still complete on the logger selected when they
// were emitted; DisableLogging never waits for an application Writer.
func DisableLogging() {
	logger.Store(log.New(io.Discard, "", 0))
}

func setLogger(l *log.Logger) {
	if l == nil {
		l = log.New(os.Stderr, "", log.LstdFlags)
	}
	logger.Store(l)
}

func logf(format string, v ...interface{}) {
	tryLogf(format, v...)
}

func tryLogf(format string, values ...interface{}) bool {
	return tryLog(fmt.Sprintf(format, values...))
}

func tryLog(message string) bool {
	entry := queuedLog{destination: logger.Load(), message: message}
	select {
	case logQueue <- entry:
		return true
	default:
		return false
	}
}

func writeLogs() {
	for entry := range logQueue {
		entry.destination.Print(entry.message)
	}
}

func (limiter *malformedLogLimiter) allow(now time.Time) (uint64, bool) {
	limiter.mu.Lock()
	defer limiter.mu.Unlock()

	if limiter.windowStart.IsZero() || now.Sub(limiter.windowStart) >= malformedLogWindow {
		suppressed := limiter.suppressed
		limiter.windowStart = now
		limiter.emitted = 1
		limiter.suppressed = 0
		return suppressed, true
	}
	if limiter.emitted >= malformedLogBurst {
		limiter.suppressed++
		return 0, false
	}
	limiter.emitted++
	return 0, true
}

func (limiter *malformedLogLimiter) restoreSuppressed(count uint64) {
	limiter.mu.Lock()
	limiter.suppressed += count
	limiter.mu.Unlock()
}

func (limiter *malformedLogLimiter) suppressedCount() uint64 {
	limiter.mu.Lock()
	defer limiter.mu.Unlock()
	return limiter.suppressed
}

func (c *Conn) logMalformedInput(parseErr error, raw []byte) {
	suppressed, allowed := c.malformedLogs.allow(time.Now())
	if !allowed {
		return
	}

	class := "unavailable"
	if len(raw) > 2 {
		class = strconv.Itoa(int(raw[2]))
	}
	messageType := "unavailable"
	if len(raw) > 3 {
		messageType = strconv.Itoa(int(raw[3]))
	}
	prefix := raw
	if len(prefix) > malformedLogPrefixLen {
		prefix = prefix[:malformedLogPrefixLen]
	}
	errorText := fmt.Sprint(parseErr)
	if len(errorText) > maxMalformedLogErrorLen {
		errorText = errorText[:maxMalformedLogErrorLen]
	}

	message := fmt.Sprintf(
		"m3ua: failed to parse M3UA message: length=%d class=%s type=%s first40=%x suppressed=%d error=%q",
		len(raw), class, messageType, prefix, suppressed, errorText,
	)
	if len(message) >= maxMalformedLogLineLen {
		message = message[:maxMalformedLogLineLen-4] + "..."
	}
	if !tryLog(message) {
		limiterDrop := suppressed + 1
		c.malformedLogs.restoreSuppressed(limiterDrop)
	}
}
