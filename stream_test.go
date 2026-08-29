// Copyright 2018-2024 go-m3ua authors. All rights reserved.
// Use of this source code is governed by a MIT-style license that can be
// found in the LICENSE file.

package m3ua

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/gomaja/go-m3ua/messages"
	"github.com/gomaja/go-m3ua/messages/params"
)

// RFC 4666 Section 1.4.7 (SCTP Stream Mapping):
//
//	"Traffic that requires sequencing SHOULD be assigned to the same stream.
//	To accomplish this, MTP3-User traffic may be assigned to individual
//	streams based on, for example, the SLS value in the MTP3 Routing Label,
//	subject of course to the maximum number of streams supported by the
//	underlying SCTP association."
//
// and rule 1 of the same section, which is unconditional:
//
//	"The DATA message MUST NOT be sent on stream 0."
//
// The library picked a stream at random for every message instead, so two
// messages carrying the same SLS routinely went out on different streams. SCTP
// only guarantees ordering *within* a stream, so that discards the very
// guarantee MTP3 users rely on — and it did it by reseeding a fresh
// math/rand source from the wall clock on each call, which also makes calls
// landing in the same clock tick pick the same stream.
//
// Which of the tests below actually discriminates is worth being explicit
// about. Stream independence only *permits* reordering; producing it takes
// packet reordering on the path. Loopback has none, so the two socket tests
// here pass with random stream selection in place — they are end-to-end sanity
// checks, not the proof. The proof is TestStreamForIsStableAcrossCalls, which
// fails immediately on a random mapping, plus local/validate-ordering.sh, which
// runs the two ends in separate containers under netem and shows the SLS-derived
// mapping holding order (0 inversions) where a stream-per-message does not.

// A given SLS must always map to the same stream, or nothing about ordering
// holds.
func TestStreamForIsStableAcrossCalls(t *testing.T) {
	c := &Association{maxMessageStreamID: 9}

	for _, sls := range []uint8{0, 1, 7, 15, 200, 255} {
		first := c.streamFor(sls)
		for i := 0; i < 100; i++ {
			if got := c.streamFor(sls); got != first {
				t.Fatalf("SLS %d mapped to stream %d then %d: the mapping must be stable", sls, first, got)
			}
		}
		if first < 1 || first > c.maxMessageStreamID {
			t.Errorf("SLS %d mapped to stream %d, outside 1..%d (stream 0 is reserved for management)",
				sls, first, c.maxMessageStreamID)
		}
	}
}

// Different SLS values must actually use the streams available, or the peer's
// negotiated stream count is wasted and everything serialises behind one
// stream.
func TestStreamForSpreadsAcrossTheNegotiatedStreams(t *testing.T) {
	c := &Association{maxMessageStreamID: 9}

	seen := map[uint16]bool{}
	for sls := 0; sls < 256; sls++ {
		seen[c.streamFor(uint8(sls))] = true
	}
	if len(seen) != int(c.maxMessageStreamID) {
		t.Errorf("256 SLS values reached %d of %d data streams; want all of them",
			len(seen), c.maxMessageStreamID)
	}
}

// A peer that negotiates a single outbound stream leaves only stream 0, which
// DATA MUST NOT use; one that negotiates two leaves stream 1. Neither may
// produce a stream the peer has not got, and neither may panic.
func TestStreamForHandlesDegenerateStreamCounts(t *testing.T) {
	for _, tt := range []struct {
		maxStream uint16
		want      uint16
	}{
		{0, 0}, // no data stream exists; the write paths refuse it
		{1, 1},
	} {
		c := &Association{maxMessageStreamID: tt.maxStream}
		for sls := 0; sls < 256; sls++ {
			if got := c.streamFor(uint8(sls)); got != tt.want {
				t.Fatalf("maxMessageStreamID=%d SLS=%d: stream %d, want %d", tt.maxStream, sls, got, tt.want)
			}
		}
	}
}

func TestCheckDataStreamEnforcesNegotiatedBounds(t *testing.T) {
	for _, test := range []struct {
		name      string
		maxStream uint16
		stream    uint16
		wantZero  bool
		wantID    uint16
	}{
		{name: "reserved zero", maxStream: 4, stream: 0, wantZero: true},
		{name: "first data stream", maxStream: 4, stream: 1},
		{name: "last negotiated stream", maxStream: 4, stream: 4},
		{name: "one above negotiated", maxStream: 4, stream: 5, wantID: 5},
		{name: "far above negotiated", maxStream: 4, stream: 0xffff, wantID: 0xffff},
		{name: "no negotiated data stream", maxStream: 0, stream: 1, wantID: 1},
		{name: "uint16 maximum is negotiated", maxStream: 0xffff, stream: 0xffff},
	} {
		t.Run(test.name, func(t *testing.T) {
			conn := &Association{maxMessageStreamID: test.maxStream}
			err := conn.checkDataStream(test.stream)

			switch {
			case test.wantZero:
				if !errors.Is(err, ErrNoDataStream) {
					t.Fatalf("checkDataStream(%d) error = %v, want ErrNoDataStream", test.stream, err)
				}
			case test.wantID != 0:
				var streamError *InvalidSCTPStreamIDError
				if !errors.As(err, &streamError) {
					t.Fatalf("checkDataStream(%d) error = %v, want *InvalidSCTPStreamIDError", test.stream, err)
				}
				if streamError.ID != test.wantID {
					t.Errorf("invalid stream ID = %d, want %d", streamError.ID, test.wantID)
				}
			default:
				if err != nil {
					t.Fatalf("checkDataStream(%d) error = %v, want nil", test.stream, err)
				}
			}
		})
	}
}

func TestOutOfRangeDataStreamMapsToInvalidStreamError(t *testing.T) {
	conn, sent := newTestConn(t, StateASPActive, RoleASP)
	conn.maxMessageStreamID = 4

	// InvalidSCTPStreamIDError is also what the monitor maps to RFC 4666's
	// Invalid Stream Identifier Error code when the fault came from a peer.
	err := conn.checkDataStream(5)
	if err == nil {
		t.Fatal("stream 5 was accepted after only streams 0..4 were negotiated")
	}
	if err := conn.handleErrors(err); err != nil {
		t.Fatalf("handleErrors: %v", err)
	}

	if got := errorCodes(*sent); len(got) != 1 || got[0] != params.ErrInvalidStreamIdentifier {
		t.Errorf("wire Error codes = %v, want [%d]", got, params.ErrInvalidStreamIdentifier)
	}
}

// RFC 4666 Section 1.4.7 rule 1: "The DATA message MUST NOT be sent on stream
// 0." A peer negotiating a single outbound stream therefore offers nowhere
// legal to carry traffic, and the library must say so rather than quietly
// breaking the rule — which it did, sending DATA on stream 0.
func TestDataIsNeverSentOnStreamZero(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
	defer cancel()

	const port = 3116
	ln := mcListen(t, mcAddr(port, "127.0.0.1"))
	asps := mcConnect(t, ctx, ln, mcAddr(port, "127.0.0.1"), []string{"127.0.0.2"}, port)

	// Explicitly asking for stream 0 must be refused.
	if _, err := asps[0].asp.WriteToStream([]byte("nope"), 0); !errors.Is(err, ErrNoDataStream) {
		t.Errorf("WriteToStream(_, 0) error = %v, want ErrNoDataStream", err)
	}
	pd := params.NewProtocolData(0x11111111, 0x22222222, params.ServiceIndSCCP, 0, 0, 1, []byte("nope"))
	if _, err := asps[0].asp.WritePDToStream(pd, 0); !errors.Is(err, ErrNoDataStream) {
		t.Errorf("WritePDToStream(_, 0) error = %v, want ErrNoDataStream", err)
	}

	// And an association that negotiated only stream 0 must refuse Write too,
	// rather than choosing stream 0 for it.
	asps[0].asp.maxMessageStreamID = 0
	if _, err := asps[0].asp.Write([]byte("nope")); !errors.Is(err, ErrNoDataStream) {
		t.Errorf("Write with no data stream = %v, want ErrNoDataStream", err)
	}
	if _, err := asps[0].asp.WritePD(pd); !errors.Is(err, ErrNoDataStream) {
		t.Errorf("WritePD with no data stream = %v, want ErrNoDataStream", err)
	}
}

// End to end: messages sent through Write, which picks the stream itself, all
// carry the Config's single SLS and must therefore arrive in order. With a
// random stream per message they do not.
func TestWriteKeepsOrderForOneSLS(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	const port = 3095
	ln := mcListen(t, mcAddr(port, "127.0.0.1"))
	asps := mcConnect(t, ctx, ln, mcAddr(port, "127.0.0.1"), []string{"127.0.0.2"}, port)

	const n = 200
	go func() {
		for i := 0; i < n; i++ {
			// Write, not WriteToStream: the library chooses the stream, and the
			// Config carries one SLS, so every message shares it.
			for {
				_, err := fmt.Fprintf(asps[0].asp, "%04d", i)
				if err == nil {
					break
				}
				time.Sleep(time.Millisecond)
			}
		}
	}()

	for i := 0; i < n; i++ {
		got, err := readWithin(t, asps[0].sgp, 10*time.Second)
		if err != nil {
			t.Fatalf("only %d of %d payloads arrived: %v", i, n, err)
		}
		if want := fmt.Sprintf("%04d", i); got != want {
			t.Fatalf("payload %d read as %q, want %q: Write is spreading one SLS across streams", i, got, want)
		}
	}
}

// WritePD carries its own SLS per message — that is the whole point of the
// per-message routing label — so the stream must follow the message's SLS, not
// the Config's.
func TestWritePDKeepsOrderPerSLS(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	const port = 3097
	ln := mcListen(t, mcAddr(port, "127.0.0.1"))
	asps := mcConnect(t, ctx, ln, mcAddr(port, "127.0.0.1"), []string{"127.0.0.2"}, port)

	const n = 150
	const sls = 7
	go func() {
		for i := 0; i < n; i++ {
			pd := params.NewProtocolData(
				0x11111111, 0x22222222, params.ServiceIndSCCP, 0, 0, sls,
				[]byte(fmt.Sprintf("%04d", i)),
			)
			for {
				_, err := asps[0].asp.WritePD(pd)
				if err == nil {
					break
				}
				time.Sleep(time.Millisecond)
			}
		}
	}()

	for i := 0; i < n; i++ {
		pd, err := readPDWithin(t, asps[0].sgp, 10*time.Second)
		if err != nil {
			t.Fatalf("only %d of %d payloads arrived: %v", i, n, err)
		}
		if want := fmt.Sprintf("%04d", i); string(pd.Data) != want {
			t.Fatalf("payload %d read as %q, want %q: one SLS was spread across streams", i, pd.Data, want)
		}
		if pd.SignallingLinkSelection != sls {
			t.Errorf("payload %d carried SLS %d, want %d", i, pd.SignallingLinkSelection, sls)
		}
	}
}

// WriteSignal accepts DATA directly, so it must make the same SLS-to-stream
// choice as WritePD. Otherwise two DATA messages with the same routing-label
// SLS can land on different SCTP streams solely because the caller used two
// exported write paths, forfeiting SCTP's in-stream sequencing guarantee.
func TestWriteSignalUsesTheDatasOwnSLS(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
	defer cancel()

	aspAssociation, sgpAssociation, err := setupConn(t, ctx, 3120)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		_ = aspAssociation.Close()
		_ = sgpAssociation.Close()
	}()

	configStream := aspAssociation.streamFor(aspAssociation.cfg.SignallingLinkSelection)
	var messageSLS uint8
	for candidate := 2; candidate < 256; candidate++ {
		if aspAssociation.streamFor(uint8(candidate)) != configStream {
			messageSLS = uint8(candidate)
			break
		}
	}
	if messageSLS == 0 && aspAssociation.streamFor(0) != configStream {
		messageSLS = 0
	}
	wantStream := aspAssociation.streamFor(messageSLS)
	if wantStream == configStream {
		t.Skipf("association negotiated only one DATA stream (%d)", wantStream)
	}

	data := messages.NewData(
		aspAssociation.cfg.NetworkAppearance.Copy(),
		params.NewRoutingContext(1),
		params.NewProtocolData(
			0x11111111, 0x22222222, params.ServiceIndSCCP, 2, 3,
			messageSLS, []byte("write-signal-sls"),
		),
		nil,
	)
	if _, err := aspAssociation.WriteSignal(data); err != nil {
		t.Fatalf("WriteSignal(DATA): %v", err)
	}
	got, err := readDataWithin(t, sgpAssociation, 10*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if string(got.ProtocolData.Data) != "write-signal-sls" {
		t.Fatalf("payload = %q, want %q", got.ProtocolData.Data, "write-signal-sls")
	}
	if gotStream := sgpAssociation.receivedStreamID(); gotStream != wantStream {
		t.Errorf("DATA with SLS %d arrived on stream %d, want %d; config SLS %d maps to %d",
			messageSLS, gotStream, wantStream, aspAssociation.cfg.SignallingLinkSelection, configStream)
	}
}

func TestOutboundSignalStreamUsesProtocolDataSLS(t *testing.T) {
	conn := &Association{maxMessageStreamID: 9}
	for _, sls := range []uint8{0, 1, 7, 15, 255} {
		t.Run(fmt.Sprintf("SLS %d", sls), func(t *testing.T) {
			data := messages.NewData(nil, nil, params.NewProtocolData(
				1, 2, params.ServiceIndSCCP, 2, 3, sls, []byte("x")), nil)
			raw, err := data.MarshalBinary()
			if err != nil {
				t.Fatal(err)
			}
			got, err := conn.outboundSignalStream(raw)
			if err != nil {
				t.Fatal(err)
			}
			if want := conn.streamFor(sls); got != want {
				t.Errorf("outboundSignalStream(DATA SLS %d) = %d, want %d", sls, got, want)
			}
		})
	}

	heartbeat := messages.NewHeartbeat(params.NewHeartbeatData([]byte("beat")))
	raw, err := heartbeat.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	if got, err := conn.outboundSignalStream(raw); err != nil || got != 0 {
		t.Errorf("outboundSignalStream(Heartbeat) = %d, %v; want stream 0, nil", got, err)
	}
}

// readPDWithin is ReadPD with a deadline, so a stalled test reports rather than
// hanging to the package timeout.
func readPDWithin(t *testing.T, c *Association, d time.Duration) (*params.ProtocolDataPayload, error) {
	t.Helper()

	type result struct {
		pd  *params.ProtocolDataPayload
		err error
	}
	out := make(chan result, 1)
	go func() {
		pd, err := c.ReadPD()
		out <- result{pd, err}
	}()

	select {
	case r := <-out:
		return r.pd, r.err
	case <-time.After(d):
		return nil, fmt.Errorf("nothing arrived within %v", d)
	}
}

func readDataWithin(t *testing.T, c *Association, d time.Duration) (*DataMessage, error) {
	t.Helper()

	type result struct {
		data *DataMessage
		err  error
	}
	out := make(chan result, 1)
	go func() {
		data, err := c.ReadData()
		out <- result{data: data, err: err}
	}()

	select {
	case r := <-out:
		return r.data, r.err
	case <-time.After(d):
		return nil, fmt.Errorf("nothing arrived within %v", d)
	}
}
