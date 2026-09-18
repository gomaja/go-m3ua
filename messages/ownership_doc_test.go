// Copyright 2018-2024 go-m3ua authors. All rights reserved.
// Use of this source code is governed by a MIT-style license that can be
// found in the LICENSE file.

package messages_test

import (
	"bytes"
	"go/parser"
	"go/token"
	"io/fs"
	"strings"
	"testing"

	"github.com/gomaja/go-m3ua/messages"
	"github.com/gomaja/go-m3ua/messages/params"
)

// Every message and every Param in this package is a plain mutable struct with
// exported fields. Nothing in either package takes a lock, so no operation on
// one can be safe for concurrent use, and a doc comment that says otherwise is
// a promise the code does not keep -- the worst kind, because a caller acts on
// it and the race that follows is silent.
//
// Removing the deprecated wrappers rewrote doc comments across both packages,
// which is exactly when such a sentence gets added by accident.
func TestNoUnprovenConcurrencyGuaranteeInTheCodec(t *testing.T) {
	// Phrases that assert safety. "concurrent" on its own is not one of them:
	// Param.Copy uses it to warn that concurrent construction is *not* safe,
	// which is the documentation working correctly.
	guarantees := []string{
		"safe for concurrent",
		"safe to call concurrently",
		"safe to use concurrently",
		"safe for use by multiple goroutines",
		"concurrency-safe",
		"concurrency safe",
		"goroutine-safe",
		"goroutine safe",
		"thread-safe",
		"thread safe",
		"may be used concurrently",
		"can be used concurrently",
	}

	for _, dir := range []string{".", "params"} {
		fileSet := token.NewFileSet()
		pkgs, err := parser.ParseDir(fileSet, dir, func(info fs.FileInfo) bool {
			return !strings.HasSuffix(info.Name(), "_test.go")
		}, parser.ParseComments)
		if err != nil {
			t.Fatalf("parsing %s: %v", dir, err)
		}
		for name, pkg := range pkgs {
			if strings.HasSuffix(name, "_test") {
				continue
			}
			for path, file := range pkg.Files {
				for _, group := range file.Comments {
					text := strings.ToLower(group.Text())
					for _, guarantee := range guarantees {
						if strings.Contains(text, guarantee) {
							t.Errorf("%s:%d documents %q; nothing in this package "+
								"synchronises anything, so the guarantee is unproven",
								path, fileSet.Position(group.Pos()).Line, guarantee)
						}
					}
				}
			}
		}
	}
}

// Data.MarshalTo documents that the destination buffer and the message share
// storage afterwards: "After a successful return d.Header.Payload aliases b: it
// shares the destination's storage rather than owning a copy. A caller that
// retains the Data must not modify or recycle b (for example into a pool) while
// it keeps the Data."
//
// That is load-bearing -- it is why DATA can be written without a second
// message-sized allocation -- and it is only true while the fast path stays the
// fast path. A test that never writes through b would not notice it becoming a
// copy, nor a copy becoming an alias.
func TestDataMarshalToAliasesTheDestinationAsDocumented(t *testing.T) {
	message := messages.NewData(nil, nil,
		params.NewProtocolData(1, 2, params.ServiceIndSCCP, 0, 0, 1, []byte("payload")), nil)

	destination := make([]byte, message.MarshalLen())
	if err := message.MarshalTo(destination); err != nil {
		t.Fatalf("MarshalTo() error = %v", err)
	}
	if len(message.Header.Payload) == 0 {
		t.Fatal("MarshalTo left no payload behind")
	}

	// Writing through the destination must show up in the message, which is
	// what makes recycling b unsafe while the Data is retained.
	destination[len(destination)-1] ^= 0xff
	if message.Header.Payload[len(message.Header.Payload)-1] != destination[len(destination)-1] {
		t.Error("Data.MarshalTo no longer aliases the destination, but still documents that it does")
	}

	// The staged path exists for extension parameters and is documented as
	// leaving the destination untouched on any error, so a failure there must
	// not have written b.
	staged := messages.NewData(nil, nil,
		params.NewProtocolData(1, 2, params.ServiceIndSCCP, 0, 0, 1, []byte("payload")), nil)
	staged.Others = []*params.Param{params.NewInfoString(strings.Repeat("x", 256))}
	staged.SetLength()
	buffer := make([]byte, staged.MarshalLen())
	if err := staged.MarshalTo(buffer); err == nil {
		t.Fatal("an oversized extension parameter marshalled without error")
	}
	if !bytes.Equal(buffer, make([]byte, len(buffer))) {
		t.Error("a failed staged MarshalTo wrote to the destination it documents as untouched")
	}
}

// Param.Copy documents the hazard that makes it necessary: "The New*() message
// constructors call SetLength on the Params handed to them, which writes to the
// caller's Param. Long-lived Params -- notably those held in an association's
// configuration and reused for every outgoing message -- must therefore be
// copied before being passed to a constructor".
//
// Both halves have to stay true. If constructors stopped writing through, the
// warning would send callers copying for no reason; if Copy stopped being a
// deep copy, the advice would not work.
func TestParamOwnershipMatchesItsDocumentedHazard(t *testing.T) {
	longLived := &params.Param{Tag: params.InfoString, Data: []byte("info")}
	if longLived.Length != 0 {
		t.Fatalf("fixture Length = %d, want 0 so the write is observable", longLived.Length)
	}
	messages.NewAspUp(params.NewAspIdentifier(1), longLived)
	if longLived.Length == 0 {
		t.Error("a constructor no longer writes the caller's Param, but Copy still " +
			"documents that it does")
	}

	source := params.NewRoutingContext(1, 2)
	copied := source.Copy()
	if copied == nil {
		t.Fatal("Copy returned nil for a non-nil Param")
	}
	source.Data[0] = 0xff
	if bytes.Equal(copied.Data, source.Data) {
		t.Error("Param.Copy shares the original's Data; it documents a deep copy")
	}
	if (*params.Param)(nil).Copy() != nil {
		t.Error("Copy of a nil Param is not nil, which its doc promises")
	}
}

// NewRoutingKeyPayload takes the repeatable RFC 4666 Section 3.6.1 groupings as
// a variadic slice and keeps its own copy, so a caller reusing the backing
// array for the next Routing Key cannot reach inside a payload it already
// handed over.
func TestRoutingKeyPayloadConstructorOwnsItsGroups(t *testing.T) {
	groups := []params.RoutingKeyGroup{
		params.NewRoutingKeyGroup(params.NewDestinationPointCode(3), nil, nil),
		params.NewRoutingKeyGroup(params.NewDestinationPointCode(4), nil, nil),
	}
	payload := params.NewRoutingKeyPayload(
		params.NewLocalRoutingKeyIdentifier(1), nil, nil, nil, groups...)

	groups[0] = params.NewRoutingKeyGroup(params.NewDestinationPointCode(99), nil, nil)
	if got := payload.Groups[0].DestinationPointCode.DestinationPointCode(); got != 3 {
		t.Errorf("payload group 0 Destination Point Code = %d, want 3; the constructor "+
			"aliased the caller's slice", got)
	}
}

// The doc comments the cleanup rewrote must still describe what the code does.
// These are the claims a reader acts on, so each is checked against the source
// that is supposed to implement it.
func TestOwnershipDocumentationStillDescribesTheCode(t *testing.T) {
	source := readSource(t, "data.go")
	for _, claim := range []string{
		"d.Header.Payload aliases b",
		"must not modify or recycle b",
	} {
		if !strings.Contains(source, claim) {
			t.Errorf("data.go no longer documents %q, which TestDataMarshalToAliasesTheDestinationAsDocumented "+
				"proves is still true", claim)
		}
	}

	source = readSource(t, "params/params.go")
	for _, claim := range []string{
		"Copy returns a deep copy of a Param",
		"which writes to the caller's Param",
	} {
		if !strings.Contains(source, claim) {
			t.Errorf("params.go no longer documents %q, which "+
				"TestParamOwnershipMatchesItsDocumentedHazard proves is still true", claim)
		}
	}
}
