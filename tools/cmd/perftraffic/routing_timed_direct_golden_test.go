package main

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/gomaja/go-m3ua"
)

var errRoutingTimedGoldenWrite = errors.New("routing timed golden write failure")

type routingTimedGoldenAssociation struct {
	id          m3ua.AssociationID
	epoch       uint64
	maximum     uint16
	written     int
	writeErr    error
	afterWrite  func(*routingTimedGoldenAssociation)
	accessors   []string
	writes      []m3ua.DataRequest
	readDataErr error
}

func (association *routingTimedGoldenAssociation) ID() m3ua.AssociationID {
	return association.id
}

func (association *routingTimedGoldenAssociation) Epoch() uint64 {
	association.accessors = append(association.accessors, "epoch")
	return association.epoch
}

func (association *routingTimedGoldenAssociation) MaxMessageStreamID() uint16 {
	association.accessors = append(association.accessors, "maximum")
	return association.maximum
}

func (association *routingTimedGoldenAssociation) ReadData(context.Context) (*m3ua.DataMessage, error) {
	if association.readDataErr != nil {
		return nil, association.readDataErr
	}
	return nil, errors.New("routing timed golden association does not provide reads")
}

func (association *routingTimedGoldenAssociation) WriteData(request m3ua.DataRequest) (int, error) {
	association.accessors = append(association.accessors, "write")
	request.ProtocolData.Data = append([]byte(nil), request.ProtocolData.Data...)
	association.writes = append(association.writes, request)
	if association.afterWrite != nil {
		association.afterWrite(association)
	}
	written := association.written
	if written < 0 {
		written = len(request.ProtocolData.Data)
	}
	return written, association.writeErr
}

func (*routingTimedGoldenAssociation) DataQueueStats() m3ua.DataQueueStats {
	return m3ua.DataQueueStats{}
}

func routingTimedGoldenWriter(testContext *testing.T) (*routingDirectWriter, *routingTimedGoldenAssociation, []byte) {
	testContext.Helper()
	topology, bindings, observations := routingPreflightFixture(testContext, "primary")
	paths, err := freezeRoutingPaths(topology, bindings, observations, "preflight", 7)
	if err != nil {
		testContext.Fatal(err)
	}
	associations := make(map[m3ua.AssociationID]routingDataAssociation, len(bindings))
	epochs := make(map[m3ua.AssociationID]uint64, len(bindings))
	var selected *routingTimedGoldenAssociation
	for index, binding := range bindings {
		association := &routingTimedGoldenAssociation{
			id: binding.SenderAssociation, epoch: uint64(10 + index), maximum: binding.MaxMessageStreamID, written: -1,
		}
		associations[binding.SenderAssociation] = association
		epochs[binding.SenderAssociation] = association.epoch
		if index == 0 {
			selected = association
		}
	}
	payload, err := buildRoutePayload(planRouteMessage("timed-golden", 9, 0), 128)
	if err != nil {
		testContext.Fatal(err)
	}
	return &routingDirectWriter{paths: paths, associations: associations, epochs: epochs, admission: make(chan struct{}, 1)}, selected, payload
}

func TestRoutingTimedGoldenDirectWriterOrdering(testContext *testing.T) {
	for _, testCase := range []struct {
		name          string
		prepare       func(*routingDirectWriter, *routingTimedGoldenAssociation) context.Context
		written       int
		writeErr      error
		wantWritten   int
		wantError     string
		wantErrorIs   error
		wantAccessors []string
		wantWrites    int
	}{
		{name: "success", written: -1, wantWritten: 128, wantAccessors: []string{"epoch", "maximum", "write", "epoch", "maximum"}, wantWrites: 1},
		{name: "already-canceled", written: -1, prepare: func(_ *routingDirectWriter, _ *routingTimedGoldenAssociation) context.Context {
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			return ctx
		}, wantErrorIs: context.Canceled},
		{name: "zero-expected-epoch", written: -1, prepare: func(writer *routingDirectWriter, association *routingTimedGoldenAssociation) context.Context {
			writer.epochs[association.id] = 0
			return context.Background()
		}, wantError: "changed after preflight"},
		{name: "stale-before-epoch", written: -1, prepare: func(_ *routingDirectWriter, association *routingTimedGoldenAssociation) context.Context {
			association.epoch++
			return context.Background()
		}, wantError: "changed after preflight", wantAccessors: []string{"epoch"}},
		{name: "stale-before-maximum", written: -1, prepare: func(_ *routingDirectWriter, association *routingTimedGoldenAssociation) context.Context {
			association.maximum++
			return context.Background()
		}, wantError: "changed after preflight", wantAccessors: []string{"epoch", "maximum"}},
		{name: "write-error", written: 0, writeErr: errRoutingTimedGoldenWrite, wantErrorIs: errRoutingTimedGoldenWrite, wantAccessors: []string{"epoch", "maximum", "write", "epoch", "maximum"}, wantWrites: 1},
		{name: "partial-write", written: 127, wantWritten: 127, wantError: "accepted 127 octets, want 128", wantAccessors: []string{"epoch", "maximum", "write", "epoch", "maximum"}, wantWrites: 1},
		{name: "changed-after-epoch", written: -1, prepare: func(_ *routingDirectWriter, association *routingTimedGoldenAssociation) context.Context {
			association.afterWrite = func(changed *routingTimedGoldenAssociation) { changed.epoch++ }
			return context.Background()
		}, wantWritten: 128, wantError: "changed during write", wantAccessors: []string{"epoch", "maximum", "write", "epoch"}, wantWrites: 1},
		{name: "changed-after-maximum", written: -1, prepare: func(_ *routingDirectWriter, association *routingTimedGoldenAssociation) context.Context {
			association.afterWrite = func(changed *routingTimedGoldenAssociation) { changed.maximum++ }
			return context.Background()
		}, wantWritten: 128, wantError: "changed during write", wantAccessors: []string{"epoch", "maximum", "write", "epoch", "maximum"}, wantWrites: 1},
		{name: "partial-write-error-changed-after-epoch", written: 127, writeErr: errRoutingTimedGoldenWrite, prepare: func(_ *routingDirectWriter, association *routingTimedGoldenAssociation) context.Context {
			association.afterWrite = func(changed *routingTimedGoldenAssociation) { changed.epoch++ }
			return context.Background()
		}, wantWritten: 127, wantError: "changed during write", wantErrorIs: errRoutingTimedGoldenWrite, wantAccessors: []string{"epoch", "maximum", "write", "epoch"}, wantWrites: 1},
	} {
		testContext.Run(testCase.name, func(testContext *testing.T) {
			writer, association, payload := routingTimedGoldenWriter(testContext)
			association.written = testCase.written
			association.writeErr = testCase.writeErr
			ctx := context.Background()
			if testCase.prepare != nil {
				ctx = testCase.prepare(writer, association)
			}
			written, err := writer.Write(ctx, 0, payload)
			if written != testCase.wantWritten || len(association.writes) != testCase.wantWrites || strings.Join(association.accessors, ",") != strings.Join(testCase.wantAccessors, ",") {
				testContext.Fatalf("written=%d writes=%d accessors=%v error=%v", written, len(association.writes), association.accessors, err)
			}
			if testCase.wantErrorIs != nil {
				if !errors.Is(err, testCase.wantErrorIs) {
					testContext.Fatalf("error=%v, want errors.Is(%v)", err, testCase.wantErrorIs)
				}
			}
			if testCase.wantError == "" && testCase.wantErrorIs == nil && err != nil {
				testContext.Fatal(err)
			}
			if testCase.wantError != "" && (err == nil || !strings.Contains(err.Error(), testCase.wantError)) {
				testContext.Fatalf("error=%v, want containing %q", err, testCase.wantError)
			}
		})
	}
}
