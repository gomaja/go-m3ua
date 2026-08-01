// Copyright 2018-2024 go-m3ua authors. All rights reserved.
// Use of this source code is governed by a MIT-style license that can be
// found in the LICENSE file.

package m3ua

import (
	"bytes"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gomaja/go-m3ua/messages"
	"github.com/gomaja/go-m3ua/messages/params"
)

func TestDistributeDataRejectsMalformedKnownParameterLengths(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*messages.Data)
	}{
		{
			name: "empty Protocol Data",
			mutate: func(data *messages.Data) {
				data.ProtocolData.Data = nil
			},
		},
		{
			name: "eleven-octet Protocol Data",
			mutate: func(data *messages.Data) {
				data.ProtocolData.Data = make([]byte, 11)
			},
		},
		{
			name: "empty Routing Context",
			mutate: func(data *messages.Data) {
				data.RoutingContext.Data = nil
			},
		},
		{
			name: "three-octet Routing Context",
			mutate: func(data *messages.Data) {
				data.RoutingContext.Data = make([]byte, 3)
			},
		},
		{
			name: "multiple DATA Routing Contexts",
			mutate: func(data *messages.Data) {
				data.RoutingContext = params.NewRoutingContext(1, 1)
			},
		},
		{
			name: "empty Network Appearance",
			mutate: func(data *messages.Data) {
				data.NetworkAppearance.Data = nil
			},
		},
		{
			name: "short Network Appearance",
			mutate: func(data *messages.Data) {
				data.NetworkAppearance.Data = make([]byte, 3)
			},
		},
		{
			name: "long Network Appearance",
			mutate: func(data *messages.Data) {
				data.NetworkAppearance.Data = make([]byte, 5)
			},
		},
		{
			name: "empty Correlation ID",
			mutate: func(data *messages.Data) {
				data.CorrelationID = params.NewParam(int(params.CorrelationID), nil)
			},
		},
		{
			name: "long Correlation ID",
			mutate: func(data *messages.Data) {
				data.CorrelationID = params.NewParam(int(params.CorrelationID), make([]byte, 5))
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			listener, applicationServer, asp, sent := distributionFixture(t, params.TrafficModeLoadshare)
			applicationServer.setASPState(asp, StateAspActive, time.Hour)
			sent.reset()

			data := distributionData(1, 1, "payload")
			data.CorrelationID = params.NewCorrelationID(7)
			test.mutate(data)
			if _, err := listener.DistributeData(data); !errors.Is(err, params.ErrInvalidLength) {
				t.Fatalf("DistributeData() error = %v, want params.ErrInvalidLength", err)
			}
			if got := sent.dataCount(); got != 0 {
				t.Fatalf("malformed DATA delivered %d messages", got)
			}
		})
	}
}

func TestNilListenerRejectsDistribution(t *testing.T) {
	var listener *Listener
	if _, err := listener.DistributeData(distributionData(1, 1, "payload")); err == nil {
		t.Fatal("nil Listener DistributeData() error = nil, want non-nil")
	}
}

func TestDistributeDataRejectsWrongKnownParameterTypes(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*messages.Data)
	}{
		{
			name: "Protocol Data",
			mutate: func(data *messages.Data) {
				data.ProtocolData.Tag = params.NetworkAppearance
			},
		},
		{
			name: "Routing Context",
			mutate: func(data *messages.Data) {
				data.RoutingContext.Tag = params.CorrelationID
			},
		},
		{
			name: "Network Appearance",
			mutate: func(data *messages.Data) {
				data.NetworkAppearance.Tag = params.RoutingContext
			},
		},
		{
			name: "Correlation ID",
			mutate: func(data *messages.Data) {
				data.CorrelationID.Tag = params.NetworkAppearance
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			listener, applicationServer, asp, sent := distributionFixture(t, params.TrafficModeLoadshare)
			applicationServer.setASPState(asp, StateAspActive, time.Hour)
			sent.reset()

			data := distributionData(1, 1, "payload")
			data.CorrelationID = params.NewCorrelationID(7)
			test.mutate(data)
			if _, err := listener.DistributeData(data); !errors.Is(err, params.ErrInvalidType) {
				t.Fatalf("DistributeData() error = %v, want params.ErrInvalidType", err)
			}
			if got := sent.dataCount(); got != 0 {
				t.Fatalf("mistyped DATA delivered %d messages", got)
			}
		})
	}
}

func TestDistributeDataRejectsInvalidEnvelopeAndDuplicateKnownParameters(t *testing.T) {
	tests := []struct {
		name string
		data *messages.Data
		want error
	}{
		{name: "nil DATA"},
		{name: "nil header", data: func() *messages.Data {
			data := distributionData(1, 1, "payload")
			data.Header = nil
			return data
		}()},
		{name: "invalid version", data: func() *messages.Data {
			data := distributionData(1, 1, "payload")
			data.Header.Version = 2
			return data
		}()},
		{name: "reserved byte", data: func() *messages.Data {
			data := distributionData(1, 1, "payload")
			data.Header.Reserved = 1
			return data
		}(), want: ErrInvalidParameterValue},
		{name: "wrong class", data: func() *messages.Data {
			data := distributionData(1, 1, "payload")
			data.Header.Class = messages.MsgClassManagement
			return data
		}(), want: messages.ErrUnexpectedMessageType},
		{name: "wrong type", data: func() *messages.Data {
			data := distributionData(1, 1, "payload")
			data.Header.Type++
			return data
		}(), want: messages.ErrUnexpectedMessageType},
		{name: "missing Protocol Data", data: func() *messages.Data {
			data := distributionData(1, 1, "payload")
			data.ProtocolData = nil
			return data
		}(), want: ErrMissingProtocolData},
		{name: "duplicate Routing Context", data: func() *messages.Data {
			data := distributionData(1, 1, "payload")
			data.Others = []*params.Param{params.NewRoutingContext(1)}
			return data
		}(), want: messages.ErrInvalidParameter},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			listener, applicationServer, asp, sent := distributionFixture(t, params.TrafficModeLoadshare)
			applicationServer.setASPState(asp, StateAspActive, time.Hour)
			sent.reset()

			_, err := listener.DistributeData(test.data)
			if test.want == nil {
				if err == nil {
					t.Fatal("DistributeData() error = nil, want non-nil")
				}
			} else if !errors.Is(err, test.want) {
				t.Fatalf("DistributeData() error = %v, want %v", err, test.want)
			}
			if got := sent.dataCount(); got != 0 {
				t.Fatalf("invalid DATA delivered %d messages", got)
			}
		})
	}
}

func TestDistributeDataAcceptsProtocolDataLengthBoundaries(t *testing.T) {
	const maximumParameterValueLength = int(^uint16(0)) - 4
	tests := []struct {
		name        string
		payloadSize int
		wantError   bool
	}{
		{name: "minimum", payloadSize: 0},
		{name: "maximum", payloadSize: maximumParameterValueLength - 12},
		{name: "one over maximum", payloadSize: maximumParameterValueLength - 11, wantError: true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			listener, applicationServer, asp, sent := distributionFixture(t, params.TrafficModeLoadshare)
			applicationServer.setASPState(asp, StateAspActive, time.Hour)
			sent.reset()

			data := distributionData(1, 1, "")
			data.ProtocolData = params.NewProtocolData(1, 2, params.ServiceIndSCCP, 0, 0, 1, make([]byte, test.payloadSize))
			result, err := listener.DistributeData(data)
			if test.wantError {
				if !errors.Is(err, params.ErrInvalidLength) {
					t.Fatalf("DistributeData() error = %v, want params.ErrInvalidLength", err)
				}
				if got := sent.dataCount(); got != 0 {
					t.Fatalf("oversized DATA delivered %d messages", got)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if result.Delivered != 1 || result.Queued {
				t.Fatalf("distribution result = %#v, want one immediate delivery", result)
			}
			if got := len(dataPayload(t, onlyData(t, sent.data()))); got != test.payloadSize {
				t.Fatalf("delivered payload length = %d, want %d", got, test.payloadSize)
			}
		})
	}
}

func TestDistributeDataResolvesAndValidatesRoutingContext(t *testing.T) {
	t.Run("omitted with one configured AS", func(t *testing.T) {
		listener, applicationServer, asp, sent := distributionFixture(t, params.TrafficModeLoadshare)
		applicationServer.setASPState(asp, StateAspActive, time.Hour)
		sent.reset()

		data := distributionData(1, 1, "implicit")
		data.RoutingContext = nil
		if _, err := listener.DistributeData(data); err != nil {
			t.Fatal(err)
		}
		if got := onlyData(t, sent.data()).RoutingContext.RoutingContext(); got != 1 {
			t.Fatalf("delivered Routing Context = %d, want 1", got)
		}
		if data.RoutingContext != nil {
			t.Fatalf("distribution mutated caller Routing Context to %v", data.RoutingContext)
		}
	})

	t.Run("omitted with multiple configured ASes", func(t *testing.T) {
		listener, applicationServer, asp, sent := distributionFixtureForContexts(
			t, params.TrafficModeLoadshare, []uint32{1, 2}, nil,
		)
		applicationServer.setASPState(asp, StateAspActive, time.Hour)
		sent.reset()

		data := distributionData(1, 1, "ambiguous")
		data.RoutingContext = nil
		if _, err := listener.DistributeData(data); !errors.Is(err, ErrMissingRoutingContext) {
			t.Fatalf("DistributeData() error = %v, want ErrMissingRoutingContext", err)
		}
		if got := sent.dataCount(); got != 0 {
			t.Fatalf("ambiguous DATA delivered %d messages", got)
		}
	})

	t.Run("explicit unconfigured AS", func(t *testing.T) {
		listener, applicationServer, asp, sent := distributionFixture(t, params.TrafficModeLoadshare)
		applicationServer.setASPState(asp, StateAspActive, time.Hour)
		sent.reset()

		if _, err := listener.DistributeData(distributionData(99, 1, "foreign")); !errors.Is(err, ErrInvalidRoutingContext) {
			t.Fatalf("DistributeData() error = %v, want ErrInvalidRoutingContext", err)
		} else {
			var contextError *RoutingContextError
			if !errors.As(err, &contextError) || len(contextError.Contexts) != 1 || contextError.Contexts[0] != 99 {
				t.Fatalf("routing-context error = %#v, want offending context 99", err)
			}
		}
		if got := sent.dataCount(); got != 0 {
			t.Fatalf("foreign DATA delivered %d messages", got)
		}
	})

	t.Run("omitted with one registered AS and no Config parameter", func(t *testing.T) {
		listener, applicationServer, asp, sent := distributionFixtureConfigured(t, params.TrafficModeLoadshare, func(config *Config) {
			config.RoutingContexts = nil
		})
		applicationServer.setASPState(asp, StateAspActive, time.Hour)
		sent.reset()

		data := distributionData(1, 1, "registered")
		data.RoutingContext = nil
		if _, err := listener.DistributeData(data); err != nil {
			t.Fatal(err)
		}
		if got := onlyData(t, sent.data()).RoutingContext.RoutingContext(); got != 1 {
			t.Fatalf("delivered Routing Context = %d, want registered context 1", got)
		}
	})
}

func TestDistributeDataValidatesNetworkAppearanceAndPreservesCorrelationID(t *testing.T) {
	t.Run("matching values", func(t *testing.T) {
		listener, applicationServer, asp, sent := distributionFixture(t, params.TrafficModeLoadshare)
		applicationServer.setASPState(asp, StateAspActive, time.Hour)
		sent.reset()

		data := distributionData(1, 1, "matching")
		data.CorrelationID = params.NewCorrelationID(0x01020304)
		if _, err := listener.DistributeData(data); err != nil {
			t.Fatal(err)
		}
		delivered := onlyData(t, sent.data())
		if got := delivered.NetworkAppearance.NetworkAppearance(); got != 0 {
			t.Fatalf("Network Appearance = %d, want 0", got)
		}
		if got := delivered.CorrelationID.CorrelationID(); got != 0x01020304 {
			t.Fatalf("Correlation ID = %#x, want %#x", got, uint32(0x01020304))
		}
	})

	t.Run("omitted optional Network Appearance", func(t *testing.T) {
		listener, applicationServer, asp, sent := distributionFixture(t, params.TrafficModeLoadshare)
		applicationServer.setASPState(asp, StateAspActive, time.Hour)
		sent.reset()

		data := distributionData(1, 1, "omitted")
		data.NetworkAppearance = nil
		if _, err := listener.DistributeData(data); err != nil {
			t.Fatal(err)
		}
		if got := onlyData(t, sent.data()).NetworkAppearance; got != nil {
			t.Fatalf("omitted Network Appearance became %v", got)
		}
	})

	t.Run("unconfigured value", func(t *testing.T) {
		listener, applicationServer, asp, sent := distributionFixture(t, params.TrafficModeLoadshare)
		applicationServer.setASPState(asp, StateAspActive, time.Hour)
		sent.reset()

		data := distributionData(1, 1, "foreign network")
		data.NetworkAppearance = params.NewNetworkAppearance(99)
		if _, err := listener.DistributeData(data); !errors.Is(err, ErrInvalidNetworkAppearance) {
			t.Fatalf("DistributeData() error = %v, want ErrInvalidNetworkAppearance", err)
		} else {
			var appearanceError *NetworkAppearanceError
			if !errors.As(err, &appearanceError) || appearanceError.Appearance != 99 {
				t.Fatalf("network-appearance error = %#v, want offending appearance 99", err)
			}
		}
		if got := sent.dataCount(); got != 0 {
			t.Fatalf("foreign-network DATA delivered %d messages", got)
		}
	})
}

func TestBroadcastFlowIdentifierAcceptsExactConfiguredLimit(t *testing.T) {
	listener, applicationServer, asp, sent := distributionFixtureConfigured(t, params.TrafficModeBroadcast, func(config *Config) {
		config.BroadcastFlowIdentifierBytes = 4
		config.BroadcastFlowIdentifier = func(*params.ProtocolDataPayload) (string, error) {
			return "1234", nil
		}
	})
	applicationServer.setASPState(asp, StateAspActive, time.Hour)
	sent.reset()

	if _, err := listener.DistributeData(distributionData(1, 1, "exact boundary")); err != nil {
		t.Fatal(err)
	}
	if correlation := onlyData(t, sent.data()).CorrelationID; correlation == nil {
		t.Fatal("exact-limit Broadcast flow was not delivered with a Correlation ID")
	}
}

func TestDistributeDataOwnsAllCallerParametersAndUnknownExtensions(t *testing.T) {
	listener, applicationServer, asp, sent := distributionFixture(t, params.TrafficModeLoadshare)
	applicationServer.setASPState(asp, StateAspActive, time.Hour)
	sent.reset()

	unknownValue := []byte{0x11, 0x22, 0x33}
	data := distributionData(1, 5, "owned payload")
	data.CorrelationID = params.NewCorrelationID(0x01020304)
	data.Others = []*params.Param{nil, params.NewParam(0xf001, unknownValue)}
	if _, err := listener.DistributeData(data); err != nil {
		t.Fatal(err)
	}
	if data.Header.Payload != nil {
		t.Fatalf("distribution populated caller Header payload with %d octets", len(data.Header.Payload))
	}
	delivered := onlyData(t, sent.data())
	wantWire, err := delivered.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}

	for _, parameter := range []*params.Param{
		data.NetworkAppearance,
		data.RoutingContext,
		data.ProtocolData,
		data.CorrelationID,
		data.Others[1],
	} {
		for index := range parameter.Data {
			parameter.Data[index] ^= 0xff
		}
	}
	for index := range unknownValue {
		unknownValue[index] = 0
	}
	data.Header.Version = 2
	data.Others = nil

	gotWire, err := delivered.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(gotWire, wantWire) {
		t.Fatalf("delivered DATA aliases caller memory:\n got % x\nwant % x", gotWire, wantWire)
	}
	if len(delivered.Others) != 1 || delivered.Others[0].Tag != 0xf001 || !bytes.Equal(delivered.Others[0].Data, []byte{0x11, 0x22, 0x33}) {
		t.Fatalf("delivered unknown parameters = %#v, want one owned extension", delivered.Others)
	}
}

func TestBroadcastRecipientsReceiveIsolatedDataCopies(t *testing.T) {
	listener, applicationServer, first, _ := distributionFixture(t, params.TrafficModeBroadcast)
	second, secondSent := addDistributionASP(t, listener, StateAspInactive, 1)
	applicationServer.setASPState(first, StateAspActive, time.Hour)
	applicationServer.setASPState(second, StateAspActive, time.Hour)
	secondSent.reset()

	var firstCopy *messages.Data
	first.signalWriter = func(message messages.M3UA) (int, error) {
		data, ok := message.(*messages.Data)
		if !ok {
			return 0, fmt.Errorf("got %T, want *messages.Data", message)
		}
		firstCopy = data
		data.NetworkAppearance.Data[3] = 9
		data.RoutingContext.Data[3] = 9
		data.ProtocolData.Data[12] = 0xff
		data.CorrelationID.Data[3] = 0xff
		data.Others[0].Data[0] = 0xff
		return data.MarshalLen(), nil
	}

	input := distributionData(1, 4, "isolated")
	input.Others = []*params.Param{params.NewParam(0xf001, []byte{0x11})}
	if _, err := listener.DistributeData(input); err != nil {
		t.Fatal(err)
	}
	secondCopy := onlyData(t, secondSent.data())
	if firstCopy == nil || firstCopy == secondCopy {
		t.Fatalf("Broadcast recipient pointers = %p and %p, want distinct", firstCopy, secondCopy)
	}
	if got := secondCopy.NetworkAppearance.NetworkAppearance(); got != 0 {
		t.Fatalf("second Network Appearance = %d, want 0", got)
	}
	if got := secondCopy.RoutingContext.RoutingContext(); got != 1 {
		t.Fatalf("second Routing Context = %d, want 1", got)
	}
	if got := string(dataPayload(t, secondCopy)); got != "isolated" {
		t.Fatalf("second payload = %q, want isolated", got)
	}
	if secondCopy.CorrelationID == nil || secondCopy.CorrelationID.CorrelationID() != 1 {
		t.Fatalf("second Correlation ID = %v, want isolated generated value 1", secondCopy.CorrelationID)
	}
	if len(secondCopy.Others) != 1 || !bytes.Equal(secondCopy.Others[0].Data, []byte{0x11}) {
		t.Fatalf("second unknown parameters = %#v, want isolated extension", secondCopy.Others)
	}
	if input.CorrelationID != nil {
		t.Fatalf("Broadcast distribution mutated caller Correlation ID to %v", input.CorrelationID)
	}
}

func TestDistributionPolicyDoesNotRaceConcurrentConfigMutation(t *testing.T) {
	var classifierCalls atomic.Int32
	listener, applicationServer, asp, sent := distributionFixtureConfigured(t, params.TrafficModeBroadcast, func(config *Config) {
		config.BroadcastFlowIdentifier = func(*params.ProtocolDataPayload) (string, error) {
			classifierCalls.Add(1)
			return "snapshotted", nil
		}
	})
	applicationServer.setASPState(asp, StateAspActive, time.Hour)
	sent.reset()

	configuredRoutingContexts := listener.Config.RoutingContexts
	configuredNetworkAppearance := listener.Config.NetworkAppearance
	stop := make(chan struct{})
	var mutations sync.WaitGroup
	mutations.Add(1)
	go func() {
		defer mutations.Done()
		for iteration := 0; ; iteration++ {
			select {
			case <-stop:
				return
			default:
			}
			configuredRoutingContexts.Data[3] = byte(2 + iteration%200)
			configuredNetworkAppearance.Data[3] = byte(2 + iteration%200)
			listener.Config.RoutingContexts = params.NewRoutingContext(uint32(2 + iteration%200))
			listener.Config.NetworkAppearance = params.NewNetworkAppearance(uint32(2 + iteration%200))
			listener.Config.RecoveryQueueMessages = iteration + 1
			listener.Config.RecoveryQueueBytes = iteration + 1
			listener.Config.BroadcastFlowCacheEntries = iteration + 1
			listener.Config.BroadcastFlowIdentifierBytes = iteration + 1
			listener.Config.BroadcastFlowIdentifier = func(*params.ProtocolDataPayload) (string, error) {
				return "live Config callback must not run", errors.New("live Config callback ran")
			}
		}
	}()

	for iteration := 0; iteration < 200; iteration++ {
		result, err := listener.DistributeData(distributionData(1, uint8(iteration), "race"))
		if err != nil {
			close(stop)
			mutations.Wait()
			t.Fatalf("distribution %d observed mutable Config: %v", iteration, err)
		}
		if result.Delivered != 1 || result.Queued {
			close(stop)
			mutations.Wait()
			t.Fatalf("distribution %d result = %#v, want one delivery", iteration, result)
		}
	}
	close(stop)
	mutations.Wait()
	if got := classifierCalls.Load(); got != 200 {
		t.Fatalf("snapshotted classifier calls = %d, want 200", got)
	}
	if got := sent.dataCount(); got != 200 {
		t.Fatalf("delivered DATA count = %d, want 200", got)
	}
}

func FuzzPrepareDistributionDataKnownParameterLengths(f *testing.F) {
	f.Add(uint8(0), true, []byte{0, 0, 0, 0})
	f.Add(uint8(1), true, []byte{0, 0, 0, 1})
	f.Add(uint8(2), true, make([]byte, 12))
	f.Add(uint8(3), true, []byte{0, 0, 0, 1})
	f.Add(uint8(0), true, []byte{0, 0, 0})
	f.Add(uint8(2), false, make([]byte, 12))

	f.Fuzz(func(t *testing.T, slot uint8, correctTag bool, value []byte) {
		if len(value) > int(^uint16(0)) {
			t.Skip()
		}
		config := NewServerConfig(
			&HeartbeatInfo{Enabled: false}, 1, 2, 0, params.TrafficModeLoadshare, 0, 0,
			[]uint32{1}, params.ServiceIndSCCP, 0, 0, 1,
		)
		registry := newApplicationServers(time.Hour, config)
		registry.get(1)
		listener := &Listener{Config: config, as: registry}
		data := distributionData(1, 1, "payload")
		data.CorrelationID = params.NewCorrelationID(1)

		slot %= 4
		expectedTags := [...]uint16{
			params.NetworkAppearance,
			params.RoutingContext,
			params.ProtocolData,
			params.CorrelationID,
		}
		tag := expectedTags[slot]
		if !correctTag {
			tag = 0xf001
		}
		parameter := params.NewParam(int(tag), bytes.Clone(value))
		switch slot {
		case 0:
			data.NetworkAppearance = parameter
		case 1:
			data.RoutingContext = parameter
		case 2:
			data.ProtocolData = parameter
		case 3:
			data.CorrelationID = parameter
		}

		owned, _, _, _, err := listener.prepareDistributionData(registry, registry.distribution, data)
		if !correctTag {
			if !errors.Is(err, params.ErrInvalidType) {
				t.Fatalf("slot %d wrong-tag error = %v, want params.ErrInvalidType", slot, err)
			}
			return
		}

		validLength := false
		switch slot {
		case 0, 1, 3:
			validLength = len(value) == 4
		case 2:
			validLength = len(value) >= 12 && len(value) <= int(^uint16(0))-4
		}
		if !validLength {
			if !errors.Is(err, params.ErrInvalidLength) {
				t.Fatalf("slot %d length %d error = %v, want params.ErrInvalidLength", slot, len(value), err)
			}
			return
		}

		switch slot {
		case 0:
			if parameter.NetworkAppearance() == 0 {
				if err != nil {
					t.Fatalf("configured Network Appearance error = %v, want nil", err)
				}
			} else if !errors.Is(err, ErrInvalidNetworkAppearance) {
				t.Fatalf("unconfigured Network Appearance error = %v, want ErrInvalidNetworkAppearance", err)
			}
		case 1:
			if parameter.RoutingContext() == 1 {
				if err != nil {
					t.Fatalf("configured Routing Context error = %v, want nil", err)
				}
			} else if !errors.Is(err, ErrInvalidRoutingContext) {
				t.Fatalf("unconfigured Routing Context error = %v, want ErrInvalidRoutingContext", err)
			}
		case 2, 3:
			if err != nil {
				t.Fatalf("valid slot %d parameter error = %v, want nil", slot, err)
			}
		}
		if err == nil {
			if _, marshalErr := owned.MarshalBinary(); marshalErr != nil {
				t.Fatalf("accepted DATA does not marshal: %v", marshalErr)
			}
		}
	})
}
