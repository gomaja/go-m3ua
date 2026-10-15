// Copyright 2018-2024 go-m3ua authors. All rights reserved.
// Use of this source code is governed by a MIT-style license that can be
// found in the LICENSE file.

package m3ua

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/gomaja/go-m3ua/messages"
	"github.com/gomaja/go-m3ua/messages/params"
	"github.com/gomaja/go-pointcode"
)

// RFC 4666 §3.3.1 requires unused NI bits to be zero on transmission.
func TestNetworkIndicatorSendPaths(t *testing.T) {
	for _, path := range []string{"WriteData", "WriteDataContext", "WriteSignal", "MTPTransfer", "MTPTransferContext"} {
		for _, ni := range []uint8{0, 1, 2, 3, 4, 255} {
			t.Run(fmt.Sprintf("%s/NI%d", path, ni), func(t *testing.T) {
				payload := transferProtocolData(0x123456, 1, []byte("network indicator"))
				payload.NetworkIndicator = pointcode.NetworkIndicator(ni)
				var err error
				var written, submissions int
				var sent []*messages.Data
				if strings.HasPrefix(path, "MTPTransfer") {
					endpoint, _, captures := newASPTransferFixture(t, validASPConfig())
					var result MTPTransferResult
					request := MTPTransferRequest{ProtocolData: payload}
					if path == "MTPTransfer" {
						result, err = endpoint.MTPTransfer(request)
					} else {
						ctx, cancel := context.WithCancel(context.Background())
						defer cancel()
						result, err = endpoint.MTPTransferContext(ctx, request)
					}
					written = result.UserDataOctets
					for _, capture := range captures {
						submissions += capture.count()
						if capture.count() > 0 {
							data, _ := capture.lastData(t)
							sent = append(sent, data)
						}
					}
				} else {
					association, capture := newDataWriteAssociation(t, 1)
					signalCalls := 0
					association.signalWriter = func(message messages.M3UA) (int, error) {
						signalCalls++
						if data, ok := message.(*messages.Data); ok {
							sent = append(sent, data)
						}
						return message.MarshalLen(), nil
					}
					request := DataRequest{AS: writeScope(1), ProtocolData: *payload}
					switch path {
					case "WriteData":
						written, err = association.WriteData(request)
					case "WriteDataContext":
						ctx, cancel := context.WithCancel(context.Background())
						defer cancel()
						written, err = association.WriteDataContext(ctx, request)
					case "WriteSignal":
						parameter := params.NewProtocolData(payload.OriginatingPointCode, payload.DestinationPointCode, payload.ServiceIndicator, pointcode.NetworkIndicator(ni), 0, 1, payload.Data)
						written, err = association.WriteSignal(messages.NewData(params.NewNetworkAppearance(7), params.NewRoutingContext(1), parameter, nil))
					}
					submissions = capture.submissions() + signalCalls
					sent = append(sent, capture.messages(t)...)
				}
				if ni <= 3 {
					if err != nil || written == 0 || submissions != 1 {
						t.Fatalf("valid NI %d: written=%d submissions=%d err=%v", ni, written, submissions, err)
					}
					if len(sent) != 1 {
						t.Fatalf("captured %d DATA, want 1", len(sent))
					}
					label, err := sent[0].ProtocolData.ProtocolData()
					if err != nil {
						t.Fatal(err)
					}
					if label.NetworkIndicator != pointcode.NetworkIndicator(ni) {
						t.Fatalf("sent NI=%d, want %d", label.NetworkIndicator, ni)
					}
					return
				}
				if err == nil || written != 0 || submissions != 0 {
					t.Fatalf("invalid NI %d reached transport: written=%d submissions=%d err=%v", ni, written, submissions, err)
				}
				if strings.HasPrefix(path, "MTPTransfer") {
					requireDataWriteError(t, err, DataNotSent, ErrInvalidMTPTransfer)
				} else {
					requireDataWriteError(t, err, DataNotSent, ErrInvalidParameterValue)
				}
				if !strings.Contains(err.Error(), "Network Indicator") || !strings.Contains(err.Error(), "RFC 4666") {
					t.Fatalf("refusal does not name NI and its governing clause: %v", err)
				}
			})
		}
	}
}

func TestNetworkIndicatorReceivePreservesUnknownOctet(t *testing.T) {
	association, _ := newDataWriteAssociation(t, 1)
	data := inboundData(1, "unknown NI")
	data.ProtocolData.Data[9] = 7
	deliver(t, association, 1, data)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	received, err := association.ReadData(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if received.ProtocolData.NetworkIndicator != 7 {
		t.Fatalf("received NI=%d, want 7", received.ProtocolData.NetworkIndicator)
	}
	if received.ProtocolData.NetworkIndicator.Valid() {
		t.Fatalf("received NI must expose Valid() false: %v", received.ProtocolData.NetworkIndicator)
	}
}

func TestNetworkIndicatorDistribution(t *testing.T) {
	for _, ni := range []uint8{0, 1, 2, 3, 4, 255} {
		t.Run(fmt.Sprint(ni), func(t *testing.T) {
			listener, server, association, capture := distributionFixture(t, params.TrafficModeLoadshare)
			server.setASPState(association, StateASPActive, time.Hour)
			capture.reset()
			data := distributionData(1, 1, "NI distribution")
			data.ProtocolData.Data[9] = ni
			result, err := listener.DistributeData(data)
			if ni <= 3 {
				if err != nil || result.Delivered != 1 || len(capture.data()) != 1 {
					t.Fatalf("valid NI: result=%+v err=%v", result, err)
				}
			} else {
				if !errors.Is(err, ErrInvalidParameterValue) || result.Delivered != 0 || result.Queued || len(capture.data()) != 0 {
					t.Fatalf("invalid NI reached distribution: result=%+v err=%v", result, err)
				}
			}
		})
	}
}

// AS-PENDING must refuse invalid NI before retaining a message for recovery.
func TestNetworkIndicatorDistributionPending(t *testing.T) {
	for _, ni := range []byte{4, 255} {
		t.Run(fmt.Sprint(ni), func(t *testing.T) {
			listener, server, association, capture := distributionFixture(t, params.TrafficModeLoadshare)
			server.setASPState(association, StateASPActive, time.Hour)
			server.setASPState(association, StateASPInactive, time.Hour)
			capture.reset()
			data := distributionData(1, 1, "invalid NI must not queue")
			data.ProtocolData.Data[9] = ni
			result, err := listener.DistributeData(data)
			if !errors.Is(err, ErrInvalidParameterValue) || result.Queued || result.Delivered != 0 {
				t.Fatalf("invalid NI retained while pending: result=%+v err=%v", result, err)
			}
			server.mu.Lock()
			queued := len(server.recoveryQueue)
			server.mu.Unlock()
			if queued != 0 || capture.dataCount() != 0 {
				t.Fatalf("invalid NI queued=%d sent=%d", queued, capture.dataCount())
			}
		})
	}
}

var _ pointcode.NetworkIndicator = DataQueueOverflowError{}.NetworkIndicator
