// Copyright 2018-2024 go-m3ua authors. All rights reserved.
// Use of this source code is governed by a MIT-style license that can be
// found in the LICENSE file.

package m3ua

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/gomaja/go-m3ua/messages"
	"github.com/gomaja/go-m3ua/messages/params"
	"github.com/gomaja/go-sctp"
)

func TestPositiveAllocationFailureRemainsIndeterminateOnEveryDataPath(t *testing.T) {
	for _, path := range []string{"typed", "typed context", "raw", "raw signal seam"} {
		for _, fullCount := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/full=%v", path, fullCount), func(t *testing.T) {
				conn, _ := newDataWriteAssociation(t, 1)
				conn.signalWriter = nil
				written := func(size int) int {
					if fullCount {
						return size
					}
					return 1
				}
				conn.dataWriter = func(frame []byte, _ *sctp.SndInfo) (int, error) {
					return written(len(frame)), syscall.ENOMEM
				}
				request := DataRequest{AS: writeScope(1), ProtocolData: simpleProtocolData("possibly queued")}
				var err error
				switch path {
				case "typed":
					_, err = conn.WriteData(request)
				case "typed context":
					ctx, cancel := context.WithCancel(context.Background())
					defer cancel()
					_, err = conn.WriteDataContext(ctx, request)
				default:
					if path == "raw signal seam" {
						conn.signalWriter = func(m messages.M3UA) (int, error) {
							return written(m.MarshalLen()), syscall.ENOMEM
						}
					}
					_, err = conn.WriteSignal(messages.NewData(
						params.NewNetworkAppearance(7), params.NewRoutingContext(1),
						params.NewProtocolData(1, 2, params.ServiceIndSCCP, 0, 0, 1, []byte("possibly queued")), nil,
					))
				}
				requireDataWriteError(t, err, DataSendIndeterminate, syscall.ENOMEM)
			})
		}
	}
}

func TestRecoveryRetriesOnlyWholeAllocationRefusals(t *testing.T) {
	for _, mode := range []uint32{params.TrafficModeLoadshare, params.TrafficModeOverride, params.TrafficModeBroadcast} {
		for _, failure := range []struct {
			name      string
			count     int
			err       error
			wantRetry bool
		}{
			{"nothing queued", 0, syscall.ENOMEM, true},
			{"positive acceptance", 1, syscall.ENOMEM, false},
			{"unknown refusal", 0, errors.New("unproven transport failure"), false},
		} {
			t.Run(fmt.Sprintf("mode=%d/%s", mode, failure.name), func(t *testing.T) {
				listener, as, peer, sent := distributionFixture(t, mode)
				as.setASPState(peer, StateASPActive, time.Hour)
				as.setASPState(peer, StateASPInactive, time.Hour)
				first := distributionData(1, 0, "first")
				second := distributionData(1, 0, "second")
				for _, data := range []*messages.Data{first, second} {
					result, err := listener.DistributeData(data)
					if err != nil || !result.Queued {
						t.Fatalf("pending DATA was not queued: %+v, %v", result, err)
					}
				}
				retrying, release := make(chan struct{}), make(chan struct{})
				var once sync.Once
				t.Cleanup(func() { once.Do(func() { close(release) }) })
				var attempts atomic.Int32
				peer.signalWriter = func(m messages.M3UA) (int, error) {
					if data, ok := m.(*messages.Data); ok {
						payload, err := data.ProtocolData.ProtocolData()
						if err != nil {
							return 0, err
						}
						if string(payload.Data) == "first" {
							switch attempts.Add(1) {
							case 1:
								return failure.count, failure.err
							case 2:
								close(retrying)
								<-release
							}
						}
					}
					return sent.write(m)
				}
				sent.reset()
				as.setASPState(peer, StateASPActive, time.Hour)
				if failure.wantRetry {
					select {
					case <-retrying:
					case <-time.After(time.Second):
						messages, bytes := recoveryBudgetUsage(listener.as.recoveryBudget)
						t.Fatalf("whole allocation refusal discarded pending DATA: first attempts=%d, delivered=%d, budget=%d/%d",
							attempts.Load(), sent.dataCount(), messages, bytes)
					}
					messages, bytes := recoveryBudgetUsage(listener.as.recoveryBudget)
					if messages != 2 || bytes != first.MarshalLen()+second.MarshalLen() {
						t.Fatalf("retry lost its FIFO reservation: budget=%d/%d", messages, bytes)
					}
				}
				once.Do(func() { close(release) })
				if !waitFor(func() bool {
					messages, bytes := recoveryBudgetUsage(listener.as.recoveryBudget)
					return messages == 0 && bytes == 0
				}, time.Second) {
					t.Fatal("recovery budget did not settle")
				}
				wantAttempts, wantDelivered := int32(1), 1
				if failure.wantRetry {
					wantAttempts, wantDelivered = 2, 2
				}
				if attempts.Load() != wantAttempts || sent.dataCount() != wantDelivered {
					t.Fatalf("recovery replay/loss: attempts=%d, delivered=%d; want %d/%d",
						attempts.Load(), sent.dataCount(), wantAttempts, wantDelivered)
				}
				delivered := sent.data()
				if failure.wantRetry && string(dataPayload(t, delivered[0])) != "first" {
					t.Fatal("retried DATA lost FIFO order")
				}
				if string(dataPayload(t, delivered[len(delivered)-1])) != "second" {
					t.Fatal("recovery lost the following DATA")
				}
			})
		}
	}
}
