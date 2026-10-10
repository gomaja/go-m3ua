package m3ua

import (
	"context"
	"fmt"
	"testing"

	"github.com/gomaja/go-m3ua/messages"
	"github.com/gomaja/go-m3ua/messages/params"
)

// Use the production authorization resolver: its per-peer inventory is the
// one an SGP validates on DATA receive (RFC 4666 Sections 3.3.1 and 4.3.4.1).
// Frames and configuration are built outside the measured receive operation.
func newReceiveRoutingContextAssociation(testContext testing.TB, count int, dynamic bool) *Association {
	testContext.Helper()
	configured := routingContexts(count)
	cfg := newSGPAssociationConfigForTest(nil, 1, params.TrafficModeLoadshare, 7, configured)
	conn := newAssociation(RoleSGP, cfg)
	conn.state = StateASPActive
	conn.errChan = make(chan error, 1)
	conn.recvStream.Store(1)
	if err := conn.resolveASPAuthorization(nil); err != nil {
		testContext.Fatal(err)
	}
	if dynamic {
		conn.addDynamicASKey(ASKey{RoutingContext: uint32(count + 1), RoutingContextSet: true,
			NetworkAppearance: 7, NetworkAppearanceSet: true}, RoutingKey{}, false)
	}
	return conn
}

func BenchmarkValidateDataRoutingContext(benchmark *testing.B) {
	for _, count := range []int{1, 32, 1000} {
		for _, dynamic := range []bool{false, true} {
			for _, peer := range []struct {
				name  string
				param *params.Param
			}{
				{"explicit", params.NewRoutingContext(uint32(count))},
				{"omitted", nil},
				{"malformed", params.NewParam(int(params.RoutingContext), []byte{1, 2, 3})},
				{"rejected", params.NewRoutingContext(uint32(count + 2))},
			} {
				benchmark.Run(fmt.Sprintf("contexts_%d/dynamic_%t/%s", count, dynamic, peer.name), func(benchmark *testing.B) {
					conn := newReceiveRoutingContextAssociation(benchmark, count, dynamic)
					benchmark.ReportAllocs()
					for benchmark.Loop() {
						_ = conn.validateDataRoutingContext(peer.param)
					}
				})
			}
		}
	}
}

func BenchmarkHandleDataRoutingContextScope(benchmark *testing.B) {
	for _, count := range []int{1, 32, 1000} {
		for _, peer := range []struct {
			name  string
			param *params.Param
		}{
			{"explicit", params.NewRoutingContext(uint32(count))},
			{"omitted", nil},
			{"invalid", params.NewRoutingContext(uint32(count + 1))},
			{"multi_value", params.NewRoutingContext(1, uint32(count))},
		} {
			benchmark.Run(fmt.Sprintf("contexts_%d/%s", count, peer.name), func(benchmark *testing.B) {
				conn := newReceiveRoutingContextAssociation(benchmark, count, false)
				data := inboundData(uint32(count), string(make([]byte, 128)))
				frame, err := data.MarshalBinary()
				if err != nil {
					benchmark.Fatal(err)
				}
				parsed, err := messages.Parse(frame)
				if err != nil {
					benchmark.Fatal(err)
				}
				data = parsed.(*messages.Data)
				// The public codec refuses multi-value DATA RCs before dispatch.
				// Substitute the peer parameter after decoding to exercise the
				// handler's own rejection of that shape as well as valid scopes.
				data.RoutingContext = peer.param
				ctx := context.Background()
				benchmark.ReportAllocs()
				for benchmark.Loop() {
					conn.handleData(ctx, data, frame)
					select {
					case <-conn.dataChan:
					case <-conn.errChan:
					default:
						benchmark.Fatal("DATA produced neither a delivery nor an error")
					}
				}
			})
		}
	}
}
