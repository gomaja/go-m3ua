package m3ua

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"slices"
	"sync"
	"testing"

	"github.com/gomaja/go-m3ua/messages/params"
)

func routingContexts(count int) []uint32 {
	contexts := make([]uint32, count)
	for index := range contexts {
		contexts[index] = uint32(index + 1)
	}
	return contexts
}

func TestOptimizedDataRoutingContextValidationPreservesSemantics(testContext *testing.T) {
	tests := []struct {
		name         string
		configured   []uint32
		peer         *params.Param
		want         error
		wantContexts []uint32
	}{
		{
			name:       "malformed empty",
			configured: []uint32{1},
			peer:       params.NewParam(int(params.RoutingContext), nil),
			want:       ErrInvalidRoutingContext,
		},
		{
			name:         "unknown single",
			configured:   routingContexts(32),
			peer:         params.NewRoutingContext(33),
			want:         ErrInvalidRoutingContext,
			wantContexts: []uint32{33},
		},
		{
			name:       "explicit zero",
			configured: []uint32{0},
			peer:       params.NewRoutingContext(0),
		},
		{
			name: "contextless",
		},
		{
			name:       "omitted among multiple",
			configured: []uint32{1, 2},
			want:       ErrMissingRoutingContext,
		},
		{
			name:         "multiple values",
			configured:   []uint32{1, 2},
			peer:         params.NewRoutingContext(1, 2),
			want:         ErrInvalidRoutingContext,
			wantContexts: []uint32{1, 2},
		},
	}
	for _, test := range tests {
		testContext.Run(test.name, func(testContext *testing.T) {
			conn, _ := newTestConnWithContexts(testContext, StateASPActive, RoleASP, test.configured...)
			err := conn.validateDataRoutingContext(test.peer)
			if !errors.Is(err, test.want) {
				testContext.Fatalf("validateDataRoutingContext error = %v, want %v", err, test.want)
			}
			if test.want == nil {
				return
			}
			var routingContextError *RoutingContextError
			if !errors.As(err, &routingContextError) {
				if len(test.wantContexts) == 0 {
					return
				}
				testContext.Fatalf("validation error = %T, want RoutingContextError", err)
			}
			if !slices.Equal(routingContextError.Contexts, test.wantContexts) {
				testContext.Fatalf("offending Routing Contexts = %v, want %v",
					routingContextError.Contexts, test.wantContexts)
			}
		})
	}
}

func TestCoallocatedReceiveRejectsInvalidProtocolData(testContext *testing.T) {
	tests := []struct {
		name  string
		param *params.Param
	}{
		{
			name: "wrong tag",
			param: &params.Param{
				Tag:  params.RoutingContext,
				Data: make([]byte, 12),
			},
		},
		{
			name: "truncated payload",
			param: &params.Param{
				Tag:  params.ProtocolData,
				Data: make([]byte, 11),
			},
		},
	}
	for _, test := range tests {
		testContext.Run(test.name, func(testContext *testing.T) {
			conn, _ := newDataWriteAssociation(testContext, 1)
			data := inboundData(1, "invalid")
			data.ProtocolData = test.param
			deliver(testContext, conn, 1, data)

			select {
			case err := <-conn.errChan:
				if !errors.Is(err, ErrFailedToPeelOff) {
					testContext.Fatalf("reported error = %v, want ErrFailedToPeelOff", err)
				}
			default:
				testContext.Fatal("invalid Protocol Data produced no error")
			}
			if queued := len(conn.dataChan); queued != 0 {
				testContext.Fatalf("invalid Protocol Data queued %d messages, want 0", queued)
			}
		})
	}
}

func TestCoallocatedReceivePreservesStateAndScopeChecks(testContext *testing.T) {
	testContext.Run("inactive association", func(testContext *testing.T) {
		conn, _ := newTestConn(testContext, StateASPInactive, RoleASP)
		deliver(testContext, conn, 1, inboundData(1, "inactive"))

		var unexpected *UnexpectedMessageError
		select {
		case err := <-conn.errChan:
			if !errors.As(err, &unexpected) {
				testContext.Fatalf("reported error = %v, want UnexpectedMessageError", err)
			}
		default:
			testContext.Fatal("inactive DATA produced no error")
		}
		if queued := len(conn.dataChan); queued != 0 {
			testContext.Fatalf("inactive DATA queued %d messages, want 0", queued)
		}
	})

	testContext.Run("invalid routing context", func(testContext *testing.T) {
		conn, _ := newDataWriteAssociation(testContext, 1)
		deliver(testContext, conn, 1, inboundData(2, "wrong scope"))

		select {
		case err := <-conn.errChan:
			if !errors.Is(err, ErrInvalidRoutingContext) {
				testContext.Fatalf("reported error = %v, want ErrInvalidRoutingContext", err)
			}
		default:
			testContext.Fatal("invalid Routing Context produced no error")
		}
		if queued := len(conn.dataChan); queued != 0 {
			testContext.Fatalf("invalid Routing Context queued %d messages, want 0", queued)
		}
	})
}

func TestCoallocatedReceiveRetainsCallerOwnedProtocolData(testContext *testing.T) {
	conn, _ := newDataWriteAssociation(testContext, 1, 2)
	deliver(testContext, conn, 1, inboundData(1, "first retained payload"))
	deliver(testContext, conn, 1, inboundData(2, "second retained payload"))

	first, err := conn.ReadData(context.Background())
	if err != nil {
		testContext.Fatalf("first ReadData: %v", err)
	}
	retained := first.ProtocolData
	first = nil
	runtime.GC()
	if got := string(retained.Data); got != "first retained payload" {
		testContext.Fatalf("retained ProtocolData = %q, want first payload", got)
	}

	second, err := conn.ReadData(context.Background())
	if err != nil {
		testContext.Fatalf("second ReadData: %v", err)
	}
	if retained == second.ProtocolData {
		testContext.Fatal("two delivered messages share one ProtocolData object")
	}
	retained.Data[0] = 'F'
	if got := string(second.ProtocolData.Data); got != "second retained payload" {
		testContext.Fatalf("mutating retained payload changed the second message to %q", got)
	}
}

func TestCoallocatedReceiveMessagesRemainIndependentAcrossConcurrentReads(testContext *testing.T) {
	conn, _ := newDataWriteAssociation(testContext, 1)
	const messageCount = 32
	conn.dataChan = make(chan *DataMessage, messageCount)
	for index := 0; index < messageCount; index++ {
		deliver(testContext, conn, 1, inboundData(1, fmt.Sprintf("message-%02d", index)))
	}

	results := make(chan *DataMessage, messageCount)
	var wait sync.WaitGroup
	for index := 0; index < messageCount; index++ {
		wait.Add(1)
		go func() {
			defer wait.Done()
			message, err := conn.ReadData(context.Background())
			if err != nil {
				testContext.Errorf("ReadData: %v", err)
				return
			}
			results <- message
		}()
	}
	wait.Wait()
	close(results)

	seenPayloads := make(map[string]struct{}, messageCount)
	seenPointers := make(map[*params.ProtocolDataPayload]struct{}, messageCount)
	for message := range results {
		seenPayloads[string(message.ProtocolData.Data)] = struct{}{}
		seenPointers[message.ProtocolData] = struct{}{}
	}
	if len(seenPayloads) != messageCount {
		testContext.Errorf("received %d distinct payloads, want %d", len(seenPayloads), messageCount)
	}
	if len(seenPointers) != messageCount {
		testContext.Errorf("received %d distinct ProtocolData objects, want %d", len(seenPointers), messageCount)
	}
}
