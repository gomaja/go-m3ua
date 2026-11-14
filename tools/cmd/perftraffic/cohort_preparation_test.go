package main

import (
	"context"
	"errors"

	"github.com/gomaja/go-m3ua"
)

// cohortRoutingControl invokes the real receiver operations within the test's
// overall context. The command's HTTP and per-step preparation deadlines are
// operational budgets, separate from this logical-time correctness fixture.
type cohortRoutingControl struct{ operations *routedPeerOperations }

func (control cohortRoutingControl) Inventory(ctx context.Context) (routingInventoryDTO, error) {
	return control.operations.preparation.inventory(ctx)
}
func (control cohortRoutingControl) Prepare(ctx context.Context, values []routingTransportDTO) error {
	return control.operations.prepare(ctx, values)
}
func (control cohortRoutingControl) Publish(ctx context.Context, ordinal uint8) error {
	return control.operations.publish(ctx, ordinal)
}
func (control cohortRoutingControl) Stop(ctx context.Context) error {
	return control.operations.stop(ctx)
}
func (control cohortRoutingControl) Start(ctx context.Context, cohort string, seed uint64) error {
	return control.operations.startPreflight(ctx, cohort, seed)
}
func (control cohortRoutingControl) Complete(ctx context.Context) ([]routingDataReceiptDTO, error) {
	return control.operations.completePreflight(ctx)
}

// prepareCohortRoutingSSNM retains the command preparation's real inventories,
// publication arming, receipt oracle, stream closure/join and final-store check.
// Each operation shares the bounded overall test context; the command keeps its
// real-time per-step budgets in prepareRoutingSSNM.
func prepareCohortRoutingSSNM(ctx context.Context, topology routingTopology, sender routingPreparationSender, control routingPreparationControl) (routingSSNMPreparationEvidence, error) {
	if sender == nil || control == nil {
		return routingSSNMPreparationEvidence{}, errors.New("routing preparation dependencies are nil")
	}
	if err := ctx.Err(); err != nil {
		return routingSSNMPreparationEvidence{}, routingPreparationFailed(err, sender, control)
	}
	senders, err := sender.Inventory()
	if err != nil {
		return routingSSNMPreparationEvidence{}, routingPreparationFailed(err, sender, control)
	}
	senderDTOs := make([]routingTransportDTO, len(senders))
	for index, inventory := range senders {
		senderDTOs[index], err = routingTransportDTOFromSender(inventory)
		if err != nil {
			return routingSSNMPreparationEvidence{}, routingPreparationFailed(err, sender, control)
		}
	}
	remote, err := control.Inventory(ctx)
	if err != nil {
		return routingSSNMPreparationEvidence{}, routingPreparationFailed(err, sender, control)
	}
	if !remote.Ready {
		return routingSSNMPreparationEvidence{}, routingPreparationFailed(errors.New("routing peer inventory is not ready"), sender, control)
	}
	peers, err := routingPeerInventoryFromDTOs(remote.Transports)
	if err != nil {
		return routingSSNMPreparationEvidence{}, routingPreparationFailed(err, sender, control)
	}
	if err := validateRoutingPeerStatuses(topology, peers, remote.ASPStatuses); err != nil {
		return routingSSNMPreparationEvidence{}, routingPreparationFailed(err, sender, control)
	}
	pairs, err := pairRoutingInventory(topology, senders, peers)
	if err != nil {
		return routingSSNMPreparationEvidence{}, routingPreparationFailed(err, sender, control)
	}
	plan, err := newRoutingSSNMPreparationPlan(topology, pairs)
	if err != nil {
		return routingSSNMPreparationEvidence{}, routingPreparationFailed(err, sender, control)
	}
	initial, stream, err := sender.SubscribeSSNM()
	if err != nil {
		return routingSSNMPreparationEvidence{}, routingPreparationFailed(err, sender, control)
	}
	oracle, err := plan.begin(sender.ASPStatuses(), initial)
	if err != nil {
		return routingSSNMPreparationEvidence{}, routingPreparationStreamFailed(err, stream, nil, nil, sender, control)
	}
	readerCtx, cancelReader := context.WithCancel(ctx)
	events := make(chan routingPreparationEvent, routingSSNMReceiptsPerPublication)
	readerDone := make(chan error, 1)
	var arm routingPreparationArm
	go func() {
		for {
			event, nextErr := stream.Next(readerCtx)
			if nextErr != nil {
				readerDone <- nextErr
				return
			}
			tagged := arm.tag(event)
			select {
			case events <- tagged:
			case <-readerCtx.Done():
				readerDone <- readerCtx.Err()
				return
			}
		}
	}()
	readerFinished := false
	var readerTerminal error
	fail := func(cause error) (routingSSNMPreparationEvidence, error) {
		cancelReader()
		closeErr := stream.Close()
		if !readerFinished {
			readerTerminal = <-readerDone
			readerFinished = true
		}
		return routingSSNMPreparationEvidence{}, routingPreparationFailed(errors.Join(cause, closeErr, readerTerminal), sender, control)
	}
	prepareCtx, cancelPrepare := context.WithCancel(ctx)
	err = control.Prepare(prepareCtx, senderDTOs)
	cancelPrepare()
	if err != nil {
		return fail(err)
	}
	for ordinal := uint8(0); ordinal < routingSSNMPublicationCount; ordinal++ {
		stepCtx, cancelStep := context.WithCancel(ctx)
		arm.set(ordinal, true)
		publishErr := control.Publish(stepCtx, ordinal)
		if publishErr != nil {
			arm.set(ordinal, false)
			cancelStep()
			return fail(publishErr)
		}
		for receipt := 0; receipt < routingSSNMReceiptsPerPublication; receipt++ {
			select {
			case tagged := <-events:
				if !tagged.armed || tagged.ordinal != ordinal {
					cancelStep()
					return fail(errors.New("routing SSNM event arrived outside its active publication"))
				}
				if err := oracle.observe(ordinal, tagged.event); err != nil {
					cancelStep()
					return fail(err)
				}
			case readerErr := <-readerDone:
				readerFinished, readerTerminal = true, readerErr
				cancelStep()
				return fail(readerErr)
			case <-stepCtx.Done():
				cancelStep()
				return fail(stepCtx.Err())
			}
		}
		arm.set(ordinal, false)
		if err := oracle.publicationComplete(ordinal); err != nil {
			cancelStep()
			return fail(err)
		}
		cancelStep()
	}
	arm.set(0, false)
	closeErr := stream.Close()
	var unexpected error
	ctxDone := ctx.Done()
	for !readerFinished {
		select {
		case <-events:
			unexpected = errors.New("routing SSNM stream retained an unexpected event after preparation")
		case readerErr := <-readerDone:
			readerFinished, readerTerminal = true, readerErr
			if !errors.Is(readerErr, m3ua.ErrSSNMSubscriptionClosed) {
				unexpected = errors.Join(unexpected, readerErr)
			}
			if drainRoutingPreparationEvents(events) {
				unexpected = errors.New("routing SSNM stream retained an unexpected event after preparation")
			}
		case <-ctxDone:
			cancelReader()
			unexpected = errors.Join(unexpected, ctx.Err())
			ctxDone = nil
		}
	}
	cancelReader()
	if closeErr != nil || unexpected != nil {
		return routingSSNMPreparationEvidence{}, routingPreparationFailed(errors.Join(closeErr, unexpected), sender, control)
	}
	evidence, err := oracle.finish(sender.SSNMKnowledge())
	if err != nil {
		return routingSSNMPreparationEvidence{}, routingPreparationFailed(err, sender, control)
	}
	return evidence, nil
}
