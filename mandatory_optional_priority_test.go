package m3ua

import (
	"sync"
	"testing"
	"time"

	"github.com/gomaja/go-m3ua/messages"
	"github.com/gomaja/go-m3ua/messages/params"
)

func TestQueuedMandatoryControlRetainsPriorityAfterDequeue(t *testing.T) {
	r := publicationRig(t, 2)
	owner := &applicationServer{state: ASActive, asps: map[*Association]State{r.a: StateASPActive}}
	r.a.notificationQueue = make(chan mandatoryControl, 1)
	written := make(chan struct{})
	r.a.notificationWriter = func(message messages.M3UA) (int, error) {
		defer close(written)
		return message.MarshalLen(), nil
	}
	queuePublication(r, 100, 10, nationalPublication(3), 1)

	r.a.asStateNotifyMu.Lock()
	var release sync.Once
	unlock := func() { release.Do(r.a.asStateNotifyMu.Unlock) }
	defer unlock()
	notification := messages.NewNotify(params.NewStatus(params.AsStateActive), nil, nil, nil)
	if err := r.a.writeMandatoryASControls([]messages.M3UA{notification}, false, false, owner); err != nil {
		unlock()
		t.Fatal(err)
	}
	dequeued := waitFor(func() bool { return len(r.a.notificationQueue) == 0 }, time.Second)
	if !dequeued {
		unlock()
		select {
		case <-written:
		case <-time.After(time.Second):
			t.Fatal("mandatory writer did not finish after releasing owner barrier")
		}
		t.Fatal("mandatory writer did not dequeue its queued entry")
	}
	// The worker owns the dequeued entry but cannot pass the owner barrier.
	// Its callback is separate from the optional SCON recorder.
	r.e.runCongestionAutomation(r.now)
	slipped := len(r.sent) != 0
	unlock()
	select {
	case <-written:
	case <-time.After(time.Second):
		t.Fatal("mandatory writer did not reach transport after releasing owner barrier")
	}
	if !waitFor(func() bool { return r.a.controlWritesPending.Load() == 0 }, time.Second) {
		t.Fatal("completed mandatory entry retained its optional-admission reservation")
	}
	if slipped {
		t.Fatal("optional SCON passed a dequeued mandatory write waiting at its owner barrier")
	}
	r.e.runCongestionAutomation(r.now)
	if len(r.sent) != 1 {
		t.Fatal("completed mandatory write did not release the current optional report")
	}
}

func TestOptionalAdmissionRechecksMandatoryWorkAfterFinalRevisionCheck(t *testing.T) {
	r := publicationRig(t, 2)
	scope := testWireScope(7, true, 1)
	binding := r.a.queueCongestionBinding(r.a.configuredLocalASKeysForStatus()[0], scope)
	checks, writes := 0, 0
	reserved := false
	defer func() {
		if reserved {
			r.a.controlWritesPending.Add(-1)
		}
	}()
	r.a.signalWriter = func(message messages.M3UA) (int, error) {
		writes++
		return message.MarshalLen(), nil
	}
	current := func() bool {
		checks++
		if checks == 2 {
			r.a.controlWritesPending.Add(1)
			reserved = true
		}
		return true
	}
	message := messages.NewSignallingCongestion(params.NewNetworkAppearance(7), params.NewRoutingContext(1), params.NewAffectedPointCode(100), nil, params.NewCongestionIndications(3), nil)
	attempted, err := r.a.tryOptionalSSNMFrame(message, []queueCongestionBinding{binding}, r.a.Epoch(), current)
	if checks != 2 {
		t.Fatalf("fixture reached %d revision checks, want two", checks)
	}
	if attempted || err != nil || writes != 0 {
		t.Fatalf("optional admission overtook mandatory work reserved at its final revision check: attempted=%v err=%v writes=%d", attempted, err, writes)
	}
}
