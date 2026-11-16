package m3ua

import (
	"errors"
	"testing"
)

func TestOfferedStatusSourceRemovalMarksBeforeAnyNewReport(t *testing.T) {
	for _, scoped := range []bool{false, true} {
		name := "association"
		if scoped {
			name = "scoped"
		}
		t.Run(name, func(t *testing.T) {
			e, a, _, partition := newStatusCommitFixture(t)
			requireOfferedStatus(t, offerStatusAt(t, e, statusCommitPC), true)
			if scoped {
				refusal := a.captureSSNMDestinationRefusal(testWireScope(7, true, 1), false)
				if err := e.ssnm.oversized([]SSNMPartition{partition}, 2, 1, a.ID(), refusal); !errors.Is(err, ErrSSNMOversizedReport) {
					t.Fatal(err)
				}
			} else {
				e.ssnm.retire(partition, a.ID())
			}
			// With no projection yet the public status is still the old known level.
			// The input removal itself must restart, independently of a later report.
			if scoped {
				_, err := e.MTPTransfer(MTPTransferRequest{ProtocolData: transferProtocolData(statusCommitPC, 0, []byte("offer"))})
				if !errors.Is(err, ErrDestinationStateUnknown) {
					t.Fatalf("invalidated destination admission=%v", err)
				}
				requireOfferedStatus(t, takeOfferedIndications(e.MTPIndications()), true)
			} else {
				requireOfferedStatus(t, offerStatusAt(t, e, statusCommitPC), true)
			}
			if !scoped {
				if err := e.ssnm.bind(partition, a.ID(), false); err != nil {
					t.Fatal(err)
				}
			}
			if scoped {
				commitStatusReport(t, e, a, partition, statusCommitPC, SSNMDestinationAvailableReport, 0)
			}
			commitStatusReport(t, e, a, partition, statusCommitPC, SSNMSignallingCongestionReport, 2)
			requireOfferedStatus(t, offerStatusAt(t, e, statusCommitPC), true)
		})
	}
}

func TestOfferedStatusSourceIndexFollowsRetainedRecordBudget(t *testing.T) {
	e, a, _, partition := newStatusCommitFixture(t)
	requireOfferedStatus(t, offerStatusAt(t, e, statusCommitPC), true)
	check := func() {
		t.Helper()
		e.ssnm.mu.Lock()
		defer e.ssnm.mu.Unlock()
		for _, state := range e.ssnm.partitions {
			active := 0
			for _, pending := range state.bindings {
				if !pending {
					active++
				}
			}
			if active != state.statusActiveBindings {
				t.Fatalf("active binding count=%d, map=%d", state.statusActiveBindings, active)
			}
			if state.statusSources == nil {
				if len(state.receivedCongestion) > 0 {
					t.Fatal("received inputs lack source index")
				}
				continue
			}
			inputs := state.statusSources
			if len(inputs.records) != len(state.receivedCongestion) {
				t.Fatalf("indexed=%d retained=%d", len(inputs.records), len(state.receivedCongestion))
			}
			links := 0
			for _, bucket := range inputs.prefixes {
				links += bucket.inputs.Len()
				revisionRecords, expired := 0, 0
				for _, revision := range bucket.revisions {
					revisionRecords += revision.records
					expired += revision.expired
					if revision.expired < 0 || revision.expired > revision.records {
						t.Fatal("invalid tied-input freshness count")
					}
				}
				if revisionRecords != bucket.inputs.Len() || expired < 0 {
					t.Fatalf("revision records=%d linked inputs=%d", revisionRecords, bucket.inputs.Len())
				}
			}
			if links != 2*len(state.receivedCongestion) || len(inputs.prefixes) > links {
				t.Fatalf("index links=%d buckets=%d records=%d", links, len(inputs.prefixes), len(state.receivedCongestion))
			}
		}
	}
	for i := 0; i < 64; i++ {
		commitStatusReport(t, e, a, partition, statusCommitPC, SSNMSignallingCongestionReport, uint8(2+i%2))
		check()
	}
	e.ssnm.retire(partition, a.ID())
	check()
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}
	check()
	e.aspRoutes.statusMarks.mu.Lock()
	defer e.aspRoutes.statusMarks.mu.Unlock()
	if len(e.aspRoutes.statusMarks.entries) != 0 {
		t.Fatal("closed endpoint retains destination marks")
	}
}
