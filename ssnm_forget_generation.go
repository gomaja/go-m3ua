package m3ua

import (
	"errors"
	"sync/atomic"
)

var errReceivedBeforeForget = errors.New("received SSNM predates destination forget")

// A generation is odd while at least one Forget is reclaiming stores. It is
// captured before route resolution and tested inside each existing mutation
// lock. A legacy/canonical commit that wins its lock is cleared by Forget;
// a later stale commit is discarded. Shared route-index allocations are not
// reclaimed. Nil is used only by local operations outside receive admission.
type receivedForgetFence struct {
	generation *atomic.Uint64
	version    uint64
}

func (f receivedForgetFence) current() bool {
	return f.generation == nil || f.version&1 == 0 && f.generation.Load() == f.version
}

// commitReceivedForgetFenced uses the store lock already required for binding
// and retention. The disabled-policy receive path adds no tracker acquisition.
func (s *ssnmState) commitReceivedForgetFenced(report SSNMReport, resolution ssnmDestinationRefusal, partitions []SSNMPartition, bindings map[SSNMPartition]bool, pending bool, fence receivedForgetFence) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !fence.current() {
		return false, nil
	}
	var first error
	for _, partition := range partitions {
		waiting := pending
		if value, held := bindings[partition]; held {
			waiting = value
		}
		if err := s.bindLocked(partition, report.Association, waiting); err != nil {
			if first == nil {
				first = err
			}
			continue
		}
		scoped := report
		scoped.Partition = partition
		if err := s.applyPreparedLocked(scoped, resolution.byPartition[partition]); err != nil && first == nil {
			first = err
		}
	}
	return true, first
}
