package m3ua

// ssnmWorkCount is an optional private operation probe. Tests attach it before
// exercising a store; production leaves it nil. It counts rows visited rather
// than elapsed time and never invokes a callback while a lock is held.
type ssnmWorkCount struct {
	peerRows, peerASRows                                                            uint64
	sourceRows, jobRows, asRows, prefixRows, copiedAS, knowledgeRows, partitionRows uint64
	copiedOrigins                                                                   uint64
}

func (w *ssnmWorkCount) visitSource() {
	if w != nil {
		w.sourceRows++
	}
}
func (w *ssnmWorkCount) visitJob() {
	if w != nil {
		w.jobRows++
	}
}
func (w *ssnmWorkCount) visitAS() {
	if w != nil {
		w.asRows++
	}
}
func (w *ssnmWorkCount) visitPrefix() {
	if w != nil {
		w.prefixRows++
	}
}
func (w *ssnmWorkCount) copyAS(n int) {
	if w != nil {
		w.copiedAS += uint64(n)
	}
}
func (w *ssnmWorkCount) visitKnowledge() {
	if w != nil {
		w.knowledgeRows++
	}
}
func (w *ssnmWorkCount) visitPartition() {
	if w != nil {
		w.partitionRows++
	}
}

func (w *ssnmWorkCount) copyOrigins(n int) {
	if w != nil {
		w.copiedOrigins += uint64(n)
	}
}
