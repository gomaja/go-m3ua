package m3ua

// beginOptionalSSNMChange excludes new optional SCON/DAUD admission and settles at
// most the one attempt holding sendMu. Its completion join excludes DATA and
// synchronous SCON. Concurrent lifetime changes share admission closure without
// joining each other's cleanup. Epoch reset and local reclaim call it before
// taking transport, destination, restart-procedure or store locks. Completion
// needs only the tracker; SSNM receive admission discards stale knowledge, while
// restart receive handling joins this optional completion (RFC 4666 §§3.4.4, 4.5.3).
func (c *Association) beginOptionalSSNMChange(forget bool) func() {
	tracker := &c.sconSubmissions
	tracker.mu.Lock()
	if tracker.optionalChanging == nil {
		tracker.optionalChanging = make(chan struct{})
	}
	tracker.optionalChanges++
	c.retireQueueCongestionLifetimes(nil)
	if forget {
		if tracker.forgetChanges == 0 {
			tracker.knowledgeGeneration.Add(1)
		}
		tracker.forgetChanges++
	}
	var pending []*sconSubmission
	for submission := range tracker.active {
		if submission.optionalAudit {
			submission.cancel(ErrAssociationClosed)
			pending = append(pending, submission)
		}
	}
	tracker.mu.Unlock()
	for _, submission := range pending {
		<-submission.done
	}
	return func() {
		tracker.mu.Lock()
		tracker.optionalChanges--
		if forget {
			tracker.forgetChanges--
			if tracker.forgetChanges == 0 {
				tracker.knowledgeGeneration.Add(1)
			}
		}
		if tracker.optionalChanges == 0 {
			close(tracker.optionalChanging)
			tracker.optionalChanging = nil
		}
		tracker.mu.Unlock()
	}
}
