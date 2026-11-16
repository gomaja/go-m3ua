package m3ua

// Marks run inside the existing active-scope/state mutation boundary. Queue
// tokens are retired immediately; short source/reservation bookkeeping cannot
// join transport, the scheduler, a subscriber, or an application callback.
// Call before beginning an offered-status scope-change span: that span owns
// restart marks and possibly registration-map locks, whereas retirement takes
// the store and may then take subscription locks. Keep the state/ACK/AS fence
// held through retirement and its permission commit to exclude old admission.
func (c *Association) retireLocalCongestionScopes(matches func(ASKey) bool) {
	c.retireLocalCongestionInputs(matches, true)
}

// Registration events already retire audit origins through their shared
// received-history batch; do not clone those origins again for local inputs.
func (c *Association) retireLocalCongestionInputs(matches func(ASKey) bool, retireAudits bool) {
	c.retireQueueCongestionLifetimes(matches)
	if s := c.ssnmStore(); s != nil {
		s.mu.Lock()
		s.retireCongestionPublicationsLocked(c.ID(), matches)
		s.retireLocalCongestionLocked(c.ID(), matches)
		if retireAudits {
			s.retireDestinationAuditScopesLocked(c.ID(), matches)
		}
		s.mu.Unlock()
	}
}
func (c *Association) retireLocalCongestionContexts(rcs []uint32) {
	if len(rcs) == 0 {
		c.retireLocalCongestionScopes(nil)
		return
	}
	set := make(map[uint32]bool, len(rcs))
	for _, rc := range rcs {
		set[rc] = true
	}
	c.retireLocalCongestionScopes(func(key ASKey) bool { return key.RoutingContextSet && set[key.RoutingContext] })
}
func (c *Association) reconcileCongestionPublications() {
	s := c.ssnmStore()
	if s == nil {
		return
	}
	// Actual source commits and scope marks retire work. This scan is bounded by
	// publication configuration and is a safety check for association teardown.
	if associationDone(c) {
		s.mu.Lock()
		s.retireCongestionPublicationsLocked(c.ID(), nil)
		s.mu.Unlock()
	}
}
