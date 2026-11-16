package m3ua

import "time"

func configureAutomationForTest(e *Endpoint, now *time.Time) func() {
	e.congestion.defaultProfile = CongestionProfile{Method: CongestionNationalWithPriorities, MaxLevel: 3}
	e.ssnm.validity = 30 * time.Second
	e.ssnm.auditing = &DestinationAuditConfig{Period: 30 * time.Second, MaxPending: 256, MaxMessagesPerSecond: 10}
	e.ssnm.now = func() time.Time { return *now }
	return func() { e.runCongestionAutomation(*now) }
}
