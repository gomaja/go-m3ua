package m3ua

import "testing"

func TestTransportCongestionAbsenceSurvivesEpochReset(t *testing.T) {
	for _, owner := range []string{"standalone", "enabled-without-socket", "disabled"} {
		t.Run(owner, func(t *testing.T) {
			a, _ := newTestConnWithContexts(t, StateASPActive, RoleASP, 1)
			if owner != "standalone" {
				e, err := NewEndpoint(EndpointConfig{Role: RoleASP, TransportCongestion: &TransportCongestionConfig{Disabled: owner == "disabled"}})
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = e.Close() })
				if !e.trackAssociation(a) {
					t.Fatal("track")
				}
			}
			before := a.TransportCongestion()
			if a.transportSnapshot.Load() != nil {
				t.Fatal("fixture has a detector contribution")
			}
			a.resetTransportCongestionEpoch()
			if a.Epoch() != before.Epoch+1 {
				t.Fatal("restart did not advance the epoch")
			}
			before.Epoch++
			if after := a.TransportCongestion(); after != before {
				t.Errorf("absent detector changed across restart: %+v", after)
			}
			if s := transportASPStatus(a); s.transportUnknown || s.congested || s.congestionLevelSet {
				t.Errorf("unsampled association gained transport policy input: %+v", s)
			}
			if a.transportSnapshot.Load() != nil {
				t.Error("restart installed an unsampled detector snapshot")
			}
		})
	}
}
