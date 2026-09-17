// Copyright 2018-2024 go-m3ua authors. All rights reserved.
// Use of this source code is governed by a MIT-style license that can be
// found in the LICENSE file.

package m3ua

import "testing"

// Establishment is reaching the state the policy waits for, not being caught in
// it. RFC 4666 Section 5.6.2 lets either IPSP initiate either exchange, so the
// peer's ASP Active can carry this end past its own readiness state before the
// transition that reached it has finished being published.
func TestEstablishmentSignalsOnceTheReadinessStateIsReachedOrPassed(t *testing.T) {
	newIPSP := func(t *testing.T, aspActive ASPProcedureMode) *Association {
		t.Helper()
		conn, _ := newTestConn(t, StateASPDown, RoleIPSP)
		conn.cfg.IPSP = &IPSPConfig{ExchangeModel: IPSPExchangeSingle}
		conn.cfg.ASPProcedures = &ASPProcedurePolicy{
			ASPUp:       ASPProcedureAutomatic,
			ASPDown:     ASPProcedureAutomatic,
			ASPActive:   aspActive,
			ASPInactive: ASPProcedureAutomatic,
		}
		return conn
	}
	established := func(c *Association) bool {
		select {
		case <-c.established:
			return true
		default:
			return false
		}
	}

	t.Run("not before the readiness state is reached", func(t *testing.T) {
		// An explicit ASP Active makes ASP-INACTIVE the readiness state.
		conn := newIPSP(t, ASPProcedureExplicit)
		conn.notifyReady()
		if established(conn) {
			t.Error("establishment was signalled while the association was still ASP-DOWN")
		}
	})

	t.Run("when it is reached", func(t *testing.T) {
		conn := newIPSP(t, ASPProcedureExplicit)
		conn.setState(StateASPInactive)
		conn.notifyReady()
		if !established(conn) {
			t.Error("establishment was not signalled at the readiness state")
		}
	})

	t.Run("when the peer has already carried it past", func(t *testing.T) {
		conn := newIPSP(t, ASPProcedureExplicit)
		// The peer's ASP Active arrives while this end is still publishing the
		// ASP-INACTIVE its own ASP Up Ack produced.
		conn.setState(StateASPActive)
		conn.notifyReady()
		if !established(conn) {
			t.Error("establishment was never signalled although the association " +
				"had passed its readiness state")
		}
	})

	t.Run("never from an SCTP teardown state", func(t *testing.T) {
		conn := newIPSP(t, ASPProcedureExplicit)
		conn.setState(StateSCTPCDI)
		conn.notifyReady()
		if established(conn) {
			t.Error("establishment was signalled from an SCTP teardown state")
		}
	})
}
