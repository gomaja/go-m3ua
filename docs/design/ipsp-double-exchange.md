# IPSP Double Exchange

RFC 4666 Sections 4.3 and 5.6.2 define IPSP Double Exchange as two independent
directions of data traffic. SCTP association initiation is a separate choice
under RFC 4666 Section 1.4.8 and never selects either M3UA direction.

## Directional model

For one local IPSP Association:

| Configuration | Procedure and traffic meaning |
| --- | --- |
| `TrafficToLocal` | The local IPSP sends ASP Up and ASP Active, consumes their acknowledgements, and accepts peer DATA in this Routing Key scope. |
| `TrafficToPeer` | The peer sends ASP Up and ASP Active, the local IPSP returns their acknowledgements, and sends DATA in this Routing Key scope. |

This follows the two independent examples in RFC 4666 Section 5.6.2:

- peer ASP Active for one Routing Context enables DATA toward that peer;
- local ASP Active for the opposite Routing Context enables DATA toward the
  local IPSP; and
- ASP Inactive and ASP Down in either direction do not change the other.

`Association.IPSPState()` returns both states. `Association.State()` and
`Association.StateChanges()` retain the remote IPSP state governing
`TrafficToPeer`; this keeps their single-state contract unambiguous. Code that
manages Double Exchange uses `IPSPState()` whenever both directions matter.

## ASPSM agreement

`IPSPASPSMExchangeDouble` implements the normal independent ASP Up and ASP Down
procedures in each direction.

`IPSPASPSMExchangeSingle` implements the agreed simplification permitted by RFC
4666 Section 4.3: one ASP Up or ASP Down exchange establishes or removes both
ASPSM directions. ASPTM remains Double Exchange, so each traffic direction
still has its own ASP Active, ASP Inactive, Routing Key, and DATA state.

The ASPSM agreement is explicit because the two IPSPs cannot infer it from SCTP
association initiation or from the first message without risking different
state models at each end.

With normal Double Exchange, `InitiateASPSM` requires `TrafficToLocal`: the
local ASP Up procedure establishes the direction in which the peer sends DATA
to this IPSP. Under the agreed single-ASPSM simplification, either direction
may cause that one exchange to establish both directions. `InitiateASPTM`
always requires `TrafficToLocal`.

## Traffic identity

Each direction owns its own:

- Traffic Mode Type and per-Routing-Context Traffic Modes;
- Network Appearance; and
- Routing Context set.

The same numeric Routing Context may exist in both directions, including under
different Network Appearances. Directional active and override inventories keep
those traffic identities independent.

Association-wide traffic fields are rejected for Double Exchange because they
cannot say which direction they describe. A non-nil `IPSPTrafficConfig` with a
nil `RoutingContexts` parameter represents one configured contextless AS. A nil
`IPSPTrafficConfig` disables that direction. A present empty Routing Context
parameter is invalid; omission, not a zero-length parameter, represents the
contextless case.

Incoming DATA is validated against `TrafficToLocal`. Outgoing DATA is selected
from `TrafficToPeer`. SCON received from the peer is scoped to
`TrafficToPeer`, while SCON originated for traffic arriving at this IPSP is
scoped to `TrafficToLocal`. RFC 4666 Sections 1.4.3.4 and 1.4.6 do not extend
the SG-ASP DUNA, DAVA, DAUD, DUPU, or DRST procedures to IPSP communication.

## Ordering and recovery

RFC 4666 Sections 4.3.4.3 and 4.3.4.4 require traffic to start only after ASP
Active Ack and ASP Inactive Ack to follow traffic quiescence. Each direction
therefore has its own active Routing Context inventory and transfer gate.

An acknowledgement waits only for traffic in the direction it changes. Normal
Double Exchange leaves the opposite direction undisturbed; only the agreed
single-ASPSM ASP Down procedure quiesces the whole communication.

T(ack) retransmission is retained per request and per Routing Context subset.
The scopes acknowledged during one SCTP association epoch are retained as a
bounded set. A later ASP Active Ack or ASP Inactive Ack caused by a request
retransmission is recognized as the completed local procedure and cannot
reverse a subsequent transition or provoke an unrelated Error. A new SCTP
association epoch discards that evidence, so an old Ack cannot authorize new
traffic.

An SCTP restart starts a new M3UA association epoch under RFC 4666 Section
4.3.3. Both Double Exchange directions and both Routing Context inventories are
reset to ASP-DOWN before any fresh ASP Up procedure begins. Association close
also resets both directions and closes every state and management indication
stream.
