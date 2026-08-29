# Migrating to v1.2

v1.2 separates the M3UA protocol role from SCTP association initiation.
RFC 4666 Section 1.4.8 recommends that both ASPs and SGPs support initiating
and accepting SCTP associations; `Dial` therefore no longer implies ASP and
`Accept` no longer implies SGP.

## Endpoint role

Create one endpoint with the required RFC role:

```go
asp, err := m3ua.NewEndpoint(m3ua.RoleASP)
sgp, err := m3ua.NewEndpoint(m3ua.RoleSGP)
```

Use `Endpoint.Dial` when that endpoint initiates SCTP, or
`Endpoint.Listen` and `Listener.Accept` when it accepts SCTP. Either
`RoleASP` or `RoleSGP` supports either orientation.

`RoleIPSP` is reserved for the explicit Single Exchange model and Double
Exchange model APIs defined by RFC 4666 Section 4.3. Calling `Dial` or
`Listen` directly on an IPSP endpoint returns `ErrUnsupportedRole` rather than
guessing either model.

A `RoleSGP` endpoint currently admits one shared protocol-state owner: one
`Listener`, which can serve multiple accepted ASP associations, or one dialed
`Association`. A second owner returns `ErrEndpointStateInUse` instead of
creating an independent AS registry for the same SGP.

Use `Listener.DistributeData` for an SGP that accepts SCTP associations and
`Association.DistributeData` for an SGP that initiates its SCTP association.
Both paths apply the same Application Server state, recovery queue, and Traffic
Mode rules. Calling `Association.DistributeData` on an ASP returns
`ErrUnsupportedRole`.

For SGP state learned from the SS7 side, the same orientation rule applies:
use the Listener methods for an SGP that accepts SCTP associations, or
`Association.SetNIFAvailable`, `Association.SetASAvailableForAS`, destination
reporting, and `Association.BeginMTP3Restart` for an SGP that initiates its SCTP
association. These Association procedures reject the ASP role.

## Renamed API

| Before v1.2 | v1.2 |
| --- | --- |
| `Config`, `ConnConfig` | `AssociationConfig` |
| `NewConfig` | `NewAssociationConfig` |
| `NewClientConfig`, `NewServerConfig` | `NewAssociationConfig` plus explicit setters |
| `Conn` | `Association` |
| package `Dial` | `Endpoint.Dial` |
| package `Listen` | `Endpoint.Listen` |
| `DefaultConnConfig` | `DefaultAssociationConfig` |
| `SelectConnConfig` | `SelectAssociationConfig` |
| `ErrConnClosed` | `ErrAssociationClosed` |
| `ErrAspIDRequired` | `ErrASPIdentifierRequired` |
| `ErrInvalidAspIdentifier` | `ErrInvalidASPIdentifier` |
| `ErrUnsupportedMode` | `ErrUnsupportedRole` |
| `StateAspDown` | `StateASPDown` |
| `StateAspInactive` | `StateASPInactive` |
| `StateAspActive` | `StateASPActive` |
| `SctpSackInfo` | `SCTPSACKInfo` |
| `SctpNoDelayInfo` | `SCTPNoDelayInfo` |
| `SetSackConfig`, `SetSctpSackConfig` | `SetSCTPSACK` |
| `SetNoDelayConfig`, `SetSctpNoDelayConfig` | `SetSCTPNoDelay` |
| `SignalingLinkSelection` | `SignallingLinkSelection` |

No compatibility aliases remain. This makes role and association ownership
visible at every call site and prevents transport orientation from selecting
M3UA procedures accidentally.

## Configuration validation

`AssociationConfig` is role-neutral, but fields that have meaning for only
one role are validated before association processing:

- `ASPIdentifier` is local ASP policy and is rejected on an SGP endpoint.
- `AuthorizeASP`, recovery queues, and Broadcast distribution policy are SGP
  policy and are rejected on an ASP endpoint.
- `Listener.SetNIFAvailable`, `SetASAvailable`, and `SetASAvailableForAS` now
  return an error and reject a non-SGP Listener with `ErrUnsupportedRole`.
- A nil `AssociationConfig` passed to `Endpoint.Dial` returns
  `ErrNilAssociationConfig`.

The configuration selected by `ListenerConfig.SelectAssociationConfig` is
validated after SCTP accept and before socket setup, monitoring, or M3UA
parsing.
