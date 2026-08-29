# Migrating to v1.2

v1.2 separates the M3UA protocol role from SCTP association initiation.
RFC 4666 Section 1.4.8 recommends that both ASPs and SGPs support initiating
and accepting SCTP associations; `Dial` therefore no longer implies ASP and
`Accept` no longer implies SGP.

## Endpoint role

Create one endpoint with the required RFC role:

```go
asp, err := m3ua.NewEndpoint(m3ua.EndpointConfig{Role: m3ua.RoleASP})
sgp, err := m3ua.NewEndpoint(m3ua.EndpointConfig{Role: m3ua.RoleSGP})
```

Configure SGP-wide recovery and distribution policy on the Endpoint rather
than on any one Association:

```go
sgp, err := m3ua.NewEndpoint(m3ua.EndpointConfig{
    Role: m3ua.RoleSGP,
    SGP: &m3ua.SGPConfig{
        RecoveryTimer: 2 * time.Second,
    },
})
```

Use `Endpoint.Dial` when that endpoint initiates SCTP, or
`Endpoint.Listen` and `Listener.Accept` when it accepts SCTP. Either
`RoleASP` or `RoleSGP` supports either orientation.

`RoleIPSP` is reserved for the explicit Single Exchange model and Double
Exchange model APIs defined by RFC 4666 Section 4.3. Calling `Dial` or
`Listen` directly on an IPSP endpoint returns `ErrUnsupportedRole` rather than
guessing either model.

A `RoleSGP` endpoint owns one shared Application Server registry, NIF state,
destination state, MTP3 restart coordinator, and recovery budget. Any number of
its `Listener` values and SCTP-initiating or accepted `Association` values use
that same SGP state. Closing one Listener or Association leaves its Endpoint and
sibling associations running; closing the Endpoint closes all of them.

Use `Listener.DistributeData` from a Listener owned by the SGP Endpoint, or
`Association.DistributeData` from an SGP Association that initiated SCTP.
Both paths apply the same Application Server state, recovery queue, and Traffic
Mode rules. Calling `Association.DistributeData` on an ASP returns
`ErrUnsupportedRole`.

SGP state learned from the SS7 side is Endpoint-wide regardless of which peer
initiated SCTP. Existing Listener and Association management methods therefore
act on the same SGP Endpoint state. These procedures reject the ASP role.

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
| `AssociationConfig.Recovery*`, `AssociationConfig.BroadcastFlow*` | `EndpointConfig.SGP` |

No compatibility aliases remain. This makes role and association ownership
visible at every call site and prevents transport orientation from selecting
M3UA procedures accidentally.

## Configuration validation

`AssociationConfig` is role-neutral, but fields that have meaning for only
one role are validated before association processing:

- `ASPIdentifier` is local ASP policy and is rejected on an SGP endpoint.
- `AuthorizeASP` is SGP Association policy and is rejected on an ASP endpoint.
- Recovery queues and Broadcast distribution policy belong to `SGPConfig`;
  supplying `EndpointConfig.SGP` for an ASP or IPSP returns
  `ErrInvalidRoleConfiguration`.
- `Listener.SetNIFAvailable`, `SetASAvailable`, and `SetASAvailableForAS` now
  return an error and reject a non-SGP Listener with `ErrUnsupportedRole`.
- A nil `AssociationConfig` passed to `Endpoint.Dial` returns
  `ErrNilAssociationConfig`.

The configuration selected by `ListenerConfig.SelectAssociationConfig` is
validated after SCTP accept and before socket setup, monitoring, or M3UA
parsing.
