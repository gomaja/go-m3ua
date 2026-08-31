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

Configure Application Server state and SGP distribution policy on the Endpoint
rather than on any one Association:

```go
sgp, err := m3ua.NewEndpoint(m3ua.EndpointConfig{
    Role: m3ua.RoleSGP,
    ApplicationServers: &m3ua.ApplicationServerConfig{
        RecoveryTimer: 2 * time.Second,
        DefaultActivationPolicy: m3ua.ASActivationPolicy{
            RequiredActiveASPs: 2,
        },
    },
    SGP: &m3ua.SGPConfig{
        RecoveryQueueMessages: 1_024,
    },
})
```

Use `Endpoint.Dial` when that endpoint initiates SCTP, or
`Endpoint.Listen` and `Listener.Accept` when it accepts SCTP. Either
`RoleASP` or `RoleSGP` supports either orientation.

An IPSP Association must select its RFC 4666 Section 4.3 exchange model
explicitly:

```go
ipsp, err := m3ua.NewEndpoint(m3ua.EndpointConfig{Role: m3ua.RoleIPSP})

associationConfig := m3ua.NewAssociationConfig(0, 0, 0, 0, 0, 0)
associationConfig.IPSP = &m3ua.IPSPConfig{
    ExchangeModel: m3ua.IPSPExchangeSingle,
    InitiateASPSM: true,
    InitiateASPTM: false,
}
```

Single Exchange and Double Exchange are implemented. `InitiateASPSM` and
`InitiateASPTM` are independent because RFC 4666 permits either IPSP to
initiate either exchange. Neither setting selects which IPSP initiates SCTP;
use `Dial` or `Listen`/`Accept` for that separate RFC 4666 Section 1.4.8
choice.

Double Exchange must move traffic policy out of the Association-wide fields and
into the two RFC 4666 data directions:

```go
associationConfig.IPSP = &m3ua.IPSPConfig{
    ExchangeModel: m3ua.IPSPExchangeDouble,
    ASPSMExchange: m3ua.IPSPASPSMExchangeDouble,
    InitiateASPSM: true,
    InitiateASPTM: true,
    TrafficToLocal: &m3ua.IPSPTrafficConfig{
        TrafficModeType: params.NewTrafficModeType(params.TrafficModeLoadshare),
        NetworkAppearance: params.NewNetworkAppearance(10),
        RoutingContexts: params.NewRoutingContext(11),
    },
    TrafficToPeer: &m3ua.IPSPTrafficConfig{
        TrafficModeType: params.NewTrafficModeType(params.TrafficModeLoadshare),
        NetworkAppearance: params.NewNetworkAppearance(20),
        RoutingContexts: params.NewRoutingContext(22),
    },
}
```

For Double Exchange, Association-wide `TrafficModeType`, `TrafficModes`,
`NetworkAppearance`, and `RoutingContexts` are rejected as ambiguous.
`TrafficToLocal` configures DATA received from the peer and the local
ASP Up/ASP Active procedure. `TrafficToPeer` configures DATA sent to the peer
and the peer ASP Up/ASP Active procedure. A non-nil direction with nil
`RoutingContexts` is a configured contextless AS; a nil direction is disabled.

`ASPSMExchange` is mandatory for Double Exchange. Use
`IPSPASPSMExchangeDouble` for the normal independent ASPSM procedures, or
`IPSPASPSMExchangeSingle` only when both IPSPs have agreed to the RFC 4666
Section 4.3 ASPSM simplification. The simplification does not merge ASPTM or
DATA state. With normal Double Exchange, `InitiateASPSM` requires
`TrafficToLocal`; with the agreed ASPSM simplification it may establish both
directions. `InitiateASPTM` always requires `TrafficToLocal`.

`Association.State()` and `Association.StateChanges()` retain the remote IPSP
state that governs `TrafficToPeer`. Use `Association.IPSPState()` whenever a
Double Exchange application needs both independent directions.

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

## ASP routes across SGs and SGPs

An ASP that reaches a destination through more than one SG now provisions its
local MTP Routes and peer SGP inventory in `EndpointConfig.ASP`. Each
`AssociationConfig.PeerSGP` identifies the remote SGP for that Association.
The Endpoint rejects a missing or unknown SGP identity and rejects an
Association whose Network Appearance or Routing Context is not a provisioned
route of that SGP.

```go
asp, err := m3ua.NewEndpoint(m3ua.EndpointConfig{
    Role: m3ua.RoleASP,
    ASP: &m3ua.ASPConfig{
        SignallingGatewaySelection: m3ua.RouteSelectionPrimaryBackup,
        MTPRoutes: []m3ua.MTPRouteConfig{{
            ID: "sccp",
            DestinationPointCode: 0x220000,
            Mask: 16,
            ServiceIndicators: []uint8{params.ServiceIndSCCP},
        }},
        SignallingGateways: []m3ua.SignallingGatewayConfig{
            // Provision SGs, their SGPs, and each peer-specific ASKey here.
        },
    },
})
```

Use `Endpoint.MTPTransfer` rather than selecting an Association in application
code. RFC 4666 Section 5.5.1.1.1 makes SGP, Association, and stream selection
part of the ASP M3UA function. Primary/backup, loadshare, and broadcast are
available independently between SGs and between the SGPs of one SG.

Use `Endpoint.MTPIndications`, `Endpoint.MTPDestinationStatus`, and
`Endpoint.MTPDestinationStatuses` for the derived MTP3-User view. The plural
snapshot is the authoritative resynchronization source after an indication
queue overflow. Association-level `SignallingStatus` remains useful for
peer-route diagnostics, but a DUNA from one SG is not an MTP-PAUSE while
another SG route remains available.

`ASPConfig.MaxAffectedPointCodesPerSSNM`,
`ASPConfig.MaxSSNMStateRecordsPerRoute`, and
`ASPConfig.MaxSSNMStateRecords` bound SSNM processing and retained route state.
Zero uses the library defaults. A deployment may set explicit positive limits
from its provisioned route inventory; exceeding one returns
`ErrASPRouteStateLimit` and closes the affected Association without partially
applying the message.

The same APIs apply whether the ASP or SGP initiated SCTP. `Dial`, `Listen`,
and `Accept` describe SCTP establishment only; `RoleASP` and `RoleSGP` select
the RFC procedures.

## Routing Key Management

An SGP or IPSP that supports the optional RFC 4666 Sections 3.6 and 4.4
procedures configures the policy on `EndpointConfig`, not on an Association:

```go
endpoint, err := m3ua.NewEndpoint(m3ua.EndpointConfig{
    Role: m3ua.RoleSGP,
    RoutingKeyManagement: &m3ua.RoutingKeyManagementConfig{
        AuthorizeRegistration: func(request m3ua.RoutingKeyRegistrationRequest) m3ua.RegistrationStatus {
            return m3ua.RegistrationSuccessfullyRegistered
        },
        AuthorizeDeregistration: func(request m3ua.RoutingKeyDeregistrationRequest) bool {
            return true
        },
        AllocateRoutingContext: nil, // Select the lowest available non-zero value.
        ProvisionedRoutingKeys: []m3ua.ProvisionedRoutingKey{
            // Optional static Routing Key inventory.
        },
        AllowDynamicRoutingKeys: true,
        MaxDynamicRoutingKeys: 1024,
        RemoveUnusedRoutingKeys: true,
    },
})
```

`AuthorizeRegistration` is mandatory whenever `RoutingKeyManagement` is
configured. Returning `RegistrationSuccessfullyRegistered` approves the
request; a defined failure `RegistrationStatus` becomes that Routing Key's REG
RSP result. `RegistrationRoutingKeyAlreadyRegistered` is determined by the
Endpoint registry rather than authorization policy. `AuthorizeDeregistration`
is optional and defaults to allowing an inactive registered ASP/IPSP to
deregister. A custom `AllocateRoutingContext` may select a non-zero unused
value; otherwise the Endpoint selects the lowest available non-zero value.

An ASP or IPSP starts the corresponding Layer Management procedure through its
Association:

```go
registrations, err := association.RegisterRoutingKeys(ctx,
    m3ua.RoutingKeyRegistration{RoutingKey: routingKey},
)
if err != nil {
    return err
}

_, err = association.DeregisterRoutingContexts(ctx,
    registrations[0].RoutingContext,
)
```

The Association must have completed ASP Up in the relevant traffic direction.
For IPSP Double Exchange, locally originated registration changes only
`TrafficToLocal`; a peer's REG REQ changes only `TrafficToPeer`. Single
Exchange uses its shared traffic scope.

If a wire Routing Key omits Network Appearance and the Association has one
configured Network Appearance, the authorization request exposes that implied
value and sets `NetworkAppearanceImplied`. If neither carries a value, the
Routing Key applies to all Network Appearances and must be the only Routing Key
registered on that Association, per RFC 4666 Section 3.6.1.

The request context bounds a local REG/DEREG wait. RFC 4666 defines no RKM
T(ack); duplicate peer requests are answered from deterministic replay state.
After a written DEREG REQ is canceled, retrying the same Routing Context returns
`ErrDeregistrationOutcomeUnknown` until the delayed DEREG RSP arrives. RFC 4666
Sections 3.6.4 and 4.4.2 correlate the response only by Routing Context, so the
Association cannot safely guess whether a response belongs to the old request
or its retry.

Unresolved REG/DEREG outcomes share a 1,024-result Association budget. A call
that could exceed it returns `ErrRKMOutcomeLimit` without writing a new request;
late responses release capacity.

## Application Server activation

`EndpointConfig.ApplicationServers` configures immutable Application Server
state for an SGP or IPSP Endpoint. `RequiredActiveASPs` is n from RFC 4666
Sections 1.4.4.1 and 4.3.2. Zero selects one. An exact `ASKey` entry overrides
the default:

```go
sgp, err := m3ua.NewEndpoint(m3ua.EndpointConfig{
    Role: m3ua.RoleSGP,
    ApplicationServers: &m3ua.ApplicationServerConfig{
        DefaultActivationPolicy: m3ua.ASActivationPolicy{
            RequiredActiveASPs: 2,
        },
        ActivationPolicies: map[m3ua.ASKey]m3ua.ASActivationPolicy{
            {
                NetworkAppearance:    10,
                NetworkAppearanceSet: true,
                RoutingContext:       20,
                RoutingContextSet:    true,
            }: {
                RequiredActiveASPs: 3,
            },
        },
    },
})
```

Strict startup is the default: the AS does not carry DATA or SSNM until n ASPs
are ASP-ACTIVE. Set `SmoothStart` only when the deployment deliberately uses
the RFC 4666 Section 4.3.2 exception that permits traffic after the first ASP
becomes active. Once an AS is active, it remains active while at least one ASP
is active; Loadshare and Broadcast peers receive the Section 3.8.2 insufficient
resources advisory while the active count is below n.

Override Traffic Mode requires an effective `RequiredActiveASPs` of one.
Invalid provisioned or dynamically registered Override combinations are
rejected before they change live AS policy. Locally configured Association
policy is rejected before SCTP setup.

## Layer Management status and ASP procedures

Endpoint owns the RFC 4666 Sections 1.6.3 and 4.2 status boundary. Every
Association tracked by an Endpoint has a stable Endpoint-local `AssociationID`.
Use exact `ASKey` values for ASP and Application Server queries; a bare Routing
Context is not a unique AS identity when Network Appearances differ.

```go
associations := endpoint.AssociationStatuses()
asps := endpoint.ASPStatuses()
applicationServers := endpoint.ApplicationServerStatuses()
routes := endpoint.MTPRouteStatuses()
destinations := endpoint.DestinationStatuses()
```

Every returned slice and SCTP address is caller-owned. `AssociationSnapshot`
reports a concurrent SCTP close through `SCTPError` instead of silently omitting
the Association.

ASP Up, ASP Down, ASP Active, and ASP Inactive can be automatic or explicitly
requested by Layer Management. A non-nil policy must set all four modes:

```go
associationConfig.ASPProcedures = &m3ua.ASPProcedurePolicy{
    ASPUp:       m3ua.ASPProcedureExplicit,
    ASPDown:     m3ua.ASPProcedureExplicit,
    ASPActive:   m3ua.ASPProcedureExplicit,
    ASPInactive: m3ua.ASPProcedureExplicit,
}

if err := association.ASPUp(ctx); err != nil {
    return err
}
if err := association.ASPActive(ctx, asKey); err != nil {
    return err
}
```

`Dial` or `Accept` returns at the readiness point selected by the policy:
ASP-DOWN when ASP Up is explicit, ASP-INACTIVE when ASP Up is automatic and ASP
Active is explicit, or ASP-ACTIVE when both are automatic. Explicit methods
validate role, state, and exact AS scope before writing, then wait for the
matching acknowledgement within the caller context. In IPSP Double Exchange,
they manage `TrafficToLocal`, the direction established by the local IPSP's ASP
procedures.

`ShutdownContext` performs only procedures configured as automatic. An
explicitly managed application must request its chosen ASP Inactive and ASP
Down procedures before shutdown when graceful withdrawal is required.

## Typed SSNM operations

An active ASP originates RFC 4666 DAUD and optional ASP-to-SGP SCON on the
Association that carries the concerned AS:

```go
scope := m3ua.SSNMScope{
    NetworkAppearance: 10,
    NetworkAppearanceSet: true,
    RoutingContexts: []uint32{20},
    RoutingContextSet: true,
}
destination := m3ua.PointCodeRange{PointCode: 0x123456}

if err := association.DestinationStateAudit(m3ua.DestinationStateAuditRequest{
    Scope: scope, Destinations: []m3ua.PointCodeRange{destination},
}); err != nil {
    return err
}
```

An SGP originates SCON or DUPU through Endpoint so the message reaches every
concerned active ASP without application-side Association iteration:

```go
err := endpoint.SignallingCongestion(m3ua.SignallingCongestionRequest{
    Scope: scope,
    Destinations: []m3ua.PointCodeRange{destination},
    CongestionLevel: 2,
    CongestionLevelSet: true,
})
if err != nil {
    var delivery *m3ua.SSNMDeliveryError
    if errors.As(err, &delivery) {
        log.Printf("delivered to %v; failed for %v", delivery.Successful, delivery.Failed)
    }
    return err
}
```

`CongestionLevelSet` distinguishes an omitted level from explicit level zero;
zero is congestion abatement. `ConcernedDestination` is accepted only for the
optional ASP-to-SGP SCON direction. DUPU accepts one unmasked destination and a
valid RFC User/Cause combination. Role-invalid and malformed requests fail
before any write.

## Management indications

`Association.ManagementIndications` reports M-NOTIFY, M-ERROR,
M-SCTP_RELEASE, and M-SCTP_RESTART with complete scope:

```go
for indication := range association.ManagementIndications() {
    log.Printf("association=%d kind=%s AS=%v destinations=%v cause=%v",
        indication.Association,
        indication.Kind,
        indication.ASKeys,
        indication.AffectedDestinations,
        indication.Cause,
    )
}
```

`ASKeys` retains Network Appearance and contextless-AS presence.
`AffectedDestinations` retains point-code masks and exact AS scope. `Cause` is
set only for local failures; a received Error uses `ErrorCode`. The indication
owns all slices, so the application may retain or modify them.

The channel is bounded and never silently drops a mandatory event. If the
consumer falls behind, the Association closes with `ErrIndicationQueueFull`.
After that failure, rebuild current state from Endpoint status snapshots rather
than treating the last indication as authoritative.

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
| `AssociationConfig.RecoveryTimer` | `EndpointConfig.ApplicationServers` |
| `AssociationConfig.RecoveryQueue*`, `AssociationConfig.BroadcastFlow*` | `EndpointConfig.SGP` |

No compatibility aliases remain. This makes role and association ownership
visible at every call site and prevents transport orientation from selecting
M3UA procedures accidentally.

## Configuration validation

`AssociationConfig` is role-neutral, but fields that have meaning for only
one role are validated before association processing:

- `ASPIdentifier` is local ASP policy and is rejected on an SGP endpoint.
- `AuthorizeASP` is SGP Association policy and is rejected on an ASP endpoint.
- Application Server recovery and activation policy belongs to
  `ApplicationServerConfig`; supplying `EndpointConfig.ApplicationServers` for
  an ASP returns `ErrInvalidRoleConfiguration`.
- Recovery queues and Broadcast distribution policy belong to `SGPConfig`;
  supplying `EndpointConfig.SGP` for an ASP or IPSP returns
  `ErrInvalidRoleConfiguration`.
- `Listener.SetNIFAvailable`, `SetASAvailable`, and `SetASAvailableForAS` now
  return an error and reject a non-SGP Listener with `ErrUnsupportedRole`.
- A nil `AssociationConfig` passed to `Endpoint.Dial` returns
  `ErrNilAssociationConfig`.
- An ASP Endpoint with routing policy requires `AssociationConfig.PeerSGP` and
  validates the Association's `ASKey` scope before SCTP setup or M3UA parsing.

The configuration selected by `ListenerConfig.SelectAssociationConfig` is
validated after SCTP accept and before socket setup, monitoring, or M3UA
parsing.
