# Migrating to v1.2

v1.2 is a deliberate break. It separates the M3UA protocol role from SCTP
association initiation, moves everything about one message into that message,
moves everything shared by several associations onto the `Endpoint`, and removes
the API shapes that made a peer's wire label look like a global identity.

There are no compatibility aliases. Every removal below has a named replacement,
and the replacement is checked against the exported surface by
`export_inventory_test.go`, so this document cannot quietly drift from the code.

Contents:

- [Endpoint role](#endpoint-role)
- [Application Server membership](#application-server-membership)
- [The I/O break: `net.Conn` is gone](#the-io-break-netconn-is-gone)
- [What the library checks and what stays yours](#what-the-library-checks-and-what-stays-yours)
- [Destinations, SSNM and restart](#destinations-ssnm-and-restart)
- [Routing Key Management](#routing-key-management)
- [ASP routes across Signalling Gateways](#asp-routes-across-signalling-gateways)
- [Local route references versus Routing Keys](#local-route-references-versus-routing-keys)
- [What is immutable, what changes at runtime, what needs a reconnect](#what-is-immutable-what-changes-at-runtime-what-needs-a-reconnect)
- [Ownership and shutdown](#ownership-and-shutdown)
- [Codec cleanup](#codec-cleanup)
- [Complete v1.1.1 API disposition](#complete-v111-api-disposition)
- [After v1.2.0: SSNM events are deltas](#after-v120-ssnm-events-are-deltas)

## Endpoint role

RFC 4666 Section 1.4.8 recommends that both ASPs and SGPs support initiating and
accepting SCTP associations; `Dial` therefore no longer implies ASP and `Accept`
no longer implies SGP.

```go
asp, err := m3ua.NewEndpoint(m3ua.EndpointConfig{Role: m3ua.RoleASP})
sgp, err := m3ua.NewEndpoint(m3ua.EndpointConfig{Role: m3ua.RoleSGP})
```

Shared state belongs to the Endpoint, not to any one Association or Listener:

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

A `RoleSGP` Endpoint owns one Application Server registry, NIF state,
destination state, MTP3 restart coordinator and recovery budget. Any number of
its Listeners and its SCTP-initiating or accepted Associations share that state.

An IPSP Association must select its RFC 4666 Section 4.3 exchange model
explicitly, and must state which procedures it initiates in
`AssociationConfig.ASPProcedures`. There is no role-implied default, because
either IPSP may initiate either exchange:

```go
ipsp, err := m3ua.NewEndpoint(m3ua.EndpointConfig{Role: m3ua.RoleIPSP})

associationConfig := m3ua.NewAssociationConfig()
associationConfig.IPSP = &m3ua.IPSPConfig{ExchangeModel: m3ua.IPSPExchangeSingle}
associationConfig.ASPProcedures = &m3ua.ASPProcedurePolicy{
    ASPUp:       m3ua.ASPProcedureAutomatic,
    ASPDown:     m3ua.ASPProcedureAutomatic,
    ASPActive:   m3ua.ASPProcedureExplicit,
    ASPInactive: m3ua.ASPProcedureAutomatic,
}
```

`IPSPConfig.InitiateASPSM` and `IPSPConfig.InitiateASPTM` are removed.
`ASPProcedurePolicy` supersedes them and covers all four procedures rather than
two, and it applies to every role instead of only to IPSP.

Double Exchange gives each direction its own Application Server inventory:

```go
associationConfig.IPSP = &m3ua.IPSPConfig{
    ExchangeModel: m3ua.IPSPExchangeDouble,
    ASPSMExchange: m3ua.IPSPASPSMExchangeDouble,
    TrafficToLocal: &m3ua.IPSPTrafficConfig{
        ApplicationServers: []m3ua.ASConfig{{
            ASKey: m3ua.ASKey{
                NetworkAppearance: 10, NetworkAppearanceSet: true,
                RoutingContext: 11, RoutingContextSet: true,
            },
            TrafficMode: params.TrafficModeLoadshare,
        }},
    },
    TrafficToPeer: &m3ua.IPSPTrafficConfig{
        ApplicationServers: []m3ua.ASConfig{{
            ASKey: m3ua.ASKey{
                NetworkAppearance: 20, NetworkAppearanceSet: true,
                RoutingContext: 22, RoutingContextSet: true,
            },
            TrafficMode: params.TrafficModeLoadshare,
        }},
    },
}
```

`TrafficToLocal` configures DATA received from the peer and the local ASP Up and
ASP Active procedures. `TrafficToPeer` configures DATA sent to the peer and the
peer's procedures. A non-nil direction with an empty inventory is a contextless
Application Server in that direction; a nil direction is disabled.

`ASPSMExchange` is mandatory for Double Exchange. Use `IPSPASPSMExchangeDouble`
for the normal independent ASPSM procedures, or `IPSPASPSMExchangeSingle` only
when both IPSPs have agreed to the Section 4.3 ASPSM simplification, which does
not merge ASPTM or DATA state.

## Application Server membership

`AssociationConfig.RoutingContexts`, `NetworkAppearance`, `TrafficModeType` and
`TrafficModes` are removed, together with the setters that wrote them. One
inventory replaces all four:

| Before | After |
| --- | --- |
| `config.SetRoutingContexts(1, 2)` | `config.SetApplicationServers(m3ua.ASConfig{ASKey: …}, m3ua.ASConfig{ASKey: …})` |
| `config.SetNetworkAppearance(7)` | `ASConfig.ASKey.NetworkAppearance` with `NetworkAppearanceSet: true` |
| `config.SetTrafficModeType(params.TrafficModeLoadshare)` | `ASConfig.TrafficMode`, per Application Server |
| `ConnConfig.TrafficModes` | `ASConfig.TrafficMode`, per Application Server |

```go
config := m3ua.NewAssociationConfig().
    SetApplicationServers(
        m3ua.ASConfig{
            ASKey: m3ua.ASKey{
                NetworkAppearance: 7, NetworkAppearanceSet: true,
                RoutingContext: 1, RoutingContextSet: true,
            },
            TrafficMode: params.TrafficModeLoadshare,
        },
        m3ua.ASConfig{
            ASKey: m3ua.ASKey{
                NetworkAppearance: 7, NetworkAppearanceSet: true,
                RoutingContext: 2, RoutingContextSet: true,
            },
            TrafficMode: params.TrafficModeOverride,
        },
    )
```

Why the change: Traffic Mode is agreed per Application Server, and RFC 4666
Section 3.7.1 defines Override, Loadshare and Broadcast as properties of an AS,
not of a transport. An association-wide mode forced every Application Server on
one association to share one mode, which is not what the protocol says.

An empty inventory is the contextless Application Server of RFC 4666 Section
3.6.1. Declaring it explicitly — an `ASConfig` whose `ASKey` leaves
`RoutingContextSet` false — is how it is given a Network Appearance or a Traffic
Mode. An Association may declare at most one contextless Application Server, and
not beside Routing-Context-scoped ones, because a message that omitted the
Routing Context would otherwise name both.

## The I/O break: `net.Conn` is gone

v1.1 presented an association as something close to a `net.Conn`, with `Read`,
`Write`, deadlines, and a family of variants for the parameters that did not fit
that shape. v1.2 removes all of it. `Association` does not implement `net.Conn`,
`io.Reader` or `io.Writer`, and never claimed to correctly: an M3UA association
carries framed messages with per-message scope, not a byte stream.

| Removed | Replacement |
| --- | --- |
| `Conn.Read`, `Conn.ReadPD` | `Association.ReadData(ctx) (*DataMessage, error)` |
| `Conn.Write`, `Conn.WritePD` | `Association.WriteData(DataRequest) (int, error)` |
| `Conn.WriteToStream`, `Conn.WritePDToStream` | `DataRequest.Stream` |
| `Conn.WriteWithRoutingContext`, `Conn.WritePDWithRoutingContext`, `Conn.WriteToStreamWithRoutingContext`, `Conn.WritePDToStreamWithRoutingContext` | `DataRequest.AS` |
| `Conn.SelectRoutingContext` | `DataRequest.AS`; nothing about a message is held on the association |
| `ConnConfig.CorrelationID`, `Config.SetCorrelationID` | `DataRequest.CorrelationID` with `CorrelationIDSet` |
| `ConnConfig.OriginatingPointCode`, `DestinationPointCode`, `ServiceIndicator`, `NetworkIndicator`, `MessagePriority`, `SignallingLinkSelection` | `DataRequest.ProtocolData`, which is the whole MTP3 routing label |

```go
written, err := association.WriteData(m3ua.DataRequest{
    AS: m3ua.ASKey{
        NetworkAppearance: 7, NetworkAppearanceSet: true,
        RoutingContext: 1, RoutingContextSet: true,
    },
    ProtocolData: params.ProtocolDataPayload{
        OriginatingPointCode:    0x111111,
        DestinationPointCode:    0x222222,
        ServiceIndicator:        params.ServiceIndSCCP,
        SignallingLinkSelection: 1,
        Data:                    payload,
    },
})
```

RFC 4666 Section 3.3.1 makes every one of those fields per-message. Holding any
of them on the association meant two goroutines sending concurrently could take
each other's scope or each other's routing label; there is now nothing to take.

`written` is the count of SS7 user octets the local transport accepted. RFC 4666
defines no acknowledgement for DATA, so it is never a claim about delivery.
Every failure is a `*DataWriteError`:

```go
var writeErr *m3ua.DataWriteError
if errors.As(err, &writeErr) {
    switch writeErr.Outcome {
    case m3ua.DataNotSent:
        // Nothing reached the transport, or the transport refused the whole
        // message (a full SCTP send buffer, or a write deadline that expired
        // waiting for one). Re-sending cannot duplicate SS7 traffic, so the
        // application may safely retry or reroute.
    case m3ua.DataSendIndeterminate:
        // Submission had begun. The peer may or may not have the message; the
        // application owns the retry decision, because a resend may duplicate.
    }
}
```

`errors.Is` and `errors.As` still reach the cause, so a `*RoutingContextError`,
an `*InvalidSCTPStreamIDError` or the transport's own error is matchable exactly
as before.

Reading is context-scoped:

```go
message, err := association.ReadData(ctx)
```

Cancelling `ctx` ends that one read. The association stays open, nothing queued
is discarded, and several goroutines may read concurrently with each message
delivered to exactly one of them. `SetReadDeadline` and `SetDeadline` still work
and still report `os.ErrDeadlineExceeded`, which is recoverable.

`DataMessage` reports three things separately, and the separation is the point:
`Scope` is what the peer put on the wire, presence bits included; `AS` is the
Application Server it resolved to; `Epoch` is the SCTP association generation.
`DataMessage.NetworkAppearance`, `NetworkAppearanceSet`, `RoutingContext` and
`RoutingContextSet` are removed — they were a projection of `Scope` that could
not represent an absent parameter or a list.

## What the library checks and what stays yours

A direct `WriteData` is checked by the library, in this order:

1. Association state. RFC 4666 Section 4.3.1 requires ASP-ACTIVE; anything else
   fails with `ErrNotEstablished` and `DataNotSent`.
2. Message structure, including the maximum Protocol Data payload one DATA can
   carry.
3. SCTP stream constraints from Section 1.4.7: stream 0 is never used for DATA,
   and an explicit stream is validated against the negotiated count. A zero
   `Stream` selects the stream this message's own SLS maps to.
4. The Application Server binding and its activation: the named `ASKey` must be
   one this Association is configured and authorized to carry, and it must have
   been activated.

Everything below is the application's, and the library deliberately does not do
it on a direct write:

- **Destination availability.** `WriteData` does not consult SSNM state. An
  application that owns outbound selection has already made that decision, and
  SSNM state is published to it separately through `Endpoint.SubscribeSSNM`.
  `Endpoint.MTPTransfer` is the path that does consult it.
- **Upper-layer routing policy.** Which Signalling Gateway, which Application
  Server, which association — unless `ASPConfig.Routing` is configured, in which
  case `MTPTransfer` owns it.
- **Retry.** Nothing is retried automatically, because a failed write does not
  prove the peer received nothing. `DataSendOutcome` is what that decision is
  made from.
- **Congestion response.** SCON is delivered; what to do about it — shed load,
  reroute, apply Message Priority — is upper-layer policy. A congestion policy
  can be installed for `MTPTransfer` only.
- **Persistence and orchestration.** The library retains no state across
  process restarts.

## Destinations, SSNM and restart

Every destination method on `Association` and `Listener` is removed. Publication
belongs to the Endpoint, because an SGP's view of the SS7 network is shared by
every ASP it serves, and retained knowledge at an ASP belongs to the Signalling
Gateway that reported it rather than to one association.

| Removed from `Association` and `Listener` | Replacement |
| --- | --- |
| `SetDestinationState`, `SetDestinationStateForNetwork`, `SetDestinationStateForNetworkAndRoutingContext`, `SetDestinationRange` and its two scoped forms | `Endpoint.ReportDestinationAvailability(DestinationAvailabilityRequest)` |
| `ReportDestinationState`, `ReportDestinationRange` and their scoped forms | `Endpoint.ReportDestinationAvailability`, `Endpoint.SignallingCongestion`, `Endpoint.DestinationUserPartUnavailable` |
| `DestinationState`, `DestinationStates`, `DestinationRanges` and their scoped forms | `Endpoint.DestinationStatus`, `Endpoint.DestinationStatuses` at an SGP; `Endpoint.SSNMKnowledge` and `Endpoint.SubscribeSSNM` at an ASP |
| `Conn.PeerCongestionLevel` | `Endpoint.SubscribeSSNM`; peer SCON reports carry `SSNMReport.PeerReported` |
| `Association.SignallingStatus`, `Conn.SignallingStatus` | `Endpoint.SubscribeSSNM`; identify the association through `SSNMReport.Association` |
| `Association.BeginMTP3Restart`, `Listener.BeginMTP3Restart` | `Endpoint.BeginMTP3Restart` |

The association-level status channel and the public `DestinationStatus` and
`DestinationRange` types are removed, without compatibility wrappers. Subscribe
before receiving reports; `SubscribeSSNM` atomically returns retained knowledge
and a subscription for subsequent changes. Each report identifies its association
and wire scope. Use `Endpoint.SSNMKnowledge` for retained peer knowledge and
`Endpoint.DestinationStatus` or `DestinationStatuses` for local SGP authority,
not an association-level cache as an application routing contract.

A report describes the message received, not a combined availability/congestion
snapshot. Read the report kind before interpreting its fields. DUPU and peer
SCON are event-only: a snapshot does not reconstruct lost user-part or peer
congestion indications. Events are deltas applied to that snapshot: a report
event's `Updated` names only the destinations it wrote. Handle
`SSNMContinuityLostEvent` with `SSNMSubscription.Resync` and account for
event-only information that remains unknown. Closing one
association does not close an Endpoint subscription; observe binding/partition
lifecycle events. Close the subscription when its consumer stops, or close the
owning Endpoint to terminate all its subscriptions.

The `DestinationState` type is removed and split in two, because RFC 4666
Section 4.5.2.2 keeps availability and congestion as two separate statuses of
the same destination:

| Before | After |
| --- | --- |
| `DestinationState` with a `DestinationCongested` value | `DestinationAvailability` — `DestinationAvailable`, `DestinationRestricted`, `DestinationUnavailable` |
| — | `CongestionState` — `Congested`, `Level`, `LevelSet` |

A DUNA, DAVA or DRST moves availability alone; a SCON moves congestion alone.
Collapsing them meant a destination returning to service silently became
uncongested, and a congested one silently became unreachable.

`SSNMScope` is renamed `WireScope`, and the rename carries a meaning change.
`WireScope` is the exact Network Appearance and Routing Context a peer put on
the wire, before any resolution. What retained knowledge is *owned by* is
`SSNMPartition`, the canonical pair of one Signalling Gateway and one
Application Server:

```go
scope := m3ua.WireScope{
    NetworkAppearance: 10, NetworkAppearanceSet: true,
    RoutingContexts:   []uint32{20},
    RoutingContextSet: true,
}
if err := association.DestinationStateAudit(m3ua.DestinationStateAuditRequest{
    Scope:        scope,
    Destinations: []m3ua.PointCodeRange{{PointCode: 0x123456}},
}); err != nil {
    return err
}
```

Consuming SSNM knowledge is `Endpoint.SubscribeSSNM`, which returns an owned
snapshot and a subscription atomically with respect to each other:

```go
snapshot, subscription, err := endpoint.SubscribeSSNM()
if err != nil {
    return err
}
defer func() { _ = subscription.Close() }()

for {
    event, err := subscription.Next(ctx)
    if err != nil {
        return err
    }
    if event.ContinuityLost {
        resynced, err := subscription.Resync()
        if err != nil {
            return err
        }
        apply(resynced)
        continue
    }
    applyDelta(event)
}
```

The store is bounded in every dimension a peer controls, and it **refuses**
rather than evicting: a bound that is reached increments
`SSNMSnapshot.RecordsRefused` or `ReportsRefused`, and a subscription that falls
behind is told so with `ContinuityLost`. Silent eviction would have handed the
application a view that looked complete and was not.

## Routing Key Management

`Association.DeregisterRoutingContexts` is removed. It took bare Routing
Contexts, which do not identify an Application Server:

```go
registrations, err := association.RegisterRoutingKeys(ctx,
    m3ua.RoutingKeyRegistration{
        RemoteAS: "as-core",
        RoutingKey: m3ua.RoutingKey{
            NetworkAppearance: 10, NetworkAppearanceSet: true,
            TrafficMode:       params.TrafficModeLoadshare,
            TrafficModeSet:    true,
            Groups: []m3ua.RoutingKeyGroup{{
                DestinationPointCode:  0x222222,
                ServiceIndicators:     []uint8{params.ServiceIndSCCP},
                OriginatingPointCodes: []m3ua.PointCodeRange{{PointCode: 0x111111}},
            }},
        },
    },
)
if err != nil {
    return err
}

_, err = association.DeregisterApplicationServers(ctx, registrations[0].ASKey)
```

`RoutingKeyPayload.Groups` is now the sole representation of a Routing Key's
traffic selector. The flat Destination Point Code, Service Indicators and
Originating Point Code List fields are gone: RFC 4666 Section 3.6.1 makes the
grouping repeatable, and a flat projection could not express a second group
without losing one.

`DeregisterApplicationServers` names the exact wire scope a registration
confirmed. A scope without a Routing Context, a scope that contradicts the
binding this Association holds, and a repeated Routing Context are all refused
before anything reaches the transport, because RFC 4666 Section 3.6.3 carries
only the Routing Context in DEREG REQ — the peer would act on that value
whatever Application Server the caller believed it was naming, and a submitted
request cannot be taken back.

RFC 4666 defines no RKM acknowledgement timer. A local wait is bounded by the
caller's context; a duplicate peer request is answered from deterministic replay
state. After a written DEREG REQ is cancelled, retrying the same Routing Context
returns `ErrDeregistrationOutcomeUnknown` until the delayed DEREG RSP arrives,
because Sections 3.6.4 and 4.4.2 correlate the response only by Routing Context.

Unresolved REG and DEREG outcomes share a 1,024-result Association budget; a
call that would exceed it returns `ErrRKMOutcomeLimit` without writing.

The responder policy is Endpoint-wide, as `EndpointConfig.RoutingKeyManagement`:

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
        AllocateRoutingContext:  nil, // Select the lowest available non-zero value.
        AllowDynamicRoutingKeys: true,
        MaxDynamicRoutingKeys:   1024,
        RemoveUnusedRoutingKeys: true,
    },
})
```

If a wire Routing Key omits the Network Appearance and the Association has one
configured, the authorization request exposes that implied value and sets
`NetworkAppearanceImplied`. If neither carries one, the Routing Key applies to
all Network Appearances and must be the only one registered on that Association,
per RFC 4666 Section 3.6.1.

## ASP routes across Signalling Gateways

Peer inventory and outbound routing are now two separate things, and only the
first is required.

```go
endpoint, err := m3ua.NewEndpoint(m3ua.EndpointConfig{
    Role: m3ua.RoleASP,
    ASP: &m3ua.ASPConfig{
        SignallingGateways: []m3ua.SignallingGatewayConfig{{
            ID: "sg-a",
            SGPs: []m3ua.SignallingGatewayProcessConfig{{
                ID: "sgp-a1",
                ApplicationServers: []m3ua.RemoteASConfig{{
                    ID:    "as-core",
                    ASKey: &asKey,
                }},
            }},
        }},
        // Routing is optional. Leaving it nil selects application-managed
        // routing: the library keeps peers, Application Servers, procedures and
        // authorization; the application owns outbound candidate selection.
        Routing: &m3ua.ASPRoutingConfig{
            SignallingGatewaySelection: m3ua.RouteSelectionPrimaryBackup,
            SignallingGatewayProcessSelection: map[m3ua.SignallingGatewayID]m3ua.RouteSelectionMode{
                "sg-a": m3ua.RouteSelectionPrimaryBackup,
            },
            Paths: []m3ua.MTPRoutePath{{
                ID:                 "via-sg-a",
                SignallingGateway:  "sg-a",
                ApplicationServers: []m3ua.RemoteASID{"as-core"},
            }},
            MTPRoutes: []m3ua.MTPRouteConfig{{
                ID:                   "sccp",
                DestinationPointCode: 0x220000,
                Mask:                 16,
                ServiceIndicators:    []uint8{params.ServiceIndSCCP},
                Paths:                []m3ua.MTPRoutePathID{"via-sg-a"},
            }},
        },
    },
})
```

| Before | After |
| --- | --- |
| `ASPConfig.MTPRoutes` | `ASPRoutingConfig.MTPRoutes` |
| `ASPConfig.SignallingGatewaySelection` | `ASPRoutingConfig.SignallingGatewaySelection` |
| `ASPConfig.CongestionPolicy` | `ASPRoutingConfig.CongestionPolicy` |
| `ASPConfig.TransferFlowCacheEntries` | `ASPRoutingConfig.TransferFlowCacheEntries` |
| `SignallingGatewayConfig.SGPSelection` | `ASPRoutingConfig.SignallingGatewayProcessSelection`, keyed by Signalling Gateway |
| `SignallingGatewayProcessConfig.Routes []SGPRoute` | `SignallingGatewayProcessConfig.ApplicationServers []RemoteASConfig` for inventory, plus `ASPRoutingConfig.Paths` for candidates |
| `SGPRoute{MTPRoute, AS}` | `MTPRoutePath{ID, SignallingGateway, ApplicationServers}`, referenced by `MTPRouteConfig.Paths` |
| `MTPTransferResult.TransmittedAssociations` | `MTPTransferResult.SuccessfulPaths []MTPTransferPath` |
| `MTPTransferError.SuccessfulSGPs`, `MTPTransferFailure.SGP` | `MTPTransferError.SuccessfulPaths`, `MTPTransferFailure.Target` |

`SGASKey{SignallingGateway, ApplicationServer}` is the canonical identity of one
Application Server reached through one Signalling Gateway. The wire scope that
names it belongs to the individual SGP, not to that identity: RFC 4666 Section
1.4.2.1 makes a Routing Context "an index into a sending node's Message
Distribution Table", so two SGPs of one Signalling Gateway may label the same
Application Server differently, and the same value at another Signalling Gateway
means something else entirely.

A failed selection is an `*MTPSelectionError` listing every candidate the route
tried and why, in the route's own candidate order. The new
`ErrDestinationStateUnknown` and `ASPRoutingConfig.AllowUnknownDestinations`
make the unknown-destination decision explicit: per-path selection fails closed
on a destination no Signalling Gateway has reported, unless a deployment opts
into sending anyway.

Note that `Endpoint.MTPIndications` and `Endpoint.MTPDestinationStatus(es)` read
a capable-but-silent Signalling Gateway as **Available**, following RFC 4666
Appendix A.2.2's negative definition of capability, while per-path selection
fails closed on **unknown**. Both are intended. One is the ASP's aggregate
MTP3-User view under Section 4.5.2.2; the other is a decision about one
candidate. They disagree for exactly one case — an established, activated,
silent Signalling Gateway — and neither is derived from the other.

## Local route references versus Routing Keys

Three things are easy to conflate, and v1.2 keeps them apart deliberately.

- **An MTP Route** (`MTPRouteConfig`) is local. It describes the MTP3
  routing-label fields this ASP uses to pick a local route: destination point
  code and mask, optional Service Indicators, optional Originating Point Codes.
  It is never sent. It is deliberately not called a Routing Key, because RFC 4666
  defines a Routing Key within one Signalling Gateway, while one local MTP Route
  here may be carried by different peer Routing Keys at different Signalling
  Gateways.
- **A path** (`MTPRoutePath`) is also local, and is a name for a candidate: one
  Signalling Gateway and its Application Servers in preference order. Routes
  reference paths by `MTPRoutePathID`, so several routes can share one candidate
  instead of repeating it. `MTPRoutePathID`, `MTPRouteID` and `RemoteASID` are
  strings you choose; none of them is on the wire.
- **A Routing Key** (`RoutingKey`) is the RFC 4666 Section 3.6.1 traffic selector
  that really is exchanged, in REG REQ, and that a Signalling Gateway answers
  with the Routing Context it assigned. That assigned Routing Context, together
  with the Network Appearance, is the `ASKey` that goes on the wire.

So an `ASKey` is peer-specific and real; an `MTPRouteID` is yours and private. A
`RemoteASConfig` binds one to the other, either statically with `ASKey` or
dynamically with `RoutingKey`, in which case the wire Routing Context is
whatever the SGP assigns during registration and is resolved per Association at
selection time rather than provisioned.

## What is immutable, what changes at runtime, what needs a reconnect

**Immutable for the life of the object.** These are deep-copied when the object
is created, so mutating the caller's struct afterwards changes nothing:

- `EndpointConfig` and everything it contains — role, ASP peer inventory and
  routing, SGP policy, Application Server activation policy, Routing Key
  Management policy, SSNM store bounds — fixed at `NewEndpoint`.
- `AssociationConfig` — fixed at `Dial`, or at `Accept` from whichever
  configuration `ListenerConfig.SelectAssociationConfig` returned. There is no
  setter on a live `Association`, and `Listener` is not a mutable configuration
  object: the promoted `Listener.SetAspIdentifier`, `SetNoDelayConfig` and
  `SetSackConfig` methods are removed.
- `ListenerConfig`, including its default `AssociationConfig` snapshot and its
  selector, fixed at `Endpoint.Listen`.

**Changes at runtime, without any reconnection.** These are protocol state, not
configuration:

- ASP and AS state, through the ASP Up, ASP Down, ASP Active and ASP Inactive
  procedures — including *partial* activation, where a Routing Context is
  acknowledged and another is refused.
- Application Server membership through RKM, when the responder allows it: a
  registration adds an Application Server to an Association's scope and a
  deregistration removes it, with no new SCTP association.
- Destination availability and congestion, through SSNM.
- NIF availability at an SGP, and an MTP3 restart cycle.
- Which path an `MTPTransfer` chooses, as availability, congestion and
  activation change.

**Needs a new Association.** Anything in the immutable set. In practice:

- Changing an Association's Application Server inventory, Network Appearance,
  Traffic Mode, ASP Identifier, heartbeat policy, procedure policy, exchange
  model or peer SGP identity.
- Changing a Listener's default configuration or its selector.

**Needs a new Endpoint.** The role itself, the ASP peer and route inventory, the
SGP recovery and distribution policy, the Application Server activation policy,
the Routing Key Management policy and the SSNM store bounds. Closing an Endpoint
closes everything it owns, so a configuration change at this level is a restart
of that Endpoint.

**Needs protocol renegotiation rather than reconfiguration.** A Traffic Mode
that is already agreed for an Application Server cannot be changed by either
side while the agreement stands; the ASP must go inactive for that Application
Server and activate again. A dynamically registered Routing Key's Routing
Context is chosen by the responder, so changing the key means deregistering and
registering again, not editing anything locally.

## Ownership and shutdown

Three scopes own resources, and each closes exactly its own:

| Call | Closes | Leaves alone |
| --- | --- | --- |
| `Association.Close` | that one association, its goroutines and its SCTP association | its Listener, its Endpoint, every sibling association |
| `Listener.Close` | the listening socket and every Association *that Listener accepted* | the Endpoint, its shared state, associations from `Dial` or another Listener |
| `Endpoint.Close` | every Listener and Association the Endpoint owns, then the shared state | nothing it owns |

All three are RFC 4666 Section 4.9 option (b): SCTP release without M3UA
withdrawal. Option (a) is `Association.ShutdownContext`, which runs the
procedures `AssociationConfig.ASPProcedures` marks automatic and then closes.
Nothing calls it for you.

The `ctx` given to `Dial` and `Accept` is the association's lifetime, not just
its handshake. Cancelling it closes the associations it produced.

## Codec cleanup

The `messages` and `messages/params` packages had two spellings of every
operation: the canonical `encoding.BinaryMarshaler` and `BinaryUnmarshaler`
names, and a family of wrappers — `Serialize`, `SerializeTo`, `Decode`,
`DecodeFromBytes`, `Len` — that logged a deprecation line and forwarded. All 114
wrappers are removed.

| Removed | Replacement |
| --- | --- |
| `(*T).Serialize` | `(*T).MarshalBinary` |
| `(*T).SerializeTo` | `(*T).MarshalTo` |
| `(*T).DecodeFromBytes` | `(*T).UnmarshalBinary` |
| `(*T).Len` | `(*T).MarshalLen` |
| `messages.Decode`, `messages.DecodeX` | `messages.Parse`, `messages.ParseX` |
| `params.Decode`, `params.DecodeX`, `params.SerializeMultiParams` | `params.Parse`, `params.ParseX`, the canonical marshallers |

Two spellings of one operation is two things a reader has to check against the
wire, and the forwarding layer wrote to the standard logger from inside a parse,
which turned a peer's malformed message into output on the host process's
stderr.

`ManagementIndication` also loses two projections. `RoutingContext` and
`RoutingContextSet` reported only the first Routing Context of an indication
that may name several, and `AffectedPointCodes` reported only the first unmasked
destination. `ASKeys` and `AffectedDestinations` replace them and retain the
whole scope, including masks, Network Appearance and contextless-AS identity.
The indication owns its slices, so the application may retain or modify them.

## Complete v1.1.1 API disposition

Every incompatible declaration and public field across the module.

| v1.1.1 declaration | v1.2.0 disposition |
| --- | --- |
| `Config`, `ConnConfig` | Replaced by `AssociationConfig`; the alias and the transport-oriented name are removed. |
| `ConnConfigSelector` | Replaced by `AssociationConfigSelector`, immutable inside `ListenerConfig` and run before M3UA parsing. |
| `Conn` | Replaced by `Association`. There is no alias, and it is not a `net.Conn`. |
| `SctpNoDelayInfo`, `SctpSackInfo` | Replaced by `SCTPNoDelayInfo` and `SCTPSACKInfo`. |
| `Dial`, `Listen` | Replaced by `Endpoint.Dial` and `Endpoint.Listen`; the M3UA role is fixed by the owning `Endpoint`. |
| `NewConfig`, `NewClientConfig`, `NewServerConfig` | Replaced by `NewEndpoint(EndpointConfig{Role: …})` plus `NewAssociationConfig()` and explicit setters. The removed constructors inferred an M3UA role from transport orientation. |
| `NewListenerConfig` | Retained with a new signature: it takes `*AssociationConfig` and its result is passed to `Endpoint.Listen`. |
| `Config.EnableHeartbeat` | Same operation on `AssociationConfig`. |
| `Config.SetAspIdentifier` | Replaced by `AssociationConfig.SetASPIdentifier`. |
| `Config.SetCorrelationID` | Removed. Correlation Id is per message: `DataRequest.CorrelationID`. |
| `Config.SetNetworkAppearance`, `Config.SetRoutingContexts`, `Config.SetTrafficModeType` | Replaced by `AssociationConfig.SetApplicationServers` and `ASConfig`. |
| `Config.SetNoDelayConfig`, `Config.SetSackConfig` | Replaced by `AssociationConfig.SetSCTPNoDelay` and `SetSCTPSACK`. |
| `ConnConfig.AspIdentifier` | Replaced by `AssociationConfig.ASPIdentifier`. |
| `ConnConfig.NetworkAppearance`, `RoutingContexts`, `TrafficModeType`, `TrafficModes` | Replaced by `AssociationConfig.ApplicationServers []ASConfig`. |
| `ConnConfig.CorrelationID`, `OriginatingPointCode`, `DestinationPointCode`, `ServiceIndicator`, `NetworkIndicator`, `MessagePriority`, `SignalingLinkSelection` | Removed. Every one is per message, in `DataRequest`. |
| `ConnConfig.SctpNoDelayInfo`, `SctpSackInfo` | Replaced by `AssociationConfig.SCTPNoDelayInfo` and `SCTPSACKInfo`. |
| `ConnConfig.RecoveryTimer` | Moved to `EndpointConfig.ApplicationServers.RecoveryTimer`, because T(r) is shared AS state. |
| `ConnConfig.RecoveryQueue*`, `BroadcastFlow*` | Moved to `EndpointConfig.SGP`; these are SGP-wide limits, not per-Association policy. |
| `HeartbeatInfo.Data` | Removed. Heartbeat Data is generated per BEAT and echoed exactly as received. |
| `Conn.Read`, `Conn.ReadPD` | Replaced by `Association.ReadData(ctx)`. |
| `Conn.Write`, `WritePD`, `WriteToStream`, `WritePDToStream`, and every `*WithRoutingContext` form | Replaced by `Association.WriteData(DataRequest)`. |
| `Conn.SelectRoutingContext` | Removed. `DataRequest.AS` names the scope per message. |
| `Conn.ActivateRoutingContexts`, `DeactivateRoutingContexts` | Same operations on `Association`; the scoped `ASPActive` and `ASPInactive` procedures are also available. |
| `Conn.AssociationStatus`, `Close`, `Done`, `Err`, `LocalAddr`, `RemoteAddr`, `Shutdown`, `ShutdownContext`, `State`, `StateChanges` | Same operations on `Association`. Endpoint-wide status is additionally available from `Endpoint`. |
| `Conn.ManagementIndications`, `MaxMessageStreamID`, `PeerASPIdentifier`, `StreamID` | Same operations on `Association`. |
| `Conn.SignallingStatus`, `Association.SignallingStatus` | Removed. Use `Endpoint.SubscribeSSNM` and report association identity. |
| `Conn.PeerCongestionLevel` | Removed. `Endpoint.SubscribeSSNM` delivers the peer's report with `SSNMReport.PeerReported` set. |
| `Conn.SetDeadline`, `SetReadDeadline`, `SetWriteDeadline` | Same operations on `Association`, now applying to `ReadData` and `WriteData`. |
| `Conn.DestinationRanges*`, `DestinationState*`, `DestinationStates*` | Removed. Use `Endpoint.DestinationStatuses` at an SGP, or `Endpoint.SSNMKnowledge` and `SubscribeSSNM` at an ASP. |
| `Conn.SetDestinationRange*`, `SetDestinationState*` | Replaced by `Endpoint.ReportDestinationAvailability`. |
| `Conn.ReportDestinationRange*`, `ReportDestinationState*` | Replaced by `Endpoint.ReportDestinationAvailability`, `SignallingCongestion` and `DestinationUserPartUnavailable`. |
| `Conn.DeregisterRoutingContexts` | Replaced by `Association.DeregisterApplicationServers(ctx, keys ...ASKey)`. |
| `Conn.BeginMTP3Restart` | Replaced by `Endpoint.BeginMTP3Restart`. |
| `Conn.SetSctpNoDelayConfig`, `SetSctpSackConfig` | Replaced by `Association.SetSCTPNoDelay` and `SetSCTPSACK`. |
| `Listener.Accept` | Returns `*Association`; selection still occurs before parsing. |
| `Listener.ActiveASPs`, `ActiveASPsForAS`, `ASPsForTraffic`, `ASPsForTrafficForAS` | Return `[]*Association`. Routing-Context-only forms fail closed when Network Appearance or contextless identity would be ambiguous. |
| `Listener.SetNIFAvailable`, `SetASAvailable`, `SetASAvailableForAS` | Retained with an `error` result, so an unsupported role or ambiguous scope cannot be ignored. |
| Promoted `Listener.SetAspIdentifier`, `SetNoDelayConfig`, `SetSackConfig` | Removed. Configure the immutable `AssociationConfig` before `Endpoint.Listen`, or return it from `SelectAssociationConfig`. |
| `Listener.BeginMTP3Restart`, and every `Listener` destination method | Removed. Publication and restart are Endpoint operations. |
| `Listener.Config` | Replaced by the embedded `Listener.AssociationConfig` snapshot. |
| `Listener.AspIdentifier`, `SignalingLinkSelection`, `SctpNoDelayInfo`, `SctpSackInfo` | Replaced by `Listener.ASPIdentifier`, `SignallingLinkSelection`, `SCTPNoDelayInfo` and `SCTPSACKInfo`. |
| `Listener.RecoveryTimer`, `RecoveryQueue*`, `BroadcastFlow*` | Removed from Listener promotion; configure `EndpointConfig.ApplicationServers` and `EndpointConfig.SGP`. |
| `ListenerConfig.DefaultConnConfig`, `SelectConnConfig` | Replaced by `DefaultAssociationConfig` and `SelectAssociationConfig`. |
| `SCTPConfig.SctpNoDelayInfo`, `SctpSackInfo` | Replaced by `SCTPNoDelayInfo` and `SCTPSACKInfo`. |
| `DestinationState`, `DestinationCongested` | Split into `DestinationAvailability` and `CongestionState`. |
| `SSNMScope` | Renamed `WireScope`, and joined by `SSNMPartition` for canonical identity. |
| `SGPRoute`, `SignallingGatewayProcessConfig.Routes`, `SignallingGatewayConfig.SGPSelection` | Replaced by `RemoteASConfig` inventory and `MTPRoutePath` candidates. |
| `ASPConfig.MTPRoutes`, `SignallingGatewaySelection`, `CongestionPolicy`, `TransferFlowCacheEntries` | Moved to `ASPRoutingConfig`, which is optional. |
| `MTPTransferResult.TransmittedAssociations`, `MTPTransferError.SuccessfulSGPs`, `MTPTransferFailure.SGP` | Replaced by `SuccessfulPaths` and `Target`, which name the whole selected target. |
| `IPSPConfig.InitiateASPSM`, `InitiateASPTM` | Replaced by `AssociationConfig.ASPProcedures`, which covers all four procedures and every role. |
| `IPSPTrafficConfig.NetworkAppearance`, `RoutingContexts`, `TrafficModeType`, `TrafficModes` | Replaced by `IPSPTrafficConfig.ApplicationServers []ASConfig`. |
| `DataMessage.NetworkAppearance`, `NetworkAppearanceSet`, `RoutingContext`, `RoutingContextSet` | Replaced by `DataMessage.Scope` and `DataMessage.AS`. |
| `ManagementIndication.AspIdentifier`, `AspIdentifierSet` | Replaced by `ASPIdentifier` and `ASPIdentifierSet`. |
| `ManagementIndication.RoutingContext`, `RoutingContextSet`, `AffectedPointCodes` | Replaced by `ASKeys` and `AffectedDestinations`, which keep the whole scope. |
| `DestinationRange`, `DestinationStatus` | Removed. Use `AffectedDestination` and the Endpoint publication APIs for local authority; `SSNMReport`, `SSNMDestinationKnowledge` and subscriptions for peer information. |
| `DestinationStatusSnapshot.CongestionLevel`, `CongestionLevelSet` | Replaced by `DestinationNetworkState`, which carries both dimensions. |
| `ErrAspIDRequired`, `ErrConnClosed`, `ErrInvalidAspIdentifier`, `ErrUnsupportedMode` | Replaced by `ErrASPIdentifierRequired`, `ErrAssociationClosed`, `ErrInvalidASPIdentifier` and `ErrUnsupportedRole`. |
| `ErrAmbiguousRoutingContext` | Removed. The keyed API refuses ambiguity at the call that would have caused it. |
| `StateAspDown`, `StateAspInactive`, `StateAspActive` | Replaced by `StateASPDown`, `StateASPInactive` and `StateASPActive`. |
| `SignalingLinkSelection` | Replaced by `SignallingLinkSelection`, in both this package and `messages/params`. |
| `MTP3Restart` value comparison | No longer supported. The handle is intentionally non-comparable; retain and use the returned pointer. |
| The 114 `Serialize`, `SerializeTo`, `Decode*`, `DecodeFromBytes` and `Len` codec wrappers | Replaced by `MarshalBinary`, `MarshalTo`, `UnmarshalBinary`, `MarshalLen` and the typed `Parse` functions. |
| `examples/client`, `examples/server` | Replaced by `examples/asp` and `examples/sgp`, so example names describe RFC roles rather than SCTP initiation. |

## Configuration validation

`AssociationConfig` is role-neutral, but fields that have meaning for only one
role are rejected before association processing:

- `ASPIdentifier` is local ASP policy and is rejected on an SGP Endpoint.
- `AuthorizeASP` is SGP Association policy and is rejected on an ASP Endpoint.
- `PeerSGP` is required for an Association of an ASP Endpoint that provisions
  peers, and invalid for an SGP Endpoint.
- `IPSP` is required for an IPSP Endpoint and invalid for ASP and SGP Endpoints.
- `EndpointConfig.ApplicationServers` on an ASP returns
  `ErrInvalidRoleConfiguration`; so does `EndpointConfig.SGP` on an ASP or IPSP.
- A nil `AssociationConfig` passed to `Endpoint.Dial` returns
  `ErrNilAssociationConfig`.
- `Listener.SetNIFAvailable`, `SetASAvailable` and `SetASAvailableForAS` return
  `ErrUnsupportedRole` on a non-SGP Listener.

The configuration `ListenerConfig.SelectAssociationConfig` returns is validated
after SCTP accept and before socket setup, monitoring or M3UA parsing.

## After v1.2.0: SSNM events are deltas

This change ships in the next minor release and breaks code written against
v1.2.0.

`SSNMEvent.States` carried the whole partition's retained knowledge after every
event, so a one-destination report cost the size of the partition once per
subscriber. It is replaced by `SSNMEvent.Updated`, which carries only what the
event changed:

| Event | v1.2.0 `States` | Now |
| --- | --- | --- |
| `SSNMReportEvent`, retained | every destination of the partition | `Updated`: the destinations the report wrote, each with both dimensions as retained after it, in point-code then mask order |
| `SSNMReportEvent`, retained by nobody (unbound partition, DUPU, DAUD, peer SCON) | empty | empty |
| `SSNMBindingAdmittedEvent`, `SSNMBindingActivatedEvent`, `SSNMBindingRetiredEvent` | every destination of the partition | empty: bindings change no destination knowledge |
| `SSNMPartitionRetiredEvent`, `SSNMPartitionInvalidatedEvent` | empty | empty: the kind discards the partition's destinations |

The field is renamed rather than redefined so that code relying on the old
meaning stops compiling instead of silently dropping destinations. A consumer
that replaced its view of a partition with each event's `States`:

```go
// view map[m3ua.SSNMPartition][]m3ua.SSNMDestinationKnowledge
if event.Kind == m3ua.SSNMReportEvent && event.States != nil {
    view[event.Partition] = event.States
}
```

patches it instead, starting from the snapshot's `Destinations` keyed by
`Destination`:

```go
// view map[m3ua.SSNMPartition]map[m3ua.PointCodeRange]m3ua.SSNMDestinationKnowledge
switch event.Kind {
case m3ua.SSNMBindingAdmittedEvent:
    if view[event.Partition] == nil {
        view[event.Partition] = make(map[m3ua.PointCodeRange]m3ua.SSNMDestinationKnowledge)
    }
case m3ua.SSNMReportEvent:
    for _, update := range event.Updated {
        view[event.Partition][update.Destination] = update
    }
case m3ua.SSNMPartitionInvalidatedEvent:
    clear(view[event.Partition])
case m3ua.SSNMPartitionRetiredEvent:
    delete(view, event.Partition)
}
```

No event removes a single destination: DAVA and a level-zero SCON are retained
as knowledge, and bounds refuse a report rather than evict. The whole retained
state still comes only from `SubscribeSSNM`, `Resync` and `SSNMKnowledge`,
unchanged. `SubscriptionQueueBytes` accounting charges 256 bytes per `Updated`
entry, as it charged per `States` entry, so the same byte budget now queues far
more one-destination reports.
