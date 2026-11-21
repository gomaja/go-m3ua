# Migrating to v1.2

v1.2 is a deliberate break. It separates the M3UA protocol role from SCTP
association initiation, moves everything about one message into that message,
moves everything shared by several associations onto the `Endpoint`, and removes
the API shapes that made a peer's wire label look like a global identity.

There are no compatibility aliases. Every removal below has a named replacement,
and the replacement is checked against the exported surface by
`export_inventory_test.go`, so this document cannot quietly drift from the code.

`ProtocolDataPayload.NetworkIndicator` and `DataQueueOverflowError.NetworkIndicator`
now use `pointcode.NetworkIndicator` from `github.com/gomaja/go-pointcode`, as does
the `ni` parameter of `params.NewProtocolDataPayload` and `params.NewProtocolData`.
Use its named constants (for example, `pointcode.NINational`) or explicitly
convert existing `uint8` variables. All DATA send APIs, including SGP distribution,
refuse NI values outside 0–3 before transport submission (RFC 4666 §3.3.1;
ITU-T Q.704 (07/96) §14.2.2). The codec still encodes any octet, and receive
delivers unknown values unchanged; applications can call `Valid()` to check them.

Contents:

- [Continued MTP status](#continued-mtp-status)

- [Local congestion publication and DATA admission](#local-congestion-publication-and-data-admission)
- [Received congestion validity and audits](#received-congestion-validity-and-audits)
- [Peer explanations, DUPU bounds and ASPAC errors](#peer-explanations-dupu-bounds-and-aspac-errors)
- [Congestion method profiles and local-user priority](#congestion-method-profiles-and-local-user-priority)

- [Concerned Destination admission and diagnostics](#concerned-destination-admission-and-diagnostics)

- [Application-owned withdrawal recovery](#application-owned-withdrawal-recovery)
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
- [Service Indicator values and Unknown cause](#service-indicator-values-and-unknown-cause)
- [Codec cleanup](#codec-cleanup)
- [Complete v1.1.1 API disposition](#complete-v111-api-disposition)
- [After v1.2.0: SSNM events are deltas](#after-v120-ssnm-events-are-deltas)

## Endpoint role

RFC 4666 Section 1.4.8 recommends that both ASPs and SGPs support initiating and
accepting SCTP associations; `Dial` therefore no longer implies ASP and `Accept`
no longer implies SGP.

```go
asp, err := m3ua.NewEndpoint(m3ua.EndpointConfig{Congestion: &m3ua.CongestionConfig{Default: m3ua.CongestionProfile{Method: m3ua.CongestionInternational}}, Role: m3ua.RoleASP})
sgp, err := m3ua.NewEndpoint(m3ua.EndpointConfig{Congestion: &m3ua.CongestionConfig{Default: m3ua.CongestionProfile{Method: m3ua.CongestionInternational}}, Role: m3ua.RoleSGP})
```

Shared state belongs to the Endpoint, not to any one Association or Listener:

```go
sgp, err := m3ua.NewEndpoint(m3ua.EndpointConfig{
    Congestion: &m3ua.CongestionConfig{Default: m3ua.CongestionProfile{Method: m3ua.CongestionInternational}},
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
ipsp, err := m3ua.NewEndpoint(m3ua.EndpointConfig{Congestion: &m3ua.CongestionConfig{Default: m3ua.CongestionProfile{Method: m3ua.CongestionInternational}}, Role: m3ua.RoleIPSP})

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

Routing Contexts must also be unique within each association inventory,
including explicit zero. Two entries with different Network Appearances or
Traffic Modes cannot reuse a Routing Context; identical duplicates are refused
as well (RFC 4666 Sections 1.2 and 1.4.2.1). The configuration error names the
duplicated value. This also applies to each SGP's static Application Server
inventory in `ASPConfig.SignallingGateways`, and separately to `TrafficToLocal`
and `TrafficToPeer` in IPSP Double Exchange. Independent SGPs and directions
may reuse a value. A registration result that assigns one Routing Context to
two provisioned canonical Application Servers is refused before installing
the results.

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

For per-call cancellation, use `WriteDataContext(ctx, request)` or
`MTPTransferContext(ctx, request)`. Cancellation can end admission and transport
buffer waits without closing the association. A full buffer returns `EAGAIN`
immediately without an application write deadline; with one, the transport waits
for space until that deadline or the context ends. A context interruption matches
`ctx.Err()` and queues nothing. A `SendMsgContext` error with zero accepted bytes
is `DataNotSent`. An unexpected partial submission is `DataSendIndeterminate`,
including a short count reported without an error. A complete send already
accepted reports success even if cancellation occurs concurrently. Application
deadlines are preserved, including changes made during the call.
Contexts that cannot be cancelled retain the ordinary send's conservative
outcome classification.
Shutdown and closed-transport refusals also report `DataNotSent`. Local SCTP
shutdown or close wakes a send waiting for buffer space. A peer's SHUTDOWN alone
does not wake it: space becoming available, the deadline or context ending, or
the association ending releases that wait.

Admission and send barriers use `github.com/gomaja/go-ctxsync`: plain and
cancellable callers share its queue and starvation handoff. Transfer readers
waiting behind a routing-state writer are admitted before the next writer.
This does not bound acquisition latency; lock holders must release and waiting
goroutines must be scheduled. Cancellation before admission reports
`DataNotSent` with a cause matching `ctx.Err()`. `Close` and `Abort` bypass the
send barrier so a parked transport write can be released.

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
`Received` records when the socket reader returned the complete message and
retains its monotonic clock reading, so `time.Since(message.Received)` includes
parsing, dispatch and receive queue wait, excluding the kernel's socket receive
buffer.
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
- **DATA retry.** DATA is not retried automatically, because a failed write does not
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
| `Conn.PeerCongestionLevel` | `Endpoint.SubscribeSSNM`; raw peer SCON carries `SSNMReport.PeerReported`, and `SSNMSnapshot.PeerCongestion` retains current SGP peer state |
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
snapshot. Read the report kind before interpreting its fields. DUPU and raw
peer SCON trigger details are event-only: a snapshot does not reconstruct lost
user-part indications or Concerned Destination history. Current SGP peer
congestion is retained separately in `SSNMSnapshot.PeerCongestion`. Events are
deltas applied to that snapshot: a report
event's `DestinationUpdates` names only the destinations it wrote. Handle
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
    Congestion: &m3ua.CongestionConfig{Default: m3ua.CongestionProfile{Method: m3ua.CongestionInternational}},
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
    Congestion: &m3ua.CongestionConfig{Default: m3ua.CongestionProfile{Method: m3ua.CongestionInternational}},
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
  deregistration removes it, with no new SCTP association. At an SGP, ASP Down
  also retires dynamic registration (RFC 4666 §4.3.4.2), as does ASP Up while
  active (§4.3.4.1). Register again before activating a retired dynamic scope;
  static membership and provisioned Routing Keys remain configured.
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

`Endpoint.MTPIndications` now returns a bounded channel for IPSPs instead of
nil, and `Endpoint.Close` closes that channel. Drain it until closure and fan
out indications to local MTP3-Users. `EndpointConfig.IPSPDestinations` maps
peer-directed AS scopes to destinations for IPSP indications and snapshots;
without those mappings the application owns destination projection.

Transport detection is enabled by default; set
`EndpointConfig.TransportCongestion.Disabled` to opt out. `EndpointConfig`,
`ASPRoutingConfig`, `MTPDestinationStatus`,
`AssociationStatus`, and `ManagementIndication` gained fields. Replace positional
composite literals for these exported structs with keyed literals.
Handle `ManagementTransportCongestion` and `ManagementDataQueueCongestion` as
independent implementation indications, named `TRANSPORT-CONGESTION` and
`DATA-QUEUE-CONGESTION`; neither names a new RFC management primitive.

## Service Indicator values and Unknown cause

Five Service Indicator constants now carry the SS7 values required by RFC 4666
§3.3.1 and §3.4.5. Earlier values mislabeled DATA on the wire.

| Constant in `params` | Before | After |
| --- | --- | --- |
| `ServiceIndBroadbandISUP` | 7 | 9 |
| `ServiceIndSatelliteISUP` | 8 | 10 |
| `ServiceIndAALType2Signalling` | 10 | 12 |
| `ServiceIndBICC` | 11 | 13 |
| `ServiceIndGatewayControlProtocol` | 12 | 14 |

Rebuild callers to use the corrected constants, and update any persisted numeric
values, filters or comparisons that used the earlier assignments. `ServiceIndUnused`,
`ServiceIndSCCP`, `ServiceIndTUP` and `ServiceIndISUP` keep their values.

`params.UserIdentityUnknown` is removed. Use `params.UnknownCause` for the
Unavailability Cause "Unknown" in the User/Cause parameter; its value remains 0.

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
| `SctpNoDelayInfo`, `SCTPNoDelayInfo` | Removed. Use `SCTPConfig.Nagle` or `AssociationConfig.SetSCTPNagle`; the zero value now disables bundling delay (`SCTP_NODELAY` on). |
| `SctpSackInfo` | Replaced by `SCTPSACKInfo`. |
| `Dial`, `Listen` | Replaced by `Endpoint.Dial` and `Endpoint.Listen`; the M3UA role is fixed by the owning `Endpoint`. |
| `NewConfig`, `NewClientConfig`, `NewServerConfig` | Replaced by `NewEndpoint(EndpointConfig{Role: …})` plus `NewAssociationConfig()` and explicit setters. The removed constructors inferred an M3UA role from transport orientation. |
| `NewListenerConfig` | Retained with a new signature: it takes `*AssociationConfig` and its result is passed to `Endpoint.Listen`. |
| `Config.EnableHeartbeat` | Same operation on `AssociationConfig`. |
| `Config.SetAspIdentifier` | Replaced by `AssociationConfig.SetASPIdentifier`. |
| `Config.SetCorrelationID` | Removed. Correlation Id is per message: `DataRequest.CorrelationID`. |
| `Config.SetNetworkAppearance`, `Config.SetRoutingContexts`, `Config.SetTrafficModeType` | Replaced by `AssociationConfig.SetApplicationServers` and `ASConfig`. |
| `Config.SetNoDelayConfig`, `AssociationConfig.SetSCTPNoDelay` | Replaced by `AssociationConfig.SetSCTPNagle` with the opposite boolean: `SetSCTPNoDelay(true)` becomes `SetSCTPNagle(false)`. No delay is now the default. `Association.SetSCTPNoDelay` remains the live socket setter. |
| `Config.SetSackConfig` | Replaced by `AssociationConfig.SetSCTPSACK`. |
| `ConnConfig.AspIdentifier` | Replaced by `AssociationConfig.ASPIdentifier`. |
| `ConnConfig.NetworkAppearance`, `RoutingContexts`, `TrafficModeType`, `TrafficModes` | Replaced by `AssociationConfig.ApplicationServers []ASConfig`. |
| `ConnConfig.CorrelationID`, `OriginatingPointCode`, `DestinationPointCode`, `ServiceIndicator`, `NetworkIndicator`, `MessagePriority`, `SignalingLinkSelection` | Removed. Every one is per message, in `DataRequest`. |
| `ConnConfig.SctpNoDelayInfo` | Replaced by `SCTPConfig.Nagle` (promoted on `AssociationConfig`); set `true` to restore the bundling delay. |
| `ConnConfig.SctpSackInfo` | Replaced by `AssociationConfig.SCTPSACKInfo`. |
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
| `Conn.PeerCongestionLevel` | Removed. `Endpoint.SubscribeSSNM` delivers raw `PeerReported` reports and separate retained peer-state deltas; snapshots include `PeerCongestion`. |
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
| `Listener.AspIdentifier`, `SignalingLinkSelection`, `SctpNoDelayInfo`, `SctpSackInfo` | Replaced by `Listener.ASPIdentifier`, `SignallingLinkSelection`, `Nagle` and `SCTPSACKInfo`. Configure `Nagle` before Listen or in a selected `AssociationConfig`. |
| `Listener.RecoveryTimer`, `RecoveryQueue*`, `BroadcastFlow*` | Removed from Listener promotion; configure `EndpointConfig.ApplicationServers` and `EndpointConfig.SGP`. |
| `ListenerConfig.DefaultConnConfig`, `SelectConnConfig` | Replaced by `DefaultAssociationConfig` and `SelectAssociationConfig`. |
| `SCTPConfig.SctpNoDelayInfo`, `SCTPConfig.SCTPNoDelayInfo` | Removed in favor of `SCTPConfig.Nagle`; `false` now sets `SCTP_NODELAY` on. |
| `SCTPConfig.SctpSackInfo` | Replaced by `SCTPConfig.SCTPSACKInfo`. |
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
subscriber. Complete replacements now use `SSNMEvent.DestinationUpdates`;
large reports use a separate patch shape:

| Event | v1.2.0 `States` | Now |
| --- | --- | --- |
| `SSNMReportObservedEvent`, retained | every destination of the partition | complete `DestinationUpdates` in point-code/mask order, or no updates before patches |
| `SSNMReportPatchEvent` | no equivalent | `ReportPatch`: one replacement or source-history continuation; use `ApplyReport` |
| `SSNMReportObservedEvent`, retained by nobody (unbound partition, DUPU, DAUD, peer SCON) | empty | empty |
| `SSNMBindingAdmittedEvent`, `SSNMBindingActivatedEvent`, `SSNMBindingRetiredEvent` | every destination of the partition | empty: bindings change no destination knowledge |
| `SSNMPartitionRetiredEvent` | empty | empty: the kind discards the partition |
| `SSNMDestinationsInvalidatedEvent` | empty | `Invalidated`: exact destination keys/removal flags, routed by `Epoch` |

The field is renamed rather than redefined so that code relying on the old
meaning stops compiling instead of silently dropping destinations. A consumer
that replaced its view of a partition with each event's `States`:

```text
// Former consumer (removed API):
// view map[m3ua.SSNMPartition][]m3ua.SSNMDestinationKnowledge
if event.Kind == m3ua.SSNMReportEvent && event.States != nil {
    view[event.Partition] = event.States
}
```

applies the explicit report shapes instead, starting from owned partition
knowledge returned by the snapshot. Both report kinds use the same helper;
this excerpt shows destination and partition changes:

```go
// view map[m3ua.SSNMPartition]*m3ua.SSNMPartitionKnowledge
// epochs map[uint64]m3ua.SSNMPartition, initialized from snapshot.Partitions.
switch event.Kind {
case m3ua.SSNMBindingAdmittedEvent:
    epochs[event.Epoch] = event.Partition
    if view[event.Partition] == nil {
        view[event.Partition] = &m3ua.SSNMPartitionKnowledge{
            Partition: event.Partition, Epoch: event.Epoch,
        }
    }
case m3ua.SSNMReportObservedEvent, m3ua.SSNMReportPatchEvent:
    if event.Epoch == 0 {
        break // Event-only report with no retained partition.
    }
    if !event.ApplyReport(view[event.Partition]) {
        // Stop applying deltas and call subscription.Resync.
        return
    }
    // Expose the completed report only when !event.ReportContinues.
case m3ua.SSNMDestinationsInvalidatedEvent:
    if !event.ApplyInvalidation(view[epochs[event.Epoch]]) {
        // Stop applying deltas and call subscription.Resync.
        return
    }
case m3ua.SSNMPartitionRetiredEvent:
    delete(view, event.Partition)
    delete(epochs, event.Epoch)
}
```

Successfully retained DAVA and level-zero SCON reports remain knowledge.
Oversized reports can remove individual dimensions through `Invalidated`. The whole retained
state still comes only from `SubscribeSSNM`, `Resync` and `SSNMKnowledge`,
unchanged. `SubscriptionQueueBytes` now charges 320 fixed bytes per `DestinationUpdates`
entry; the former charge was 256. Individual peer/source rows remain 256 bytes
plus Routing Context storage. The snapshot-to-delta change still avoids
repeating the whole partition, but custom byte budgets must allow for the
larger destination update.


## SCTP path status and controls

`AssociationStatus` replaces `PrimaryCongestionWindow`,
`PrimarySmoothedRTTTicks`, `PrimaryRetransmissionTimeout` and `PrimaryMTU`
with `Primary sctp.PathInfo`. Migrate reads to `Primary.Cwnd`,
`Primary.SRTTTicks`, `Primary.RTO` and `Primary.MTU`, respectively. No aliases
remain. `Paths []sctp.PathInfo` reports all current peer paths and
`LocalAddrs *sctp.Addr` reports live local addresses. Separate socket reads
make this a non-atomic snapshot; hidden Potentially Failed paths carry zero
metrics, and paths removed during the query are omitted.

`SetSCTPPrimaryAddr`, `SCTPPathParams`/`SetSCTPPathParams` and
`SCTPPathThresholds`/`SetSCTPPathThresholds` expose runtime path controls.
Before passing an `SCTPPathParams` result to `SetSCTPPathParams`, clear
`HeartbeatInterval` when `Heartbeat` is false, `PathMTU` when `PMTUD` is true,
and `SACKDelay` when `DelayedSACK` is false. An association-wide threshold update applies retained
association defaults for nil fields to every path, overwriting differing
per-path thresholds.
`SCTPConfig.PathParams`, `PathThresholds` and `PFExposure` configure every new
Dial or Accept before its M3UA handshake. Wildcard settings cover current and
future paths (RFC 6458 Section 8.1.12; RFC 7829 Section 7.2).
`ManagementSCTPStatus` is appended to the indication kinds; it carries an
owned `PeerAddrChange` through the existing optional management subscription.
Applications switching on indication kinds should handle it and continue
draining the bounded stream. Potentially Failed transitions require
`sctp.PFExposeEnabled` (RFC 7829 Section 7.3).

## PR-SCTP now requires opt-in

go-m3ua no longer offers PR-SCTP by default. Dial's INIT omits
Forward-TSN-Supported, and a Listener's INIT ACK returns a peer's offer in an
Unrecognized Parameter (RFC 3758 §§3.3.1–3.3.3 and §4.2; RFC 7496 §4.5). A peer
that requires PR-SCTP may decline the association. Set
`SCTPConfig.PartialReliability = true` before setup to restore the offer.
go-m3ua's own sends are always fully reliable. On hosts that run Listeners, also
set `net.sctp.prsctp_enable` to match (0 for the default): Linux answers an SCTP
restart of a Listener's association from that sysctl; see
[the standards contract](standards.md#sctp-message-boundaries-and-reliability).

Opting in lets the peer abandon messages: a whole abandoned message never
arrives and nothing reports it, and abandonment after partial delivery ends the
association with a cause matching `sctp.ErrMessageInterrupted`. A peer can
decline the offer without error; read `Association.SCTPPRSupported()` or
`AssociationStatus.PRSupported` for the negotiated result. There is no setter
for a live association; see
[the standards contract](standards.md#sctp-message-boundaries-and-reliability).

A Listener fixes the offer from `DefaultAssociationConfig` at Listen. Every
`SelectAssociationConfig` result must carry that same bool value. False is
explicit, so a fresh zero-value config under a true default is refused with
`ErrInvalidSCTPConfig`; the Listener continues serving matching associations.

## Inbound DATA queue overflow

`DataQueueStats`, `DataQueueOverflowError`, and `ManagementIndication` gained
fields for scoped overflow lifecycle and tracking loss. Replace positional
composite literals for these exported structs with keyed literals; positional
literals written against the previous field lists no longer compile.

`DataQueueStats.Congested` remains true until the queue has stayed at or below
half capacity for one second. Overflow SCON preserves received NA/RC while using
the live SSNM scope checks and withdrawal barrier, including the independent
Double Exchange direction. A pending onset is discarded if its episode ended
before transport submission. Local end does not send an international-form wire
clear; peer validity-timer or audit/current MTP procedure recovery remains
necessary (RFC 4666 Sections 3.4.4, 4.3.1 and 5.6.2).

## Application-owned withdrawal recovery

Unsolicited ASP Inactive Ack recovery is a deliberate breaking behavior
change. The library closes the affected traffic gates and emits
`ManagementASPWithdrawal` (`ASP-WITHDRAWAL`) through `ManagementIndications`;
it no longer sends ASP Active on its own, even for the last active scope with
automatic startup configured. This applies to ASP and both IPSP exchange
models. Subscribe before the handshake and let application policy choose
whether to call `association.ASPActive(ctx, indication.ASKeys...)` or, for a
known numeric scope, `association.ActivateRoutingContexts(1)`. These are
ordinary activation procedures. The new management kind distinguishes the
unsolicited scopes from confirmations of the application's own deactivation.
There is no compatibility option.

RFC 4666 §4.3.4.4 uses lowercase "should" for returning to the previous state;
§4.3.4.3 permits application Layer Management to initiate ASP Active. The new
report is an implementation indication: §1.6.3 defines no
unsolicited-withdrawal primitive. A contextless AS beside numeric scopes
cannot be activated separately on the wire; only call `ASPActive(ctx)` without
keys when application policy chooses the whole inventory. Automatic activation
after a solicited ASP Up Ack is unchanged. See [the recovery
example](../README.md#recovery-after-peer-withdrawal).

Known inactive or pending-only scopes are ignored, including alongside an
active withdrawal. They do not stop the application's activation or consume
initial startup. Each reported ASKey preserves its own Network Appearance,
including dynamically registered ASes; Notify indications use the same scope
resolution. Numeric reported keys can be passed directly to ASPActive.
Inactive Ack may omit Routing Context (§3.7.4, §4.3.4.4). On a multi-AS
association, omission while an explicit Inactive is outstanding remains
ambiguous and is rejected with ErrMissingRoutingContext without changing any
scope or request. An Ack of an omitted request still confirms it; without an
outstanding Inactive request, omission withdraws the active scopes. Explicit
Active Ack correlation retains the echo requirement of §4.3.4.3.

Cancelling a pending activation returns an error wrapping
ErrASPProcedureSuperseded with its cause while the association remains open.
Unaccepted control sends are cancelled, and the handler waits only for the
cancelled request's transport call, before the withdrawal report. It does not
join unrelated writers through shared mutexes.

SCTP acceptance can win cancellation and cannot be retracted. In that case
one ManagementError immediately names the ASKeys that the message carried and
says the peer may consider them active. For an unsolicited withdrawal, this
error follows ManagementASPWithdrawal. The application chooses whether to
send ASPInactive to confirm inactivity or ASPActive to recover. Confirmed
application Inactive and ASP Down cancellation use the same reporting rule. A
waiting ASPActive call also publishes its ordinary failure ManagementError.
While its context and association remain live, it waits for the cancelling
handler's reports first. Context cancellation independently returns ctx.Err();
its failure indication may then precede the handler's reports. Association close
ends the wait with the terminal association error. Public initial procedure
writes, ActivateRoutingContexts and DeactivateRoutingContexts join any withdrawal
that has retired their request before exposing its result. Internal Up/Active
entry, startup and restart actions submit control messages without waiting for a
procedure result; the result itself remains unpublished until the withdrawal
commits and reports. Orderly shutdown still waits for its committed result.
Nonwaiting activation and deactivation otherwise return after submission without
waiting for an Ack; they add no generic procedure-failure indication. Failure to
submit the required Up-Ack Error returns that independent write failure instead
of a retirement cause whose withdrawal was not reported.

Automatic entry actions serialize state changes through their initial control
submission, preserving ASP Up before ASP Active. Explicit calls release the
state lock before writing or waiting for an Ack. If a newer local ASP Up or ASP
Down replaces the same pending procedure, or orderly shutdown cancels pending
procedures, callers now receive `ErrASPProcedureSuperseded` instead of
`ErrAssociationClosed` on a live association. The association stays open for
local replacement; shutdown still closes it after its withdrawal sequence.

Once `ShutdownContext` claims the association, it also refuses every new
`ASPUp`, `ASPActive`, `ASPInactive`, `ASPDown`, `ActivateRoutingContexts` and
`DeactivateRoutingContexts` request with an `"ASP procedure superseded by orderly shutdown"` error wrapping
`ErrASPProcedureSuperseded`. This is a breaking change: finish explicit procedures
before starting shutdown. Queued callers are checked again after acquiring the
procedure gate, and registration is ordered with the shutdown claim so callers
that passed validation cannot send a late request. A state change during
withdrawal also retains the shutdown refusal for an already validated caller.
Only shutdown's own
ASPIA/ASPDN may register during withdrawal (RFC 4666 §§4.3.4.4, 4.3.4.2, 4.9).
The four waiting ASP methods retain their procedure-failure `ManagementError`;
the routing-context methods return immediate refusals without adding an
indication. With an all-explicit policy, shutdown still just closes the
association.

No acknowledgement evidence is retained for a cancelled send. Later Active
Acks use the ordinary duplicate and validation rules; M3UA has no request
identifier that correlates one with the cancelled message. A later valid Active
Ack without an outstanding request or duplicate history retains its grant and
reports `ManagementASPActivation`, with `PreviousState` and `PreviousIPSPState`.
The application decides whether to retain that grant or request inactivity
(RFC 4666 §§3.8.1 and 4.3.4.3). The accepted-send report itself changes no state
and sends no replacement request.

## Concerned Destination admission and diagnostics

SCON sends with Concerned Destination now require ASP-to-SGP direction and
exactly one unmasked Affected Point Code (RFC 4666 §3.4.4). Typed
`SignallingCongestion` and raw `WriteSignal` refuse ambiguous lists/ranges before
writing, with `ErrInvalidParameterValue`. Split the report into single-DPC
messages with the correct triggering origin, or omit Concerned Destination when
there is no single trigger. SCON lists and ranges without it remain supported.

Receiving remains lenient about this mapping mismatch: congestion information
for all destinations is delivered on `SubscribeSSNM`, without a protocol Error.
For an ambiguous mapping, `SSNMReport.ConcernedDestinationSet` is false and
`ConcernedDestination` is zero. The new optional
`UnmappedConcernedDestination *uint32` holds the received value for diagnostics,
including point code zero; it must never be used as a TFC trigger. A valid
single-DPC trigger retains the existing fields and has a nil diagnostic.
Layer Management consumes that diagnostic through the existing SSNM subscription;
no additional M-ERROR primitive is generated for an accepted SCON. Existing
framing, direction and scope checks remain in force.

The added `SSNMReport` field breaks positional composite literals; use keyed
literals. Each subscription owns its scalar diagnostic copy. SPMC assessment
and SS7 TFC generation remain with the application/NIF.

## Congestion method profiles and local-user priority

Every role must configure `EndpointConfig.Congestion.Default.Method` explicitly.
Nil Congestion, zero Default.Method and zero-method entries in Networks or
AssociationConfig.CongestionProfiles are refused with ErrInvalidRoleConfiguration.
There is no default or inference from NI, MP or SCON: the method belongs to the
application's SS7 network. `CongestionNotConfigured` and the temporary resolver are
removed; the retained method constants keep their existing values 1, 2 and 3.
Use the required `-congestion-method` flag in examples, perftraffic and perfchurn.
National MaxLevel zero still selects 3 as an engineering level-count default.

ASP/IPSP local-user DATA and all SCON sends strictly validate the method, including
serialized `WriteSignal` messages, with known-not-sent errors before a write.
Existing framing, payload-size, stream, state and binding errors keep their
precedence over method admission. An Endpoint SCON publication refused by one
association's profile reports that target in `SSNMDeliveryError` and keeps the
healthy association available for subsequent traffic.
With national priorities, ASP/IPSP local-user offers below effective congestion are refused;
equal priority is permitted. The effective level is capped at MaxLevel so the
highest configured priority survives known excessive levels. Unknown present
congestion conservatively refuses the candidate. SGP transit does not use this
discard rule. SGP NIF transit with MP0..3 is relayed unchanged even when the
method does not allow that MP. The same lifetime diagnostic counts and reports
these accepted NIF inputs; raw precheck plus final admission counts only once.
MP>3 is refused at the SGP under the selected two-bit fallback: the exact 1992
TTC reference and ANSI width were not established, and no national-variant
compliance claim is added. Custom ASP policy can tighten but cannot bypass it. Route alternatives
must agree on method and maximum; incompatible configuration or resolved bindings
report `ErrCongestionMethodMismatch` without changing ASP activation.

Received well-formed mismatches remain accepted. SCON retains its original level
and reports `SSNMReport.ProfileMismatches` through `SubscribeSSNM`. DATA keeps its
original MP and is delivered. Its first mismatch emits
`ManagementCongestionProfileMismatch`; `CongestionProfileDiagnostics()` provides
the lifetime count and latest owned diagnostic without per-message primitive
flooding. Consumers with exhaustive management-kind switches must handle the new
kind. Added exported struct fields require positional literals to become keyed.

National queue overflow contributes numeric MaxLevel; the bounded optional
scheduler retains its current report across full-buffer refusals and reserves a
submitted onset's capacity for its eventual level-zero clear. Other local
sources can delay that clear after queue drain. International retains
omitted-level onset without a numeric clear.
Deferred SCON, including queue onset and recovery, belongs to the binding that
created it. Retirement or replacement completed before transport admission
prevents its old publication even if the same Routing Context is active again.
Once admitted, a SCON submission settles before a local registration replacement
can commit. A change cancels only submissions carrying that Routing Context and
registration direction, then joins their completion before changing the binding.
An SCTP-accepted submission remains successful. Unrelated registration results,
absent removals and removals for an old version leave a current submission alone.
The binding update joins cancellation independently of peer send space or an
application write deadline. Short dynamic-key locks are released before transport
calls; Close, Abort and the control-write watchdog can also release a full-buffer
sender. Association epoch is checked at
admission; this does not guarantee peer receipt or exclude a remote restart
during a waiting send.
Clears never withdraw received SS7 congestion or transport pressure. Applications
must not equate local drain with peer receipt of a clear. These profiles implement
only the named Q.704 (07/96) procedures read; the covering-note erratum could not be
retrieved, and no ANSI/TTC or corrected-text compliance is claimed.

A congestion-refused required broadcast recipient is now an explicit per-target
failure after available AS/SGP alternatives are exhausted; it is not silently
removed from the recipient set. Accepted copies remain in SuccessfulPaths and are
not retried. Consumers with exhaustive MTPSelectionReason switches must handle
MTPCandidateCongestionMethodMismatch. Existing management-kind numeric values are
preserved; the new mismatch kind is appended.

### DAUD when a peer method cannot express retained congestion

RFC 4666 §4.5.3 says:

> The SGP SHOULD respond to a DAUD message with the MTP3 availability/congestion status of the routeset associated with each Destination Point Codes in the DAUD message.

It also says:

> Where the SGP does not maintain the congestion status of the SS7 destination, the response to a DAUD message should always only be a DAVA, DRST, or DUNA message, as appropriate.

This implementation maintains the congestion status. If an association's
configured method cannot express that status, its DAUD response omits only the
profile-refused SCON, sends every remaining congestion and availability reply in
the planned order, and reports the partial response once per audit through
`ManagementError`, with the local refusal in `Cause` and the affected audit scope.
It sends no peer Error for this local mismatch and keeps the association open.
This is an engineering decision and a declared deviation from the congestion
status SHOULD for the affected destinations; the exception for an SGP that does
not maintain congestion does not apply. The authoritative congestion remains
retained. An audit delivers what a publication could deliver to this association.
Reply-budget refusal remains a separate whole-audit Destination Status Unknown
response before any status prefix. A real transport failure still stops writing.

Queued MTP3 restart/publication bursts likewise continue after a profile-refused
SCON and return the existing per-target refusal after attempting the other
messages. Successful messages are never replayed.

### Known-clear congestion in national DAUD replies

An SGP now records whether it knows a destination's congestion separately from
the level. Under `CongestionNationalWithPriorities` or
`CongestionNationalWithoutPriorities`, a DAUD for an available or restricted
destination whose congestion is known to be clear is answered with SCON(0)
before the DAVA or DRST, following the RFC 4666 §5.4.1 example ("SG State:
Uncongested/Available") and the §4.5.3 order of SCON before the availability
reply. Previously the reply was the DAVA or DRST alone, and an ASP that still
held an earlier congestion level kept it, because a bare DAVA leaves congestion
unchanged.

Upgrade consequences:

- National audit replies can carry additional SCON messages whose Congestion
  Indications value is 0.
- Those messages count against the same reply message and byte budgets. An
  audit that only just fitted before can now exceed them and receive the
  whole-audit Destination Status Unknown response, still with no status prefix.
- A destination whose congestion is unknown is answered with availability alone,
  as before (RFC 4666 §5.4.3). Under `CongestionInternational` a known-clear
  destination is also answered with availability alone; that is not a method
  refusal and produces no `ManagementError`.

## Retained SGP peer congestion and policy inputs

SGP peer SCON now survives snapshot/resync independently of authoritative SS7
knowledge (RFC 4666 §§1.3.2.4, 1.4.2.4). Update exhaustive `SSNMEvent.Kind`
switches for `SSNMPeerCongestionEvent`; apply `PeerCongestion` as an upsert or
remove it when `PeerCongestionRemoved` is true. Replace the peer view from
`SSNMSnapshot.PeerCongestion` on resync. Raw `PeerReported` reports remain
available. They are observations, not a replayable log of TFC triggers.

New fields in `SSNMSnapshot`, `SSNMEvent` and `SGPConfig` break positional
composite literals; use named fields. Peer retention shares existing SSNM
record and byte budgets, including expanded AS/range records and scope copies.
A workload that previously produced only raw reports can now exhaust retention
and increment `RecordsRefused`/`ReportsRefused` with a resource-loss event. Review
configured budgets and loss handling; a fresh snapshot cannot recover refused
state. Explicit level zero is retained as clear; absent peer state is unknown.
Oversized peer SCON removes only records for its resolved AS and overlapping
network scope; unresolved scope alone permits association-wide peer removal.
Oversized DAUD preserves all knowledge; non-peer reports preserve peer records.
SS7 destination invalidation is scoped as described below. Apply peer removal deltas
independently of the destination-only partition-invalidated event.

Use `Endpoint.CongestionInputs(as)` for copied AS membership, peer state, local
contributions and independent transport inputs. Honor `Consistent` and component versions: one
retry bounds collection, and `Revision` covers SSNM only. Inspect
`VersionsBefore` and `VersionsAfter` for membership/SSNM versions and each peer
binding generation, opaque `LocalGeneration`, transport observation and
`TransportPresent`. `LocalGeneration` versions local lifetime/queue evidence;
compare it without interpreting its numeric value. `Local` now
keeps queue/application contributions separately inspectable; queue overload
also keeps its management stream and continuity contract. Missing AS returns
`ErrNoConfiguredAS`.

An optional `SGPConfig.CongestionPolicy` (`SGPCongestionPolicy`) veto runs
outside Endpoint, AS and delivery
locks before the write barrier. A decision still stale after one revalidation
returns known-not-sent `ErrCongestionDecisionStale`; handle it using the existing
unsent delivery contract. Accepted/indeterminate copies must not be replayed.
`SGPConfig.PreferUncongestedPeers` defaults to false; opt-in loadshare ranking
uses the latest matching peer record by `Revision`, then maximum peer/transport
severity; a newer matching clear supersedes an older range. Equal ranks retain
original active order and SLS modulo selection. Override retains its active
member, and broadcast reports each refused required member explicitly. Enabling
ranking can change SLS assignments and missequence traffic (RFC 4666 §1.4.7).

Peer state never changes authoritative DAUD replies. The application/NIF still
determines SPMC congestion, alternate capacity and SS7 TFC actions under
RFC 4666 §1.4.6; there is no automatic TFC API or SPMC verdict.

### Whole-message allocation refusals

A transport refusal matching `syscall.ENOMEM` with no accepted bytes now
reports `DataNotSent`, including typed, raw and Endpoint-selected DATA paths.
Previously ordinary sends reported `DataSendIndeterminate` for this error,
which also caused pending AS recovery to discard a never-queued copy.
Applications inspecting `DataWriteError.Outcome` can now retry that copy;
`errors.Is` still reaches `ENOMEM`. Recovery keeps the copy and its FIFO
reservation until retry or expiry.

This classification is bounded to the supported single-association send path
of the pinned transport and the inspected Linux implementations. Positive
counts with an error and short successes remain indeterminate and must not be
automatically replayed. Ordinary sends also leave unknown causes indeterminate;
the cancellable transport path retains its documented zero-byte guarantee.
No explicit-EOR continuation or multi-association `SCTP_SENDALL` send is
introduced.

## Peer explanations, DUPU bounds and ASPAC errors

`ManagementIndication` adds `DiagnosticInformation []byte` for received ERR and
`InfoString []byte` for received NTFY (RFC 4666 §§3.8.1, 3.8.2, 4.2). Update
positional struct literals to named fields. Each delivered indication owns its
bytes; nil means absent, while a non-nil empty slice means present and empty.
`Description` continues to describe the error code or status, rather than
containing the peer's explanation. Local errors leave these peer fields nil.

DUPU typed origination replaces the eight-identity whitelist and Cause 0–2
limit with structural bounds: User ≤255 and Cause ≤15. Raw `WriteSignal` and
inbound DUPU now enforce the same bounds; those paths previously admitted the
full 16-bit fields. Callers passing User >255 or Cause >15 through those paths
must change their inputs. RFC 4666 §3.4.5 makes the MTP3 variant definitive;
Q.704 (07/96) §15.17 and Q.Imp704 (12/00) §§2.2.3–2.2.4 supply this envelope.
The caller remains responsible for assigned values in its variant. Structural
acceptance does not assign reserved values. Codec fields remain 16 bits, and
single-destination, zero-mask, role and scope restrictions remain in force.

An ASPAC with undefined Traffic Mode Type (including 0, 4 and 0xffffffff) now
receives Unsupported Traffic Mode Type (`0x05`) rather than Invalid Parameter
Value (`0x11`), without activation or ASPAC ACK (RFC 4666 §3.8.1).
`params.ErrInvalidTrafficMode` identifies this top-level decode fault and also matches
`params.ErrInvalidValue` through `errors.Is`. Standalone parameter and message
codecs remain strict. Other invalid parameters, malformed lengths, and earlier
faults retain their previous error selection. REG REQ keeps its existing
per-key Unsupported/Invalid Traffic Handling Mode registration result under
§4.4.1; this change does not alter that receive path.


## Scoped SSNM invalidation

`SSNMPartitionInvalidatedEvent` is removed without an alias. Its replacement,
`SSNMDestinationsInvalidatedEvent`, cannot silently compile into an old
whole-partition handler. Its `Partition` is empty: use `Epoch` to identify the
snapshot partition. `SSNMEvent.Invalidated` now contains
`SSNMDestinationInvalidation` entries with `Destination`, `AvailabilityRemoved`
and `CongestionRemoved`, rather than full retained records. Update positional
event literals and consumers of the previous removal payload.

For a replica holding `SSNMPartitionKnowledge` values, the library method avoids
writing a second patch implementation:

```go
for i := range snapshot.Partitions {
    if event.ApplyInvalidation(&snapshot.Partitions[i]) {
        break
    }
}
```

It applies only the matching event kind/epoch, preserves unrelated dimensions,
provenance and bindings, and is idempotent. The explicit map example above shows how to patch a destination-only replica.
A replica that also retains source history must apply the independent source IDs
described below, or use the helper to update both dimensions and sources.

An oversized report invalidates only overlapping resolved AS/network knowledge.
Admission-time resolution is retained independently of current source
configuration, including after deregistration and RC reuse. Canonical owners
still identify the AS across sibling labels. Only unresolved report scope permits
whole-partition invalidation. Oversized DAUD removes no knowledge because it
requests current state (RFC 4666 §4.5.3); it still emits resource loss and
increments `ReportsRefused`. Resync returns survivors, not discarded history.

For dimension-only invalidation, a default partition has at most 2,048
dimension records, hence at most 2,048 removal keys. At 16 accounted bytes per
key, 512 base bytes and at most 160 bytes of reason, a publication costs at most **33,440 bytes**, below the default
**1,048,576-byte** queue. No RC lists, report values or configured partition-name
strings are repeated. Custom retention limits use chunks of at most 2,048 keys,
with the same dimension-only publication bound. Source-removal arrays have an
independent cap and a separate combined bound described below. Each chunk has
a contiguous event revision and increments `PartitionsInvalidated`; peer-only removals do not.
Last-binding retirement retains its counter semantics. Aggregate queued bursts,
smaller custom byte limits and slow readers can still lose continuity and need
Resync; scoped invalidation itself adds no receive-side wait. The optional
DAUD restart/forget cancellation join described below is a separate lifetime
change and can be reached from restart receive handling.

Destination retention now charges **128 fixed bytes + 4 per wire RC + 16 per
resolved admission AS entry**. This includes the private resolution and can
refuse a write sooner under a fixed `MaxBytes` limit. The minimum accepted
`MaxBytes` is now 400 (256 for a partition plus 144 for one resolved contextless
dimension); smaller nonzero budgets are rejected during configuration. Peer
record accounting is unchanged. Dimension invalidation accounting remains
16 bytes per entry; the source-history migration below adds separately charged
source records and removal identities.

## Received congestion validity and audits

Nil `CongestionConfig.Validity` and `Auditing` preserve explicit DAUD behavior.
Opt in independently with `&CongestionValidityConfig{}` and
`&DestinationAuditConfig{}`. Enabled zero fields select 30-second
`After`/`Period`, 256 `MaxPending` and 10 `MaxMessagesPerSecond`; negative fields
fail construction. Auditing is ASP-only. These are engineering defaults under
optional RFC 4666 §§3.4.4/4.5.3, not standardized or negotiated timer values.

`OnActivation` configures isolation scopes after committed activation.
DUNA/DRST availability audits are independent of congestion method; congestion
periodic work requires `CongestionNationalWithPriorities`. A job keeps its
original source/epoch/scope/range and independent per-AS origins and deadlines.
Separate SCON(0) clears only corresponding congestion origins. DAVA clears
availability/isolation origins and resets surviving congestion deadlines without
clearing congestion. DAUD narrows its outgoing RC list to still-due origins;
original scope remains retained. Positive job replacement is staged and restores
prior work/charges on a budget refusal. No healthy sibling is substituted and
no audit reactivates a withdrawn scope.

A cause retires only when newer applicable clear prefixes collectively cover its
range for every AS still needing that cause, including interleaved reports. A
partial clear keeps work for the remaining range; expired or otherwise unknown
congestion still needs an audit. Coverage follows bounded retained prefixes,
without per-point audit history. Three fixed `uint64` report-version fields
distinguish congestion, availability and isolation causes; the job's 256-byte
base accounting is unchanged.

Received availability and congestion apply to their resolved AS and Network
Appearance. A report scoped to one AS supplies no authority for an unrelated AS.
Lookup and audit retirement use this same rule. Auditing retains independent
`AvailabilitySources`; validity or auditing retains independent
`CongestionSources`, so interleaved clears cannot overwrite another audited AS's
evidence. With both policies nil, the scalar cache still filters by AS; a later
scalar can displace older AS evidence, leaving that AS unknown rather than
borrowing the later statement. Nil policies add no source-history records.

Canonical transfer selection, observed status and direct national-priority DATA
admission use the configured SG/AS aggregate. RFC 4666 §§1.2, 1.4.2.1 and
3.3.1 allow coordinated SGPs to use different local NA/RC labels for that
owner; a sibling's report is not filtered by another SGP's numeric labels.
Standalone projections and explicit source-scoped reads retain AS/network
filtering. Source history, audit origins and invalidation retain their
original reporter and scope; a shared route view does not widen them.

New fields in `CongestionConfig`, `SSNMAvailability`, `SSNMCongestion`,
`PeerCongestionState`, `SSNMDestinationKnowledge`, `MTPDestinationStatus` and
`SSNMEvent` break unkeyed struct literals; use keyed fields. Extend event
switches for `SSNMCongestionExpiredEvent` and `SSNMPublicationOutcomeEvent`. Expiry
consumers must replace the previous `DestinationUpdates`/`PeerCongestion` replacement logic
with `event.ApplyExpiry(&snapshot)` or equivalent version-matched freshness
updates. `SSNMEvent.Expired` carries owned `SSNMCongestionExpiry` identities
with partition `Epoch`, `Destination`, `AS`, original statement `Revision` and
`Peer`. Top-level `Partition` and `Epoch` are empty, including when the event
spans partitions. Only matching retained sources, their scalar copies and
identified peer records expire; historical values, severity and provenance
remain unchanged. The helper does not advance the snapshot revision; the caller
orders the subscription stream. Each commit revalidates and publishes at most
256 candidates atomically, then unlocks before another chunk and cancellation
check. Each ID costs 56 accounted bytes. The default 16,384-record ceiling
bounds an expiry-only pass to 64 compact SSNM subscription events and 950,272
bytes. Other SSNM events and bursts retain their independent limits and real
overflow still requires `Resync`. This bound does not limit the separate MTP
indication queue or compare expiry with receipt.

Queued `DestinationUpdates` destinations now charge 320 fixed bytes, reflecting the
larger public destination shape. Each individual peer or availability/congestion
source row still charges 256 bytes, plus its Routing Context storage. Review
custom subscription byte budgets: report updates can reach them sooner. This
queued-payload change leaves source-removal IDs at 48, expiry IDs at 56, the
230,048-byte combined invalidation maximum and the 950,272-byte compact expiry
pass bound unchanged; retained record and job charges are separate.

Expiry may produce more MTP indications than receipt when transport pressure
masks the received level but expiry changes `ReceivedCongestionUnknown`. A
projection emits at most one indication per destination whose exposed status
changes; it does not suppress that uncertainty change. If the bounded indication
queue overflows, it emits `ResyncRequired` and the consumer must recover current
statuses with `MTPDestinationStatuses`. The compact expiry bound applies to SSNM
subscription events, not this MTP indication queue.
`SSNMDestinationKnowledge.CongestionSources` carries each admitted source's
resolved `AS`, raw scope, report version and local timestamps.

`SSNMAvailability.AS` and `SSNMDestinationKnowledge.AvailabilitySources` are
additional breaking fields: auditing now retains each availability statement's
resolved AS independently. `CongestionSources` is retained with validity or
auditing. Snapshots and report updates own both histories; preserve a
destination that retains either history after its scalar copy disappears.

Apply both `SSNMEvent.CongestionSourcesInvalidated` and
`SSNMEvent.AvailabilitySourcesInvalidated`. The availability array uses the new
`SSNMAvailabilitySourceInvalidation{Destination, AS, Revision}` type; congestion
IDs retain `SSNMCongestionSourceInvalidation{Destination, AS, Revision}`.
`ApplyInvalidation` patches scalar dimension IDs and both source arrays by the
matching partition `Epoch`, preserving unrelated state; `Partition` is empty.
Manual replicas must match destination, AS and original statement revision,
preserve sorted order, and handle all three independently owned arrays. Each
array caps at 2,048 entries/chunk. Dimension IDs cost 16 bytes and each
availability/congestion source ID costs 48. The combined maximum becomes `512 +
160 + 2048*(16+48+48) = 230,048` bytes; 33,440 still describes dimension-only
invalidation. Custom queues and bursts can still require `Resync`. Keep the
earlier scoped-dimension and congestion-source migrations alongside this
additional availability-source migration.

For `PublicationKind=CongestionPublicationDAUD`, `SSNMEvent.PublicationCount`
on `SSNMPublicationOutcomeEvent` counts each completed job
outcome for `submitted`, `send-failed` and transport `send-deferred`, including consecutive equal reasons
and same-key refresh during transport. The refreshed successor preserves its
newer causes and deadlines. Every removed job contributes exactly once to
`reporter-retired` regardless of retirement-pass order. Other reasons count job
outcome transitions. Counts remain per job: a batched DAUD can complete several
jobs. Consume aggregate `Reason` counts instead
of assuming one event per job.
The seven fixed reasons are queue-full, rate-deferred, reporter-retired,
send-deferred, contended, send-failed and submitted. Each report, pass or
retirement batch emits at most one event per reason. Repeated contention and rate
deferrals of a retained job are suppressed. `ReportSet` is false; a homogeneous source is
in `Report.Association`, with zero denoting mixed sources. A homogeneous original
scope of at most 64 raw RCs is in `Report.Scope`; otherwise `PublicationScopeOmitted`
is true and scope is empty. No APC list, Info String or configured names are
carried. Diagnostic payloads are at most 928 conservative accounted bytes
(actual fixed reasons at most 784). Seven diagnostics use at most seven slots
and 6,496 conservative bytes, plus the actual report-observation and report-patch
events. Only an unfragmented observation adds one slot. Reports keep separate
accounting. A submitted outcome/local report proves neither peer receipt nor
response correlation.

Review budgets when enabling validity. Scalar destination dimensions cost `128 +
4*rawRC + 16*resolvedAS`. Each independent availability or congestion source
adds one record and `144 + 4*rawRC` bytes per resolved AS. Auditing enables
availability history; validity or auditing enables congestion history. Nil
policies retain the previous scalar record counts and costs. The minimum
positive `MaxBytes` is 400. One single-RC auditing availability statement or
validity/auditing SCON plus its partition needs 552 bytes; a one-binding/one-
partition job adds 472 before Info String, totaling 1,024 for that statement and
job. General jobs cost `256 + len(info) + 8*rawRC + 144*bindings +
64*uniquePartitions`; each binding contains an 80-byte origin and 64-byte
lifetime token. Each source also consumes a retained record. These units are
portable accounting, not heap/RSS bounds. Registration removal, version/epoch
replacement and source loss release affected history/job charges while retaining
unaffected static membership and the other IPSP registration direction. With
validity or auditing enabled, admission-time tracker → dynamic-binding → store
fencing uses the same source token for retention and job insertion; remembered
Association source epochs advance monotonically.

`Association.ForgetDestinations` also releases the calling ASP/IPSP reporter's
canonical availability, congestion source history and periodic audit origins,
then refreshes projections. Canonical siblings, peer ASP congestion, independent
transport observations and configured isolation work survive. Its return value
still counts only released legacy destination rows; it does not include
canonical records or audit jobs. An unconditional forget-generation fence
rejects stale received admissions and legacy writes delayed after route
resolution, including when validity and auditing are both nil. Canonical
publication checks the same generation. Bindings stay live for genuinely new
reports.

With both policies nil, receive admission captures one atomic forget-generation
load and validates it under the existing legacy/canonical mutation locks. It
adds no submission-tracker acquisition or source-history records. Full source-
epoch and registration-token capture remains enabled only with validity or
auditing; the unconditional forget barrier does not extend that separate policy.

The forget generation is also checked under the existing route-cache mutex
before a delayed report can publish derived indications. `ForgetDestinations`
does not reclaim pre-existing shared SG route-index allocations; its reclaim
contract covers the reporter-owned stores and periodic work described here.

Handle `MTPDestinationStatus.ReceivedCongestionUnknown` as local uncertainty,
not measured clear. Another fresh received or transport contribution may remain
congested. Newer expired statements must not reveal older superseded statements.
Bare DAVA does not clear congestion and expiry emits no SCON(0), DAVA or RESUME.
It imposes no TTL on independent transport observations and negotiates no peer
validity period.

`Endpoint.Close` cancels and joins the automation worker through its operations
wait after Association closure, outside locks needed for completion. Association
withdrawal/teardown do not join it. Late REG/DEREG dispatcher/request handling
can retain RKM correlation/lifecycle locks while joining a cancelled tracked
optional SCON/DAUD; completion takes only the tracker and optional submission never
takes either RKM lock. One `NoWait` attempt retains the pinned transport's
internal mutex/application-deadline limits (RFC 6458 §5.3.4; Errata 6111).

Restart and `ForgetDestinations` now exclude new optional SCON/DAUD admission,
cancel an admitted optional attempt and settle it before changing the epoch or
reclaiming knowledge. Concurrent lifetime changes share admission closure without joining each
other’s cleanup. Activation reconciliation leaves its bookkeeping untouched
while admission is closed, so the final owner can schedule configured work. Completion joins occur outside transport, destination, store and
procedure locks and settle only optional SCON/DAUD, never DATA or synchronous SCON.
At most one optional attempt holds the Association writer lock. This introduces
a cancellation-completion wait reachable from restart receive handling and the
state monitor; ordinary received status processing rejects a changing or stale
admission instead of joining that gate. No universal elapsed-time bound is
claimed for the transport's internal mutex or application deadline.

See the
[design](design/endpoint-management-and-ssnm.md#received-congestion-validity-and-audits)
for scope, budget, event and lifetime details.

### Derived SSNM indexes and work bounds

Received-source deltas now use exact destination buckets and are not built when
there are no registered subscriptions. Snapshots visit source rows once; congestion
projections share an invalidated prefix aggregate instead of reducing broad
sources separately for each destination. Availability uses the same incremental
all/scoped input index as offered-status marking, with at most two links per
source and 25 prefix probes per query. Availability changes retain a warm
congestion aggregate; combined history reads cost at most 50 probes/nodes.
Coverage, scope matching and retirement
share operation-wide indexes. Received-report and snapshot values/order, logical
charges and overflow barriers are preserved. Lifecycle retirement aggregates
removed-job diagnostics once per event, with monotonically increasing stream
revisions and exactly one count per removed job.

Multi-context withdrawal, registration and deregistration select their contexts
before retiring history and audit origins. Peer AS retirement uses exact reporter/
AS record buckets; legacy destination retirement filters the withdrawn context
set once. Registration gates still settle tracked submissions before publishing
replacement versions. SGP peer changes additionally serialize the short retirement/
map-publication span with peer admission, outside transport and subscriber waits.

The retained congestion aggregate has at most 50 nodes per congestion source
and at most two insertions per source. Other derived indexes follow retained source,
job and origin counts. These allocations are outside `SSNMStateConfig.MaxBytes`
logical accounting; the limit is not a total heap/RSS bound. Binding/configured
inventory counts and output/sorting costs keep their separate bounds. See the
[retained-state work design](design/endpoint-management-and-ssnm.md#retained-state-work-and-derived-indexes)
for cold-build, mutation-interval, fixed-prefix and per-operation bounds.

## Continued MTP status

`MTPIndication` gains `Cause MTPStatusCause` and `Reason MTPStatusReason`;
`CongestionConfig` gains `MaxStatusDestinations int`. Replace unkeyed composite
literals for these exported structs with keyed literals. The new fields and
types have no compatibility aliases.

The cause zero value is `MTPStatusCauseNone`, including PAUSE/RESUME. Known congestion
onset, repeat and clear use `MTPStatusSignallingNetworkCongested`; inspect
`Destination.Congested` and the level fields to distinguish them. The reason
zero value is `MTPStatusTransition`; repeated/initial offered-message status
uses `MTPStatusOfferedTraffic`. Only explicit received-validity expiration uses
`MTPStatusCauseNone` and `MTPStatusValidityExpired`, not a measured clear or
continued known congestion. Existing Kind, Destination, ResyncRequired and
stream lifetime semantics remain in force. `Reason` identifies the trigger of
a notification carrying a coalesced current aggregate snapshot; it provides no
per-field causal history. Transport uncertainty uses the normal transition
reason, never received-validity expiry. Availability, transport or lifecycle
operations exposing already-expired knowledge also keep `MTPStatusTransition`;
an uncertain noncongested result uses `MTPStatusCauseNone` and never claims measured congestion
clear. These trigger values are engineering policy; RFC 4666 §3.4.4 permits a
validity timer without defining them.

As a conservative engineering policy, either `ReceivedCongestionUnknown` or
`TransportCongestionUnknown` suppresses repeats and retires the automatic
interval, even if `Congested` stays true from a surviving contribution.

Each live exact-DPC LRU entry owns one restart mark in a single Endpoint
registry. Matching uses shared immutable routing configuration and the changed
input's scope; entries retain no association or source snapshots. Actual
availability, congestion or freshness changes, including changes selected by
the newest covering input, uncertainty, expiry and renewal after expiry,
commit marks before route projection. Binding admission/retirement, association
attachment, Active/Done/close and traffic-permission changes also mark affected
entries at commit. An eligible routed offer consumes its mark and resets the interval.
A later onset marks it again even if an offer already consumed the preceding
clear, so coalesced projection cannot hide that restart.

Equal renewals preserve the interval when all relevant newest inputs,
including ties, remain unchanged and fresh. Timestamp/revision-only changes
and changes outside the entry's point-code or configured SG/AS route scope
also preserve it. Canonical mark matching uses that configured owner, just like
canonical transfer and observation; peer-local NA/RC labels do not narrow it
(RFC 4666 §§1.2, 1.4.2.1, 3.3.1). Standalone reads and source-specific
invalidation keep their independent applicability rules. Association and lifecycle changes may conservatively mark
all provisioned routes, including initially inactive or
later-attached alternatives; per-AS permission changes mark the routes whose
permission changed. Relevant changes can therefore cause extra initial
reminders even while another contribution sustains congestion. The indication
still carries the current coalesced route projection, without recomputing a
fresh canonical aggregate for the offer. Marks retire with their LRU entries
and Endpoint close and retain no event history.

Mark matching is bounded by the entry budget, the change's own scope, the
24-bit point-code width and configured candidates. It does not scan retained
source history or recompute an aggregate per entry. A shared input index
resolves a received source's newest covering input with at most 25 prefix
checks per query. This also detects an equal broad reassertion replacing a
newer narrow clear, including equivalent wire-scope aliases. An expired input
tied for newest revision conservatively makes renewal restart the interval;
it does not change the canonical projection, status payload or Cause/Reason.
Removing that input lets unchanged fresh renewals preserve cadence again.
Shared per-prefix/per-revision counts track retained records and expirations.
For H retained received-source records, the index owns at most 2H links and
2H revision groups, released with their source keys. It retains no additional
statements, destination aggregate or interval-by-association table. These
structural bounds are engineering contracts, not portable heap-byte or RSS
measurements.

Availability history uses one shared incremental prefix index for ordinary
projection and offered-status marks. Each retained source has at most two links
(all-source and normalized scoped inputs); a query checks at most 25 prefixes.
Availability changes do not rebuild the separate congestion aggregate. Admission,
withdrawal and forget fence their actual availability and scalar removals with
marks, including when a remaining source supplies the same value after the
scalar copy disappears. Equal current-input renewals preserve cadence. Restart
settles optional DAUD before its existing epoch-reset fence; compact expiry
identities leave the final validity notification as the only expiry origin.
These operations add no wait to offered transfer beyond short bookkeeping locks.

Bounded atomic flags per association preserve semantic transport Known/Level
changes, including clear, later onset and positive-level changes, across
coalesced projection. A commit fence begins before snapshot publication and
stays visible until a pending reset has been posted. Consumers preserve the
Endpoint dirty hint while a commit is in progress, so an offer consuming a
clear cannot absorb a later onset or erase an in-flight snapshot change.
Storage is four booleans per association plus the Endpoint hint, with no event
history, additional sampler locks or user callbacks.

The next routed offer or projection conservatively retires counters for the
association's configured routes, covering activation before projection.
Ordinary observations reset only while the association is Active at
consumption; ordinary samples consumed while inactive preserve cadence.
Forced epoch retirement is independent of Active. Sampling is fenced even
when it starts inactive because activation can cross snapshot publication.
Another contribution may sustain congestion, so a reset can produce an extra
initial reminder without a false clear; the limit is no later than eight
offers, not exactly eight. Equal Known/Level observations, including changes
to diagnostic fields or timestamps alone, and unrelated peer timers preserve
the interval. Sampling never waits for route or AS locks.

Routed ASP `MTPTransfer` now publishes offered-traffic status under international
and national-without-priorities profiles: offer 1, then no later than eight
offers after each indication while the destination remains known congested
(Q.704 (07/96) §§11.2.3.1/11.2.5). An uninterrupted interval reports at 1/9/17; resets can
report earlier. Consumers must accept repeats without a state transition.
These are validated offers before transport, including failed sends; invalid
requests, broadcast copies, gateway choices, policy reevaluation and recovery
retries do not add offers. Raw `WriteData` and `WriteSignal` never count.
National-with-priorities remains transition-driven (§11.2.4).

`MaxStatusDestinations` is the Endpoint-wide LRU limit for exact-DPC intervals,
including DPCs inside masked routes, with route/method generation identity.
Zero selects 4096 as an engineering default; negative values are rejected.
Eviction restarts initial notification on the next known-congested offer.
Effective clear, either uncertainty, lifecycle retirement and route/method
generation changes reset intervals. Equal renewal preserves cadence when all
relevant newest inputs, including ties, remain unchanged and fresh. `Endpoint.Close` ends this state. The existing overflow/resync mechanism applies without making
`MTPTransfer` wait for the stream consumer.

For IPSP or application-owned routing, use `NewMTPStatusProcedure(profile)`
once per destination and local MTP3 boundary. It is safe for concurrent calls,
has no hidden destination map and must not be copied after use. Call
`Offered(status)` once per original validated offer with your effective
aggregate status; it returns `(indication, error)`. Handle the error before
distributing a non-nil indication. `(nil, nil)` means nothing is due, including
under national-with-priorities. A changed
`Destination` resets automatically; supplying clear or uncertain status to
`Offered` also resets the interval. The helper sees snapshots only. Call
`Reset()` on known clear, uncertainty, expiry or local lifecycle/method
retirement between offers, including a source ending masked by a surviving
contribution. The application must order source updates and resets with its
offers at its own routing boundary. `Endpoint.SubscribeSSNM` can supply ordered
source updates; handle subscription loss by resetting and resynchronizing.
The coalesced `Endpoint.MTPIndications` stream alone cannot reconstruct hidden
source endings. IPSP and SGP peer-input procedures have the same application-owned
ordering responsibility. Reset keeps the configured profile; construct a new
procedure for a changed method. Routed ASP `MTPTransfer` maintains restart marks
and fenced transport reset flags internally, so do not also call the helper
for those offers.

`MTPIndications` is one bounded consumer stream; fan each primitive out to
**each** registered MTP3-User. Concurrent readers compete for messages rather
than multicast them. Level-4 users still decide when to resume transmission
(Q.704 (07/96) §11.2.3.3). RFC 4666 §1.4.6 describes this current-MTP3-procedure
service without an uppercase MUST; it does not turn optional SCON/TFC into a
mandatory report.

## Local congestion publication and DATA admission

Automatic local SCON now uses bounded retry instead of a one-shot overflow
attempt. Explicit `Association.SignallingCongestion`, `Endpoint.SignallingCongestion`
and `Association.DestinationStateAudit` remain synchronous and return actual
submission errors; they do not acquire retries. A national clear can be delayed,
and an onset superseded before submission may never appear on the wire. Local
observability survives failed publication. A successful submission proves
neither peer receipt nor delivery of abatement.

Update unkeyed literals and exhaustive management-kind/publication-kind switches
for the following additions. There are no compatibility aliases.

| New API or field | Migration contract |
| --- | --- |
| `CongestionConfig.Publication` | `*CongestionPublicationConfig`; nil and zero fields select the defaults below. |
| `CongestionPublicationConfig.MaxPending`, `MaxPendingPerAssociation`, `MaxBytes` | Endpoint-wide slot/metadata ceilings of 64, 32 per association and 64 KiB by default. Negative values are invalid; per-association capacity must not exceed `MaxPending`. |
| `CongestionPublicationConfig.RetryInterval`, `MinReportInterval` | Zero selects 100 ms retry and a 1 s interval after successful submission. Negative durations are invalid. These are engineering choices, not peer-negotiated timers. |
| `LocalCongestionRequest.Source`, `Association`, `Epoch`, `AS`, `Destination`, `Congestion` | A source identifier of 1–64 UTF-8 bytes of Unicode letters/digits or `_`, `-`, `.`, `:`, `/`, current association epoch, active association member/binding of the exact AS, exact 24-bit DPC and method-consistent `CongestionState`. The `$` namespace is reserved; queue records use `$queue:<OPC>`. |
| `LocalCongestionContribution.Request`, `Scope`, `Revision` | Copied source request and wire scope with a library-assigned revision. No caller revision or TTL is accepted. |
| `Endpoint.ReportLocalCongestion` | Declares or replaces one source's state. National numeric level must agree with `Congested`; omitted national levels represent unknown congestion. International input has `LevelSet=false`, `Level=0`; `Congested=false` declares local clear. Contradictions return `ErrInvalidParameterValue`; contention or changing admission can return `ErrCongestionDecisionStale`, requiring a complete retry with current scope. |
| `Endpoint.WithdrawLocalCongestion` | Takes association, epoch, AS, destination and source; removes only that contribution. Explicit zero declares only that source clear; withdrawal removes it. Absence is an idempotent success only with a current active association member/binding; contention can return `ErrCongestionDecisionStale`. |
| `PeerCongestionVersion.LocalGeneration` | New opaque component for local lifetime/queue transitions. Compare it with the other version components; do not interpret the numeric value. Update unkeyed literals. |
| `CongestionInputSnapshot.Local` | Owned queue/application contribution values, separately inspectable. Shared `SSNMStateConfig` record/byte/per-partition limits can refuse retention with typed `ErrSSNMResourceLoss` and loss events. |
| `CongestionPublicationKind`, `CongestionPublicationLocalSCON`, `CongestionPublicationDAUD` | Distinguish optional local reports and automatic audits. |
| `CongestionPublicationState.Association`, `Epoch`, `Scope`, `Destinations`, `Kind` | Identify the optional publication scope; snapshot slices and scopes belong to the caller. |
| `CongestionPublicationState.ConcernedDestination`, `ConcernedDestinationSet` | Preserve a triggering origin separately from omitted CD; different origins do not merge. A clear retains its reservation identity but omits CD on the wire because recovery has no fresh trigger. |
| `CongestionPublicationState.DesiredCongestion`, `DesiredRevision`, `SubmittedRevision`, `Pending` | Separate current local intent from last submitted revision; congestion is meaningful only for local SCON. Same-state coalescing can leave desired revision ahead with `Pending=false`. Do not infer peer delivery from `SubmittedRevision`. |
| `CongestionPublicationState.Attempts`, `LastAttempt`, `LastError` | Attempt diagnostics. Local SCON attempts belong to the reservation; DAUD attempts describe the transient batch, not cumulative audit history. `LastError` is at most 256 bytes and is not an error-identity contract. |
| `Endpoint.CongestionPublications` | Returns bounded owned publication snapshots: at most `MaxPending`; completed clears leave the snapshot. |
| `CongestionPublicationCounters` and `Endpoint.CongestionPublicationCounters` | Lifetime `Queued`, `Submitted`, `Coalesced`, `Deferred`, `Refused` and `Failed` counters. `Queued` counts reservation/transient admissions, not unique audit jobs; deferral decisions can recur. `Failed` includes actual EAGAIN transport refusals. |
| `ManagementCongestionPublication`, `ManagementIndication.CongestionPublication` | Bounded publication state on the existing management channel, independent of logging: new reservations, visible refusals, changed desired state for the updated key, and each actual attempt. Same-state coalescing adds no state-change observation. Apply its existing queue-continuity contract. |
| `SSNMPublicationOutcomeEvent` | Replaces and removes `SSNMAuditOutcomeEvent`; handle both `CongestionPublicationDAUD` and `CongestionPublicationLocalSCON` through `SSNMEvent.PublicationKind`. No compatibility alias remains. |
| `SSNMEvent.PublicationCount`, `PublicationScopeOmitted` | Replace and remove `AuditCount`, `AuditScopeOmitted`. Counts are logical job outcomes, not transport packets. Existing audit aggregation and scope-omission semantics apply when `PublicationKind` is DAUD. |
| `SSNMEvent.CongestionPublication` | Owned exact state for a single local SCON outcome; nil for aggregated audit diagnostics. |
| `AssociationConfig.CongestionAdmission` | `*CongestionAdmissionConfig`; nil preserves disabled DATA backpressure. Requires the backlog monitor when enabled. |
| `CongestionAdmissionConfig.ChargedRatioLimit` | Zero selects 0.9. Require a finite ratio strictly between zero and one. |
| `ErrDataCongestionRefused` | Sentinel wrapped by the existing typed known-not-sent DATA error. Inspect with `errors.Is`; treat it as an ordinary send failure whose DATA was not submitted. No new wire Error code is assigned. |

Queue and application sources aggregate maximum or unknown congestion only
within one association, AS and local DPC. Queue drain does not remove an
application source; received SCON, local overload and outbound transport remain
independent. Contributions persist until explicit withdrawal, `ForgetDestinations` or
lifecycle retirement. Local input does not prove aggregate AS readiness or authorize DATA/SPMC delivery;
optional wire admission checks applicable delivery readiness separately. The AS
has no authoritative local-destination inventory;
admission validates the numeric destination and current binding. Each retained
local source costs one record and `320 + len(Source) + 8*wireRC` portable
accounted bytes, not a heap-size bound. SGP local contributions are NIF/application inputs and do not
originate SGP-to-ASP SS7 SCON. There is no host/CPU detector.

A slot is reserved before onset submission. A submitted national onset retains
its slot and byte reservation until clear submission or scope retirement;
audits use only unreserved transient slots while waiting in their own bounded
deadline store. An unsent onset is cancelled on local clear; repeated desired
states coalesce. An already pending clear can be superseded in place by a newer
onset. If blocked clears exhaust capacity, new onsets are visibly refused or
deferred rather than evicting a clear or exceeding the limits. International
recovery retires work without numeric zero. No periodic local SCON reassertion
is installed.

One worker tries the existing barriers and rechecks live state, epoch and
revision, then makes one `NoWait` attempt. Contention, EAGAIN and pending mandatory
control defer work. Each pass permits at most one optional attempt per association
across both kinds, subject to rate limits. The existing global audit
`MaxMessagesPerSecond` ceiling is additional to the shared per-association
`MinReportInterval` after successful submission; it is not a promised cadence. An
optional attempt already admitted
can race a newly starting mandatory write; there is no preemption or absolute
fairness guarantee. No tracker/state lock crosses transport. Retirement fences
old work without activating an AS. `Endpoint.Close` cancels and joins the worker;
association teardown does not join it.

The former audit-only outcome event and field names are removed. Consume
`SSNMPublicationOutcomeEvent` using `PublicationKind`, `PublicationCount` and
`PublicationScopeOmitted`. DAUD retains per-reason aggregation of logical jobs;
one DAUD packet can complete several jobs. Local SCON can carry an exact owned
`CongestionPublication` state. The management channel also reports new reservations, visible capacity refusals
and changed desired state for the updated key, plus one
`ManagementCongestionPublication` state per actual attempted transport batch;
SSNM diagnostics describe logical job outcomes. Each channel reports its own
outcome once, rather than adding a second audit-only outcome alongside the
common one. A successful local DAUD report remains a separate submission
observation, now once per wire batch rather than once per canonical partition.
Its `Partition` and `Epoch` are empty; `Scope` and `Destinations` identify the
submitted batch. Consumers must stop using raw automatic DAUD observation count
as a canonical-owner count; logical-job `PublicationCount` remains separate.
This cardinality change has no compatibility alias. Publication observations
share the existing 64-slot association management queue; subscribed consumers
must drain it for the association lifetime. Repeated source intent coalesces,
while actual retry attempts still produce observations.
Transient admitted audits appear in the common publication snapshot;
waiting deadlines stay in the separately bounded audit store. Completed clear
reservations leave the snapshot; consume indications/events for the final
observation. Lifecycle or pre-transport retirement of local SCON does not promise
a separate publication outcome notification. Neither channel nor counter proves
peer receipt.

DAUD completion reconciles `Pending` with surviving deadline jobs. An EAGAIN
after a covering DAVA removed the last cause reports the actual failed attempt
once with `Pending=false`; a surviving or renewed cause retains its deadline.
An endpoint cancelled before completion has no pending retry.
The flag describes the completion observation and can become obsolete after
later lifecycle changes. Consumers must not infer retry work from `LastError`
alone.

The opt-in DATA gate reads cached charged/limit evidence under send serialization
for every typed, raw and distributed DATA send. It adds no socket query. At or
above the ratio, before the first valid sample, or with invalid/stale evidence,
it returns the known-not-sent refusal. Control bypasses the gate. This adds no
kernel-memory reservation: a single large message, retransmissions or direct
descriptor users may consume remaining charge. It gives control no delivery
guarantee, and `ControlWriteTimeout` retains close-on-expiry for permanent stalls.
This resource gate is separate from national Message Priority admission and does
not manufacture SCON or DUNA (RFC 4666 §§1.4.6–1.4.7; RFC 6458 §5.3.4; Errata 6111).

SSNM subscriptions keep their existing bounded continuity-loss/`Resync` contract.
A legal multi-AS report or a combined report/expiry burst can overflow an unread
default queue; optional scheduling supplies no whole-burst-fit guarantee.


### Individual event bounds and burst recovery

- `SignallingGatewayID` and `RemoteASID` configuration values now have a
  255-octet ceiling, rejected through `ErrInvalidASPConfig`. ASP and IPSP
  destination `MTPRouteID` values now have the same ceiling; IPSP failures use
  `ErrInvalidRoleConfiguration`. These are byte limits on local names, not
  wire-format limits or character counts. RFC 4666 §3.4.1's INFO String ceiling
  motivates the engineering value; it does not prescribe a local-name limit.
  SGP/path IDs are not included in these event payloads and receive no new cap.
  Local source IDs keep the existing stricter 64-octet limit.
- Explicit SSNM and activation-audit requests now reject RC/APC lists exceeding
  16,382 entries, and nonempty RC lists with `RoutingContextSet=false`, using
  `ErrInvalidParameterValue`. Set the presence flag or supply an empty list.
  The parameter count follows RFC 4666 §3.2's 16-bit length/header format.
- `SSNMEvent.ReportContinues`, `ReportPatch.SourcesContinued` and `ApplyReport` describe
  oversized report fragments. Only the first fragment carries the raw report;
  later fragments replace destinations or append their source histories. All
  events have distinct increasing revisions; statement revisions stay intact.
  Replace manual unconditional `DestinationUpdates` replacement with `ApplyReport` and
  finish through `ReportContinues=false` before exposing a complete report.
  Apply once in order. A missing/out-of-order stream requires `Resync`; the
  helper does not validate arbitrary fragment reordering. Existing reports
  that fit the ceiling keep their shape. Consumers must handle the new fields;
  no alias or silent source truncation is supplied.

Defaults remain **256 slots and 1 MiB**. Each individual event fits this byte
budget, including names, source history patches and diagnostic strings; each
individual MTP indication fits its count-only channel. A whole legal report
plus one pass may exceed either subscription bound. Queued deltas precede one
continuity-loss marker; subsequent deltas remain suppressed until successful
`Resync` replaces the replica with exact retained state and restarts the stream.
The gap is visible; raw report/diagnostic history discarded in it is not replayed.
MTP still emits at most one indication per changed destination and uses
`ResyncRequired` on overflow.

For sizing with validity, auditing and publication enabled, use the accounting
weights and exact `sum_E A(E)` plus event count for the admitted fan-out/history,
including initial queue occupancy. A conservative budget formula uses
`N=MaxRecords`, `p=min(N,MaxRecordsPerPartition,MaxRecordsPerPeer)`, captured
fan-out `F<=16382`, `I=min(N,F*ceil(p/2048))`, publication `Q=MaxPending` and
`B=MaxBytes`:

```
reportSlots <= F*(2+p)+3*N+I+8
reportBytes <= F*(1022+1048575+262144*p)+3*N*66806+672*I+48*N+6160
passSlots   <= ceil(N/256)+Q+7
passBytes   <= 512*ceil(N/256)+56*N+B+272*Q+5488
```

Add report and pass capacities for a conservative lossless burst queue, and
separately include concurrent unrelated producers. These are loose upper bounds,
not jointly attained maxima; the design document derives every component and
per-event ceiling. With defaults the pass-only envelope is 135 events / 1,038,704
bytes, leaving little space for a preceding report. Raising either queue limit
alone is insufficient when the other binds. Keeping defaults and recovering
through `Resync` is supported; no whole-report-plus-pass default-fit guarantee
remains.

### Explicit report patches and complete identity admission

`SSNMReportEvent` is removed, with no alias. Use `SSNMReportObservedEvent`
(`String()` is `report-observed`) for the single raw observation and any
complete `DestinationUpdates`. `SSNMEvent.Updated` is removed, also without an
alias. A consumer of the old whole-destination contract must change its source;
mechanically treating partial history as a replacement is no longer possible
through that field.

Partial replacements and tails use `SSNMReportPatchEvent` (`report-patch`) and
`SSNMEvent.ReportPatch *SSNMReportPatch`, never `DestinationUpdates`.
`ReportPatch.State` contains one destination patch;
`ReportPatch.SourcesContinued` appends only history. The former top-level
`SSNMEvent.SourcesContinued` is removed. `ApplyReport` accepts the observation
and patch kinds, rejects mixed shapes, and applies owned data once in order.
Continue through `ReportContinues=false` before exposing the whole report.
Update switches, unkeyed literals, replicas and event-name telemetry. No
compatibility alias or hidden fallback keeps the former contract compiling.
The 512-byte base, 320-byte destination/patch charge and other accounting
weights are unchanged; queue defaults remain 256 slots and 1 MiB.

Local SCON completion now retires its work when the endpoint has cancelled,
the store is closed or the current submission job is no longer live. Actual
transport results still populate counters and final observations; a failed or
superseded attempt cannot advertise a retry on the cancelled worker. The flag
is an observation at completion reconciliation, not an immutable future promise.

`IdentityLengthError` exposes `Field`, `Length` and `Limit` without retaining
the offending name, and unwraps to `ErrInvalidParameterValue`. Configuration
wrappers additionally preserve `ErrInvalidASPConfig` or
`ErrInvalidRoleConfiguration`. The 255-octet caps now cover registration
requests (ASP and both IPSP directions), saved response metadata, result-batch
installation, the canonical binding setter, unprovisioned
`AssociationConfig.PeerSGP.SignallingGateway`, route references/selections,
congestion peer selectors and transfer route requests. An invalid registration
batch sends and installs nothing; a valid 255-octet name is returned unchanged.
REG RSP carries no name: results use validated local request metadata.
Read-only bool lookups still return false for unknown names and insert nothing.
Local contribution report/withdrawal retain their stricter 64-octet limit and
now return the same typed length details when exceeded. SGP process and route
path IDs are outside these charged identity classes and receive no new cap.

### Integrated publication lifetimes and application status identities

`MTPStatusProcedure.Offered` now returns `(*MTPIndication, error)` rather than
only `*MTPIndication`, without an alias or wrapper for the former signature.
Handle the error before using the indication. Its caller-supplied
`MTPDestinationStatus.Destination.MTPRoute` now has the same 255-octet cap as
configured route names; 256 octets return `IdentityLengthError` wrapping
`ErrInvalidParameterValue` without retaining the name, advancing the count or
resetting the destination. This applies even to clear/unknown inputs and the
national-priority method. Valid offers retain their prior cadence and resets.
The cap is local engineering policy, not an RFC limit on local names.

SCON and DAUD completion now retire retry work when the association lifetime
has ended, including the interval between `Done` and transport/store cleanup.
The endpoint may still be live. Actual admitted attempts keep their result and
counters; `Pending` is a live-work observation at reconciliation, not a promise
against a later lifecycle change. A successful admitted DAUD can still produce
its actual local submission observation; it does not authorize another retry.

Registration and partition retirement apply the admitted/selected set once to
local input and publication work. A submitted national clear remains reserved
until submission or scope retirement; grouped retirement now releases those
reservations at the same event boundary. Stale registration versions and the
other IPSP Double Exchange direction preserve unrelated local inputs. Received
history/audit retirement keeps the existing shared batch and source indexes.

Cached DATA refusal remains an ordinary known-not-sent routed send failure.
The original validated request counts as an offer before target selection, so
refusal counts once, including broadcast; no candidate/copy adds another offer.
Local SPMC contributions and publication outcomes remain separate from outbound
received SS7/transport congestion. They do not reset an unchanged routed
interval. Effective received/transport/eligibility changes retain commit-time
restart marks. Neither cached admission nor optional publication reserves kernel
memory or guarantees mandatory-control delivery.
