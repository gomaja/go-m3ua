# go-m3ua

Simple M3UA protocol implementation in the Go programming language.

[![CI status](https://github.com/gomaja/go-m3ua/actions/workflows/go.yml/badge.svg)](https://github.com/gomaja/go-m3ua/actions/workflows/go.yml)
[![Security status](https://github.com/gomaja/go-m3ua/actions/workflows/security.yml/badge.svg)](https://github.com/gomaja/go-m3ua/actions/workflows/security.yml)
[![Go Reference](https://pkg.go.dev/badge/github.com/gomaja/go-m3ua.svg)](https://pkg.go.dev/github.com/gomaja/go-m3ua)
[![GitHub](https://img.shields.io/github/license/mashape/apistatus.svg)](https://github.com/gomaja/go-m3ua/blob/main/LICENSE)

## Quickstart

Applications upgrading from v1.0 should read the
[v1.2 migration guide](./docs/migration-v1.2.md).

### Installation

Run `go mod tidy` in your project's directory to collect the required packages automatically.

_This project follows [the Release Policy of Go](https://go.dev/doc/devel/release#policy)._

_Full SCTP socket validation runs on Linux. Non-Linux systems can build and run
non-socket tests, but production M3UA associations require OS SCTP support._

### Trying Examples

Working examples are available in [examples directory](./examples/).
The examples below run an SGP that accepts an SCTP association and an ASP that
initiates one. RFC 4666 Section 1.4.8 also permits the opposite SCTP
orientation.

```shell-session
# Run the SGP first.
cd examples/sgp
go run m3ua-sgp.go

# Run the ASP.
cd examples/asp
go run m3ua-asp.go
```

There is also an example for Point Code format conversion, which works like this;

```shell-session
$ ./pc-conv -raw 1234 -variant 3-8-3
2023/04/05 06:07:08 PC successfully converted.
        Raw: 1234, Formatted: 0-154-2, Variant: 3-8-3
$ 
$ ./pc-conv -str 1-234-5 -variant 4-3-7
2023/04/05 06:07:08 PC successfully converted.
        Raw: 29957, Formatted: 1-234-5, Variant: 4-3-7
```

### For Developers

Create an `Endpoint` with an explicit RFC 4666 role. `Dial` and
`Listen`/`Accept` state only which endpoint initiates the SCTP association;
they do not determine whether M3UA runs as an ASP or SGP.

RFC 4666 Section 1.2 names the M3UA protocol entities `ASP`, `SGP`, `IPSP`,
`AS`, and `Association`. Section 1.4.8 uses client/server only for which peer
initiates the SCTP association. Accordingly, this API never uses Client or
Server as an M3UA role: `RoleASP`, `RoleSGP`, and `RoleIPSP` select protocol
procedures, while `Dial`, `Listen`, and `Accept` describe SCTP establishment.

The base `AssociationConfig` is role-neutral and is snapshotted for each M3UA
association. Role-specific setters must then match the Endpoint; this ASP
example sets an ASP Identifier:

```go
config := m3ua.NewAssociationConfig().
    EnableHeartbeat(3*time.Second, 10*time.Second).
    SetApplicationServers(m3ua.ASConfig{
        ASKey: m3ua.ASKey{
            NetworkAppearance: 7, NetworkAppearanceSet: true,
            RoutingContext: 1, RoutingContextSet: true,
        },
        TrafficMode: params.TrafficModeLoadshare,
    })
config.SetASPIdentifier(1) // ASP-only
```

`ApplicationServers` is the whole of the association's membership. Each entry
names one Application Server by the exact `ASKey` a message must name, and
carries the Traffic Mode agreed for that one Application Server — not for the
association. An empty inventory is the contextless Application Server of RFC
4666 Section 3.6.1.

The configuration holds no message defaults. Every DATA carries its own MTP3
routing label, Application Server scope and Correlation Id, as RFC 4666 Section
3.3.1 defines them, so there is nothing about a message for an association to
hold.

An IPSP Association must select an RFC 4666 Section 4.3 exchange model
explicitly, and must state which procedures it initiates: RFC 4666 permits
either IPSP to initiate either exchange, so there is no role-implied default.

```go
ipsp, err := m3ua.NewEndpoint(m3ua.EndpointConfig{Role: m3ua.RoleIPSP})
if err != nil {
    log.Fatal(err)
}

config.IPSP = &m3ua.IPSPConfig{ExchangeModel: m3ua.IPSPExchangeSingle}
config.ASPProcedures = &m3ua.ASPProcedurePolicy{
    ASPUp:       m3ua.ASPProcedureAutomatic,
    ASPDown:     m3ua.ASPProcedureAutomatic,
    ASPActive:   m3ua.ASPProcedureExplicit,
    ASPInactive: m3ua.ASPProcedureAutomatic,
}
```

Double Exchange gives each direction of data traffic its own Routing Key,
Network Appearance, Traffic Mode, and ASP/IPSP state as required by RFC 4666
Sections 4.3 and 5.6.2:

```go
config.IPSP = &m3ua.IPSPConfig{
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

Each direction declares its own Application Server inventory and nothing else;
RFC 4666 Section 5.6.2 keeps the two directions independent, so neither reads
the other's membership, Network Appearance or Traffic Mode.

With the normal `IPSPASPSMExchangeDouble` procedure, an automatic `ASPUp`
requires `TrafficToLocal`, because that ASP Up establishes the direction in
which the peer sends DATA to the local IPSP. The agreed
`IPSPASPSMExchangeSingle` simplification may establish both directions with one
ASP Up exchange. An automatic `ASPActive` always requires `TrafficToLocal`.

`TrafficToLocal` is the traffic the peer sends to this IPSP after this IPSP's
ASP Up/ASP Active procedures succeed. `TrafficToPeer` is the traffic this IPSP
sends after the peer's ASP Up/ASP Active procedures succeed. A non-nil direction
with an empty inventory is the contextless Application Server in that direction;
a nil direction is disabled. `Association.IPSPState()` reports both directions.

`IPSPASPSMExchangeDouble` is the normal independent ASPSM exchange.
`IPSPASPSMExchangeSingle` enables only the agreed ASPSM simplification described
by RFC 4666 Section 4.3; ASPTM and DATA remain independently directional.
See the [Double Exchange design](./docs/design/ipsp-double-exchange.md).

`ASPProcedures` does not describe SCTP initiation. The same IPSP configuration
works with `Dial` or with `Listen`/`Accept`; the remote IPSP uses its own
Association policy. At least one IPSP must initiate each required exchange; both
may initiate, and simultaneous exchanges are supported.

`HeartbeatInfo` controls RFC 4666 M3UA BEAT/BEAT Ack liveness only. It is
separate from SCTP HEARTBEAT path management, which remains transport/kernel
behavior below go-m3ua.

RFC-strict parsing is the default. If a known peer sends an optional INFO String
that is not valid UTF-8, enable the explicit compatibility policy for that peer:

```go
config.Compatibility = m3ua.AcceptInvalidOptionalInfoString()
```

Compatibility decisions are surgical: after the approved INFO String tolerance,
the normal message-specific RFC validation still runs.

For custom interop decisions, install a tolerator and accept only classified
violations you have approved:

```go
config.Compatibility = m3ua.CompatibilityPolicy{
    Tolerator: m3ua.ToleratorFunc(func(v m3ua.ProtocolViolation) m3ua.ProtocolDecision {
        if v.Kind == m3ua.ViolationInvalidOptionalInfoString {
            return m3ua.ProtocolAccept
        }
        return m3ua.ProtocolReject
    }),
}
```

Create an ASP Endpoint. `ASPConfig.SignallingGateways` provisions the peers and
the Application Servers each SGP serves; `ASPConfig.Routing` is the optional
outbound route inventory, and leaving it nil hands outbound candidate selection
to the application. An Application Server's name is local to its Signalling
Gateway, while the Routing Context and Network Appearance that label it are
peer-specific `ASKey` values bound per SGP; neither is a global route
identifier.

A route names path candidates rather than peers directly. `Paths` provisions
each candidate once — one Signalling Gateway and its Application Servers in
preference order — and `MTPRouteConfig.Paths` references them by name, so
several routes can share one candidate:

```go
peer := m3ua.SGPIdentity{
    SignallingGateway: "sg-a",
    SignallingGatewayProcess: "sgp-a1",
}
asKey := m3ua.ASKey{
    NetworkAppearance: 7, NetworkAppearanceSet: true,
    RoutingContext: 1, RoutingContextSet: true,
}
endpoint, err := m3ua.NewEndpoint(m3ua.EndpointConfig{
    Role: m3ua.RoleASP,
    ASP: &m3ua.ASPConfig{
        SignallingGateways: []m3ua.SignallingGatewayConfig{{
            ID: peer.SignallingGateway,
            SGPs: []m3ua.SignallingGatewayProcessConfig{{
                ID: peer.SignallingGatewayProcess,
                ApplicationServers: []m3ua.RemoteASConfig{{
                    ID: "as-core",
                    ASKey: &asKey,
                }},
            }},
        }},
        Routing: &m3ua.ASPRoutingConfig{
            SignallingGatewaySelection: m3ua.RouteSelectionPrimaryBackup,
            SignallingGatewayProcessSelection: map[m3ua.SignallingGatewayID]m3ua.RouteSelectionMode{
                peer.SignallingGateway: m3ua.RouteSelectionPrimaryBackup,
            },
            Paths: []m3ua.MTPRoutePath{{
                ID: "via-sg-a",
                SignallingGateway: peer.SignallingGateway,
                ApplicationServers: []m3ua.RemoteASID{"as-core"},
            }},
            MTPRoutes: []m3ua.MTPRouteConfig{{
                ID: "sccp",
                DestinationPointCode: 0x220000,
                Mask: 16,
                ServiceIndicators: []uint8{params.ServiceIndSCCP},
                Paths: []m3ua.MTPRoutePathID{"via-sg-a"},
            }},
        },
    },
})
if err != nil {
    log.Fatal(err)
}
defer func() { _ = endpoint.Close() }()

config.PeerSGP = &peer
remote, err := sctp.ResolveSCTPAddr("sctp", PEER_ADDRESS)
if err != nil {
    log.Fatal(err)
}

// ctx is the association's lifetime, not just its handshake: cancelling it
// closes the association.
ctx, cancel := context.WithCancel(context.Background())
defer cancel()

association, err := endpoint.Dial(ctx, "m3ua", nil, remote, config)
if err != nil {
    log.Fatalf("Failed to establish M3UA association: %s", err)
}
```

`endpoint.Close` is the one deferred close an ASP needs: it closes every
Association the Endpoint owns along with the shared state none of them owns
individually. See [Ownership and shutdown](#ownership-and-shutdown) below.

For an ASP with provisioned routes, submit the RFC 4666 MTP-TRANSFER request to
the Endpoint. It resolves the MTP Route, selects the path candidate, SGP and
Association, applies that peer's `ASKey`, and derives the SCTP stream from the
Protocol Data SLS:

```go
result, err := endpoint.MTPTransfer(m3ua.MTPTransferRequest{
    MTPRoute: "sccp",
    ProtocolData: params.NewProtocolDataPayload(
        opc, dpc, params.ServiceIndSCCP, ni, priority, sls, d,
    ),
})
if err != nil {
    log.Fatalf("MTP-TRANSFER failed: %s", err)
}
for _, path := range result.SuccessfulPaths {
    log.Printf("%d user octets reached AS %q of SGP %q on association %d",
        result.UserDataOctets, path.ApplicationServer,
        path.SGP.SignallingGatewayProcess, path.Association)
}
```

`SuccessfulPaths` names every target the payload reached, in selection order. It
reports what this node sent, not what the far end received: RFC 4666 defines no
acknowledgement for DATA.

When no candidate can carry the request, the error is an `*MTPSelectionError`
listing every candidate the route tried and why each was refused, in the route's
own candidate order. That is what an alternate-path recovery decision is made
from:

```go
var selection *m3ua.MTPSelectionError
if errors.As(err, &selection) {
    for _, rejection := range selection.Rejections {
        log.Printf("%s via %q: %s", rejection.ApplicationServer, rejection.Path, rejection.Reason)
    }
    if errors.Is(err, m3ua.ErrDestinationStateUnknown) {
        // Every candidate was refused only because no Signalling Gateway has
        // reported this destination. Audit it, or set
        // ASPRoutingConfig.AllowUnknownDestinations to send anyway.
    }
}
```

Consume Endpoint-wide derived MTP-PAUSE, MTP-RESUME, and MTP-STATUS
indications. An individual Association ending does not close this channel:

```go
for indication := range endpoint.MTPIndications() {
    if indication.ResyncRequired {
        statuses := endpoint.MTPDestinationStatuses()
        _ = statuses // Replace the application's route snapshot atomically.
        continue
    }
    log.Printf("%s: %#v", indication.Kind, indication.Destination)
}
```

These indications are the MTP3-User's *derived* view over every provisioned
route, and they read a silent Signalling Gateway as one that can carry traffic,
because RFC 4666 Appendix A.2.2 defines capability negatively: established,
activated, and no report of inaccessibility or MTP restart. `MTPTransfer`
decides about one candidate and reads the canonical SSNM store, where an absent
availability record is absence, so it refuses with `ErrDestinationStateUnknown`
unless `AllowUnknownDestinations` is set. The two disagree for exactly one case
— an established, activated, silent Signalling Gateway — and neither is derived
from the other.

### Application-managed routing

Leaving `ASPConfig.Routing` nil keeps the peer and Application Server inventory,
the procedures and the authorization, and hands outbound candidate selection to
the application. Nothing has to be invented to get there: no dummy MTP Route, no
adopting `Endpoint.MTPTransfer`. The application discovers what it can reach
from the SSNM knowledge the Endpoint retains, and sends on the association it
chose:

```go
endpoint, err := m3ua.NewEndpoint(m3ua.EndpointConfig{
    Role: m3ua.RoleASP,
    ASP: &m3ua.ASPConfig{SignallingGateways: gateways}, // Routing left nil.
})
```

`Endpoint.SubscribeSSNM` is the discovery source, and is described under
[Layer Management and SSNM operations](#layer-management-and-ssnm-operations).
`Endpoint.MTPIndications` is not the discovery source here. Every ASP Endpoint
has that channel, whatever its `ASPConfig`, so it is non-nil — but nothing ever
arrives on it, because these indications are derived from provisioned MTP
Routes and this ASP has none. A receive blocks until `Endpoint.Close` closes
the channel, so treat it as idle rather than as something to wait on.

Association-level `WriteData` and `ReadData` are the canonical DATA API. A
request names its Application Server scope exactly and carries the whole MTP3
routing label, so concurrent senders on one association never take each other's
scope:

```go
written, err := association.WriteData(m3ua.DataRequest{
    AS: m3ua.ASKey{
        NetworkAppearance:    7,
        NetworkAppearanceSet: true,
        RoutingContext:       1,
        RoutingContextSet:    true,
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

`written` is the SS7 user octets accepted by the local transport; RFC 4666
defines no acknowledgement for DATA, so it is never a claim about delivery.
Every failure is a `*m3ua.DataWriteError` whose `Outcome` is `DataNotSent` —
nothing reached the transport, so a resend cannot duplicate — or
`DataSendIndeterminate`, where submission had begun and the application owns the
retry decision. `errors.Is` and `errors.As` still reach the cause.

A zero `Stream` selects the negotiated stream this message's own Signalling
Link Selection maps to, which is what keeps one SLS in sequence (Section 1.4.7);
an explicit stream is validated against the negotiated maximum, and stream 0 is
never used for DATA.

```go
message, err := association.ReadData(ctx)
if err != nil {
    log.Fatal(err)
}

log.Printf("DATA for %+v arrived on stream %d: %x",
    message.AS, message.Stream, message.ProtocolData.Data)
```

Cancelling `ctx` ends that one read: the association stays open and nothing
queued is discarded.

Inbound DATA is classified from three separate things, and keeping them separate
is the point. `message.Scope` is the Network Appearance and Routing Context
exactly as the peer put them on the wire, presence bits included, which is what
an answer sent back in the same scope must use. `message.AS` is the Application
Server they resolved to, which is what distributing work by Application Server
must use. `message.Epoch` is the SCTP association epoch, which changes when the
peer restarts the association, so traffic from before a restart is
distinguishable from traffic after one:

```go
switch {
case !message.Scope.RoutingContextSet:
    // The peer omitted the Routing Context, so this is the single contextless
    // Application Server the association coordinates.
    handleContextless(message)
case message.AS == coreAS:
    handleCore(message)
default:
    log.Printf("DATA for unexpected AS %+v (wire scope %+v)", message.AS, message.Scope)
}
```

An endpoint that accepts SCTP associations uses `ListenerConfig` to select a
separate immutable `AssociationConfig` per association before M3UA parsing. The
selector runs after SCTP accept and before any M3UA message is parsed, so it is
where an accepted peer is bound to its authorized policy:

```go
listenerConfig := m3ua.NewListenerConfig(defaultAssociationConfig)
listenerConfig.SelectAssociationConfig = func(info m3ua.AcceptInfo) (*m3ua.AssociationConfig, error) {
    // info.RemoteAddr holds every address the peer confirmed for this
    // multi-homed SCTP association, not one representative address.
    return configForPeer(info.RemoteAddr)
}

endpoint, err := m3ua.NewEndpoint(m3ua.EndpointConfig{Role: m3ua.RoleSGP})
if err != nil {
    log.Fatal(err)
}
defer func() { _ = endpoint.Close() }()

listener, err := endpoint.Listen("m3ua", local, listenerConfig)
```

Accept in as many goroutines as the endpoint expects peers. Two of `Accept`'s
errors mean opposite things: an `*AssociationEstablishmentError` concerns one
peer and the Listener is still serving, while anything else is permanent for
that loop. A loop that returns on both stops accepting without saying so:

```go
for range concurrency {
    go func() {
        for {
            association, err := listener.Accept(acceptCtx)
            if err != nil {
                var establishment *m3ua.AssociationEstablishmentError
                if errors.As(err, &establishment) {
                    log.Printf("rejected %s: %v", establishment.RemoteAddr, establishment)
                    continue
                }
                return
            }
            go func() {
                defer func() { _ = association.Close() }()
                serve(association)
            }()
        }
    }()
}
```

See the [SGP example](./examples/sgp) for the whole program.

### Ownership and shutdown

Three scopes own resources, and each closes exactly its own:

| Call | Closes | Leaves alone |
| --- | --- | --- |
| `Association.Close` | that one association, its goroutines and its SCTP association | its Listener, its Endpoint, every sibling association |
| `Listener.Close` | the listening socket and every Association *that Listener accepted* | the Endpoint, its shared AS/NIF/destination/restart state, associations from `Dial` or another Listener |
| `Endpoint.Close` | every Listener and every Association the Endpoint owns, dialled and accepted alike, then the shared state | nothing it owns |

Closing one association is still visible to peers through M3UA, because an ASP
leaving an Application Server can change that AS's state and produce a Notify to
the others. That is RFC 4666 Section 4.3.2 behaviour, not teardown reaching
sideways.

All three release SCTP without sending ASP Inactive or ASP Down, which is RFC
4666 Section 4.9 option (b). Option (a) is `Association.ShutdownContext`, which
performs the procedures `AssociationConfig.ASPProcedures` marks automatic and
then closes. Nothing calls it for the application:

```go
shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
defer cancel()
if err := association.ShutdownContext(shutdownCtx); err != nil {
    log.Printf("graceful withdrawal did not complete: %s", err)
}
// SCTP is released either way; Endpoint.Close then has nothing left to do
// for this association.
_ = endpoint.Close()
```

The `ctx` passed to `Dial` and `Accept` is the association's lifetime, not just
its handshake. Cancelling it closes the associations it produced, so an accept
loop that wants to stop accepting without dropping live traffic closes the
Listener instead.

That also makes it the wrong context to derive from an interrupt signal when the
application wants a graceful withdrawal. The association's monitor closes it as
soon as the context is done, so `ShutdownContext` finds an association already
in ASP-DOWN, sends neither ASP Inactive nor ASP Down, and returns nil — option
(a) silently becomes option (b). Give the association a context of its own and
cancel it only after the withdrawal has returned:

```go
notifyCtx, stopNotify := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
defer stopNotify()

associationCtx, closeAssociation := context.WithCancel(context.Background())
defer closeAssociation()

association, err := endpoint.Dial(associationCtx, "m3ua", nil, remote, config)
...
<-notifyCtx.Done()            // The signal stops the work, not the association.
_ = association.ShutdownContext(shutdownCtx) // Written while associationCtx is live.
```

## Routing Key Management

An SGP or IPSP Endpoint enables the optional RFC 4666 Sections 3.6 and 4.4
Routing Key Management procedures with an immutable authorization and Routing
Context allocation policy:

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
        AllowDynamicRoutingKeys: true,
        MaxDynamicRoutingKeys: 1024,
        RemoveUnusedRoutingKeys: true,
    },
})
```

An ASP or IPSP registers and deregisters Routing Keys through its established
Association:

```go
results, err := association.RegisterRoutingKeys(ctx, m3ua.RoutingKeyRegistration{
    RemoteAS: "as-core",
    RoutingKey: m3ua.RoutingKey{
        NetworkAppearance: 10,
        NetworkAppearanceSet: true,
        TrafficMode: params.TrafficModeLoadshare,
        TrafficModeSet: true,
        Groups: []m3ua.RoutingKeyGroup{{
            DestinationPointCode: dpc,
            ServiceIndicators: []uint8{params.ServiceIndSCCP},
            OriginatingPointCodes: []m3ua.PointCodeRange{{
                PointCode: opc,
                Mask: 0,
            }},
        }},
    },
})
if err != nil {
    log.Fatal(err)
}

_, err = association.DeregisterApplicationServers(ctx, results[0].ASKey)
```

A successful result reports both the canonical Application Server it bound,
`results[0].RemoteAS`, and the exact wire scope the peer assigned it,
`results[0].ASKey`. Deregistration names that scope, so a request that would
contradict the binding the Association holds, or that names no Routing Context
at all, is refused before it reaches the transport.

The responder handles each Routing Key in a batch independently, preserves
deterministic results for duplicate requests, rejects ambiguous overlaps, and
keeps provisioned and dynamically created keys in one collision-checked
registry. A Routing Key that omits Network Appearance uses the Association's
single configured appearance when one exists. Without one, it applies to all
Network Appearances and RFC 4666 Section 3.6.1 permits no second Routing Key on
that Association.

RFC 4666 defines no RKM acknowledgement timer. Caller context cancellation
bounds a local wait; peer retransmissions are handled idempotently rather than
by inventing an RKM T(ack). If cancellation occurs after a DEREG REQ is written,
the same Routing Context cannot be retried until its delayed DEREG RSP arrives:
`DeregisterApplicationServers` returns `ErrDeregistrationOutcomeUnknown` because
RFC 4666 Sections 3.6.4 and 4.4.2 provide no transaction identifier that could
distinguish the old response from the retry.

An Association retains at most 1,024 unresolved REG/DEREG outcomes. A new
request that could exceed that bound returns `ErrRKMOutcomeLimit` before writing
to the Association. A delayed response releases capacity, so the application
can retry without reconnecting once the peer resolves an older outcome.

## Layer Management and SSNM operations

Endpoint exposes keyed RFC 4666 Layer Management snapshots for Associations,
ASPs, Application Servers, MTP Routes, and destinations. Exact `ASKey` values
retain Network Appearance and contextless-AS identity:

```go
associationStatuses := endpoint.AssociationStatuses()
aspStatuses := endpoint.ASPStatuses()
applicationServerStatuses := endpoint.ApplicationServerStatuses()
```

`AssociationConfig.ASPProcedures` selects automatic or explicit ASP Up, ASP
Down, ASP Active, and ASP Inactive behavior independently from SCTP initiation.
Explicit methods wait for the matching acknowledgement within the supplied
context:

```go
if err := association.ASPUp(ctx); err != nil {
    return err
}
if err := association.ASPActive(ctx, asKey); err != nil {
    return err
}
```

An active ASP uses `Association.DestinationStateAudit` and optional
`Association.SignallingCongestion`. An SGP uses
`Endpoint.ReportDestinationAvailability` for DUNA, DAVA and DRST,
`Endpoint.SignallingCongestion` for SCON, and
`Endpoint.DestinationUserPartUnavailable` for DUPU; each records the shared state
a later RFC 4666 Section 4.5.3 audit is answered from and fans out to the
concerned active ASPs. Partial fan-out returns `*SSNMDeliveryError` with stable
successful and failed Association IDs.

### Consuming SSNM knowledge

`Endpoint.SubscribeSSNM` returns what the Endpoint currently knows together with
a subscription delivering every later change. The two are atomic with respect to
each other: no report is in both, and none is in neither, so there is no window
in which a change is lost between reading a snapshot and starting to listen.

```go
snapshot, subscription, err := endpoint.SubscribeSSNM()
if err != nil {
    return err
}
defer func() { _ = subscription.Close() }()

apply(snapshot) // The application's starting view, owned by the application.

for {
    event, err := subscription.Next(ctx)
    if err != nil {
        return err
    }
    if event.ContinuityLost {
        // The bounded queue refused this consumer's backlog. The stream is no
        // longer a complete history, so replace the view rather than patch it.
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

Events are deltas, and `applyDelta` patches the view rather than replacing it. A
report event's `Updated` holds only the destinations that report wrote, each
with both dimensions as retained afterwards, so it replaces those entries of
`event.Partition` and leaves every other destination alone. Binding events set
or remove `event.Binding`; `SSNMPartitionRetiredEvent` drops the partition and
`SSNMPartitionInvalidatedEvent` drops its destinations. A one-destination report
therefore costs the same however much the partition holds. The whole view comes
only from the snapshot, which `Resync` replaces.

Knowledge is owned by canonical identity, not by the label that carried it:
`event.Partition` names one Signalling Gateway and one Application Server, while
`event.Report.Scope` is the exact Network Appearance and Routing Context the
peer put on the wire. A `Routing Context` value on one Signalling Gateway means
something else on another, so a report learned through one SGP survives that
SGP's association and is never confused with a same-numbered scope elsewhere.

Every event's `Revision` is strictly greater than the snapshot's and strictly
increases, and `event.Epoch` is the binding generation, so knowledge from before
a source reset is never mistaken for knowledge after one.

The store is bounded in every dimension a peer can grow — records per partition,
per peer and per Endpoint, bytes, partitions, subscribers and queue depth — and
it **refuses** rather than evicting. `SSNMSnapshot.RecordsRefused`,
`ReportsRefused` and `PartitionsInvalidated` count what a bound refused, and
`LastResourceLoss` says what happened most recently. A subscription that falls
behind is told so with `ContinuityLost` instead of being handed a stream with a
silent hole in it. `Endpoint.SSNMKnowledge` returns the same snapshot without
opening a subscription.

### Management indications

`Association.ManagementIndications` reports M-NOTIFY, M-ERROR,
M-SCTP_RELEASE, and M-SCTP_RESTART. Each indication owns its slices and carries
its `AssociationID`, exact `ASKeys`, affected destination masks and scope, and
local cause where applicable. A full bounded queue closes the Association with
`ErrIndicationQueueFull` rather than silently losing a mandatory event.

See the [Endpoint management and SSNM design](./docs/design/endpoint-management-and-ssnm.md),
the [procedure coverage](./docs/procedure-coverage.md), and the
[v1.2 migration guide](./docs/migration-v1.2.md).

## Supported Features

### Messages

| Class    | Message                                         | Supported | Notes                                                          |
|----------|-------------------------------------------------|-----------|----------------------------------------------------------------|
| Transfer | Payload Data Message (DATA)                     | Yes       | [RFC4666#3.3](https://www.rfc-editor.org/rfc/rfc4666#section-3.3) |
| SSNM     | Destination Unavailable (DUNA)                  | Yes       | [RFC4666#3.4](https://www.rfc-editor.org/rfc/rfc4666#section-3.4) |
|          | Destination Available (DAVA)                    | Yes       |                                                                |
|          | Destination State Audit (DAUD)                  | Yes       |                                                                |
|          | Signalling Congestion (SCON)                    | Yes       |                                                                |
|          | Destination User Part Unavailable (DUPU)        | Yes       |                                                                |
|          | Destination Restricted (DRST)                   | Yes       |                                                                |
| ASPSM    | ASP Up                                          | Yes       | [RFC4666#3.5](https://www.rfc-editor.org/rfc/rfc4666#section-3.5) |
|          | ASP Up Acknowledgement (ASP Up Ack)             | Yes       |                                                                |
|          | ASP Down                                        | Yes       |                                                                |
|          | ASP Down Acknowledgement (ASP Down Ack)         | Yes       |                                                                |
|          | Heartbeat (BEAT)                                | Yes       |                                                                |
|          | Heartbeat Acknowledgement (BEAT Ack)            | Yes       |                                                                |
| RKM      | Registration Request (REG REQ)                  | Yes       | Strict codec and SGP/IPSP responder procedure per [RFC4666#3.6](https://www.rfc-editor.org/rfc/rfc4666#section-3.6) and [RFC4666#4.4](https://www.rfc-editor.org/rfc/rfc4666#section-4.4). |
|          | Registration Response (REG RSP)                 | Yes       | Split responses, partial results, replay, and ASP/IPSP correlation are covered. |
|          | Deregistration Request (DEREG REQ)              | Yes       | Multi-Routing-Context requests and active-AS rejection are covered. |
|          | Deregistration Response (DEREG RSP)             | Yes       | Split responses, status validation, replay, and scope removal are covered. |
| ASPTM    | ASP Active                                      | Yes       | [RFC4666#3.7](https://www.rfc-editor.org/rfc/rfc4666#section-3.7) |
|          | ASP Active Acknowledgement (ASP Active Ack)     | Yes       |                                                                |
|          | ASP Inactive                                    | Yes       |                                                                |
|          | ASP Inactive Acknowledgement (ASP Inactive Ack) | Yes       |                                                                |
| MGMT     | Error                                           | Yes       | [RFC4666#3.8](https://www.rfc-editor.org/rfc/rfc4666#section-3.8) |
|          | Notify                                          | Yes       |                                                                |

### Parameters

| Type          | Parameters                   | Supported | Notes |
|---------------|------------------------------|-----------|-------|
| Common        | INFO String                  | Yes       |       |
|               | Routing Context              | Yes       |       |
|               | Diagnostic Information       | Yes       |       |
|               | Heartbeat Data               | Yes       |       |
|               | Traffic Mode Type            | Yes       |       |
|               | Error Code                   | Yes       |       |
|               | Status                       | Yes       |       |
|               | ASP Identifier               | Yes       |       |
| M3UA-specific | Network Appearance           | Yes       |       |
|               | User/Cause                   | Yes       |       |
|               | Congestion Indications       | Yes       |       |
|               | Concerned Destination        | Yes       |       |
|               | Routing Key                  | Yes       |       |
|               | Registration Result          | Yes       |       |
|               | Deregistration Result        | Yes       |       |
|               | Local Routing Key Identifier | Yes       |       |
|               | Destination Point Code       | Yes       |       |
|               | Service Indicators           | Yes       |       |
|               | Originating Point Code List  | Yes       |       |
|               | Protocol Data                | Yes       |       |
|               | Registration Status          | Yes       |       |
|               | Deregistration Status        | Yes       |       |

## Compliance

This project targets RFC 4666 with current IANA SIGTRAN/SCTP assignments and
SCTP behavior from RFC 9260 where it affects the M3UA transport. See the
[standards and security contract](docs/standards.md), the
[RFC 4666 conformance matrix](docs/rfc4666-conformance.md), and the
[ecosystem audit](docs/compliance.md).

The v1.2 API intentionally uses RFC entity and primitive names. See the
[v1.2 migration guide](docs/migration-v1.2.md) for the breaking role,
configuration, I/O, destination, restart and Routing Key Management changes.

- [Migrating to v1.2](docs/migration-v1.2.md)
- [Procedure coverage and optional-procedure exclusions](docs/procedure-coverage.md)
- [v1.2.0 release notes](docs/release-v1.2.0.md)

## LICENSE

[MIT](https://github.com/gomaja/go-m3ua/blob/main/LICENSE)
