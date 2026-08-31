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
config := m3ua.NewAssociationConfig(
    0x111111, // OriginatingPointCode
    0x222222, // DestinationPointCode
    params.ServiceIndSCCP, // ServiceIndicator
    0,                     // NetworkIndicator
    0,                     // MessagePriority
    1,                     // SignallingLinkSelection
)
config.
    EnableHeartbeat(3*time.Second, 10*time.Second).
    SetTrafficModeType(params.TrafficModeLoadshare).
    SetNetworkAppearance(7).
    SetRoutingContexts(1)
config.SetASPIdentifier(1) // ASP-only
```

An IPSP Association must select an RFC 4666 Section 4.3 exchange model
explicitly. Single Exchange and Double Exchange are supported. The ASPSM and
ASPTM initiators are independent because RFC 4666 permits either IPSP to
initiate either exchange:

```go
ipsp, err := m3ua.NewEndpoint(m3ua.EndpointConfig{Role: m3ua.RoleIPSP})
if err != nil {
    log.Fatal(err)
}

config.IPSP = &m3ua.IPSPConfig{
    ExchangeModel: m3ua.IPSPExchangeSingle,
    InitiateASPSM: true,
    InitiateASPTM: false,
}
```

Double Exchange gives each direction of data traffic its own Routing Key,
Network Appearance, Traffic Mode, and ASP/IPSP state as required by RFC 4666
Sections 4.3 and 5.6.2:

```go
config.IPSP = &m3ua.IPSPConfig{
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

With the normal `IPSPASPSMExchangeDouble` procedure, `InitiateASPSM` requires
`TrafficToLocal`, because that ASP Up establishes the direction in which the
peer sends DATA to the local IPSP. The agreed
`IPSPASPSMExchangeSingle` simplification may establish both directions with
one ASP Up exchange. `InitiateASPTM` always requires `TrafficToLocal`.

`TrafficToLocal` is the traffic the peer sends to this IPSP after this IPSP's
ASP Up/ASP Active procedures succeed. `TrafficToPeer` is the traffic this IPSP
sends after the peer's ASP Up/ASP Active procedures succeed. A non-nil traffic
direction with no `RoutingContexts` represents a configured contextless AS; a
nil direction is disabled. `Association.IPSPState()` reports both directions.

`IPSPASPSMExchangeDouble` is the normal independent ASPSM exchange.
`IPSPASPSMExchangeSingle` enables only the agreed ASPSM simplification described
by RFC 4666 Section 4.3; ASPTM and DATA remain independently directional.
See the [Double Exchange design](./docs/design/ipsp-double-exchange.md).

`InitiateASPSM` and `InitiateASPTM` do not describe SCTP initiation. The same
IPSP configuration works with `Dial` or with `Listen`/`Accept`; the remote IPSP
uses its own Association policy. At least one IPSP must initiate each required
exchange; both may initiate, and simultaneous exchanges are supported.

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

Create an ASP Endpoint with its local MTP Route and provisioned SG/SGP route. The
Routing Context and Network Appearance are peer-specific `ASKey` values; they
are not global route identifiers:

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
        SignallingGatewaySelection: m3ua.RouteSelectionPrimaryBackup,
        MTPRoutes: []m3ua.MTPRouteConfig{{
            ID: "sccp",
            DestinationPointCode: 0x220000,
            Mask: 16,
            ServiceIndicators: []uint8{params.ServiceIndSCCP},
        }},
        SignallingGateways: []m3ua.SignallingGatewayConfig{{
            ID: peer.SignallingGateway,
            SGPSelection: m3ua.RouteSelectionPrimaryBackup,
            SGPs: []m3ua.SignallingGatewayProcessConfig{{
                ID: peer.SignallingGatewayProcess,
                Routes: []m3ua.SGPRoute{{MTPRoute: "sccp", AS: asKey}},
            }},
        }},
    },
})
if err != nil {
    log.Fatal(err)
}

config.PeerSGP = &peer
remote, err := sctp.ResolveSCTPAddr("sctp", PEER_ADDRESS)
if err != nil {
    log.Fatal(err)
}

ctx := context.Background()
ctx, cancel := context.WithCancel(ctx)
defer cancel()

association, err := endpoint.Dial(ctx, "m3ua", nil, remote, config)
if err != nil {
    log.Fatalf("Failed to establish M3UA association: %s", err)
}
defer func() { _ = association.Close() }()
```

For an ASP with provisioned routes, submit the RFC 4666 MTP-TRANSFER request to
the Endpoint. It resolves the MTP Route, aggregates route state across SGs,
selects the SGP and Association, applies that peer's `ASKey`, and derives the
SCTP stream from the Protocol Data SLS:

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
log.Printf("sent through %d Association(s)", result.TransmittedAssociations)
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

Association-level reads remain the DATA receive API:

```go

buf := make([]byte, m3ua.DefaultReadBufferSize)
n, err := association.Read(buf)
if err != nil {
    log.Fatal(err)
}

log.Printf("Successfully read M3UA data: %x", buf[:n])
```

See the [SGP example](./examples/sgp) for accepting SCTP associations.

An endpoint that accepts SCTP associations uses `ListenerConfig` to select a
separate immutable `AssociationConfig` per association before M3UA parsing:

```go
listenerConfig := m3ua.NewListenerConfig(defaultAssociationConfig)
listenerConfig.SelectAssociationConfig = func(info m3ua.AcceptInfo) (*m3ua.AssociationConfig, error) {
    return configForPeer(info.RemoteAddr)
}

endpoint, err := m3ua.NewEndpoint(m3ua.EndpointConfig{Role: m3ua.RoleSGP})
if err != nil {
    log.Fatal(err)
}
listener, err := endpoint.Listen("m3ua", local, listenerConfig)
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

_, err = association.DeregisterRoutingContexts(ctx, results[0].RoutingContext)
```

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
`DeregisterRoutingContexts` returns `ErrDeregistrationOutcomeUnknown` because
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
`Endpoint.SignallingCongestion` and `Endpoint.DestinationUserPartUnavailable`
to update shared state and fan out to the concerned active ASPs. Partial fan-out
returns `*SSNMDeliveryError` with stable successful and failed Association IDs.

`Association.ManagementIndications` reports M-NOTIFY, M-ERROR,
M-SCTP_RELEASE, and M-SCTP_RESTART. Each indication owns its slices and carries
its `AssociationID`, exact `ASKeys`, affected destination masks and scope, and
local cause where applicable. A full bounded queue closes the Association with
`ErrIndicationQueueFull` rather than silently losing a mandatory event.

See the [Endpoint management and SSNM design](./docs/design/endpoint-management-and-ssnm.md)
and [v1.2 migration guide](./docs/migration-v1.2.md).

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
configuration, and ASP routing changes.

## LICENSE

[MIT](https://github.com/gomaja/go-m3ua/blob/main/LICENSE)
