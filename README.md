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
explicitly. Single Exchange is supported. The ASPSM and ASPTM initiators are
independent because RFC 4666 permits either IPSP to initiate either exchange:

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
| RKM      | Registration Request (REG REQ)                  | No        | Dynamic RKM is not implemented; RKM is answered with Unsupported Message Class per [RFC4666#4.4.1](https://www.rfc-editor.org/rfc/rfc4666#section-4.4.1). |
|          | Registration Response (REG RSP)                 | No        | Same as above. |
|          | Deregistration Request (DEREG REQ)              | No        | Same as above. |
|          | Deregistration Response (DEREG RSP)             | No        | Same as above. |
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
