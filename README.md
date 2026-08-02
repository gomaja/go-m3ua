# go-m3ua

Simple M3UA protocol implementation in the Go programming language.

[![CI status](https://github.com/gomaja/go-m3ua/actions/workflows/go.yml/badge.svg)](https://github.com/gomaja/go-m3ua/actions/workflows/go.yml)
[![golangci-lint](https://github.com/gomaja/go-m3ua/actions/workflows/golangci-lint.yml/badge.svg)](https://github.com/gomaja/go-m3ua/actions/workflows/golangci-lint.yml)
[![Go Reference](https://pkg.go.dev/badge/github.com/gomaja/go-m3ua.svg)](https://pkg.go.dev/github.com/gomaja/go-m3ua)
[![GitHub](https://img.shields.io/github/license/mashape/apistatus.svg)](https://github.com/gomaja/go-m3ua/blob/main/LICENSE)

## Quickstart

### Installation

Run `go mod tidy` in your project's directory to collect the required packages automatically.

_This project follows [the Release Policy of Go](https://go.dev/doc/devel/release#policy)._

_Full SCTP socket validation runs on Linux. Non-Linux systems can build and run
non-socket tests, but production M3UA associations require OS SCTP support._

### Trying Examples

Working examples are available in [examples directory](./examples/).
Just executing the following commands, you can see the client and server setting up M3UA connection.

```shell-session
# Run Server first
cd examples/server
go run m3ua-server.go

// Run Client then
cd examples/client
go run m3ua-client.go
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

The API design is kept as similar as possible to other protocols in standard `net` package. To establish M3UA connection as client/server, you can use `Dial()` and `Listen()`/`Accept()` without caring about the underlying SCTP association, as go-m3ua handles it together with M3UA ASPSM & ASPTM procedures.

Here is an example to develop your own M3UA client using go-m3ua.

First, you need to create `*Config` used to setup/maintain M3UA connection.

```go
config := m3ua.NewClientConfig(
    &m3ua.HeartbeatInfo{
        Enabled:  true,
        Interval: time.Duration(3 * time.Second),
        Timer:    time.Duration(10 * time.Second),
    },
    0x11111111, // OriginatingPointCode
    0x22222222, // DestinationPointCode
    1,          // AspIdentifier
    params.TrafficModeLoadshare, // TrafficModeType
    0,                     // NetworkAppearance
    0,                     // CorrelationID
    []uint32{1, 2},        // RoutingContexts
    params.ServiceIndSCCP, // ServiceIndicator
    0, // NetworkIndicator
    0, // MessagePriority
    1, // SignalingLinkSelection
)
// set nil on unnecessary paramters.
config.CorrelationID = nil
```

RFC-strict parsing is the default. If a known peer sends an optional INFO String
that is not valid UTF-8, enable the explicit compatibility policy for that peer:

```go
config.Compatibility = m3ua.AcceptInvalidOptionalInfoString()
```

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

Then, prepare network addresses and context and try to connect with `Dial()`.

```go
// setup SCTP peer on the specified IPs and Port.
raddr, err := sctp.ResolveSCTPAddr("sctp", SERVER_IPS)
if err != nil {
    log.Fatal(err)
}

ctx := context.Background()
ctx, cancel := context.WithCancel(ctx)
defer cancel()

conn, err := m3ua.Dial(ctx, "m3ua", nil, raddr, config)
if err != nil {
    log.Fatalf("Failed to dial M3UA: %s", err)
}
defer conn.Close()
```

Now you can `Read()` / `Write()` data from/to the remote endpoint.

```go
if _, err := conn.Write(d); err != nil {
    log.Fatalf("Failed to write M3UA data: %s", err)
}
log.Printf("Successfully sent M3UA data: %x", d)

buf := make([]byte, 1500)
n, err := conn.Read(buf)
if err != nil {
    log.Fatal(err)
}

log.Printf("Successfully read M3UA data: %x", buf[:n])
```

See [example/server directory](./examples/server) for server example.

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

This project targets RFC 4666 with current IANA SIGTRAN/SCTP assignments and SCTP behavior from RFC 9260 where it affects the M3UA transport. See [docs/compliance.md](docs/compliance.md) for the current audit notes.

The module is still pre-v1. Some exported APIs may change before v1.0.0.

## LICENSE

[MIT](https://github.com/gomaja/go-m3ua/blob/main/LICENSE)
