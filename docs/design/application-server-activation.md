# Application Server Activation Policy

RFC 4666 Sections 1.4.4.1, 3.8.2, 4.3.2, 4.3.4.3, 4.3.4.5,
5.1.4, and 5.2.3 define the n+k Application Server redundancy model. SCTP
association initiation is independent of this policy. The policy applies to
the Application Servers maintained by an SGP or IPSP Endpoint.

## Immutable Endpoint policy

`EndpointConfig.ApplicationServers` owns Application Server policy shared by
all Associations of an SGP or IPSP Endpoint:

```go
type ASActivationPolicy struct {
    RequiredActiveASPs int
    SmoothStart        bool
}

type ApplicationServerConfig struct {
    RecoveryTimer            time.Duration
    DefaultActivationPolicy ASActivationPolicy
    ActivationPolicies      map[ASKey]ASActivationPolicy
}
```

`RequiredActiveASPs` is n. Zero selects one, preserving the RFC 4666 1+0 and
1+1 defaults. Negative values are invalid. `SmoothStart` explicitly enables
the exception in Section 4.3.2 that lets the first active ASP start traffic
before n ASPs are active.

An exact `ASKey` policy overrides the default. Network Appearance presence and
Routing Context presence are part of the key, so equal numeric Routing
Contexts in different Network Appearances remain independent. A contextless
Application Server uses `RoutingContextSet=false`. Non-zero values whose
corresponding presence flag is false are invalid rather than silently
canonicalized.

The configuration and its map are deeply snapshotted by `NewEndpoint`. It is
valid for SGP and IPSP roles and rejected for an ASP role. `RecoveryTimer`
moves from `SGPConfig` because T(r) belongs to Application Server state and
RFC 4666 applies the same state procedures to an IPSP.

## State and traffic gate

RFC 4666 Section 4.3.2 requires this state behavior:

| Previous state | Active ASPs | Result |
| --- | ---: | --- |
| AS-DOWN or AS-INACTIVE | one to n-1 | AS-INACTIVE |
| AS-INACTIVE | n or more | AS-ACTIVE |
| AS-INACTIVE with smooth start | one or more | AS-ACTIVE |
| AS-ACTIVE | one or more | AS-ACTIVE |
| AS-ACTIVE | zero | AS-PENDING and start T(r) |
| AS-PENDING | one or more | AS-ACTIVE and stop T(r) |
| AS-PENDING | zero | AS-PENDING until T(r) expires |

Override Traffic Mode always requires n=1. A configured or registered
Override mode for an Application Server whose effective n is not one is
rejected before SCTP setup when it is local Association policy, or as an
unsupported traffic-handling mode when proposed by a peer or Registration
Request. No rejected combination changes live AS policy.

Individual ASP-ACTIVE membership is recorded immediately after ASP Active Ack,
but DATA and SSNM distribution are unavailable until the Application Server is
AS-ACTIVE. This distinction is required during strict startup, where fewer
than n ASPs have completed ASP Active but none may yet receive traffic.

## Resource notifications

Once an AS is carrying traffic, fewer than n active ASPs in Loadshare or
Broadcast mode means resources are insufficient. Override never reports this
condition.

The SGP or IPSP sends `Notify(Insufficient ASP Resources Active in AS)` only to
ASP-INACTIVE members, as required by RFC 4666 Section 3.8.2. Each inactive ASP
is notified once per continuous shortage. An ASP that becomes active is
removed from the notified set, so a later withdrawal can be reported again.

When the active count returns to n, the peer sends `Notify(AS-ACTIVE)` to every
member except ASP-DOWN, even though the AS remained AS-ACTIVE throughout the
shortage. This is the restoration sequence shown by RFC 4666 Section 5.2.3.

Strict startup below n does not report insufficient resources because the AS
is not yet handling traffic. Smooth start and recovery from AS-PENDING do
report a shortage when they activate the AS below n.

All state, shortage, and restoration notifications use the existing ordered
per-AS event queue. The event is released only after the related ASP Active,
ASP Inactive, ASP Up, or ASP Down acknowledgement, satisfying RFC 4666 Section
4.3.4.5. Duplicate state messages and no-op state publications create no new
notification event.

## Dynamic Routing Key Management

An Application Server created by Registration inherits the deeply snapshotted
policy selected by its exact `ASKey`. A REG REQ that proposes Override for an
effective n other than one returns `Unsupported Traffic Handling Mode` and
does not publish a Routing Key or mutate the live AS.

## Validation

The implementation is verified at four boundaries:

- configuration defaults, exact-key overrides, role checks, invalid values,
  and post-construction caller mutation;
- AS state transitions for strict startup, smooth start, withdrawal,
  restoration, pending recovery, and Override rejection;
- ordered wire-message construction and target scoping for state and resource
  notifications; and
- concurrent activation, withdrawal, registration, distribution, and Endpoint
  close under the race detector.
