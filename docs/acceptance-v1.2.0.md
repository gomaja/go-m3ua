# v1.2.0 acceptance status

The v1.2.0 release exists. Publication does not establish that every planned
performance and interoperability acceptance criterion has passed.

## Scope of existing evidence

The original cross-slice campaign assessed candidate
`64113c7ce1edf971d5ace9b8204b398ba430b552` against measurable baseline
`35647ff67768b933d9d83b65146390cc7643bd4f`, using go-sctp v1.0.4.
Those results do not validate subsequent dependency or implementation changes.

Existing build, unit, race, fuzz and integration results support the scenarios
they actually exercise. Passing those checks is not a completeness claim for
all roles, configurations, failure conditions or performance workloads.

## Outstanding acceptance

Follow-up work is tracked in [#35](https://github.com/gomaja/go-m3ua/issues/35)
and [#44](https://github.com/gomaja/go-m3ua/issues/44). It includes:

- corrected traffic-scope resolution, SSNM activation ownership and bounded
  restart-state retention, with negative and concurrent regression coverage;
- capacity evidence that rejects mismatched workloads, rates and provenance;
- sustainable, loss-free capacity with demonstrated stable backlog, including
  routing, SSNM load, subscriber pressure and configuration churn;
- the complete matched-load CPU-efficiency, latency, allocation and retained
  memory acceptance matrix; and
- interoperability with an independently implemented peer.

Earlier same-implementation integration runs are not independent-peer
interoperability evidence. Diagnostic allocation measurements do not establish
whole-path acceptance. A partial CPU or latency workload cannot stand in for
the full required matrix.

Unmeasured or inconclusive criteria remain open. Acceptance thresholds must not
be relaxed to fit observations, and each corrective change requires validation
at its exact revision before its corresponding acceptance claim is closed.

## Public integration contract

See the [migration guide](migration-v1.2.md), [release notes](release-v1.2.0.md)
and [conformance matrix](rfc4666-conformance.md) for API and protocol scope.
Applications remain responsible for their deployment security, routing policy
and operational qualification. No performance figure from one environment is
a universal capacity guarantee.
