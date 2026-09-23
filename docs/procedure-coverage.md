# Procedure coverage

Audit date: 2026-09-19.

This document answers two questions the
[conformance matrix](rfc4666-conformance.md) does not: for each M3UA procedure,
**which roles, directions and states are actually exercised**, and **which
optional procedures are supported as procedures rather than only as codecs**.

Codec support is not procedure completion. A message this library can build and
parse but never originates, or never acts on when it arrives, is a codec and is
listed as such below. Every claim here names the test functions that demonstrate
it; `TestEveryProcedureCoverageCitationExists` in `export_inventory_test.go`
fails if a name in this file stops existing, so the citations cannot rot
silently.

Role shorthand: **ASP**, **SGP**, **SE** = IPSP Single Exchange, **DE** = IPSP
Double Exchange. "Local" is a procedure this node originates; "peer" is one it
receives and acts on.

Socket-backed tests skip themselves where the operating system has no SCTP, so
the integration rows are Linux evidence. Everything else runs anywhere.

## 1. DATA — RFC 4666 Sections 3.3 and 4.1

| Aspect | Roles | Direction | States | Evidence |
| --- | --- | --- | --- | --- |
| Typed send | ASP, SGP, SE, DE | local | ASP-ACTIVE only | `TestConcurrentWriteDataCarriesEachMessagesOwnLabelAndScope`, `TestWriteDataReturnsThePayloadLength`, `TestTypedDataCrossesBothSCTPInitiationDirections` |
| Typed receive | ASP, SGP, SE, DE | peer | ASP-ACTIVE, per Routing Context | `TestReadDataDeliversThePayloadWhole`, `TestReadDataDeliversEachMessageToExactlyOneReader`, `TestReceivedDataHonoursPerRoutingContextActivation` |
| Per-message scope and routing label | ASP, SGP, DE | both | ASP-ACTIVE | `TestReadDataReportsTheReceivedScopeStreamAndEpoch`, `TestReadDataReturnsCallerOwnedData` |
| Stream selection from SLS; stream 0 refused | ASP, SGP, SE | both | any | `TestStreamSelectionHandlesEveryNegotiatedCount`, `TestWriteSignalDoesNotPutDataOnStreamZero`, `TestDataOnStreamZeroIsRejected`, `TestMTPTransferSelectsAvailableSGPAndSLSStream` |
| Partial and failed submission classified | ASP, SGP | local | ASP-ACTIVE | `TestWriteDataClassifiesPartialSubmissionAsIndeterminate`, `TestBroadcastPartialWriteKeepsSynchronizationPending` |
| Inbound queue overflow | ASP, SGP | peer | ASP-ACTIVE | `TestDataQueueOverloadIsObservable`, `TestFullDataQueueStillAnswersSignalling`, `TestDataQueueRecoversAfterOverflow`, `TestLocalCongestionTellsThePeerWithSCON` |
| Admission and authorization | ASP, SGP, SE, DE | local | INACTIVE→ACTIVE barrier | `TestScopedActivationAndDeactivationAPIsArmTAckBeforeWriting` |
| Contextless Application Server | ASP, SGP, SE, DE | both | ASP-ACTIVE | `TestEncodedDataMatchesTheCodec` (scope), and the RKM and IPSP contextless rows below |
| Epoch across SCTP restart | ASP, SGP | peer, local | any | `TestReadDataReportsTheEpochTheMessageArrivedIn`, `TestBroadcastActivationDropsThePreviousFlowEpoch` |

What the library checks on a direct `WriteData`, in order: association state
(Section 4.3.1 ASP-ACTIVE), message structure, the Section 1.4.7 stream
constraints, and the Application Server binding with its activation. Destination
availability is deliberately **not** checked — see
[Migrating to v1.2](migration-v1.2.md#what-the-library-checks-and-what-stays-yours).

## 2. ASPSM — RFC 4666 Sections 3.5, 4.3.4.1, 4.3.4.2 and 4.3.4.6

| Procedure | Roles | Direction | Evidence |
| --- | --- | --- | --- |
| ASP Up sent, acknowledgement awaited | ASP, SE, DE | local | `TestAssociationASPUpWaitsForAcknowledgement`, `TestASPAsksForTrafficAfterAspUpAck` |
| ASP Up received, Ack sent | SGP, SE, DE | peer | `TestDuplicateAspUpIsAcked`, `TestSGPRecoversAfterDuplicateAspUp` |
| ASP Up Ack received, including unsolicited | ASP, SE, DE | peer | `TestSGPHoldsStateOnUnsolicitedAspUpAck` |
| ASP Down sent and retransmitted | ASP, SE, DE | local | `TestShutdownRetransmitsAspDownUntilAcked` |
| ASP Down received, Ack always sent | SGP, SE, DE | peer | `TestHandleAspDownAlwaysAcks` |
| ASP Down Ack received, solicited and unsolicited | ASP, SE, DE | peer | `TestSolicitedAspDownAckIsAccepted`, `TestUnsolicitedAspDownAckReturnsTheASPToItsPreviousState` |
| BEAT sent on a live association | ASP, SGP, SE | local | `TestHeartbeatKeepsAssociationAlive` |
| BEAT received, Ack unconditional | all | peer | `TestHeartbeatAckedInEveryState`, `TestHeartbeatAckPreservesExtensionParametersFromWire` |
| T(ack) retransmission, budget and expiry | ASP, SE, DE | local | `TestAssociationASPUpReportsTAckExpiry` |
| One state published per message | all | both | `TestExactlyOneStatePublishedPerMessage` |

An SGP never runs a T(ack) procedure: RFC 4666 makes the ASP or IPSP the
initiator of every ASPSM and ASPTM request.

## 3. ASPTM — RFC 4666 Sections 3.7, 4.3.4.3 and 4.3.4.4

| Aspect | Roles | Direction | Evidence |
| --- | --- | --- | --- |
| ASP Active sent, Ack awaited | ASP, SE, DE | local | `TestScopedActivationAndDeactivationAPIsArmTAckBeforeWriting` |
| ASP Active received, Ack sent; refused from ASP-DOWN | SGP, SE, DE | peer | `TestRejectedAspActiveAckDoesNotStorm` |
| **Partial activation** — some Routing Contexts acknowledged, the rest refused | SGP, SE, DE responding; ASP requesting | both | `TestMixedRoutingContextAcksTheServedOnesAndRefusesTheRest`, `TestWhollyUnservedRoutingContextGetsNoAck` |
| ASP Inactive, scoped withdrawal, traffic quiesced before the Ack | ASP, SGP, SE, DE | both | `TestASPInactiveAckWaitsUntilScopedTrafficIsHalted` |
| Override displaces the incumbent | SGP, SE, DE | peer | `TestFigure4TransitionsCellByCell`, `TestTrafficModeCanBeConfiguredPerApplicationServer` |
| Loadshare and Broadcast add without displacing | SGP | peer, local | `TestBroadcastActivationDropsThePreviousFlowEpoch` |
| Traffic mode negotiation and rejection | all | both | `TestAspActiveWithIncompatibleTrafficModeIsRefused`, `TestAspActiveWithoutTrafficModeIsAccepted`, `TestTrafficModeTypeOutsideTheDefinedValuesIsRejected`, `TestASPRejectsActiveAckWithWrongRequestedTrafficMode`, `TestDynamicTrafficModeIsEchoedAndCannotChangeAfterAgreement` |
| n+k activation, strict startup, smooth start, T(r) | SGP, SE | local policy | `TestRecoveryTimerResolvesPending`, `TestASPActiveBeforeRecoveryTimerRestoresActive`, `TestNotifyIsSentOnASStateChange` |

## 4. SSNM — RFC 4666 Sections 3.4 and 4.5

| Message | Directions implemented | Evidence for origination | Evidence for reception |
| --- | --- | --- | --- |
| DUNA | SGP originates, ASP acts | `TestEndpointAvailabilityPublicationUsesTheExactRequestedScope`, `TestEndpointPublicationSpansEveryChildWithoutCrossSignalling` | `TestDUNAMarksDestinationUnavailable` |
| DAVA | SGP originates, ASP acts | `TestEndpointAvailabilityAndCongestionMoveIndependently` | `TestDAVARestoresDestination`, `TestDAVAAcceptedOnlyDuringPendingActivationAndDUPURejected` |
| DRST | SGP originates, ASP acts | `TestEndpointPublicationSplitsRoutingContextsAnIsolationSeparates` | `TestDRSTMarksDestinationRestricted` |
| DAUD | ASP originates, SGP answers | `TestSSNMOperationAssociationWire`, `TestSSNMOperationValidation` | `TestDAUDIsAnsweredFromDestinationState`, `TestDAUDAnswersEveryAuditedPointCode`, `TestDAUDForAnUnknownPointCodeIsAnsweredWithDUNA`, `TestDAUDForAKnownAvailablePointCodeIsAnsweredWithDAVA`, `TestDestinationAuditOfACongestedUnavailableDestinationReportsOnlyDUNA`, `TestSSNMOperationDAUDRetainsCongestionLevel` |
| SCON, SGP to ASP | SGP originates, ASP acts | `TestSSNMOperationEndpointFanout`, `TestSSNMOperationEndpointCongestionPresenceAndAbatement` | `TestSCONReportsCongestion`, `TestSCONFromAnSGPDoesUpdateTheDestination` |
| **SCON, ASP to SGP (optional)** | ASP originates, SGP acts | `TestLocalCongestionTellsThePeerWithSCON`, `TestSSNMOperationAssociationWire` | `TestSCONFromAnASPIsAcceptedAtAnSGP`, `TestSCONFromAnASPDoesNotRewriteTheSGsRoutingState` |
| DUPU | SGP originates, ASP acts | `TestSSNMOperationEndpointFanout`, `TestDUPUWithASingleUnmaskedPointCodeIsAccepted`, `TestDUPUWithAMaskOrSeveralPointCodesIsRejected` | `TestDUPULeavesDestinationReachable` |

The route-independent SSNM store and its subscription — snapshot atomicity,
`Next`, `Resync`, bounded refusal and continuity loss, binding lifecycle and
epochs — are covered by `ssnm_subscription_test.go`, `ssnm_state_test.go` and
`ssnm_lifecycle_test.go`, including
`TestSSNMStateSourceResetAndFreshReactivationStartANewEpoch`.

## 5. MGMT — RFC 4666 Section 3.8

Every Error code RFC 4666 Section 3.8.1 assigns is decoded and named on receipt.
The table below is about **generation**: which codes this library ever puts on
the wire.

| Code | Name | Generated | Evidence |
| --- | --- | --- | --- |
| 0x01 | Invalid Version | yes | version negotiation suite in `errorgen_test.go` |
| 0x03 | Unsupported Message Class | yes | `conformance_test.go` |
| 0x04 | Unsupported Message Type | yes | `conformance_test.go` |
| 0x05 | Unsupported Traffic Handling Mode | yes | `aspsm_test.go` |
| 0x06 | Unexpected Message | yes | `errorgen_test.go` |
| 0x07 | Protocol Error | yes | `errorgen_test.go` |
| 0x09 | Invalid Stream Identifier | yes | `recvstream_test.go` |
| 0x0D | Refused – Management Blocking | yes | `nif_test.go` |
| **0x0E** | **ASP Identifier Required** | **no** | see exclusions |
| 0x0F | Invalid ASP Identifier | yes | `peeraspid_test.go` |
| 0x11 | Invalid Parameter Value | yes | `errorgen_test.go` |
| 0x12 | Parameter Field Error | yes | `conformance_test.go` |
| 0x13 | Unexpected Parameter | yes | `conformance_test.go` |
| **0x14** | **Destination Status Unknown** | **no** | see exclusions |
| 0x15 | Invalid Network Appearance | yes | `datacontext_test.go` |
| 0x16 | Missing Parameter | yes | `errorgen_test.go` |
| 0x19 | Invalid Routing Context | yes | `errorgen_test.go`, `notify_scope_test.go` |
| 0x1A | No Configured AS for ASP | yes | `routingcontext_test.go` |

An Error is never sent in response to an Error, as Section 3.8.1 requires.

Notify covers AS-INACTIVE, AS-ACTIVE, AS-PENDING, Insufficient ASP Resources
Active in AS, Alternate ASP Active and ASP Failure, in both directions, with the
Routing Context scoping described under
[errata 2065](standards.md#rfc-4666-errata): `TestNotifyIsSentOnASStateChange`
and the `notify_scope_test.go` suite.

## 6. RKM — RFC 4666 Sections 3.6 and 4.4 (optional)

Both sides of all four messages are implemented.

| Aspect | Roles | Evidence |
| --- | --- | --- |
| REG REQ and REG RSP, requester | ASP, SE, DE | `TestRKMRequesterCollectsSplitResponses` |
| REG REQ and REG RSP, responder | SGP, SE, DE | `rkm_registry_test.go` |
| DEREG REQ and DEREG RSP, both sides | ASP, SGP, SE, DE | `rkm_procedure_test.go` |
| Split and partial results | both | `TestRKMRequesterCollectsSplitResponses` |
| Deterministic replay of duplicate peer requests | responder | `rkm_procedure_test.go` |
| Authorization, allocation and collision checking | SGP, SE | `rkm_registry_test.go`, `rkm_policy_test.go` |
| Refused when RKM is not configured, or in the wrong direction | SGP, ASP | `rkm_test.go` |
| Bounded unresolved outcomes | requester | `rkm_procedure_test.go` |

RFC 4666 defines no RKM acknowledgement timer, so none is invented: a local wait
is bounded by the caller's context, and a responder answers a retransmitted
request from deterministic replay state.

## 7. Timers and retransmission

| Timer | Configuration | Evidence |
| --- | --- | --- |
| T(ack) and its retry budget | `AssociationConfig.TAck`, `TAckRetries` | `TestAssociationASPUpReportsTAckExpiry`, `TestShutdownRetransmitsAspDownUntilAcked` |
| T(beat) | `HeartbeatInfo` | `TestHeartbeatKeepsAssociationAlive` |
| T(r), the AS recovery timer | `ApplicationServerConfig.RecoveryTimer` | `TestRecoveryTimerResolvesPending`, `TestASPActiveBeforeRecoveryTimerRestoresActive` |
| M3UA handshake budget | `AssociationConfig.EstablishTimeout` | `TestEstablishTimeoutBoundsTheM3UAHandshake` |
| One SCTP association attempt | `SCTPConfig.InitTimeout` | `dial_test.go` |
| Send-buffer wait for messages the library sends itself | `AssociationConfig.ControlWriteTimeout` | `TestLibraryRepliesWaitForAStalledPeerToResume`, `TestStalledPeerClosesTheAssociationAfterControlWriteTimeout`, `TestSSNMPublicationWaitsOutAFullSendBuffer` |

A full SCTP send buffer is backpressure, not a failure. Acknowledgements, BEAT
Ack, Error, Notify, destination state replies and publications, registration
responses and ASP procedure requests wait for buffer space instead of closing
the association on the transport's EAGAIN. The wait is bounded, because a
waiting write holds the socket and whatever ordering barrier its caller holds.
The default of 5 s covers two consecutive T3-rtx expiries from RTO.Min (RFC 9260
Sections 6.3.3 and 16). Writes the application asks for, such as `WriteData`
and `WriteSignal`, still report a full buffer to the caller.

T(ack) retransmission is bounded rather than unbounded. RFC 4666 says to resend
"until it receives an ASP Up Ack message", and the equivalent for the other three
requests; a budget turns a peer that will never answer into a reportable failure
instead of indefinite load on a network already in trouble.

## 8. Unexpected messages

| Case | Evidence |
| --- | --- |
| Unknown message class | `conformance_test.go` |
| Unknown message type within a known class | `conformance_test.go` |
| Unsupported version | `errorgen_test.go` |
| Message valid but wrong for the current state | `statemachine_test.go`, `TestDataOnStreamZeroIsRejected` |
| Message valid but wrong for this node's role | `TestSGPHoldsStateOnUnsolicitedAspUpAck`, `endpoint_role_test.go` |
| Truncated, unparseable, or bare-header message | `conformance_test.go` |
| Message on a stream it may not use | `recvstream_test.go` |
| Wrong SCTP Payload Protocol Identifier | `ppid_test.go` |

## 9. Restart, shutdown and recovery

| Aspect | Roles | Evidence |
| --- | --- | --- |
| MTP3 restart, Section 4.6 | SGP | `mtp3restart_test.go`, `sgp_publication_test.go` |
| SCTP restart and re-establishment | ASP, SGP, SE, DE | `restart_test.go`, `TestSSNMStateSourceResetAndFreshReactivationStartANewEpoch` |
| NIF unavailable, Section 4.7 | SGP | `nif_test.go`, `nif_quiescence_test.go`, `nif_askey_test.go` |
| Graceful ASP withdrawal, Sections 4.9 and 5.3 | ASP, SE, DE | `shutdown_test.go` |
| Association, Listener and Endpoint close scopes | all | `TestClosingOneSGPAssociationKeepsEndpointAndSiblingAlive`, `TestClosedAssociationIsForgottenByItsListener`, `endpoint_role_test.go` |
| Concurrent accepts stay independent | SGP | `TestConcurrentAcceptsAreIndependent` |

## 10. Multi-SG ASP routing

| Aspect | Evidence |
| --- | --- |
| Path, SGP, Association and stream selection | `TestMTPTransferSelectsAvailableSGPAndSLSStream`, `mtp_transfer_test.go` |
| Alternate-path recovery and failback | `mtp_transfer_test.go`, `mtp_routing_projection_test.go`, `asp_routes_test.go` |
| Derived MTP-PAUSE, MTP-RESUME and MTP-STATUS | `mtp_indication_test.go` |
| Per-Signalling-Gateway SSNM partitioning | `ssnm_lifecycle_test.go`, `asp_routes_test.go` |
| Both SCTP initiation orientations, on live sockets | `asp_multi_sg_integration_test.go` |

## Optional procedures: supported

Each of these is optional in RFC 4666 and is implemented as a procedure, not
only as a codec.

| Optional capability | Section | Notes |
| --- | --- | --- |
| Routing Key Management | 1.4.2.3, 3.6, 4.4 | Both sides, all four messages, static and dynamic keys in one collision-checked registry |
| ASP destination auditing with DAUD | 4.5.3 | The ASP originates it; the SGP answers available, restricted, congested, unavailable and unknown from retained state |
| M3UA heartbeat, BEAT and BEAT Ack | 4.3.4.6 | Either role, independent of SCTP HEARTBEAT path management |
| SCON from an ASP to an SGP | 3.4.4 | Including Concerned Destination, and deliberately kept out of the SG's own destination state |
| Correlation Id | 3.3.1 | Per message, on every DATA path, with presence distinguished from zero |
| Network Appearance | 3.3.1, 3.4 | Per Application Server, with presence distinguished from zero |
| Contextless Application Server | 3.6.1 | ASP, SGP and both IPSP exchange models |
| Destination Restricted | 3.4.6 | Sent and acted on |
| Override, Loadshare, Broadcast and n+k redundancy | 1.4.4, Appendix A | Configurable per Application Server |
| Message Priority congestion policy | 1.4.6 | A caller-supplied policy chooses among congested candidates |
| Multiple SGPs and SGs at an ASP | 1.3.2.5, 4.5.2.2 | Per-SG route state and a derived MTP3-User view |
| Either SCTP initiation orientation | 1.4.8 | The M3UA role never follows `Dial` or `Accept` |

## Optional procedures: concrete exclusions

These are the places where the codec exists and the procedure does not, or where
the capability is out of the library's scope. Nothing else in RFC 4666 is left
unimplemented.

| Excluded | Section | What exists | What does not |
| --- | --- | --- | --- |
| Error code **0x0E, ASP Identifier Required** | 3.8.1, 4.3.4.1 | The code is decoded and named on receipt, the sentinel `ErrASPIdentifierRequired` exists, and the encoder would emit it | Nothing raises it. There is no setting that makes the ASP Identifier mandatory at an SGP, so no ASP Up is refused for omitting one. An SGP that needs its peers identified constrains them in `SelectAssociationConfig` and `AuthorizeASP`, and reads what was sent with `Association.PeerASPIdentifier`. Duplicate identifiers are a different rule and **are** enforced, as error 0x0F. |
| Error code **0x14, Destination Status Unknown** | 3.8.1, 5.4 | The code is decoded, named and delivered as a management indication when a peer sends it | It is never generated. RFC 4666 makes it a MAY: an SG *may* decline to disclose a destination's status to an ASP that is not authorized to know it. This library always answers a DAUD from its retained state and has no authorization model for refusing one. |
| **RKM acknowledgement timer** | 3.6, 4.4 | Context-bounded local waits and idempotent responder replay | No T(ack) for RKM, because RFC 4666 defines none. Inventing one would guess at a retransmission cadence the specification does not set. |
| **TLS over SCTP** | RFC 3788 Section 7 (MAY) | Nothing | RFC 3436 requires TLS on all bi-directional streams of the association, which the SCTP dependency does not implement. IPsec is the documented answer; see [standards.md](standards.md). |
| **IPsec, peer authentication, confidentiality** | RFC 4666 Section 6 via RFC 3788 | Nothing in this library | A node-level deployment responsibility. Using go-m3ua alone is not an RFC 3788 conformance claim. |
| **SCTP zero-checksum operation** | RFC 9653 | Nothing | Not negotiated; ordinary kernel CRC32c remains in force. |
| **STARTTLS message class** | RFC 3788 Sections 6 and 10 | Nothing | Not claimed. The RFC's own two section numbers disagree with each other and with IANA; see [standards.md](standards.md#rfc-3788-security-message-discrepancy). |

Two RFC 4666 errata are Held for Document Update and are followed as
interoperability decisions rather than as corrections: 2065 (scoped Notify) and
4475 (Routing Key Service Indicator padding). Erratum 2518 is Rejected and is not
applied. Their dispositions are in [standards.md](standards.md#rfc-4666-errata).
