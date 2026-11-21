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

RFC 4666 §1.4.1 inter-SGP coordination is a deployment duty rather than an
implemented library procedure. One SGP Endpoint shares state among its own
associations; no inter-process coordination protocol is implemented or claimed
by the procedure tests below. The
[inter-SGP coordination boundary](design/endpoint-management-and-ssnm.md#inter-sgp-coordination-boundary)
maps MTP3 routeset, SPMC and remote AS/ASP state to the existing local APIs and
states where no observation or control API exists.

Role shorthand: **ASP**, **SGP**, **SE** = IPSP Single Exchange, **DE** = IPSP
Double Exchange. "Local" is a procedure this node originates; "peer" is one it
receives and acts on.

Socket-backed tests skip themselves where the operating system has no SCTP, so
the integration rows are Linux evidence. Everything else runs anywhere.

## 1. DATA — RFC 4666 Sections 3.3 and 4.1

Typed receive retains the socket reader's monotonic time in `DataMessage.Received`,
including parsing, dispatch and receive queue wait but excluding the kernel receive buffer.

| Aspect | Roles | Direction | States | Evidence |
| --- | --- | --- | --- | --- |
| Typed send | ASP, SGP, SE, DE | local | ASP-ACTIVE only | `TestConcurrentWriteDataCarriesEachMessagesOwnLabelAndScope`, `TestWriteDataReturnsThePayloadLength`, `TestTypedDataCrossesBothSCTPInitiationDirections` |
| Typed receive | ASP, SGP, SE, DE | peer | ASP-ACTIVE, per Routing Context | `TestReadDataDeliversThePayloadWhole`, `TestReadDataDeliversEachMessageToExactlyOneReader`, `TestReadDataReportsReceiveTimeBeforeTheApplicationWait`, `TestReceivedDataHonoursPerRoutingContextActivation` |
| Per-message scope and routing label | ASP, SGP, DE | both | ASP-ACTIVE | `TestReadDataReportsTheReceivedScopeStreamAndEpoch`, `TestReadDataReturnsCallerOwnedData` |
| Stream selection from SLS; stream 0 refused | ASP, SGP, SE | both | any | `TestStreamSelectionHandlesEveryNegotiatedCount`, `TestWriteSignalDoesNotPutDataOnStreamZero`, `TestDataOnStreamZeroIsRejected`, `TestMTPTransferSelectsAvailableSGPAndSLSStream` |
| Partial and failed submission classified | ASP, SGP | local | ASP-ACTIVE | `TestWriteDataClassifiesPartialSubmissionAsIndeterminate`, `TestBroadcastPartialWriteKeepsSynchronizationPending`, `TestWriteDataReportsATransportRefusalAsNotSent`, `TestFullSendBufferReportsDataAsNotSent`, `TestExpiredWriteDeadlineReportsDataAsNotSent` |
| Inbound queue overflow | ASP, IPSP send; all roles report locally through implementation indication `ManagementDataQueueCongestion` (`DATA-QUEUE-CONGESTION`) | received NA/RC/DPC/OPC | ASP-ACTIVE | `TestOverflowSCONCarriesTheReceivedScope`, `TestOverflowSCONSocketScopesAndSustainedDrain`, `TestOverflowAtSGPDoesNotReportSS7DestinationCongestion`, `TestOverflowIPSPOnsetOmitsConcernedDestination` |
| Inbound queue sustained-drain recovery | all | local management only | active episode | `TestOverflowEpisodeEndsAfterSustainedDrain`, `TestOverflowEpisodeStaysWhileOccupancyStaysHigh`, `TestOverflowDrainThresholdAndDwellBoundaries`, `TestOverflowManagementSurvivesFailedSCON`, `TestOverflowScopeLimitReportsContinuityLoss` |
| Admission and authorization | ASP, SGP, SE, DE | local | INACTIVE→ACTIVE barrier | `TestScopedActivationAndDeactivationAPIsArmTAckBeforeWriting` |
| Contextless Application Server | ASP, SGP, SE, DE | both | ASP-ACTIVE | `TestEncodedDataMatchesTheCodec` (scope), and the RKM and IPSP contextless rows below |
| Epoch across SCTP restart | ASP, SGP | peer, local | any | `TestReadDataReportsTheEpochTheMessageArrivedIn`, `TestBroadcastActivationDropsThePreviousFlowEpoch` |
| Managed peer AS distribution: Override, Loadshare, Broadcast (RFC 4666 §§4.3, 4.3.4.3) | SE, DE | local, both handles | AS-ACTIVE | `TestIPSPDistributionTrafficModes`, `TestIPSPDistributionBroadcastSynchronization`, `TestIPSPManagedDistributionSCTP` |
| Managed recovery queue, T(r) discard and copied input (§4.3.2) | SE, DE | local | AS-PENDING, reactivation, expiry | `TestIPSPDistributionRecoveryAndExpiry`, `TestIPSPDistributionTimerExpiry`, `TestIPSPDistributionPeerDownExpiry`, `TestIPSPManagedDistributionSCTP` |
| SE requester peer state, DE peer-only registry and Endpoint/NA/RC isolation (§§4.3, 4.3.4.3.1) | SE, DE | local, both handles | active, absent, inactive | `TestIPSPDistributionSingleExchangeInitiatorOwnsPeerState`, `TestIPSPDistributionRequiresPeerApplicationServer`, `TestIPSPDistributionSeparatesEndpointNetworkAndDirection` |
| All send paths refuse SI 0, including both SGP/IPSP distribution handles (§§1.4.3.2, 1.4.3.4); MTP3-User DATA unchanged | SGP, SE, DE | local | AS-ACTIVE, AS-PENDING, AS-INACTIVE | `TestIPSPDistributionRefusesManagementData`, `TestIPSPManagedRawDataRefusesManagementData`, `TestIPSPDistributionLeavesDirectDataUnchanged`, `TestDistributionPolicyZeroValueKeepsAdmission` |
| Managed IPSP peer congestion and configured cached DATA admission (§1.4.6) | DE | local, both handles | ASP-ACTIVE | `TestIPSPDistributionKeepsCongestionAdmission` |

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
| Owed Up Ack duplicates preserve state and gates | ASP, SE, DE | peer | `TestDuplicateUpAckLeavesActiveTrafficUnchanged`, `TestOwedDuplicateUpAckStillAbsorbed` |
| Unowed Up Ack: Error then withdrawal report, application recovery; inactive absorption (§4.3.4.1) | ASP, SE, DE | peer | `TestUnowedUpAckWhileActiveSendsErrorAndReportsExactScopes`, `TestErrorPrecedesWithdrawalIndication`, `TestUnowedUpAckWhileDownReportsInactiveWithoutAutomaticActive`, `TestUnowedUpAckWhileInactiveSendsNoError`, `TestIPSPUnowedUpAckReportsAffectedDirections` |
| ASP Down sent and retransmitted | ASP, SE, DE | local | `TestShutdownRetransmitsAspDownUntilAcked` |
| ASP Down received, Ack always sent | SGP, SE, DE | peer | `TestHandleAspDownAlwaysAcks` |
| ASP Down Ack received, solicited and unsolicited | ASP, SE, DE | peer | `TestSolicitedAspDownAckIsAccepted`, `TestUnsolicitedDownAckCommitsDownWithoutErrorOrAutomaticUp`, `TestUnsolicitedDownAckReportsPriorActiveScopes`, `TestIPSPUnsolicitedDownAckReportsWithoutRecovery` |
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
| Unsolicited Inactive Ack after reactivation; delayed duplicate before reactivation (RFC 4666 §4.3.4.4) | ASP | local | `TestASPWithdrawsOnUnsolicitedInactiveAckAfterReactivation`, `TestASPIgnoresDelayedDuplicateInactiveAck`, `TestASPApplicationReactivationNamesOnlyWithdrawnScope`, `TestInactiveAckHistoryRetiresOnlyReactivatedScopes` |
| Peer Single Exchange reactivation (§4.3.4.3.1, §5.6.1); independent active-scope withdrawal; pending first activation is not active (§4.3.4.4) | ASP, SE | local | `TestIPSPSingleExchangePeerReactivationRetiresInactiveAckHistory`, `TestASPWithdrawsWhileAnotherScopeAwaitsInactiveAck`, `TestASPPendingFirstActivationDoesNotAuthorizeInactiveAck`, `TestASPApplicationReactivatesAfterSGPWithdrawalSCTP` |
| Override displaces the incumbent | SGP, SE, DE | peer | `TestFigure4TransitionsCellByCell`, `TestTrafficModeCanBeConfiguredPerApplicationServer` |
| Loadshare and Broadcast add without displacing | SGP | peer, local | `TestBroadcastActivationDropsThePreviousFlowEpoch` |
| Valid Active Ack without request or duplicate history: exact ASP-ACTIVATION report, retain DATA, application choice (§3.8.1, §4.3.4.3) | ASP, SE, DE | peer | `TestUnsolicitedActiveAckReportsExactScopes`, `TestUnsolicitedActiveAckSendsNoErrorAndDeliversDATA`, `TestASPInactiveAfterUnsolicitedActiveAckCloses`, `TestIPSPUnsolicitedActiveAckReportsLocalGrant` |
| Traffic mode negotiation and rejection | all | both | `TestAspActiveWithIncompatibleTrafficModeIsRefused`, `TestAspActiveWithoutTrafficModeIsAccepted`, `TestTrafficModeTypeOutsideTheDefinedValuesIsRejected`, `TestASPRejectsActiveAckWithWrongRequestedTrafficMode`, `TestDynamicTrafficModeIsEchoedAndCannotChangeAfterAgreement` |
| Application-owned recovery after unsolicited Inactive Ack (RFC 4666 §4.3.4.4, §4.3.4.4.1) | ASP, SE, DE | application policy | `TestInactiveAckLeavesReactivationToApplication`, `TestIPSPInactiveAckReportsWithdrawalWithoutReactivation`, `TestInactiveAckApplicationActivationUsesOrdinaryProcedure` |
| Exact unsolicited withdrawal report, including mixed confirmations and contextless AS (RFC 4666 §1.6.3, §4.3.4.4) | ASP, SE, DE | implementation indication | `TestInactiveAckReportsOnlyUnsolicitedScopes`, `TestOmittedInactiveAckReportsContextlessAndDynamicScopes`, `TestInactiveAckReactivatedScopeIsNotSolicitedByOldPartialRequest`, `TestDelayedPartialInactiveAckDoesNotCancelApplicationReactivation`, `TestOmittedPartialInactiveAckPreservesIndependentApplicationActivation` |
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
| 0x05 | Unsupported Traffic Mode Type | yes | `aspsm_test.go`, `TestUndefinedASPActiveModeDrawsUnsupportedMode`, `TestASPActiveModeErrorKeepsParameterPrecedence` |
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

`AssociationConfig.Subscribe` can install `StateChanges` and
`ManagementIndications` readers before the handshake. RFC 4666 Sections 1.6.3,
4.3.4.1 and 4.3.4.3 define the M-ASP_UP and M-ASP_ACTIVE confirms exposed as
state transitions. `TestDialSubscribeReceivesHandshakeIndications` and
`TestAcceptSubscribeReceivesEachPeersHandshake` cover their early delivery,
and `TestDialSubscribeCountsEveryPeerHandshakeNotify` compares M-NOTIFY counts
with the peer's successful writes. The opening ASP-DOWN is the initial state,
not a transition (Section 4.3.1); `TestOpeningASPDownIsNotAStateChange` covers
every role.

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
| SGP retirement on ASP Down and ASP Up while active (§4.3.4.2; §4.3.4.1), through the DEREG path (§4.4.2), with current membership checked at each AS-state Notify write after both queues (§4.3.4.5) | SGP | `dynamic_registration_lifetime_test.go`, `as_notification_membership_test.go`, `as_notification_membership_linux_test.go`, `TestSGPASPDownRetiresRegistrationOverSCTP` |

RFC 4666 defines no RKM acknowledgement timer, so none is invented: a local wait
is bounded by the caller's context, and a responder answers a retransmitted
request from deterministic replay state.

## 7. Timers and retransmission

| Timer | Configuration | Evidence |
| --- | --- | --- |
| T(ack) and its retry budget | `AssociationConfig.TAck`, `TAckRetries` | `TestAssociationASPUpReportsTAckExpiry`, `TestShutdownRetransmitsAspDownUntilAcked` |
| T(beat) | `HeartbeatInfo` | `TestHeartbeatKeepsAssociationAlive` |
| T(r), the AS recovery timer | `ApplicationServerConfig.RecoveryTimer` | `TestRecoveryTimerResolvesPending`, `TestASPActiveBeforeRecoveryTimerRestoresActive` |
| M3UA handshake budget, starting after `Subscribe` returns | `AssociationConfig.EstablishTimeout` | `TestEstablishTimeoutBoundsTheM3UAHandshake`, `TestSubscribeReturnsBeforeHandshakeAndBudgetStart` |
| RTO bounds and association failure threshold | `SCTPConfig.RTOInfo`, `AssociationMaxRetrans`; runtime getters and setters | `TestSCTPRTOSocketConfiguration`, `TestSCTPRTOSelectedRefusalKeepsListenerServing`, `TestSCTPAssociationMaxRetransSetupOrdering` |
| One SCTP association attempt | `SCTPConfig.InitTimeout`, setup RTO.Initial beyond the budget | `dial_test.go`, `dial_rto_linux_test.go` |
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

Dial sets up the attempt with RTO.Initial beyond InitTimeout so its single INIT
is not retransmitted inside the budget. Once established, RTO.Initial returns
to the socket's value, including `SCTPConfig.RTOInfo` when configured. The
setup override leaves the configured RTO.Min and RTO.Max unchanged
([RFC 6458 Section 8.1.1](https://www.rfc-editor.org/rfc/rfc6458.html#section-8.1.1)).
On Linux each path keeps the setup value as its current RTO until its first RTT
measurement, retransmission timeout or association restart
([RFC 9260 Section 6.3.1 rule C1](https://www.rfc-editor.org/rfc/rfc9260.html#section-6.3.1)).
A DATA chunk lost before that measurement, normally the first M3UA message,
therefore waits about InitTimeout + 1 s for retransmission; see
[sctp/lksctp-tools issue 74](https://github.com/sctp/lksctp-tools/issues/74).

`RTOInfo` is applied before connect or listen (RFC 6458 Section 8.1.1).
A listener's selected value is applied after accept: zero fields retain the
listener's values. On Linux it changes the association's bounds immediately,
but existing paths keep the listener's Initial until their first RTT measurement
or retransmission timeout, and T5-shutdown-guard keeps five times the listener's
Max. The selected Initial reaches paths added later and paths reset by restart;
see `ListenerConfig` and the lksctp-tools issue above. `AssociationMaxRetrans`
is applied after path settings (RFC 6458 Section 8.1.2); Linux refuses it with
EINVAL on a multi-homed association if it exceeds the sum of path limits, as
advised by the Note in RFC 9260 Section 8.2. Status and Endpoint snapshots
include the association parameters and each path's independently current RTO.

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
| SCTP path status, primary selection, per-path controls and pre-handshake configuration | all | `TestSCTPPathSocketManagement`, `TestSCTPPathEventRouting`, `TestSCTPPathInfoRacesAndErrors` |
| SCTP restart and re-establishment | ASP, SGP, SE, DE | `restart_test.go`, `TestSSNMStateSourceResetAndFreshReactivationStartANewEpoch` |
| NIF unavailable, Section 4.7 | SGP | `nif_test.go`, `nif_quiescence_test.go`, `nif_askey_test.go` |
| Graceful ASP withdrawal, Sections 4.9 and 5.3 | ASP, SE, DE | `shutdown_test.go` |
| Association, Listener and Endpoint close scopes | all | `TestClosingOneSGPAssociationKeepsEndpointAndSiblingAlive`, `TestClosedAssociationIsForgottenByItsListener`, `endpoint_role_test.go` |
| Abortive release, SCTP ABORT of RFC 9260 Sections 9.1 and 11.1.4 | all | `TestAbortSendsABORTWhereCloseCompletesASHUTDOWN`, `TestAbortedAssociationIsLostAtItsGoM3UAPeer`, `abort_test.go` |
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
| ASP destination auditing with DAUD | 4.5.3 | The ASP originates it; the SGP answers available, restricted, congested, unavailable and unknown from retained state. National authoritative known clear sends SCON(0) before DAVA/DRST (§5.4.1; `TestNationalDAUDKnownClearSendsSCONZeroBeforeAvailability`); absent congestion (§5.4.3) and international known clear send availability alone, without a local refusal. Unavailable/restart-isolated/unknown destinations send only DUNA. A profile-refused SCON is omitted with one local management indication; every other reply continues (`TestDAUDProfileRefusalPreservesAvailabilityAndOtherReplies`). This is a [declared §4.5.3 deviation](rfc4666-conformance.md#daud-congestion-profile-deviation). |
| M3UA heartbeat, BEAT and BEAT Ack | 4.3.4.6 | Either role, independent of SCTP HEARTBEAT path management |
| Local SCON from an ASP or IPSP | 3.4.4 | Including Concerned Destination, and deliberately kept out of the SG's own destination state |
| Correlation Id | 3.3.1 | Per message, on every DATA path, with presence distinguished from zero |
| Network Appearance | 3.3.1, 3.4 | Per Application Server, with presence distinguished from zero |
| Contextless Application Server | 3.6.1 | ASP, SGP and both IPSP exchange models |
| Destination Restricted | 3.4.6 | Sent and acted on |
| Override, Loadshare, Broadcast and n+k redundancy | 1.4.4, Appendix A | Configurable per Application Server |
| Message Priority congestion policy | 1.4.6 | A caller-supplied policy filters combined received/transport levels per association |
| Outbound SCTP pressure | 1.4.6, 4.5.2.1–2 | Periodic bounded sampling, continuous-coverage threshold dwell, explicit unknown/stale status, destination/AS eligibility shared with transfer selection, ASP/IPSP destination MTP-STATUS and implementation indication `ManagementTransportCongestion` (`TRANSPORT-CONGESTION`); no automatic SCON |
| Multiple SGPs and SGs at an ASP | 1.3.2.5, 4.5.2.2 | Per-SG route state and a derived MTP3-User view |
| Either SCTP initiation orientation | 1.4.8 | The M3UA role never follows `Dial` or `Accept` |

### Continued local-user MTP status

RFC 4666 §1.4.6 describes current-MTP3-procedure status delivery without an
uppercase MUST; optional peer SCON/SG TFC remains MAY. For international and
national-without-priorities methods, routed ASP `MTPTransfer` implements the
Q.704 (07/96) §§11.2.3.1/11.2.5 message-count procedure: status at offer 1,
then no later than eight offers after each indication. Without a reset this is
1/9/17; resets can report earlier. A failed transport attempt counts once;
invalid requests, candidates, callbacks, retries and broadcast copies do not
add offers. Raw `WriteData`/`WriteSignal` never count. National-with-priorities
uses transition status (§11.2.4), not periodic offered-message status.

`NewMTPStatusProcedure(profile)` supplies the explicit procedure for IPSP or
application-owned routing, one instance per destination/local MTP3 boundary,
with concurrent calls and no hidden destination map. Supply effective aggregate
status to `Offered` once per original validated request. Handle its error before
using the indication; `(nil, nil)` means nothing is due. Route names above 255
octets return `IdentityLengthError` wrapping `ErrInvalidParameterValue` without
retention or cadence changes. A changed destination resets automatically; supplying clear
or uncertain status to `Offered` also resets the interval. The helper sees
snapshots only. Call
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

The routed path maintains a bounded Endpoint-wide exact-DPC LRU; zero
`MaxStatusDestinations` selects 4096 and negative values are invalid. Eviction
restarts initial notification. These resource choices are engineering policy.

Conservative engineering policy suppresses repeats and retires automatic
intervals when either `ReceivedCongestionUnknown` or `TransportCongestionUnknown`
is true, even if `Congested` survives from another contribution. An uncertain
status supplied to `Offered` resets the helper. Transport uncertainty uses a
normal transition reason, never received-validity expiry.

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

Only explicit expiration sets `MTPStatusValidityExpired`. An availability,
transport or lifecycle operation exposing already-expired knowledge retains
`MTPStatusTransition`; an uncertain noncongested result has `MTPStatusCauseNone` and never
claims measured congestion clear. These trigger values are engineering policy,
not requirements of the optional timer in RFC 4666 §3.4.4.

Cause/reason fields identify transition, offered status and validity expiry;
expiry does not mean measured clear or produce continuing known-congestion
status. The existing bounded stream/overflow-resync contract remains in force,
and the application fans each primitive to each registered MTP3-User.
Level-4 resumption remains the user's decision (§11.2.3.3). See
[continued status](../README.md#continued-mtp-status) for field values and resets.

### SGP peer congestion inputs

RFC 4666 §§1.3.2.4 and 1.4.2.4 supply the remote-ASP state and distribution
model. Peer SCON is retained separately from authoritative SS7 destination
knowledge, with shared SSNM budgets, snapshot/delta/resync support and
association epoch/AS lifecycle cleanup. Retention cases are named in
`TestPeerSCONSurvivesSubscriptionResync`,
`TestPeerSCONNeverChangesAuthoritativeDAUDState`,
`TestPeerRetentionRejectsAWholeOverBudgetReport` and
`TestPeerWithdrawalRetiresOnlyItsScopes`. `CongestionInputs` exposes copied
membership, peer, local and transport components with version vectors and bounded
retry; `Consistent` is Endpoint-local and `Revision` covers SSNM only. `Local`
keeps queue/application contributions separate; its snapshot does not replace
queue management continuity. `PeerCongestionVersion.LocalGeneration` is an
opaque component for local lifetime/queue transitions; consumers compare versions
without interpreting its value.

`SGPCongestionPolicy` vetoes candidates outside Endpoint, AS and delivery locks;
a decision still stale after one revalidation returns known-not-sent
`ErrCongestionDecisionStale`. `SGPConfig.PreferUncongestedPeers` defaults false
and ranks loadshare members by maximum peer/transport pressure after choosing
the latest matching peer record by `Revision`. Newer matching clears supersede
older ranges; equal ranks retain active-member order and SLS modulo selection.
Override retains its member and broadcast exposes every required recipient's
veto explicitly. `TestDistributionConsultsSeparatePeerAndTransportInputs` and
`TestSGPTransitVetoAppliesWithoutPeerPreference` cover those separate selection
inputs and the default-disabled preference. Rerouting can missequence SLS
traffic (§1.4.7).

`TestPeerPreferenceUsesLatestMatchingRangeAndWireScope` covers overlapping
range/RC-scope precedence. Lock ordering is exercised by
`TestPeerRetentionDoesNotHoldBindingLockWhileWaitingForSampler`; recovery slot,
expiry and indeterminate-copy cases are named in
`TestRecoveryPolicyWaitCannotReviveExpiredDataOrLoseTheDeliverySlot` and
`TestRecoveryBroadcastNeverRetriesAnIndeterminateCopy`.

`TestOversizedPeerSCONRetiresOnlyResolvedAS` covers scoped peer refusal,
subscription replay and preserved sibling inputs;
`TestOversizedDAUDPreservesPeerCongestion` covers the audit boundary.
`TestRecoveryRetriesOnlyWholeAllocationRefusals` covers zero-byte `ENOMEM`
retry and positive/unknown no-replay controls in all three traffic modes.
`TestPositiveAllocationFailureRemainsIndeterminateOnEveryDataPath` covers
typed, cancellable and raw DATA submission counts.

These mechanisms do not claim SPMC-wide congestion or alternate capacity.
RFC 4666 §1.4.6 permits implementation-dependent SS7 TFC triggering; the
application/NIF owns that decision and its SS7 wire evidence. Resync supplies
current retained knowledge, never historical Concerned Destination triggers.

## Optional procedures: concrete exclusions

These are the places where the codec exists and the procedure does not, or where
the capability is out of the library's scope. Congestion-method profiles and Message Priority admission are implemented.
Routed ASP continued status is automatic. IPSP AS selection and recovery use
`DistributeData`; applications needing continued status at an explicit local-user
boundary use `NewMTPStatusProcedure` (RFC 4666 §§1.4.3.4 and 1.4.6), and fan out
indications to each user. Optional
received-SCON validity and automatic ASP audits are configured independently
through `CongestionConfig.Validity` and `Auditing`; see
[validity and audit policy](#received-congestion-validity-and-audits). Transport route
preference is opt-in: there is no cross-association SLS changeover barrier
(RFC 4666 §1.4.7).

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

Inbound DATA queue episodes use the received NA/RC/DPC/OPC, with a 32-scope
limit and visible tracking loss. The queue capacity is association-wide.
Occupancy is observed on enqueue/dequeue and every 100 ms during an episode;
local end requires at most 50% occupancy sustained for 1 s. Management sees onset
and end even if SCON cannot be written. SGP local queue exhaustion sends no SCON
because it does not establish SS7 destination congestion (RFC 4666 Sections
1.4.6 and 3.4.4).

Overflow SCON follows the configured method: international onset omits Congestion
Indications and has no wire clear; national onset uses the configured MaxLevel
and national recovery can submit numeric zero. Local clear cancels an onset that
was never submitted. A submitted national onset reserves bounded capacity until
its clear is submitted or its lifetime retires. Clear submission does not prove
peer receipt. The queue source and independently submitted application sources
remain separate; no host/CPU detector is installed. Outbound backlog pressure
alone still produces no automatic SCON.

After `ManagementASPWithdrawal`, application policy decides whether and when to
restore a non-empty `indication.ASKeys` with `association.ASPActive(ctx, indication.ASKeys...)`,
or a known numeric scope with `association.ActivateRoutingContexts(1)`. The report
is named `ASP-WITHDRAWAL`, not an RFC M- primitive. No activation is sent until
that application request, including after last-scope withdrawal with automatic
startup configured. Automatic activation following ASP Up Ack remains unchanged.
In IPSP Double Exchange, select the reported TrafficToLocal keys before calling
ASPActive. TrafficToPeer recovery is a decision for the remote application;
shared ASPSM does not make ASPTM shared (§4.3, §5.6.2).
An empty Up/Down withdrawal has no active memberships to restore; use
`PreviousState` to choose the next lifecycle procedure. Calling `ASPActive`
without keys selects every configured AS, so an empty report is not a scoped
activation request.
A contextless AS beside numeric ASes requires a whole-inventory policy decision
before `ASPActive(ctx)` without keys (RFC 4666 §4.3.4.3).


Withdrawals apply only to active scopes; mixed messages ignore their inactive
portions. `TestInactiveAckMixedDuplicateWithdrawsActiveScope`,
`TestInactiveAckDuplicatePreservesApplicationActivation`, and
`TestInactiveAckOvertakingStartupPreservesInitialActivation` cover these rules.
`TestInactiveAckUsesDynamicNetworkAppearance` and
`TestNotifyUsesDynamicNetworkAppearance` cover registered AS identities.
`TestInactiveAckBlockedRetryEndsBeforeWithdrawalReport` pins cancellation of an
unaccepted transport send; `TestCancelledAcceptedActiveIsReportedImmediately`
pins immediate M-ERROR reporting after the withdrawal report.
`TestInactiveAckJoinDoesNotWaitForCompetingWriters` pins the per-call join.
No cancelled-send evidence changes later Active Ack handling. Cancelled callers
receive `ErrASPProcedureSuperseded`, not a false association-closed error.

`TestConfirmedInactiveReportsSupersededApplicationActivation` and
`TestConfirmedDownReportsSupersededApplicationActivation` retain the generic
M-ERROR for unsuccessful application procedures (RFC 4666 §1.6.3).
`TestWithdrawalReportsPrecedeSupersededApplicationFailure` checks that the
cancelling handler publishes its withdrawal and acceptance reports before
releasing the procedure waiter while its context and association remain live.
`TestASPActiveContextCancellationEscapesWithdrawalDrain`,
`TestActivateRoutingContextsWaitsForRetiredResultPublication` and
`TestAssociationCloseReleasesASPActiveDuringWithdrawalDrain` check prompt
independent context and close exits while admitted DATA holds the drain; a retired
nonwaiting activation joins the withdrawal publication before exposing its result.
A cancelled context may put the generic failure before the handler's reports.
The T(ack) retry path continues without waiting for that drain.

### Acknowledgement withdrawal publication

`TestRetiredInitialWritesWaitForWithdrawalPublication` holds the state commit
while public Up, Active and Inactive initial writers resume.
`TestInternalSubmissionDoesNotJoinWithdrawalPublication` keeps internal
submission independent while its request result waits for the commit and report.
`TestShutdownInitialInactiveWaitsForWithdrawalPublication` covers orderly shutdown.
`TestAutomaticEntrySerializesStateThroughSubmission` preserves automatic
registration and submission under the state lock.
`TestConcurrentASPUpDoesNotCloseOpeningAssociation` covers replacement during
startup in all ASP/IPSP exchange models.
`TestAutomaticActivationPrecedesConcurrentASPDown` checks automatic Active before
concurrent Down. `TestAutomaticASPUpSupersedesExplicitCallerAccurately` and
`TestShutdownSupersedesPendingProceduresAccurately` distinguish local replacement
from association closure.
`TestShutdownRefusesEveryExplicitASPEntryPoint` covers all six public
ASPSM/ASPTM request-registering methods and their existing failure-indication
contracts. `TestShutdownRefusesExplicitASPBeforeWaitingOnGate`,
`TestShutdownRefusesExplicitASPProcedureWaitingOnGate`,
`TestShutdownRefusesASPProcedurePausedBeforeRegistration` and
`TestShutdownRefusesASPUpPausedBeforeRegistration` check admission before the
gate, after queuing, and after validation. Shutdown owns withdrawal from its
terminating claim (RFC 4666 §4.9); `TestShutdownClaimOrdersWithRequestRegistration`
checks that claim against request registration.
`TestShutdownRefusalWinsStateCheckAfterValidation` preserves shutdown refusal
when a later state check observes a concurrent withdrawal.
`TestShutdownRefusesWholeMixedModeActivation` keeps a multi-message activation
batch on one side of the shutdown claim.
`TestShutdownRefusalPreservesExistingProcedureOwnership` prevents refused calls
from completing an Up or retiring an activation.
`TestShutdownOwnsWithdrawalForASPAndIPSP` checks acknowledged shutdown-owned
ASPIA/ASPDN and refusal of competing procedures in ASP, IPSP Single Exchange and
IPSP Double Exchange. The Linux raw-peer test
`TestShutdownRefusesExplicitUpwardProceduresOnWire` holds the first withdrawal
Ack for both inactive-first and down-first policies, checks Up/Active refusal,
no later upward messages, successful shutdown and orderly transport EOF.
`WriteSignal` is raw message submission and does not register a T(ack) request.
`TestCloseRejectsLatePendingProcedureRegistration`
prevents registration after teardown has swept pending requests. `TestSharedIPSPDownReportFollowsPerASDownCommit` checks shared
IPSP DOWN memberships and T(r) before monitor publication.
`TestAcknowledgementReportsFollowEveryStatusView` checks Up, Down, Active and
Inactive reports against the association, both IPSP directions and per-AS status.
`TestSingleExchangePartialWithdrawalStatusUsesTrafficLedger` preserves an
unaffected active scope while checking the withdrawn scope's shared status.


`TestPeerManagementExplanationsAreOwned` and
`TestManagementPublicationCopiesPeerExplanations` cover owned peer ERR diagnostics
and NTFY INFO String delivery, including absent and present-empty values
(RFC 4666 §4.2). `TestDUPUStructuralBoundsAcrossAdmissionPaths` covers typed,
raw and inbound User ≤255/Cause ≤15 admission (§3.4.5; Q.704 (07/96) §15.17;
Q.Imp704 (12/00) §§2.2.3–2.2.4). Reserved-value cases test structural bounds,
not assignments in any MTP3 variant. DUPU destination, mask, role and scope
restrictions continue to apply.

## Received congestion validity and audits

RFC 4666 §3.4.4 optionally permits local validity (lowercase may); §4.5.3 permits
periodic and isolation DAUD (MAY), with the lowercase must exclusion for numeric
zero congestion-origin work. Nil policies preserve explicit operations.
`Auditing` is ASP-only; congestion-origin auditing requires the national
multiple-level/message-priority method. Availability and configured activation
origins remain independent. Original source/epoch/scope/range jobs retain per-AS
deadlines and narrow outgoing RCs to still-due origins. Separate SCON(0)/DAVA
clears retire only their causes; a refused positive replacement restores prior
work and charges. No sibling fallback or automatic activation occurs.

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

With validity or auditing enabled, received history and job insertion use the
same captured registration token, with tracker → dynamic-binding → store fencing
and monotonic source epochs. RKM/version/epoch/source loss removes affected
history and charges while unaffected static AS membership and independent local
direction survive. `SSNMCongestionSourceInvalidation{Destination, AS, Revision}`
values in `SSNMEvent.CongestionSourcesInvalidated` independently remove
congestion history. `SSNMAvailability.AS` identifies resolved availability
applicability. `SSNMDestinationKnowledge.AvailabilitySources` carries the
independent statements retained when auditing is enabled; `CongestionSources` is
retained when validity or auditing is enabled. DestinationUpdates snapshot entries own both
histories. `SSNMEvent.AvailabilitySourcesInvalidated` adds independent
`SSNMAvailabilitySourceInvalidation{Destination, AS, Revision}` removal IDs.
Consumers must apply scalar dimension IDs and both source arrays, or use
`ApplyInvalidation`, which patches all three while preserving other knowledge
and history-only destinations. These public field additions break unkeyed
literals; update snapshot and event consumers as well as construction sites.
Removal events route by partition `Epoch`, with `Partition` empty. Each of the
three arrays independently caps at 2,048 IDs: dimension IDs cost 16 bytes,
availability and congestion source IDs each cost 48. The dimension-only maximum
remains 33,440 bytes; the combined maximum is `512 + 160 + 2048*(16+48+48) =
230,048` bytes. Custom queues/bursts can still need resynchronization.

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

Scalar retention is `128 + 4*rawRC + 16*resolvedAS` bytes. Each independent
availability or congestion source adds one record and `144 + 4*rawRC` bytes per
resolved AS. Availability history requires auditing; congestion history requires
validity or auditing. Nil policies retain the previous scalar record count and
cost. `MaxBytes` must be at least 400. A single-RC availability statement with
auditing, or a SCON with validity/auditing, plus its partition needs 552 bytes;
a one-binding/one-partition audit job adds 472 before Info String, totaling
1,024 for that statement and job. General jobs charge `256 + len(info) + 8*rawRC
+ 144*bindings + 64*uniquePartitions`. These are portable accounting units, not
measured heap/RSS limits.

Audit outcomes aggregate at most one event for each of seven fixed reasons per report,
pass or retirement batch. `PublicationCount` counts each completed job outcome
for `submitted`, `send-failed` and transport `send-deferred`, including consecutive equal reasons and a
same-key refresh during transport; each removed job contributes exactly once to
`reporter-retired` regardless of retirement-pass order. Deferred and contended
work still counts outcome transitions. Counts remain per job: one batched DAUD
can contribute several completed outcomes;
`PublicationScopeOmitted` marks mixed original scopes or more than 64 raw RCs, and zero
`Report.Association` marks mixed sources. No APC lists, Info Strings or names
are replayed. Repeated contention and rate deferrals are suppressed. A refreshed successor
keeps its newer causes and deadlines when the prior attempt completes. One event
costs at most 928 conservative bytes (784 with actual reasons). Seven diagnostics
consume at most seven slots and 6,496 conservative bytes, in addition to the
actual report-observation and report-patch events. Only an unfragmented
observation adds one slot. Report payloads and aggregate bursts retain their
separate limits against defaults of 256 slots/1 MiB.

Expiry events carry owned `SSNMEvent.Expired` identities of type
`SSNMCongestionExpiry{Epoch, Destination, AS, Revision, Peer}`. Call
`event.ApplyExpiry(&snapshot)` on the owned full snapshot. Each identity names
the Endpoint-unique partition epoch and original statement version; only
matching source records, their matching scalar copy, or the identified peer
record become `CongestionExpired`. Values, severity, timestamps, provenance and
the snapshot's stream revision remain unchanged. The caller orders the event
stream. Expiry uses neither `DestinationUpdates` nor `PeerCongestion`; top-level
`Partition` and `Epoch` are empty because one event may span partitions. Each
commit rechecks at most 256 candidates and publishes their freshness changes
atomically under the store lock, then releases it before the next chunk and
cancellation check. Each ID costs 56 accounted bytes. At the default
16,384-record ceiling, an expiry-only pass queues at most 64 compact SSNM events
and 950,272 bytes in the default SSNM subscription. This bound excludes reports,
other queued bursts and the separate derived MTP indication channel; SSNM
subscription overflow still requires `Resync`.

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

Expiry publishes local unknown and historical severity, never measured clear,
a negotiated peer lifetime, a transport-observation TTL or an invented
SCON(0)/DAVA/RESUME. DAVA does not clear congestion. `Endpoint.Close` cancels and
joins the operation-owned worker after Association closure, outside locks needed
for completion; Association withdrawal/teardown never join it. Late REG/DEREG
processing may hold RKM correlation/lifecycle locks while joining a cancelled
tracked optional send. Completion needs only the tracker; optional submission
never takes those RKM locks. The single `NoWait` attempt keeps the pinned
transport's internal mutex/deadline limitations (RFC 6458 §5.3.4; Errata 6111).

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
and [migration
guide](migration-v1.2.md#received-congestion-validity-and-audits). These
contract descriptions are not additional wire, timing or resource acceptance
results.

## Local congestion publication and DATA admission

| Procedure | Boundary | Behavior |
| --- | --- | --- |
| Application local contributions | `ReportLocalCongestion`, `WithdrawLocalCongestion`, `CongestionInputSnapshot.Local` | Current association/epoch/active association member or binding of the exact AS/exact 24-bit DPC; no aggregate AS-readiness or DATA/SPMC authorization; independent named sources, no TTL; maximum or unknown aggregation only within that local scope; shared SSNM retention/loss limits. |
| Automatic local SCON | `CongestionConfig.Publication`, `CongestionPublications`, `CongestionPublicationCounters` | Bounded coalescing/retry, separate desired/submitted state, unsent-onset cancellation, national clear reservations, distinct CD-origin keys, international no numeric clear; SGP local evidence stays local. |
| Automatic DAUD | Existing `CongestionConfig.Auditing` plus common optional capacity | Due jobs stay in their bounded deadline store when transient publication capacity is unavailable; one optional attempt per association per pass across both kinds. |
| Optional submission | Existing traffic/send barriers and NoWait primitive | Final live epoch/state/revision recheck; contention/EAGAIN defer; mandatory pending/waiting control takes precedence; no tracker/state lock crosses transport. |
| Cached DATA gate | `AssociationConfig.CongestionAdmission` | Nil disabled; zero ratio selects 0.9; finite 0<ratio<1 requires backlog sampling. All typed/raw/distributed DATA uses cached evidence under send serialization, without a socket query. Startup/invalid/stale/at-threshold samples return known-not-sent `ErrDataCongestionRefused`; control bypasses it. |

`CongestionPublicationConfig` defaults to 64 Endpoint-wide slots, 32 per
association, 64 KiB, 100 ms retry and a 1 s interval after successful submission.
These are
engineering defaults. Negative fields and a per-association ceiling above the
Endpoint ceiling are invalid. New onsets without reservation capacity are
visibly refused/deferred before submission. Submitted national onsets keep
capacity until clear submission or lifecycle retirement; audits do not evict
blocked clears. Queue drain does not clear application contributions, and a
failed wire report never erases local observability. `ManagementCongestionPublication`
exposes bounded diagnostic state independently of logging. Explicit
`SignallingCongestion` and `DestinationStateAudit` remain synchronous. A
successful automatic DAUD publishes one raw local send observation per wire
batch, with empty `Partition`/`Epoch`, instead of one per canonical owner. Its
`Scope`/`Destinations` describe the batch; logical-job outcome counts remain
separate. This changes observation cardinality.

RFC 4666 §1.4.6 permits local ASP/IPSP SCON with MAY and leaves SS7 TFC/SPMC
policy to the application/NIF. Section 1.4.7 prohibits DATA on stream zero with
MUST NOT, recommends the listed ordering/management mappings with SHOULD, and
permits SSNM on any stream. The AS has no authoritative local-destination inventory; contribution admission
validates the numeric value and current binding. No stream mapping or cached ratio creates kernel
memory reservation or a control-delivery guarantee. A single large message,
retransmission or direct descriptor user can consume remaining charge.
`ControlWriteTimeout` still closes a permanently stalled association. Recovery
of finite pressure and retirement of stale work are distinct from delivery
through an indefinite blackhole. See the [design](design/endpoint-management-and-ssnm.md#local-congestion-publication-and-data-admission)
and [migration guide](migration-v1.2.md#local-congestion-publication-and-data-admission).

SSNM subscriptions keep their existing bounded continuity-loss/`Resync` contract.
Every individual event fits the default subscription byte budget: configured
SG/AS and ASP/IPSP destination route names are capped at 255 octets, and oversized
report histories use explicit bounded fragments (`ApplyReport`, `ReportContinues`
and `ReportPatch.SourcesContinued`). The stricter local-source limit remains 64 octets.
A legal multi-AS report or a combined report/expiry burst can still overflow an
unread default queue. Queued deltas precede an explicit continuity-loss marker;
`Resync` recovers exact retained state and resumes the stream, without replaying
discarded raw observations or diagnostics. Defaults remain 256 slots / 1 MiB;
there is no whole-report-plus-pass default-fit guarantee. MTP indication overflow
continues to use `ResyncRequired`, with at most one indication per changed
destination. See the endpoint-management design for per-event ceilings and queue
sizing with validity, auditing and publication enabled.
