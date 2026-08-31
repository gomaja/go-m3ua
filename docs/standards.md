# M3UA standards and security contract

Audit date: 2026-08-31.

This document defines which authorities govern go-m3ua v1.2.0 and where the
library boundary ends. The implementation status for each RFC 4666 area is in
the [conformance matrix](rfc4666-conformance.md).

## Authority order

Standards decisions use the following order:

1. Published RFC relationships and status from both the
   [RFC Editor RFC 4666 record](https://www.rfc-editor.org/rfc/rfc4666.json)
   and the [IETF Datatracker RFC 4666 record](https://datatracker.ietf.org/doc/rfc4666/).
2. Verified RFC errata as corrections to the published text.
3. Live IANA protocol registries for assigned numbers.
4. Held errata and active Internet-Drafts as non-normative implementation
   context.
5. Rejected errata and expired drafts as historical evidence only.

An Internet-Draft never overrides a published RFC. A Held for Document Update
erratum never becomes normative merely because this project adopts the proposed
interpretation.

## Current published documents

| Subject | Current authority | Relationship and project use |
| --- | --- | --- |
| M3UA | [RFC 4666](https://www.rfc-editor.org/rfc/rfc4666.html) | Proposed Standard. It obsoletes RFC 3332 and has no `Updated by` or `Obsoleted by` relationship in the [RFC Editor record](https://www.rfc-editor.org/rfc/rfc4666.json). The [Datatracker record](https://datatracker.ietf.org/doc/rfc4666/) and [incoming relationship API](https://datatracker.ietf.org/api/v1/doc/relateddocument/?target__name=rfc4666&limit=100&format=json) agree. |
| SIGTRAN security | [RFC 3788](https://www.rfc-editor.org/rfc/rfc3788.html) | Proposed Standard with no formal update or replacement. RFC 4666 [Section 6](https://www.rfc-editor.org/rfc/rfc4666.html#section-6) requires implementations to follow its normative guidance. Its concrete protocol references require the modernization described below. |
| SCTP | [RFC 9260](https://www.rfc-editor.org/rfc/rfc9260.html) | Current Proposed Standard SCTP base specification. It obsoletes RFCs 4460, 4960, 6096, 7053, and 8540 and has no later formal `Updated by` or `Obsoleted by` relationship in the [RFC Editor record](https://www.rfc-editor.org/rfc/rfc9260.json). |
| SCTP zero-checksum extension | [RFC 9653](https://www.rfc-editor.org/rfc/rfc9653.html) | Current optional extension for negotiated zero-checksum operation when an alternate error-detection method provides at least equivalent integrity. It does not formally update RFC 9260: the RFC Editor records and the Datatracker incoming-relationship API list no `Updates` relationship. |
| SCTP authenticated chunks | [RFC 4895](https://www.rfc-editor.org/rfc/rfc4895.html) | Current optional SCTP-AUTH extension. It protects selected SCTP chunks but does not provide M3UA payload confidentiality and is not a substitute for RFC 3788 transport or network protection. |
| TLS over SCTP | [RFC 3436](https://www.rfc-editor.org/rfc/rfc3436.html) | Current TLS-over-SCTP mapping, updated by [RFC 8996](https://www.rfc-editor.org/rfc/rfc8996.html) to prohibit TLS 1.0 and TLS 1.1. TLS support is optional under RFC 3788 [Section 7](https://www.rfc-editor.org/rfc/rfc3788.html#section-7). |
| DTLS over SCTP | [RFC 6083](https://www.rfc-editor.org/rfc/rfc6083.html) | Current DTLS-over-SCTP mapping, also updated by RFC 8996. It is not referenced by RFC 3788 and does not silently replace the RFC 3788 TLS procedure. |
| SCTP with IPsec | [RFC 3554](https://www.rfc-editor.org/rfc/rfc3554.html) | Current guidance for preserving SCTP multihoming with IPsec, referenced by RFC 3788 [Section 5](https://www.rfc-editor.org/rfc/rfc3788.html#section-5). |

The Datatracker document records, including its
[RFC 4666](https://datatracker.ietf.org/doc/rfc4666/) and
[RFC 3788](https://datatracker.ietf.org/doc/rfc3788/) records, agree with the
RFC Editor on the publication status of every document above. Its
incoming-relationship API agrees that RFC 4666, RFC 3788, and RFC 9260 have no
later formal update or replacement and that RFC 8996 updates RFC 3436 and RFC
6083. RFC 9653 extends SCTP without declaring an `Updates: 9260` relationship.
The same API does not return every update relationship shown by some supporting
RFC Editor records, including the RFC 6040 and RFC 7619 updates in the
[RFC 4301 RFC Editor record](https://www.rfc-editor.org/rfc/rfc4301.json), which
are absent from the matching
[Datatracker incoming-relationship API result](https://datatracker.ietf.org/api/v1/doc/relateddocument/?target__name=rfc4301&limit=100&format=json).
This is recorded as a database discrepancy; the modernization table uses the
RFC Editor's published relationships rather than treating an absent
Datatracker API result as evidence that no relationship exists.

The [SIGTRAN working group](https://datatracker.ietf.org/wg/sigtran/documents/)
is concluded and has no active M3UA Internet-Draft. The active
[TSVWG documents](https://datatracker.ietf.org/wg/tsvwg/documents/) include
`draft-ietf-tsvwg-sctp-dtls-chunk` and
`draft-ietf-tsvwg-dtls-chunk-key-management`. They may change future SCTP
security choices but are not normative for v1.2.0. The expired
`draft-ietf-tsvwg-dtls-over-sctp-bis` and
`draft-ietf-tsvwg-rfc4895-bis` are historical context only.

## RFC 4666 errata

The [RFC 4666 errata registry](https://www.rfc-editor.org/errata/rfc4666)
contains no Verified erratum.

| Errata ID | Status | Effect on this project |
| --- | --- | --- |
| [2065](https://www.rfc-editor.org/errata/eid2065) | Held for Document Update, Technical | Proposes making Routing Context conditional in Notify so an `Alternate ASP Active` notification cannot deactivate unrelated Application Servers. go-m3ua implements scoped Notify handling as a deliberate project interpretation. It is not described as a correction to RFC 4666. |
| [4475](https://www.rfc-editor.org/errata/eid4475) | Held for Document Update, Editorial | Proposes changing Routing Key Service Indicator padding in RFC 4666 Section 3.6.1 from the impossible `32-byte alignment` wording to `32-bit alignment`. go-m3ua uses the ordinary RFC 4666 Section 3.2 four-octet TLV alignment as an explicit project interpretation. |
| [2518](https://www.rfc-editor.org/errata/eid2518) | Rejected, Technical | Proposed replacing M3UA-specific RKM parameter tags with SUA common tags. The verifier rejected the non-interoperable change. go-m3ua retains the RFC 4666 and IANA M3UA assignments. |

Held and Rejected errata are reported here rather than applied as normative
text. Any later status change requires a fresh audit and a dedicated change.

## SCTP and security-extension errata

The current [RFC 9260 errata registry](https://www.rfc-editor.org/errata/rfc9260)
contains five Verified errata:

- [Errata 7148](https://www.rfc-editor.org/errata/eid7148) corrects INIT ACK
  `a_rwnd` handling while an Association is in `COOKIE-WAIT`.
- [Errata 7387](https://www.rfc-editor.org/errata/eid7387) corrects a restart
  example to use the T1-cookie timer after sending COOKIE ECHO.
- [Errata 8402](https://www.rfc-editor.org/errata/eid8402) distinguishes the
  T1-init timer for INIT from the T1-cookie timer for COOKIE ECHO.
- [Errata 7852](https://www.rfc-editor.org/errata/eid7852) requires verification
  tag validation before chunk processing or Association state changes.
- [Errata 7147](https://www.rfc-editor.org/errata/eid7147) is an editorial
  quotation correction with no protocol behavior change.

RFC 9260 [Errata 8772](https://www.rfc-editor.org/errata/eid8772) is Held for
Document Update and proposes repeating the generic chunk-length definition in
the SACK section. Errata
[7988](https://www.rfc-editor.org/errata/eid7988),
[8387](https://www.rfc-editor.org/errata/eid8387), and
[8774](https://www.rfc-editor.org/errata/eid8774) are Rejected and are not
applied.

go-m3ua v1.2.0 depends on
[`github.com/gomaja/go-sctp` v1.0.2](https://github.com/gomaja/go-sctp/releases/tag/v1.0.2),
which uses operating-system SCTP sockets rather than implementing the SCTP
packet state machine. INIT, COOKIE, SACK, verification-tag, retransmission, and
path procedures therefore belong to the deployed kernel's RFC 9260
implementation. go-sctp and go-m3ua remain responsible for setting socket
policy correctly, preserving SCTP notifications and errors, and not
contradicting those procedures. This boundary creates no go-sctp API gap for
the RFC 9260 errata listed above.

RFC 9653 [Section 7.1](https://www.rfc-editor.org/rfc/rfc9653.html#section-7.1)
defaults alternate error detection and zero-checksum acceptance to disabled.
go-m3ua v1.2.0 does not negotiate SCTP-over-DTLS or enable another alternate
error-detection method, so ordinary kernel CRC32c behavior remains in force.
The optional RFC 9653 socket control is therefore not required for this
release's RFC 4666 or RFC 9260 conformance claim.

Related extension registries contain no Verified erratum that changes this
release. RFC 4895 [Errata 995](https://www.rfc-editor.org/errata/eid995) is Held
and concerns an unused reference. RFC 6083
[Errata 5744](https://www.rfc-editor.org/errata/eid5744) is Held and proposes
clarifying key switchover; [Errata 6323](https://www.rfc-editor.org/errata/eid6323)
is Rejected. RFCs 3436, 3554, and 3788 have no listed errata.

## IANA contract

The live [Signaling User Adaptation Layer Assignments](https://www.iana.org/assignments/sigtran-adapt/sigtran-adapt.xhtml)
govern message classes, message types, and parameter tags. The implementation
uses the M3UA values assigned there and preserves unknown extensions according
to RFC 4666 [Section 3.1.2](https://www.rfc-editor.org/rfc/rfc4666.html#section-3.1.2),
[Section 3.2](https://www.rfc-editor.org/rfc/rfc4666.html#section-3.2), and
[Section 7.3](https://www.rfc-editor.org/rfc/rfc4666.html#section-7.3).

The remaining transport assignments are:

- SCTP Payload Protocol Identifier `3` is M3UA in the
  [SCTP Parameters registry](https://www.iana.org/assignments/sctp-parameters/sctp-parameters.xhtml#sctp-parameters-25).
  RFC 4666 [Section 7.1](https://www.rfc-editor.org/rfc/rfc4666.html#section-7.1)
  also permits PPID `0` as unspecified and forbids other PPIDs for M3UA.
- Service name `m3ua` and port `2905/sctp` are registered in the
  [Service Name and Transport Protocol Port Number Registry](https://www.iana.org/assignments/service-names-port-numbers/service-names-port-numbers.xhtml?search=2905).
  RFC 4666 [Section 7.2](https://www.rfc-editor.org/rfc/rfc4666.html#section-7.2)
  recommends that SGPs listen on that SCTP port and permits statically
  configured SCTP ports. The registry's TCP entry does not make TCP an M3UA
  transport; RFC 4666 defines M3UA over SCTP.

### RFC 3788 Security Message discrepancy

RFC 3788 [Section 6](https://www.rfc-editor.org/rfc/rfc3788.html#section-6)
says STARTTLS uses message class `10`, while
[Section 10](https://www.rfc-editor.org/rfc/rfc3788.html#section-10) says IANA
reserved message class `12`. The live IANA registry assigns Security Messages
class `12`, STARTTLS type `1`, and STARTTLS_ACK type `2`. No RFC 3788 erratum
resolves the conflict.

If optional STARTTLS support is added, go-m3ua will use registered class `12`
and will record that choice as an interoperability decision, not a silent
correction to RFC 3788. v1.2.0 does not claim STARTTLS support.

## Security modernization

RFC 3788 remains current, but several concrete protocols cited by its 2004
profile are obsolete. The RFC Editor does not list a document that formally
updates RFC 3788, so the following table is a deployment modernization policy,
not an invented standards relationship.

| RFC 3788 reference | Current document | Migration impact |
| --- | --- | --- |
| RFCs 2960 and 3309, SCTP | [RFC 9260](https://www.rfc-editor.org/rfc/rfc9260.html) | SCTP behavior must be evaluated against RFC 9260 and its Verified errata, not the obsolete SCTP documents named by RFC 3788. |
| RFC 3332, M3UA | [RFC 4666](https://www.rfc-editor.org/rfc/rfc4666.html) | M3UA protocol behavior uses RFC 4666 exclusively. |
| RFC 2401, IPsec architecture | [RFC 4301](https://www.rfc-editor.org/rfc/rfc4301.html), as updated by RFCs 6040 and 7619 | Network policy and security-association design belong to deployment configuration. |
| RFC 2406, ESP | [RFC 4303](https://www.rfc-editor.org/rfc/rfc4303.html) | Deployments requiring RFC 3788 confidentiality and integrity use current ESP, not RFC 2406. |
| RFCs 2407 and 2409, IPsec DOI and IKEv1 | [RFC 7296](https://www.rfc-editor.org/rfc/rfc7296.html) and its updates | Literal RFC 3788 Section 5 conformance still requires the obsolete IKEv1 Main Mode, Aggressive Mode, and Quick Mode capabilities because no RFC formally updates that requirement. The project-recommended secure deployment profile uses current IKEv2 policy and algorithms instead; it is deliberately non-equivalent and cannot by itself establish literal RFC 3788 conformance. |
| RFC 2246, TLS 1.0 | [RFC 9846](https://www.rfc-editor.org/rfc/rfc9846.html) | RFC 9846 is the current TLS 1.3 specification. RFC 3436 is updated by RFC 8996, which prohibits TLS 1.0 and TLS 1.1. The obsolete mandatory cipher suite text in RFC 3788 is not a modern deployment baseline. |
| RFC 3280, certificate profile | [RFC 5280](https://www.rfc-editor.org/rfc/rfc5280.html) and its updates | Certificate validation and trust policy belong to the security layer chosen by the deployment. |
| RFC 2560, OCSP | [RFC 6960](https://www.rfc-editor.org/rfc/rfc6960.html) and its updates | Revocation checking belongs to the deployment security layer. |
| Expired M2PA and SUA drafts | [RFC 4165](https://www.rfc-editor.org/rfc/rfc4165.html) and [RFC 3868](https://www.rfc-editor.org/rfc/rfc3868.html) | These references do not change M3UA behavior; they remove expired drafts from the security context. |

## Library and deployment boundary

go-m3ua is an M3UA protocol library. It is responsible for:

- strict RFC 4666 framing, parameter, state, scope, stream, and PPID handling;
- explicit role validation using ASP, SGP, IPSP, AS, SG, and Association
  names from RFC 4666 [Section 1.2](https://www.rfc-editor.org/rfc/rfc4666.html#section-1.2);
- bounded peer-controlled parsing, queues, timers, route state, and error
  generation;
- opt-in, classified compatibility exceptions that do not weaken unrelated
  validation; and
- APIs that let the application bind an accepted Association to an authorized
  peer policy before M3UA parsing begins.

The deployment is responsible for:

- IPsec ESP and IKE support required of a SIGTRAN node by RFC 3788
  [Section 7](https://www.rfc-editor.org/rfc/rfc3788.html#section-7), supplied
  by the host or another deployment layer even when policy does not enable
  IPsec on every Association;
- the legacy IKEv1, IPsec DOI, and Quick Mode capabilities required by RFC 3788
  [Section 5](https://www.rfc-editor.org/rfc/rfc3788.html#section-5) if the
  operator makes a literal RFC 3788 conformance claim, or an explicit
  non-equivalent modernization decision to use current IKEv2 instead;
- optional TLS, DTLS, private-network, or additional protection selected by
  deployment policy, none of which removes the mandatory IPsec-support
  requirement;
- mutual peer authentication, keys, certificates, trust anchors, revocation,
  algorithm policy, rekeying, and secret storage;
- firewalling, routing, anti-spoofing, rate limits outside the process, and
  control-plane isolation;
- authorization of configured SCTP addresses, Network Appearances, Routing
  Keys, Routing Contexts, and ASP Identifiers; and
- operational monitoring, audit retention, incident response, and regulatory
  requirements.

Using go-m3ua alone does not make a node compliant with RFC 3788. A literal
conformance claim requires the application and deployment to supply every
legacy capability that current RFC 3788 still mandates. A deployment that
instead follows the project-recommended current IKEv2 profile must describe it
as a secure modernization, not as literal RFC 3788 conformance. The library
must not embed keys, create implicit trust, downgrade protection, or claim that
SCTP checksum, cookie, multihoming, or heartbeat behavior provides
confidentiality or peer authentication.
