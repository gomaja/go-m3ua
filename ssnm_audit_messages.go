package m3ua

import (
	"encoding/binary"
	"errors"

	"github.com/gomaja/go-m3ua/messages"
	"github.com/gomaja/go-m3ua/messages/params"
)

func (c *Association) auditAffectedPointCodeLimit() int {
	// The APC TLV's 16-bit length includes its four-byte header.
	return min(c.ssnmAffectedPointCodeLimit(), (65535-4)/4)
}

type auditMessageKey struct {
	kind     uint8
	level    uint8
	levelSet bool
	contexts string
}
type auditMessageGroup struct {
	key      auditMessageKey
	contexts []uint32
	pcs      []uint32
}

func (c *Association) destinationAuditResponse(d *messages.DestinationStateAudit, pcs []uint32, masks []uint8) ([]messages.M3UA, error) {
	records, restarts, limit := c.destinationAuditSnapshot()
	entries, err := c.planDestinationAudit(d, pcs, masks, records, restarts, limit)
	if err != nil {
		return nil, err
	}
	budget := destinationAuditMessageBudget(d, entries, limit, len(pcs), c.auditAffectedPointCodeLimit())
	return buildDestinationAuditMessages(d, entries, c.auditAffectedPointCodeLimit(), budget)
}

const auditMaxMessage = DefaultReadBufferSize &^ 3
const auditRoutingContextChunk = (auditMaxMessage - 8 - 8 - 4 - 4 - 8 - 4) / 4

func auditMessageHeader(d *messages.DestinationStateAudit, contexts int, congestion bool) int {
	header := 8 + 4 // Common header and APC TLV header.
	if d.NetworkAppearance != nil {
		header += 8
	}
	if d.RoutingContext != nil {
		header += 4 + 4*contexts
	}
	if congestion {
		header += 8
	}
	return header
}

// Derive both wire bounds from the store's configured record limit, rather than
// imposing an entry cap. Complete scope plans form at most one RC set per scope.
// Charge each set/chunk for two APC lists per piece and 48 state-group headers
// per pass (more than the eight encoded availability/congestion groups).
// Header-sensitive chunk sizes also cover large RC lists and configured APC
// limits. Ordinary retained/affected/completed state cannot exhaust this budget.
func destinationAuditMessageBudget(d *messages.DestinationStateAudit, entries []destinationAuditReply, records, requests, limit int) auditReplyBudget {
	pieces := destinationAuditPieces(records, requests)
	seen := make(map[string]bool)
	var budget auditReplyBudget
	for _, entry := range entries {
		key := auditContextKey(entry.routingContexts)
		if seen[key] {
			continue
		}
		seen[key] = true
		for offset := 0; offset < max(1, len(entry.routingContexts)); offset += auditRoutingContextChunk {
			count := min(auditRoutingContextChunk, len(entry.routingContexts)-offset)
			header := auditMessageHeader(d, count, true)
			chunk := min(limit, (auditMaxMessage-header)/4)
			chunks := pieces / uint64(chunk)
			if pieces%uint64(chunk) != 0 {
				chunks++
			}
			messages := auditMultiply(2, auditAdd(chunks, 48))
			budget.messages = auditAdd(budget.messages, messages)
			budget.bytes = auditAdd(budget.bytes, auditAdd(auditMultiply(8, pieces), auditMultiply(uint64(header), messages)))
		}
	}
	return budget
}

func auditContextKey(contexts []uint32) string {
	key := make([]byte, 4*len(contexts))
	for i, rc := range contexts {
		binary.BigEndian.PutUint32(key[4*i:], rc)
	}
	return string(key)
}

// RFC 4666 §4.5.3 explicitly permits DUNA/DAVA lists and requires SCON before
// the availability it qualifies. Section 3.4.4 permits congestion of multiple
// destinations/ranges; §3.4.6 gives DRST DUNA's APC format and description.
// Group SCON by congestion alone and availability by its message type. Sending
// all SCON groups first preserves the required order even across list chunks.
func buildDestinationAuditMessages(d *messages.DestinationStateAudit, entries []destinationAuditReply, limit int, budget auditReplyBudget) ([]messages.M3UA, error) {
	var groups []auditMessageGroup
	indices := make(map[auditMessageKey]int)
	add := func(key auditMessageKey, contexts []uint32, pc uint32) {
		if index, ok := indices[key]; ok {
			groups[index].pcs = append(groups[index].pcs, pc)
			return
		}
		indices[key] = len(groups)
		groups = append(groups, auditMessageGroup{key, contexts, []uint32{pc}})
	}
	for _, r := range entries {
		scope := auditContextKey(r.routingContexts)
		pc := uint32(r.mask)<<24 | r.pointCode
		kind := uint8(messages.MsgTypeDestinationAvailable)
		switch r.state.Availability {
		case DestinationUnavailable:
			kind = messages.MsgTypeDestinationUnavailable
		case DestinationRestricted:
			kind = messages.MsgTypeDestinationRestricted
		}
		add(auditMessageKey{kind: kind, contexts: scope}, r.routingContexts, pc)
		if r.state.Availability != DestinationUnavailable && r.state.Congestion.Congested {
			level := r.state.Congestion.Level
			if !r.state.Congestion.LevelSet {
				level = 0
			}
			add(auditMessageKey{messages.MsgTypeSignallingCongestion, level, r.state.Congestion.LevelSet, scope}, r.routingContexts, pc)
		}
	}
	var replies []messages.M3UA
	var bytes uint64
	appendMessage := func(m messages.M3UA) error {
		size := uint64(m.MarshalLen())
		if uint64(len(replies)) >= budget.messages || size > budget.bytes-bytes {
			return ErrSSNMResourceLoss
		}
		bytes += size
		replies = append(replies, m)
		return nil
	}
	for _, congestionPass := range []bool{true, false} {
		for _, group := range groups {
			if (group.key.kind == messages.MsgTypeSignallingCongestion) != congestionPass {
				continue
			}
			// Keep complete messages within the existing default read size as well
			// as the APC/RC TLV limits. Very large RC sets are split independently.
			const rcLimit = auditRoutingContextChunk
			for rcOffset := 0; rcOffset < max(1, len(group.contexts)); rcOffset += rcLimit {
				contexts := group.contexts
				if len(contexts) > 0 {
					contexts = contexts[rcOffset:min(len(contexts), rcOffset+rcLimit)]
				}
				header := auditMessageHeader(d, len(contexts), congestionPass && group.key.levelSet)
				chunk := min(limit, (auditMaxMessage-header)/4)
				if chunk < 1 {
					return nil, ErrSSNMResourceLoss
				}
				for offset := 0; offset < len(group.pcs); offset += chunk {
					end := min(len(group.pcs), offset+chunk)
					apc := params.NewAffectedPointCode(group.pcs[offset:end]...)
					var rc *params.Param
					if d.RoutingContext != nil {
						rc = params.NewRoutingContext(contexts...)
					}
					var m messages.M3UA
					switch group.key.kind {
					case messages.MsgTypeSignallingCongestion:
						var level *params.Param
						if group.key.levelSet {
							level = params.NewCongestionIndications(group.key.level)
						}
						m = messages.NewSignallingCongestion(d.NetworkAppearance.Copy(), rc, apc, nil, level, nil)
					case messages.MsgTypeDestinationUnavailable:
						m = messages.NewDestinationUnavailable(d.NetworkAppearance.Copy(), rc, apc, nil)
					case messages.MsgTypeDestinationRestricted:
						m = messages.NewDestinationRestricted(d.NetworkAppearance.Copy(), rc, apc, nil)
					default:
						m = messages.NewDestinationAvailable(d.NetworkAppearance.Copy(), rc, apc, nil)
					}
					if err := appendMessage(m); err != nil {
						return nil, err
					}
				}
			}
		}
	}
	return replies, nil
}

// RFC 4666 §3.8.1 permits Destination Status Unknown when an SG declines an
// audit. Preserve every requested APC plus its associated NA/RC; send no status prefix.
// The request report remains published, and Layer Management sees the refusal.
func (c *Association) refuseDestinationAudit(d *messages.DestinationStateAudit) error {
	networkAppearance, routingContext := d.NetworkAppearance.Copy(), d.RoutingContext.Copy()
	// Section 3.8.1 requires the associated NA and/or RC in this error,
	// including scope supplied by configuration when the DAUD omits it.
	if networkAppearance == nil {
		if appearance, set := c.outboundNetworkAppearance(); set {
			networkAppearance = params.NewNetworkAppearance(appearance)
		}
	}
	if routingContext == nil {
		if contexts, set := c.destinationRoutingContexts(nil); set {
			routingContext = params.NewRoutingContext(contexts...)
		}
	}
	scope := c.ssnmWireScope(networkAppearance, routingContext)
	keys := c.managementASKeys(networkAppearance, routingContext)
	c.notifyManagement(&ManagementIndication{Kind: ManagementError, ErrorCode: params.ErrDestinationStatusUnknown, Description: "destination audit exceeds local reply message/byte budget", RoutingContexts: scope.RoutingContexts, NetworkAppearance: scope.NetworkAppearance, NetworkAppearanceSet: scope.NetworkAppearanceSet, ASKeys: keys, AffectedDestinations: managementAffectedDestinations(keys, d.AffectedPointCode)})
	_, err := c.writeControl(messages.NewError(params.NewErrorCode(params.ErrDestinationStatusUnknown), routingContext, networkAppearance, d.AffectedPointCode.Copy(), nil))
	return err
}

func auditResourceError(err error) bool { return errors.Is(err, ErrSSNMResourceLoss) }
