package m3ua

// Sparse prefixes carry evidence and job keys, never individual point codes.
// Every insertion follows at most24 edges in the RFC 4666 §3.4.1 point-code domain.
type auditPrefixNode struct {
	children                    [2]*auditPrefixNode
	statement                   auditRangeStatement
	hasStatement                bool
	clearRevision, minimumClear uint64
	jobs                        []destinationAuditKey
}

func (n *auditPrefixNode) insert(destination ssnmDestinationKey, work *ssnmWorkCount) *auditPrefixNode {
	for mask := uint8(maxPointCodeMask); ; mask-- {
		work.visitPrefix()
		if mask <= destination.mask {
			return n
		}
		branch := destination.pointCode >> (mask - 1) & 1
		if n.children[branch] == nil {
			n.children[branch] = &auditPrefixNode{}
		}
		n = n.children[branch]
	}
}
func (n *auditPrefixNode) add(statement auditRangeStatement, work *ssnmWorkCount) {
	node := n.insert(statement.destination, work)
	if !node.hasStatement || statement.revision > node.statement.revision {
		node.statement = statement
		node.hasStatement = true
	}
}
func (n *auditPrefixNode) summarize(inherited auditRangeStatement, work *ssnmWorkCount) {
	work.visitPrefix()
	if n.hasStatement && n.statement.revision > inherited.revision {
		inherited = n.statement
	}
	n.clearRevision = 0
	if inherited.revision > 0 && !inherited.needed {
		n.clearRevision = inherited.revision
	}
	n.minimumClear = n.clearRevision
	for branch, child := range n.children {
		minimum := n.clearRevision
		if child != nil {
			child.summarize(inherited, work)
			minimum = child.minimumClear
		}
		if branch == 0 || minimum < n.minimumClear {
			n.minimumClear = minimum
		}
	}
}
func (n *auditPrefixNode) clearAt(destination ssnmDestinationKey, work *ssnmWorkCount) uint64 {
	for mask := uint8(maxPointCodeMask); ; mask-- {
		work.visitPrefix()
		if mask <= destination.mask {
			return n.minimumClear
		}
		child := n.children[destination.pointCode>>(mask-1)&1]
		if child == nil {
			return n.clearRevision
		}
		n = child
	}
}
func (n *auditPrefixNode) relation(destination ssnmDestinationKey, work *ssnmWorkCount) (covered, intersects bool) {
	for mask := uint8(maxPointCodeMask); ; mask-- {
		work.visitPrefix()
		if n.hasStatement {
			return true, true
		}
		if mask <= destination.mask {
			return false, n.children[0] != nil || n.children[1] != nil
		}
		child := n.children[destination.pointCode>>(mask-1)&1]
		if child == nil {
			return false, false
		}
		n = child
	}
}
func (n *auditPrefixNode) coveredJobs(destination ssnmDestinationKey, work *ssnmWorkCount) []destinationAuditKey {
	for mask := uint8(maxPointCodeMask); ; mask-- {
		work.visitPrefix()
		if mask <= destination.mask {
			break
		}
		n = n.children[destination.pointCode>>(mask-1)&1]
		if n == nil {
			return nil
		}
	}
	var keys []destinationAuditKey
	var collect func(*auditPrefixNode)
	collect = func(node *auditPrefixNode) {
		work.visitPrefix()
		keys = append(keys, node.jobs...)
		for _, child := range node.children {
			if child != nil {
				collect(child)
			}
		}
	}
	collect(n)
	return keys
}
