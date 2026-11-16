package m3ua

import "testing"

// The oracle enumerates only a synthetic six-bit domain. Production indexes
// never enumerate a point code or depend on the number of points in a range.
func auditCoveragePointOracle(target ssnmDestinationKey, statements []auditRangeStatement, after uint64) bool {
	for pc := target.pointCode; pc < target.pointCode+uint32(1)<<target.mask; pc++ {
		latest, needed := after, true
		for _, statement := range statements {
			if statement.revision > latest && destinationRangePrefix(pc, statement.destination.mask) == statement.destination.pointCode {
				latest, needed = statement.revision, statement.needed
			}
		}
		if needed {
			return true
		}
	}
	return false
}
func TestAuditCoverageSummaryMatchesPointOracle(t *testing.T) {
	var random uint32 = 17
	next := func() uint32 { random = random*1664525 + 1013904223; return random }
	for sample := 0; sample < 128; sample++ {
		var statements []auditRangeStatement
		tree := &auditPrefixNode{}
		for i := 0; i < 32; i++ {
			mask := uint8(next() % 25)
			statement := auditRangeStatement{destination: ssnmDestinationKeyFor(PointCodeRange{PointCode: next() % 64, Mask: mask}), revision: uint64(i + 1), needed: next()%3 == 0}
			statements = append(statements, statement)
			tree.add(statement, nil)
		}
		tree.summarize(auditRangeStatement{needed: true}, nil)
		for mask := uint8(0); mask <= 6; mask++ {
			for pc := uint32(0); pc < 64; pc += uint32(1) << mask {
				target := ssnmDestinationKey{pointCode: pc, mask: mask}
				for after := uint64(0); after <= 33; after++ {
					got := tree.clearAt(target, nil) <= after
					if want := auditCoveragePointOracle(target, statements, after); got != want {
						t.Fatalf("sample%d target%+v after%d: indexed%v point-oracle%v", sample, target, after, got, want)
					}
				}
			}
		}
	}
}
func FuzzAuditCoverageSummaryMatchesPointOracle(f *testing.F) {
	f.Add([]byte{6, 0, 0, 24, 1, 1, 0, 0, 0, 0, 1})
	f.Add([]byte{1, 0, 0, 1, 0, 0, 0, 0, 0, 1, 1, 0})
	f.Fuzz(func(t *testing.T, input []byte) {
		if len(input) < 2 {
			return
		}
		target := ssnmDestinationKeyFor(PointCodeRange{PointCode: uint32(input[0] % 64), Mask: input[1] % 7})
		var statements []auditRangeStatement
		tree := &auditPrefixNode{}
		for start := 2; start+2 < len(input) && start < 98; start += 3 {
			statement := auditRangeStatement{destination: ssnmDestinationKeyFor(PointCodeRange{PointCode: uint32(input[start] % 64), Mask: input[start+1] % 25}), revision: uint64(len(statements) + 1), needed: input[start+2]&1 != 0}
			statements = append(statements, statement)
			tree.add(statement, nil)
		}
		tree.summarize(auditRangeStatement{needed: true}, nil)
		after := uint64(input[0]) % uint64(len(statements)+2)
		if got, want := tree.clearAt(target, nil) <= after, auditCoveragePointOracle(target, statements, after); got != want {
			t.Fatalf("target%+v after%d: indexed%v point-oracle%v", target, after, got, want)
		}
	})
}
