package m3ua

// The first configured match remains definitive, including when an omitted
// appearance precedes an explicit one for the same Routing Context.
type receivedStaticASCandidate struct {
	key   ASKey
	order int
}
type receivedStaticASIndex map[ASKey]receivedStaticASCandidate

func newReceivedStaticASIndex(keys []ASKey, work *ssnmWorkCount) receivedStaticASIndex {
	index := make(receivedStaticASIndex, len(keys))
	for i, key := range keys {
		work.visitAS()
		normalized := key
		if !normalized.NetworkAppearanceSet {
			normalized.NetworkAppearance = 0
		}
		if _, held := index[normalized]; !held {
			index[normalized] = receivedStaticASCandidate{key, i}
		}
	}
	return index
}
func (index receivedStaticASIndex) find(key ASKey, work *ssnmWorkCount) (ASKey, bool) {
	wildcard := key
	wildcard.NetworkAppearance, wildcard.NetworkAppearanceSet = 0, false
	work.visitAS()
	candidate, held := index[wildcard]
	if key.NetworkAppearanceSet {
		work.visitAS()
		exact, present := index[key]
		if present && (!held || exact.order < candidate.order) {
			candidate, held = exact, true
		}
	}
	return candidate.key, held && receivedStaticASMatches(candidate.key, key)
}
