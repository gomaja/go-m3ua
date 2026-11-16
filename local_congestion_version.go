package m3ua

// Overlapping local marks use an active-writer count, so two concurrent
// writers cannot accidentally expose a stable generation between their flips.
// None of these helpers waits for state, a scheduler or transport progress.
func (c *Association) beginLocalInputChange() {
	c.localInputChanges.Add(1)
	c.localInputGeneration.Add(1)
}
func (c *Association) finishLocalInputChange() {
	c.localInputGeneration.Add(1)
	c.localInputChanges.Add(-1)
}
func (c *Association) localCongestionInputGeneration() uint64 {
	before := c.localInputGeneration.Load()
	changing := c.localInputChanges.Load() != 0
	after := c.localInputGeneration.Load()
	value := after << 1
	if changing || before != after {
		value |= 1
	}
	return value
}
