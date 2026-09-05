package sessionv2

// ReceiveCounters are cumulative authenticated FEC handler events. The owner
// serializes mutations and sampling with all other Controller operations.
type ReceiveCounters struct {
	ReceivedFECBundles          uint64
	PacketScratchRejections     uint64
	NewGroupRejections          uint64
	OriginalAdmissionRejections uint64
	DecodedGroups               uint64
	CompletedGroups             uint64
	ExpiredGroups               uint64
}

func (c *ReceiveCounters) Add(other ReceiveCounters) {
	c.ReceivedFECBundles += other.ReceivedFECBundles
	c.PacketScratchRejections += other.PacketScratchRejections
	c.NewGroupRejections += other.NewGroupRejections
	c.OriginalAdmissionRejections += other.OriginalAdmissionRejections
	c.DecodedGroups += other.DecodedGroups
	c.CompletedGroups += other.CompletedGroups
	c.ExpiredGroups += other.ExpiredGroups
}

type ReceiveStatistics struct {
	ReceiveCounters
	PendingGroups        int
	DecodedPendingGroups int
	PendingOriginals     int
}

// ReceiveStatistics samples fixed-size counters and current gauges without
// copying paths or inspecting deadlines. Counters survive Controller.Close.
func (c *Controller) ReceiveStatistics() ReceiveStatistics {
	if c == nil {
		return ReceiveStatistics{}
	}
	s := ReceiveStatistics{ReceiveCounters: c.receiveCounters, PendingGroups: len(c.groups), DecodedPendingGroups: c.decodedGroups}
	if c.originals != nil {
		s.PendingOriginals = c.originals.Snapshot().Pending
	}
	return s
}
