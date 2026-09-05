package mpudp

func (r *v2Peer) receiveStatistics() *V2ReceiveStatistics {
	r.mu.Lock()
	defer r.mu.Unlock()
	totals := r.retiredReceive
	var pendingGroups, decodedPendingGroups, pendingOriginals int64
	for session := range r.sessions {
		if session.controller == nil || session.closed {
			continue
		}
		s := session.controller.ReceiveStatistics()
		totals.Add(s.ReceiveCounters)
		pendingGroups += int64(s.PendingGroups)
		decodedPendingGroups += int64(s.DecodedPendingGroups)
		pendingOriginals += int64(s.PendingOriginals)
	}
	credit := r.credits.Snapshot()
	return &V2ReceiveStatistics{
		ReceivedFECBundles: totals.ReceivedFECBundles, PacketScratchRejections: totals.PacketScratchRejections,
		NewGroupRejections: totals.NewGroupRejections, OriginalAdmissionRejections: totals.OriginalAdmissionRejections,
		DecodedGroups: totals.DecodedGroups, CompletedGroups: totals.CompletedGroups, ExpiredGroups: totals.ExpiredGroups,
		PendingGroups: pendingGroups, DecodedPendingGroups: decodedPendingGroups, PendingOriginals: pendingOriginals,
		CreditBytes: credit.Bytes, CreditReservations: int64(credit.Reservations),
	}
}
