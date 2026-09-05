package sessionv2

import (
	"bytes"
	"errors"
	"slices"
	"time"
	"unsafe"

	"github.com/mofelee/mpudp/internal/creditv2"
	"github.com/mofelee/mpudp/internal/fecv2"
	"github.com/mofelee/mpudp/internal/recvwindow"
	"github.com/mofelee/mpudp/internal/wirev2"
)

type pendingGroup struct {
	logical   uint32
	context   wirev2.EncodingContext
	admitted  time.Time
	shards    [][]byte
	present   int
	fragments []fecv2.Fragment
	lease     *creditv2.Lease
	previous  uint64
	next      uint64
}

const pendingGroupMetadataBytes = uint64(unsafe.Sizeof(pendingGroup{})) + 64

func (c *Controller) sendLookup(epoch uint32) (wirev2.EncodingContext, bool) {
	return c.sendContext, c.contextAcknowledged && epoch == c.sendContext.Epoch
}

func (c *Controller) receiveLookup(epoch uint32) (wirev2.EncodingContext, bool) {
	return c.receiveContext, c.receiveAckSent && epoch == c.receiveContext.Epoch
}

func (c *Controller) driveData(now time.Time, result *Result) error {
	for len(result.Sends) < MaxSendsPerStep {
		if c.out == nil {
			var active [256]uint16
			count := 0
			for i := range c.paths {
				if c.eligible(&c.paths[i]) {
					active[count] = c.paths[i].id
					count++
				}
			}
			if count == 0 {
				return ErrNotReady
			}
			output, err := c.queue.SealWithWorkspace(now, c.forced > c.completed, c.outputWorkspace)
			if err != nil || output == nil {
				return err
			}
			c.out, c.outNext = output, 0
			view, _ := output.View()
			for i := range view.Group.Shards {
				c.outPaths[i] = active[(int(view.GroupID%uint64(count))+i)%count]
			}
			q := c.queue.Snapshot()
			c.outThrough = q.NextDatagramID - 1 - uint64(q.QueuedDatagrams)
		}
		view, ok := c.out.View()
		if !ok {
			return ErrInvalid
		}
		p := &c.paths[c.outPaths[c.outNext]-1]
		if now.Before(p.pacedAt) {
			return nil
		}
		if !c.eligible(p) {
			for i := range c.paths {
				if c.eligible(&c.paths[i]) {
					p = &c.paths[i]
					c.outPaths[c.outNext] = p.id
					break
				}
			}
			if !c.eligible(p) {
				return ErrNotReady
			}
		}
		// InitialAssembly keeps the conservative two-packet allowance charged
		// between synchronous sends; original admission cannot consume it.
		bundle := wirev2.FECBundle{Header: wirev2.Header{Type: wirev2.TypeFECBundle, SessionID: c.setup.ID}, Route: p.route(), Records: []wirev2.FECRecord{{GroupID: view.GroupID, EncodingEpoch: view.EncodingEpoch, LogicalBytes: view.Group.LogicalBytes, ShardIndex: uint8(c.outNext), Payload: view.Group.Shards[c.outNext]}}}
		packet, err := c.sendAuth.AppendFECBundle(nil, bundle, c.sendLookup, int(p.sendBudget))
		if err != nil {
			return err
		}
		sent := c.emit(p, p.sender, p.binding, packet, now, result)
		clear(packet)
		if !sent {
			return nil
		}
		if failure := result.Sends[len(result.Sends)-1].Err; failure != nil && c.sticky == nil {
			c.sticky, c.failedFrom = failure, c.completed+1
		}
		c.outNext++
		if c.outNext == len(view.Group.Shards) {
			c.completed = c.outThrough
			c.out.Release()
			c.out, c.outNext = nil, 0
		}
	}
	return nil
}

func (c *Controller) reserveGroup(record wirev2.FECRecord, now time.Time) (*pendingGroup, error) {
	if len(c.groups) == c.cfg.MaxPendingGroups {
		return nil, creditv2.ErrResourceLimit
	}
	context := c.receiveContext
	n := uint64(context.DataShards) + uint64(context.ParityShards)
	// Reserve retained shards, concurrent reconstruction workspace, owned
	// decoded logical bytes, descriptor/slice storage, and bounded map metadata.
	charge := 2*n*uint64(context.ShardBytes) + uint64(context.MaxLogicalBytes) + 2*n*uint64(unsafe.Sizeof([]byte{})) + uint64(context.MaxDescriptors)*uint64(unsafe.Sizeof(fecv2.Fragment{})) + pendingGroupMetadataBytes
	lease, err := c.setup.Scope.Reserve(creditv2.Claim{Bytes: charge})
	if err != nil {
		return nil, err
	}
	return &pendingGroup{logical: record.LogicalBytes, context: context, admitted: now, shards: make([][]byte, int(n)), lease: lease}, nil
}

func (c *Controller) insertGroup(id uint64, group *pendingGroup) {
	// enter enforces monotonic admission times; the fixed timeout preserves
	// this order for expiry even when GroupIDs arrive out of order.
	group.previous = c.groupTail
	if c.groupTail == 0 {
		c.groupHead = id
	} else {
		c.groups[c.groupTail].next = id
	}
	c.groupTail = id
	c.groups[id] = group
}

func (c *Controller) receiveBundle(now time.Time, envelope wirev2.AuthenticatedEnvelope, budget int, restricted bool, result *Result) error {
	c.receiveCounters.ReceivedFECBundles++
	if !c.receiveAckSent {
		return ErrNotReady
	}
	// InitialControl covers one receive-hard-cap payload and a full record
	// array. Receive serializes use; discard both before that workspace is reused.
	bundle, err := wirev2.DecodeFECBundle(envelope, c.receiveLookup, budget)
	if err != nil {
		return err
	}
	defer func() {
		for _, record := range bundle.Records {
			clear(record.Payload)
		}
		bundle.Records = nil
	}()
	for _, record := range bundle.Records {
		group := c.groups[record.GroupID]
		if group == nil {
			if restricted || c.groupWindow.State(record.GroupID) != recvwindow.Unseen {
				continue
			}
			group, err = c.reserveGroup(record, now)
			if err != nil {
				if errors.Is(err, creditv2.ErrResourceLimit) {
					c.receiveCounters.NewGroupRejections++
				}
				return err
			}
			c.groupWindow.Admit(record.GroupID)
			c.insertGroup(record.GroupID, group)
		}
		if !now.Before(group.admitted.Add(c.cfg.GroupTimeout)) {
			c.expireGroup(record.GroupID, group)
			continue
		}
		if group.logical != record.LogicalBytes || group.context.Epoch != record.EncodingEpoch {
			return ErrProtocol
		}
		if group.fragments != nil {
			if err := c.admitOriginals(record.GroupID, group, now, result); err != nil {
				return err
			}
			continue
		}
		if previous := group.shards[record.ShardIndex]; previous != nil {
			if !bytes.Equal(previous, record.Payload) {
				return ErrProtocol
			}
			continue
		}
		group.shards[record.ShardIndex] = slices.Clone(record.Payload)
		group.present++
		if group.present >= int(group.context.DataShards) {
			fragments, err := c.receiveCodec.Decode(group.logical, group.shards)
			if err != nil {
				c.expireGroup(record.GroupID, group)
				return err
			}
			c.receiveCounters.DecodedGroups++
			for _, shard := range group.shards {
				clear(shard)
			}
			group.shards, group.fragments = nil, fragments
			c.decodedGroups++
			// Decode owns exactly logical bytes, including the manifest prefix,
			// plus the returned descriptor backing array. Its reconstruction
			// workspace and our shards are gone before returning their credit.
			retained := uint64(group.logical) + uint64(cap(fragments))*uint64(unsafe.Sizeof(fecv2.Fragment{})) + pendingGroupMetadataBytes
			if err := group.lease.ShrinkBytes(retained); err != nil {
				c.expireGroup(record.GroupID, group)
				return err
			}
			if err := c.admitOriginals(record.GroupID, group, now, result); err != nil {
				return err
			}
		}
	}
	return nil
}

func (c *Controller) admitOriginals(id uint64, group *pendingGroup, now time.Time, result *Result) error {
	deliveries, err := c.originals.AddGroup(now, group.fragments)
	if err != nil {
		if errors.Is(err, creditv2.ErrResourceLimit) {
			c.receiveCounters.OriginalAdmissionRejections++
			c.retryStorage = now.Add(time.Millisecond)
			return nil
		}
		c.expireGroup(id, group)
		return err
	}
	result.Deliveries = append(result.Deliveries, deliveries...)
	c.groupWindow.Finish(id, recvwindow.Completed)
	c.receiveCounters.CompletedGroups++
	c.releaseGroup(id, group)
	return nil
}

func (c *Controller) retryGroups(now time.Time, result *Result) error {
	if c.decodedGroups == 0 || now.Before(c.retryStorage) {
		return nil
	}
	for id, group := range c.groups {
		if group.fragments != nil {
			if err := c.admitOriginals(id, group, now, result); err != nil {
				return err
			}
		}
	}
	return nil
}

func (c *Controller) releaseGroup(id uint64, group *pendingGroup) {
	if group.previous == 0 {
		c.groupHead = group.next
	} else {
		c.groups[group.previous].next = group.next
	}
	if group.next == 0 {
		c.groupTail = group.previous
	} else {
		c.groups[group.next].previous = group.previous
	}
	delete(c.groups, id)
	group.previous, group.next = 0, 0
	if group.fragments != nil {
		c.decodedGroups--
	}
	for _, shard := range group.shards {
		clear(shard)
	}
	for _, fragment := range group.fragments {
		clear(fragment.Payload)
	}
	group.shards, group.fragments = nil, nil
	group.lease.Release()
	group.lease = nil
}

func (c *Controller) expireGroups(now time.Time) {
	for c.groupHead != 0 {
		id := c.groupHead
		group := c.groups[id]
		if now.Before(group.admitted.Add(c.cfg.GroupTimeout)) {
			break
		}
		c.expireGroup(id, group)
	}
}

func (c *Controller) expireGroup(id uint64, group *pendingGroup) {
	c.groupWindow.Finish(id, recvwindow.Expired)
	c.receiveCounters.ExpiredGroups++
	c.releaseGroup(id, group)
}
