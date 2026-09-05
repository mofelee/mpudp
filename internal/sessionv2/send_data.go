package sessionv2

import (
	"time"

	"github.com/mofelee/mpudp/internal/transport"
	"github.com/mofelee/mpudp/internal/wirev2"
)

func (c *Controller) driveOwned(now time.Time, result *Result) error {
	if c.sends == nil || c.sends.closing {
		return ErrClosed
	}
	if err := c.prepareOwnedControls(now, result); err != nil {
		return err
	}
	c.expireOwnedData(now, result)
	if c.contextAcknowledged && !c.ready() && !c.pendingPathWork() {
		return transport.ErrNoAvailablePaths
	}
	if !c.ready() || c.out != nil {
		return nil
	}
	output, err := c.queue.SealWithWorkspace(now, c.forced > c.completed, c.outputWorkspace)
	if err != nil || output == nil {
		return err
	}
	c.out = output
	view, _ := output.View()
	c.sends.remaining, c.sends.groupQueued, c.sends.cursor = len(view.Group.Shards), now, 0
	clear(c.sends.shards[:])
	clear(c.outPaths[:])
	q := c.queue.Snapshot()
	c.outThrough = q.NextDatagramID - 1 - uint64(q.QueuedDatagrams)
	return nil
}

func (c *Controller) completeOwnedShard(shard int, failure error) {
	if c.sends.shards[shard] == shardTerminal {
		return
	}
	c.sends.shards[shard] = shardTerminal
	c.sends.remaining--
	if failure != nil && c.sticky == nil {
		c.sticky, c.failedFrom = failure, c.completed+1
	}
	if c.sends.remaining == 0 {
		c.completed = c.outThrough
		c.out.Release()
		c.out, c.outNext = nil, 0
		c.sends.groupQueued = time.Time{}
	}
}

func (c *Controller) expireOwnedData(now time.Time, result *Result) {
	if c.out == nil || now.Before(c.sends.groupQueued.Add(c.cfg.MaxQueueResidence)) {
		return
	}
	view, _ := c.out.View()
	for i := range view.Group.Shards {
		if c.sends.shards[i] != shardWaiting {
			continue
		}
		if len(result.Sends) == MaxSendsPerStep {
			return
		}
		result.Sends = append(result.Sends, SendAttempt{Type: wirev2.TypeFECBundle, PathID: c.outPaths[i], Bytes: int(c.sendContext.ShardBytes) + 94, Err: ErrSendExpired})
		c.completeOwnedShard(i, ErrSendExpired)
	}
}

func (c *Controller) takeOwnedData(now time.Time, slot *sendSlot) (*SendIntent, error) {
	if c.out == nil || !now.Before(c.sends.groupQueued.Add(c.cfg.MaxQueueResidence)) {
		return nil, nil
	}
	view, ok := c.out.View()
	if !ok {
		return nil, ErrInvalid
	}
	packetBytes := int(c.sendContext.ShardBytes) + 94
	for offset := range view.Group.Shards {
		shard := (c.sends.cursor + offset) % len(view.Group.Shards)
		if c.sends.shards[shard] != shardWaiting {
			continue
		}
		p := c.preferredOwnedPath(view.GroupID, shard)
		if !c.eligible(p) {
			for i := range c.paths {
				candidate := &c.paths[i]
				if c.eligible(candidate) && c.pathCanDispatch(candidate, now, packetBytes) {
					p = candidate
					break
				}
			}
		}
		if !c.eligible(p) || !c.pathCanDispatch(p, now, packetBytes) {
			continue
		}
		bundle := wirev2.FECBundle{Header: wirev2.Header{Type: wirev2.TypeFECBundle, SessionID: c.setup.ID}, Route: p.route(), Records: []wirev2.FECRecord{{GroupID: view.GroupID, EncodingEpoch: view.EncodingEpoch, LogicalBytes: view.Group.LogicalBytes, ShardIndex: uint8(shard), Payload: view.Group.Shards[shard]}}}
		packet, err := c.sendAuth.AppendFECBundle(make([]byte, 0, packetBytes), bundle, c.sendLookup, int(p.sendBudget))
		if err != nil {
			return nil, err
		}
		if !c.pathCanDispatch(p, now, cap(packet)) {
			clear(packet)
			return nil, ErrInvalid
		}
		*slot = sendSlot{pathID: p.id, pathGeneration: p.generation, transportGeneration: p.transportGeneration, binding: p.binding, route: p.route(), native: p.nativeTiming, source: sendData, shard: shard, issued: now, expires: c.sends.groupQueued.Add(c.cfg.MaxQueueResidence)}
		intent := c.installIntent(slot, p, p.sender, packet)
		c.outPaths[shard] = p.id
		c.sends.shards[shard] = shardDispatched
		c.sends.cursor = (shard + 1) % len(view.Group.Shards)
		return intent, nil
	}
	return nil, nil
}

func (c *Controller) ownedDataDeadline(sendCapacity bool) time.Time {
	if c.out == nil {
		return time.Time{}
	}
	view, _ := c.out.View()
	available := sendCapacity && c.freeSendSlot() != nil
	var next time.Time
	include := func(at time.Time) {
		if next.IsZero() || at.Before(next) {
			next = at
		}
	}
	for i := range view.Group.Shards {
		if c.sends.shards[i] != shardWaiting {
			continue
		}
		include(c.sends.groupQueued.Add(c.cfg.MaxQueueResidence))
		p := c.preferredOwnedPath(view.GroupID, i)
		if !available {
			continue
		}
		if c.eligible(p) && p.sendToken == 0 {
			include(later(c.last, p.pacedAt))
		} else if !c.eligible(p) {
			for j := range c.paths {
				candidate := &c.paths[j]
				if c.eligible(candidate) && candidate.sendToken == 0 {
					include(later(c.last, candidate.pacedAt))
				}
			}
		}
	}
	return next
}

// Waiting shard descriptors have no admitted path or packet owner. This is a
// placement preference only; admission occurs atomically in installIntent.
func (c *Controller) preferredOwnedPath(group uint64, shard int) *pathState {
	return &c.paths[(int(group%uint64(len(c.paths)))+shard)%len(c.paths)]
}
