package sessionv2

import (
	"math/rand"
	"testing"
	"time"

	"github.com/mofelee/mpudp/internal/fecv2"
	"github.com/mofelee/mpudp/internal/recvwindow"
)

func deadlineController(t *testing.T) *Controller {
	t.Helper()
	window, err := recvwindow.New(65536)
	if err != nil {
		t.Fatal(err)
	}
	c := &Controller{started: true, cfg: Config{GroupTimeout: 100 * time.Millisecond}, groups: make(map[uint64]*pendingGroup), groupWindow: window}
	t.Cleanup(c.Close)
	return c
}

func checkGroupDeadlineIndex(t *testing.T, c *Controller) {
	t.Helper()
	var want time.Time
	decoded := 0
	for _, group := range c.groups {
		due := group.admitted.Add(c.cfg.GroupTimeout)
		if want.IsZero() || due.Before(want) {
			want = due
		}
		if group.fragments != nil {
			decoded++
		}
	}
	if decoded != 0 {
		retry := later(c.last, c.retryStorage)
		if want.IsZero() || retry.Before(want) {
			want = retry
		}
	}
	if got := c.NextDeadline(); !got.Equal(want) || c.decodedGroups != decoded {
		t.Fatalf("deadline %v, want %v; decoded %d, want %d", got, want, c.decodedGroups, decoded)
	}
	var previous uint64
	seen := make(map[uint64]bool)
	for id := c.groupHead; id != 0; {
		group := c.groups[id]
		if group == nil || seen[id] || group.previous != previous {
			t.Fatalf("invalid expiry links at group %d", id)
		}
		if previous != 0 && group.admitted.Before(c.groups[previous].admitted) {
			t.Fatal("expiry list reordered admission times")
		}
		seen[id], previous, id = true, id, group.next
	}
	if previous != c.groupTail || len(seen) != len(c.groups) {
		t.Fatalf("expiry list retains %d of %d groups, tail %d, want %d", len(seen), len(c.groups), c.groupTail, previous)
	}
}

func TestGroupDeadlineIndexRemovalExpiryAndClose(t *testing.T) {
	c := deadlineController(t)
	start := time.Unix(1700000000, 0)
	groups := make(map[uint64]*pendingGroup)
	for i, id := range []uint64{9, 2, 7, 4} {
		group := &pendingGroup{admitted: start.Add(time.Duration(min(i, 2)) * time.Millisecond)}
		c.insertGroup(id, group)
		c.groupWindow.Admit(id)
		groups[id] = group
	}
	c.last = start.Add(3 * time.Millisecond)
	c.releaseGroup(2, groups[2])
	c.releaseGroup(4, groups[4])
	checkGroupDeadlineIndex(t, c)
	c.expireGroups(start.Add(c.cfg.GroupTimeout - time.Nanosecond))
	if len(c.groups) != 2 {
		t.Fatal("group expired before its original deadline")
	}
	c.expireGroups(start.Add(c.cfg.GroupTimeout))
	if c.groupWindow.State(9) != recvwindow.Expired || c.groupHead != 7 || c.groupTail != 7 {
		t.Fatal("head expiry lost the surviving group")
	}
	checkGroupDeadlineIndex(t, c)
	groups[7].fragments = []fecv2.Fragment{{DatagramID: 7}}
	c.decodedGroups++
	c.retryStorage = c.last.Add(time.Millisecond)
	checkGroupDeadlineIndex(t, c)
	c.Close()
	if c.groupHead != 0 || c.groupTail != 0 || c.decodedGroups != 0 || !c.NextDeadline().IsZero() {
		t.Fatal("Close retained deadline or retry state")
	}
	for _, group := range groups {
		if group.previous != 0 || group.next != 0 {
			t.Fatal("removed group retained expiry links")
		}
	}
}

func TestGroupDeadlineIndexMatchesMapScan(t *testing.T) {
	c := deadlineController(t)
	random := rand.New(rand.NewSource(63))
	ids := random.Perm(1024)
	admitted := 0
	start := time.Unix(1700000000, 0)
	for step := range 2048 {
		now := start.Add(time.Duration(step/2) * time.Millisecond)
		c.last, c.retryStorage = now, now.Add(time.Millisecond)
		if admitted < len(ids) && random.Intn(3) != 0 {
			id := uint64(ids[admitted] + 1)
			group := &pendingGroup{admitted: now}
			if admitted%3 == 0 {
				group.fragments = []fecv2.Fragment{{DatagramID: id}}
				c.decodedGroups++
			}
			c.insertGroup(id, group)
			c.groupWindow.Admit(id)
			admitted++
		}
		if admitted != 0 {
			id := uint64(ids[random.Intn(admitted)] + 1)
			if group := c.groups[id]; group != nil {
				c.releaseGroup(id, group)
			}
		}
		if step%7 == 0 {
			c.expireGroups(now)
			for _, group := range c.groups {
				if !now.Before(group.admitted.Add(c.cfg.GroupTimeout)) {
					t.Fatal("due group was omitted from expiry")
				}
			}
		}
		checkGroupDeadlineIndex(t, c)
	}
	c.expireGroups(c.last.Add(c.cfg.GroupTimeout))
	checkGroupDeadlineIndex(t, c)
	if len(c.groups) != 0 {
		t.Fatal("final expiry retained groups")
	}
}
