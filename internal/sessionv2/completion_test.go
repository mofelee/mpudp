package sessionv2

import (
	"io"
	"testing"
)

type completionPath struct{ testPath }

func (completionPath) Available() bool { panic("completion polled path availability") }

func TestCompletionPreservesFenceStateWithoutPathWork(t *testing.T) {
	for _, c := range []*Controller{nil, {}} {
		if through, failedFrom, err := c.Completion(); through != 0 || failedFrom != 0 || err != nil {
			t.Fatal("empty controller reports send progress")
		}
	}
	c := &Controller{started: true, contextAcknowledged: true, completed: 5,
		paths: []pathState{{active: true, sender: &completionPath{}, sendEpoch: 2, sendBudget: 1200}}}
	if through, failedFrom, err := c.Completion(); through != 5 || failedFrom != 0 || err != nil {
		t.Fatal("successful send frontier changed")
	}
	c.completed, c.failedFrom, c.sticky = 9, 7, io.ErrClosedPipe
	for _, closeController := range []bool{false, true} {
		if closeController {
			c.Close()
		}
		if through, failedFrom, err := c.Completion(); through != 9 || failedFrom != 7 || err != io.ErrClosedPipe {
			t.Fatalf("sticky failure or frontier changed after Close=%t", closeController)
		}
	}
}
