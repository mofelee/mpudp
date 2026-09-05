//go:build linux

package mpudp

import (
	"fmt"
	"testing"
)

func TestV2NegotiatedPathStorageWithDefaultListenerCapacity(t *testing.T) {
	for _, aggregate := range []bool{false, true} {
		t.Run(fmt.Sprintf("aggregation_%t", aggregate), func(t *testing.T) {
			server, listener := v2LoopbackListener(t, "0.0.0.0:0", aggregate)
			if server.config.Limits.MaxEndpointsPerSession != 256 {
				t.Fatal("fixture lost production listener capacity")
			}
			_, sender, receiver := v2LoopbackDial(t, server, listener, []string{"127.0.0.1", "127.0.0.2", "127.0.0.3", "127.0.0.4", "127.0.0.5"}, aggregate)
			for _, session := range []DatagramSession{sender, receiver} {
				if got := len(v2SessionSnapshot(t, session).Paths); got != 5 {
					t.Fatalf("installed controller retained %d paths after negotiating 5", got)
				}
			}
			v2ExchangeOwned(t, sender, receiver, "five-path upload")
			v2ExchangeOwned(t, receiver, sender, "five-path download")
		})
	}
}
