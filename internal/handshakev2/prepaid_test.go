package handshakev2

import (
	"errors"
	"slices"
	"testing"

	"github.com/mofelee/mpudp/internal/creditv2"
	"github.com/mofelee/mpudp/internal/wirev2"
)

func TestPrepaidInstallationAtFullCapacity(t *testing.T) {
	bound := limits()
	bound.MaxPeerBytes, bound.MaxSessionBytes = testReceiveBytes+PacketReservationBytes, testReceiveBytes+PacketReservationBytes
	client, server := newSide(t, false, 1, nil), newSide(t, true, 1, &bound)
	policy := server.policy
	policy.Receive.Bytes = 0
	policy.Initial = []creditv2.Claim{{Bytes: 2048}, {Bytes: testReceiveBytes - 2048}}
	server.engine.config.Listener = &policy
	var prepaid []*creditv2.Lease
	disposed := false
	server.engine.config.Install = func(setup Setup) (func(), error) {
		if len(setup.Initial) != 2 || setup.Initial[0].Snapshot().Bytes != 2048 || setup.Initial[1].Snapshot().Bytes != testReceiveBytes-2048 {
			t.Fatal("prepaid component lease mapping changed")
		}
		if lease, err := setup.Scope.Reserve(creditv2.Claim{Bytes: 1}); !errors.Is(err, creditv2.ErrResourceLimit) || lease != nil {
			t.Fatal("fixture did not exhaust initial capacity")
		}
		prepaid = slices.Clone(setup.Initial)
		storage := make([]byte, testReceiveBytes)
		server.storage[setup.ID] = storage
		setup.Initial[0] = nil
		return func() {
			for _, lease := range prepaid {
				if lease.Snapshot().Released {
					t.Fatal("engine released prepaid storage before disposal")
				}
			}
			clear(storage)
			delete(server.storage, setup.ID)
			prepaid[0].Release()
			disposed = true
		}, nil
	}
	begin(t, client, 1, 1, startTime)
	deliver(t, pop(t, client, wirev2.TypeHello), server, true, startTime)
	deliver(t, pop(t, server, wirev2.TypeChallenge), client, false, startTime)
	result := deliver(t, pop(t, client, wirev2.TypeFinish), server, true, startTime)
	if len(result.Established) != 1 || len(result.Established[0].Initial) != 2 || result.Established[0].Initial[0] == nil {
		t.Fatal("callback slice mutation changed published lease handles")
	}
	result.Established[0].Initial[1] = nil
	deliver(t, pop(t, server, wirev2.TypeReady), client, false, startTime)
	closeSide(t, server, startTime)
	closeSide(t, client, startTime)
	if !disposed || !prepaid[0].Snapshot().Released || !prepaid[1].Snapshot().Released {
		t.Fatal("prepaid leases survived disposer/engine cleanup")
	}
}

func TestPrepaidClaimsValidationAndRollback(t *testing.T) {
	client := newSide(t, false, 1, nil)
	base := client.policy
	base.Receive.Bytes = 0
	base.Initial = []creditv2.Claim{{Bytes: testReceiveBytes}}
	if err := validatePolicy(base, false); err != nil {
		t.Fatalf("empty base with full prepaid initial claim rejected: %v", err)
	}
	for _, claims := range [][]creditv2.Claim{
		{{Bytes: testReceiveBytes - 1}},
		{{Bytes: testReceiveBytes, PendingAccept: true}},
		{{Bytes: testReceiveBytes, BusinessStream: true}},
		{{Bytes: testReceiveBytes}, {}},
		{{Bytes: testReceiveBytes}, {Bytes: ^uint64(0)}},
		make([]creditv2.Claim, MaxInitialReservations+1),
	} {
		policy := base
		policy.Initial = claims
		if err := validatePolicy(policy, false); !errors.Is(err, ErrInvalid) {
			t.Fatalf("invalid prepaid claims accepted: %+v %v", claims, err)
		}
	}
	bound := limits()
	bound.MaxReservations = 2
	server := newSide(t, true, 1, &bound)
	policy := server.policy
	policy.Receive.Bytes = 0
	policy.Initial = []creditv2.Claim{{Bytes: 2048}, {Bytes: testReceiveBytes - 2048}}
	server.engine.config.Listener = &policy
	begin(t, client, 1, 1, startTime)
	hello := pop(t, client, wirev2.TypeHello)
	result, err := server.engine.Receive(startTime, reverse(hello, true), hello.packet)
	if !errors.Is(err, creditv2.ErrResourceLimit) || len(result.Sends) != 1 || result.Sends[0].Type != wirev2.TypeReject || server.ledger.Snapshot().Bytes != 0 || server.ledger.Snapshot().SessionSlots != 0 || server.ledger.Snapshot().Reservations != 0 {
		t.Fatalf("partial prepaid admission retained credits: %+v %v %+v", result, err, server.ledger.Snapshot())
	}
	closeSide(t, client, startTime)
	closeSide(t, server, startTime)
}
