package sessionv2

import (
	"errors"
	"fmt"
	"slices"
	"testing"
	"unsafe"

	"github.com/mofelee/mpudp/internal/creditv2"
	"github.com/mofelee/mpudp/internal/wirev2"
)

func TestInitialControlSeparatelyPrepaysDirectionalAuthentication(t *testing.T) {
	for _, paths := range []int{1, 256} {
		cfg := configFor(profile(paths), false)
		claims, err := RequiredInitialClaims(cfg)
		if err != nil {
			t.Fatal(err)
		}
		n := uint64(cfg.LocalProfile.DataShards) + uint64(cfg.LocalProfile.ParityShards)
		controllerAndPaths := uint64(unsafe.Sizeof(Controller{})) + uint64(paths)*uint64(unsafe.Sizeof(pathState{}))
		codecs := 16*n*n + 512*n + 16384
		receive := uint64(cfg.LocalProfile.Payload.ReceiveHardCap) + wirev2.MaxFECRecords*uint64(unsafe.Sizeof(wirev2.FECRecord{}))
		if got := claims[InitialControl].Bytes; got != controllerAndPaths+codecs+receive+2*wirev2.AuthenticatorStateBytes {
			t.Fatalf("standing authentication reused another component's allowance: %d", got)
		}
	}
}

func TestDirectionalAuthenticatorsUseStandingPrepayment(t *testing.T) {
	setup, cfg, peer := scratchSetup(t, configFor(profile(1), false), 0)
	before := peer.Snapshot()
	c, err := New(setup, cfg)
	if err != nil || peer.Snapshot() != before {
		t.Fatalf("authenticator installation reserved again at full capacity: %v", err)
	}
	t.Cleanup(c.Close)
	if c.sendAuth == nil || c.receiveAuth == nil || c.sendAuth == c.receiveAuth {
		t.Fatal("controller did not install independent directional state")
	}
	if len(setup.Initial) != InitialCount || c.controlLease.Snapshot().Bytes < 2*wirev2.AuthenticatorStateBytes {
		t.Fatal("standing authentication storage lost its initial credit owner")
	}
	packet, err := wirev2.AppendEstablished(nil, wirev2.Header{Type: wirev2.TypeClose, SessionID: setup.ID}, wirev2.Route{PathID: 1, Generation: 1, BudgetEpoch: 1}, make([]byte, 8), c.receiveKey)
	if err != nil {
		t.Fatal(err)
	}
	envelope, err := wirev2.ParseEnvelope(packet)
	if err != nil {
		t.Fatal(err)
	}
	for range 32 {
		if _, err := c.receiveAuth.Authenticate(envelope); err != nil {
			t.Fatal(err)
		}
		if _, err := c.sendAuth.Authenticate(envelope); !errors.Is(err, wirev2.ErrAuthentication) {
			t.Fatalf("send key verified incoming direction: %v", err)
		}
	}
	if peer.Snapshot() != before {
		t.Fatal("authentication changed standing reservation ownership")
	}
	send, receive := c.sendAuth, c.receiveAuth
	setup.Scope.Close()
	if peer.Snapshot().Bytes != before.Bytes {
		t.Fatal("scope Close prematurely revoked standing hash credit")
	}
	c.Close()
	c.Close()
	if c.sendAuth != nil || c.receiveAuth != nil || peer.Snapshot().Usage != (creditv2.Usage{}) {
		t.Fatal("controller Close retained authentication storage or credit")
	}
	for _, auth := range []*wirev2.Authenticator{send, receive} {
		if _, err := auth.Authenticate(envelope); !errors.Is(err, wirev2.ErrInvalidKey) {
			t.Fatalf("disposed controller left a usable authenticator: %v", err)
		}
	}
}

func TestDirectionalAuthenticationRequiresBothPrepaidOwners(t *testing.T) {
	for _, missing := range []uint64{1, wirev2.AuthenticatorStateBytes, 2 * wirev2.AuthenticatorStateBytes} {
		t.Run(fmt.Sprint(missing), func(t *testing.T) {
			setup, cfg, peer := scratchSetup(t, configFor(profile(1), false), missing)
			before := peer.Snapshot()
			if c, err := New(setup, cfg); c != nil || !errors.Is(err, creditv2.ErrInvalid) || peer.Snapshot() != before {
				t.Fatalf("missing standing credit consumed initial ownership: %v", err)
			}
			for _, lease := range setup.Initial {
				if _, err := setup.Scope.BindBytes(lease, lease.Snapshot().Bytes); err != nil {
					t.Fatalf("rejected constructor bound another component: %v", err)
				}
			}
		})
	}
}

func TestDirectionalAuthenticationRollsBackLaterConstructorFailure(t *testing.T) {
	setup, cfg, peer := scratchSetup(t, configFor(profile(1), false), 0)
	before := peer.Snapshot()
	controlBytes := setup.Initial[InitialControl].Snapshot().Bytes
	setup.Initial = slices.Clone(setup.Initial)
	setup.Initial[InitialGroupWindow] = nil
	if c, err := New(setup, cfg); c != nil || !errors.Is(err, creditv2.ErrInvalid) {
		t.Fatalf("invalid later lease did not fail construction: %v", err)
	}
	after := peer.Snapshot()
	if after.Bytes != before.Bytes-controlBytes || after.Reservations != before.Reservations-1 {
		t.Fatal("constructor rollback retained the prepaid authentication owner")
	}
	for _, index := range []int{InitialQueue, InitialOriginalWindow} {
		lease := setup.Initial[index]
		if _, err := setup.Scope.BindBytes(lease, lease.Snapshot().Bytes); err != nil {
			t.Fatalf("constructor rollback consumed an untouched component: %v", err)
		}
	}
}
