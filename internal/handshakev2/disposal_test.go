package handshakev2

import (
	"errors"
	"io"
	"sync"
	"testing"
	"unsafe"

	"github.com/mofelee/mpudp/internal/creditv2"
	"github.com/mofelee/mpudp/internal/wirev2"
)

func configureDeferred(t *testing.T, s *side) (*Setup, *func(), *int) {
	t.Helper()
	var installed Setup
	var finish func()
	calls := 0
	install := s.engine.config.Install
	s.engine.config.Install = nil
	s.engine.config.InstallDeferred = func(setup Setup) (func(func()), error) {
		installed = cloneSetup(setup)
		dispose, err := install(setup)
		return func(release func()) {
			calls++
			if !setup.Scope.Snapshot().Closed {
				t.Fatal("deferred disposal started before Scope.Close")
			}
			finish = func() {
				dispose()
				// Component release remains compatible with engine ownership.
				for _, lease := range installed.Initial {
					lease.Release()
				}
				var workers sync.WaitGroup
				for range 16 {
					workers.Add(1)
					go func() {
						defer workers.Done()
						release()
					}()
				}
				workers.Wait()
				release()
			}
		}, err
	}
	return &installed, &finish, &calls
}

func completeExchange(t *testing.T, client, server *side) wirev2.SessionID {
	t.Helper()
	begin(t, client, 1, 1, startTime)
	deliver(t, pop(t, client, wirev2.TypeHello), server, true, startTime)
	deliver(t, pop(t, server, wirev2.TypeChallenge), client, false, startTime)
	deliver(t, pop(t, client, wirev2.TypeFinish), server, true, startTime)
	result := deliver(t, pop(t, server, wirev2.TypeReady), client, false, startTime)
	return result.Established[0].ID
}

func TestDeferredDisposalKeepsCreditsUntilCleanup(t *testing.T) {
	for _, role := range []string{"initiator", "responder"} {
		for _, cause := range []string{"session", "engine", "remote", "ledger"} {
			t.Run(role+"/"+cause, func(t *testing.T) {
				bound := limits()
				bound.MaxSessions = 1
				bound.MaxPeerBytes = testReceiveBytes + PacketReservationBytes + DeferredDisposalBytes
				bound.MaxSessionBytes = bound.MaxPeerBytes
				client, server := newSide(t, false, 1, &bound), newSide(t, true, 1, &bound)
				target, other := client, server
				if role == "responder" {
					target, other = server, client
				}
				target.policy.Receive.Bytes = 1024
				target.policy.Initial = []creditv2.Claim{{Bytes: 2048}, {Bytes: testReceiveBytes - 3072}}
				if role == "responder" {
					target.engine.config.Listener = &target.policy
				}
				setup, finish, calls := configureDeferred(t, target)
				id := completeExchange(t, client, server)
				if role == "responder" {
					if err := target.engine.MarkAccepted(startTime, id); err != nil {
						t.Fatal(err)
					}
				}
				before := target.ledger.Snapshot()
				switch cause {
				case "session":
					if _, err := target.engine.CloseSession(startTime, id); err != nil {
						t.Fatal(err)
					}
				case "engine":
					if _, err := target.engine.Close(startTime); err != nil {
						t.Fatal(err)
					}
				case "remote":
					if _, err := other.engine.CloseSession(startTime, id); err != nil {
						t.Fatal(err)
					}
					deliver(t, pop(t, other, wirev2.TypeClose), target, role == "responder", startTime)
				case "ledger":
					target.ledger.Close()
					advance(t, target, startTime)
				}
				if *calls != 1 || *finish == nil || target.engine.Snapshot().Established != 0 {
					t.Fatal("retirement did not return with one pending cleanup")
				}
				after := target.ledger.Snapshot()
				if after.Bytes != testReceiveBytes+DeferredDisposalBytes || after.SessionSlots != 1 || after.EstablishedSessions != 1 || after.Reservations != 4 || after.PendingAccepts != 0 || after.Bytes > before.Bytes {
					t.Fatalf("retained storage lost its credit: before=%+v after=%+v", before, after)
				}
				for _, lease := range setup.Initial {
					if lease.Snapshot().Released {
						t.Fatal("initial component storage released before cleanup")
					}
				}
				if scope, lease, err := target.ledger.BeginSession(creditv2.Claim{Bytes: 1}); err == nil || scope != nil || lease != nil {
					t.Fatal("retiring Session slot was reused before cleanup")
				}
				if _, err := target.engine.Close(startTime); err != nil || *calls != 1 {
					t.Fatal("repeated Close repeated disposal or failed")
				}
				(*finish)()
				closeSide(t, target, startTime)
				closeSide(t, other, startTime)
			})
		}
	}
}

func TestDeferredInstallationFailureRetainsPartialStorage(t *testing.T) {
	for _, cause := range []string{"error", "closed-scope", "nil-disposer"} {
		t.Run(cause, func(t *testing.T) {
			client, server := newSide(t, false, 1, nil), newSide(t, true, 1, nil)
			setup, finish, calls := configureDeferred(t, server)
			install := server.engine.config.InstallDeferred
			server.engine.config.InstallDeferred = func(s Setup) (func(func()), error) {
				if cause == "nil-disposer" {
					return nil, nil
				}
				dispose, err := install(s)
				if cause == "error" {
					return dispose, io.ErrClosedPipe
				}
				s.Scope.Close()
				return dispose, err
			}
			begin(t, client, 1, 1, startTime)
			deliver(t, pop(t, client, wirev2.TypeHello), server, true, startTime)
			deliver(t, pop(t, server, wirev2.TypeChallenge), client, false, startTime)
			packet := pop(t, client, wirev2.TypeFinish)
			result, err := server.engine.Receive(startTime, reverse(packet, true), packet.packet)
			if !errors.Is(err, ErrInstallation) || len(result.Established) != 0 || server.engine.Snapshot().Established != 0 {
				t.Fatalf("failed installation published state: %+v %v", result, err)
			}
			if cause != "nil-disposer" {
				if *calls != 1 || *finish == nil || !setup.Scope.Snapshot().Closed || server.ledger.Snapshot().Bytes != testReceiveBytes+DeferredDisposalBytes || server.ledger.Snapshot().PendingAccepts != 1 {
					t.Fatal("failed installation released partial storage too early")
				}
				(*finish)()
			}
			closeSide(t, server, startTime)
			closeSide(t, client, startTime)
		})
	}
}

func TestDeferredAdmissionRollbackAndPendingExpiry(t *testing.T) {
	for _, cause := range []string{"bytes", "reservations", "pending"} {
		t.Run(cause, func(t *testing.T) {
			bound := limits()
			if cause == "bytes" {
				bound.MaxPeerBytes = testReceiveBytes + PacketReservationBytes + DeferredDisposalBytes - 1
				bound.MaxSessionBytes = bound.MaxPeerBytes
			}
			if cause == "reservations" {
				bound.MaxReservations = 2
			}
			client, server := newSide(t, false, 1, nil), newSide(t, true, 1, &bound)
			_, _, calls := configureDeferred(t, server)
			begin(t, client, 1, 1, startTime)
			packet := pop(t, client, wirev2.TypeHello)
			_, err := server.engine.Receive(startTime, reverse(packet, true), packet.packet)
			if cause == "pending" {
				if err != nil || server.ledger.Snapshot().Bytes != testReceiveBytes+PacketReservationBytes+DeferredDisposalBytes {
					t.Fatal("pending attempt did not prepay disposal storage")
				}
				advance(t, server, startTime.Add(Lifetime))
			} else if !errors.Is(err, creditv2.ErrResourceLimit) {
				t.Fatalf("admission accepted insufficient disposal capacity: %v", err)
			}
			if *calls != 0 {
				t.Fatal("uninstalled attempt invoked disposer")
			}
			closeSide(t, server, startTime.Add(Lifetime))
			closeSide(t, client, startTime.Add(Lifetime))
		})
	}
}

func TestDeferredInstallerSelectionAndMetadataBound(t *testing.T) {
	s := newSide(t, true, 1, nil)
	psk, _ := testProfile(t, 1)
	config := s.engine.config
	deferred := func(Setup) (func(func()), error) { return func(release func()) { release() }, nil }
	config.InstallDeferred = deferred
	if _, err := New(psk, config); !errors.Is(err, ErrInvalid) {
		t.Fatal("both installers accepted")
	}
	config.Install, config.InstallDeferred = nil, nil
	if _, err := New(psk, config); !errors.Is(err, ErrInvalid) {
		t.Fatal("no installer accepted")
	}
	config.InstallDeferred = deferred
	engine, err := New(psk, config)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := engine.Close(startTime); err != nil {
		t.Fatal(err)
	}
	// Include the escaping method-value closure and conservative rounding.
	if uint64(unsafe.Sizeof(retiredStorage{}))+64 > DeferredDisposalBytes {
		t.Fatal("fixed owner exceeds prepaid metadata allowance")
	}
}
