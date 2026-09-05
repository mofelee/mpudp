# V2 Deferred Installation Disposal

[中文版](v2-deferred-disposal.zh-CN.md)

`handshakev2.Config.InstallDeferred` permits an adapter to finish installed
storage cleanup after the engine's disposal callback returns. It is a
prerequisite for bounded send workers in #22. The public Datagram runtime still
uses synchronous `Install`; this increment adds no workers, network queue or
throughput claim.

## Ownership

Exactly one of `Install` and `InstallDeferred` must be configured. Both run
after authenticated FINISH/READY and credit promotion, before responder READY.
The deferred installer returns a disposer of type `func(releaseStorage func())`.
A successful installation requires a nonnil disposer and an open credit scope.

At retirement the engine closes the scope, removes the protocol attempt and
clears its packet/key storage. It then invokes the disposer once with an
idempotent, concurrency-safe release callback. The adapter stops admissions,
cancels work and clears all retained initial storage before calling that
callback. Engine methods remain serialized by their existing owner. The
release callback only accesses its private lease owner and the shared credit
ledger; it can run after engine Close and does not reenter the engine.

The adapter must join worker and carrier cleanup outside any owner lock needed
by completion delivery. Engine `Close` begins deferred cleanup and returns;
the adapter's public `Close` must wait for that cleanup. The handshake engine
does not create a cleanup goroutine, wait loop or retired-session collection.

## Credits And Failures

Deferred mode reserves `DeferredDisposalBytes = 512` and one extra byte-only
lease before HELLO/CHALLENGE. This pays for a fixed owner holding at most sixteen
initial lease handles, the base receive lease, its metadata lease and the release
callback. It retains no attempt, engine, packet buffer, directional key or
adapter reference. It is independent of the advertised receive minimum and
must fit the same Peer/Session byte and reservation ceilings. Synchronous mode
keeps its existing admission claims.

The base `Receive`, all `Initial` claims, disposal metadata and Session slot
remain charged until cleanup releases them. `MarkAccepted` still only releases
the responder's pending-accept count. Component owners may release their copied
initial handles after clearing their storage; subsequent engine release remains
idempotent. Closing the shared ledger prevents admission without revoking live
claims. A retired Session with outstanding claims continues to consume its slot.

A disposer returned with an installation error follows the same deferred cleanup
path. A nil disposer means the installer already cleared partial storage, so
the engine releases its claims immediately. Success with a nil disposer or a
closed scope fails installation. Attempts that never reach installation,
including admission rollback and handshake expiry, release their metadata
synchronously. The engine never publishes an established result for failure.

The release callback does not prove that an adapter cleared its storage. That
ordering remains the adapter's ownership contract, just as with synchronous
disposal. Future worker integration must demonstrate bounded completion delivery,
initial and receive credit retention, cleanup joins, and zero network activity
after public Close returns.

## Verification

Deterministic tests cover initiator and responder retirement through local
Session close, engine close, authenticated remote close and ledger close. They
verify retained byte/reservation/Session counts, initial-handle lifetime,
pending-accept behavior, blocked replacement admission, installation errors,
closed-scope failures, nil disposers, exact admission bounds, pending expiry and
concurrent repeated release. Existing synchronous handshake, fuzz and public
runtime regressions remain applicable.
