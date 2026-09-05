# V2 Peer Send Workers

[中文版](v2-send-workers.zh-CN.md)

Linux fixed/session Datagram uses a fixed Peer-wide send pool for established
control and FEC packets. The protocol owner still serializes authentication,
encoding, admission and state transitions. Bootstrap and best-effort CLOSE
emission remain synchronous, with bounded contexts. This implements worker
execution and ownership, not the remaining scheduler, health detector or
throughput acceptance in #16/#22.

## Execution And Capacity

`limits.max_send_workers` starts exactly 1..32 send workers, default 8. Each
worker has one job slot and one reliable result slot. A slot remains busy until
the protocol owner consumes its result, so completion never competes with the
lossy ingress or diagnostic channels. A coalesced wake is only a hint; result
storage retains every terminal outcome. There is no per-packet or per-Session
worker goroutine.

The owner rotates through a ring of Sessions and calls `TakeSend` only for idle
workers. The controller permits one admitted packet per logical path and at
most `min(max_send_workers, negotiated paths)` per Session. Waiting FEC shard
descriptors remain bounded at group level without packet/path admission.
Configured per-path packet/byte ceilings are enforced; the effective complete
UDP packet must fit `max_path_queued_bytes`. One sealed FEC group remains owned
until all its shards reach terminal outcomes.

Workers use a Session-derived 20ms context. The 100ms queue-residence deadline
is checked before invocation; a send that begins in time receives its separate
execution timeout. Native Carrier and listener write-lock acquisition also
observes the caller context, so a canceled waiter does not wait for an unrelated
write to finish or change that write's deadline. Custom Send remains the custom
adapter's implementation and must honor its context to provide bounded I/O.

Native senders capture generation and address backing when creating their
binding. Rebuild cannot select a replacement socket for an old authenticated
route. Listener replies retain destination source/OOB and their original shared
socket; multiple inbound workers may therefore wait on the same socket write
gate. Eight configured workers with one five-path Session permit at most five
simultaneous Session intents, not eight independent socket writes.

After invocation or cancellation before invocation, the worker releases its
private packet owner and publishes a scalar completion. Native attempt times
measure entry into the connection write, after lock/deadline setup. Unknown
custom timing preserves nil-error Send success with conservative finish-time
pacing. Neither enqueue, dispatch nor UDP success proves remote delivery.

The owner consumes completions before dispatching more work. A returned intent
is registered before processing other controller results, preserving ownership
if an earlier failure closes the Session. When every Peer slot is busy, the
driver suppresses send-readiness deadlines but retains queue expiry, retry and
receive maintenance. Pacing delays never occupy workers.

## Admission And Cleanup

Before creating a Peer, fixed worker/channel metadata is deducted from its byte
ceiling. Controller initial claims prepay bounded packet slots and completion
metadata. These are owned-storage bounds, not RSS or Go runtime stack/GC bounds.

Initiator construction uses [prepared serial dial](v2-prepared-serial-dial.md).
The wrapper and all opened sockets share one reserved scope across construction
and pre-installation fallback; there is no release/reacquire interval. The
runtime cannot dispatch cleanup while a Carrier constructor is still active.
Failure after scope promotion or partial installation is terminal for this
prepared API. Legacy internal concurrent BeginDial retains its existing policy.

Installed Sessions use [deferred disposal](v2-deferred-disposal.md). Closing
stops admission, cancels sends and wakes public waiters under the owner lock.
Restricted controller completions remain legal after Scope.Close. Initial
controller storage is finalized only after every packet and result returns.

One fixed cleanup worker closes retired Carrier sockets outside the protocol
lock. Waiting cleanup is represented by existing bounded Session records; the
worker has one job/result pair. A failed path can retire its Carrier while the
remaining Session continues. Final Session cleanup waits for construction,
active sends and any individual Carrier cleanup before clearing wrapper/path
references and invoking the retained storage-release continuation.

Session.Close joins this final cleanup. Listener.Close joins its inbound
Sessions before closing the shared listener socket; a dual Peer's outbound
Sessions remain independent. Peer.Close stops admission, joins construction,
drains send/cleanup results, joins the fixed workers and then closes the ledger.
Parent-context cancellation also starts retirement and keeps the driver alive
until cleanup completes; nested Close and failed NewSession do not require a
concurrent Peer.Close. Call Peer.Close to join the idle fixed workers and ledger.
Repeated Close and CloseGracefully join the same final result. Slow cleanup may
hold admission capacity and delay other cleanup, but does not hold the protocol
mutex. Arbitrary custom Close code has no forced timeout.

## Verification Boundary

Deterministic controller tests cover release/token identity, shuffled outcomes,
native/custom timing, stale paths and control versions, full credits and
maintenance deadlines. Runtime tests cover fixed worker counts, blocked paths,
public admission/receive progress, Flush failure frontiers, full completion
mailboxes and Session/Listener/Peer cleanup with retained credit. Native socket
tests cover cancellation while waiting for a busy write gate.

Use the [measurement guide](../performance/v2-measurement.md) for one-source
workers=1 versus workers=8 comparisons. Worker counts are explicit in both
endpoint configurations and retained separately from probe-process counts.
Throughput/allocation and timing-enabled observations are separate experiments.
The pool alone does not establish host headroom, independent listener socket
capacity, fault convergence, or the three 300-second performance gates.
