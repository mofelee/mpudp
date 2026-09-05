# V2 Design Review

These documents propose the implementation contract for issues
[#20](https://github.com/mofelee/mpudp/issues/20),
[#21](https://github.com/mofelee/mpudp/issues/21),
[#22](https://github.com/mofelee/mpudp/issues/22),
[#23](https://github.com/mofelee/mpudp/issues/23),
[#24](https://github.com/mofelee/mpudp/issues/24),
[#25](https://github.com/mofelee/mpudp/issues/25),
[#13](https://github.com/mofelee/mpudp/issues/13) and
[#14](https://github.com/mofelee/mpudp/issues/14).
They are pending review and deterministic vectors. They neither freeze v2
nor change the currently implemented [v1 protocol](../WIRE_PROTOCOL.md).

| Document | Scope |
|---|---|
| [Joint contract](v2-joint-contract.md) | Packing, actual overhead/capacity, repair, MTU migration, KCP and mux ownership |
| [Registry](v2-registry.md) | Proposed byte/type/TLV assignments, fixed bootstrap transcript/KDF, path/epoch rules, reliable/control records |
| [Configuration/API](v2-configuration-api.md) | Concrete defaults/bounds, strict compatibility matrix, scheduling profile, public operation semantics |
| [KCP evidence](evidence/20260905-kcp/README.md) | Pinned resend=0 early-retransmit reproduction and explicit-policy candidate patch |
| [smux evidence](evidence/20260905-smux/README.md) | Pinned unread-tail loss and pre-SYN admission/isolation candidate hooks |

The dependency evidence includes exact upstream SHAs, patches, fixture source,
commands, results and limitations. Fixtures use `.go.txt` so the root module
does not acquire KCP/smux dependencies or compile external test packages.
The patches are review artifacts, not maintained dependency forks.

No issue acceptance item is completed by publishing this proposal. Runtime
strict parsing, codecs/KDF vectors, replay/ownership/cancellation tests,
measured performance/latency, platform integration and promoted-SHA checks
remain implementation work in the linked issues.
