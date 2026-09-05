# V2 安装存储的延后清理

[English](v2-deferred-disposal.md)

`handshakev2.Config.InstallDeferred` 允许适配器在引擎清理回调返回后，继续完成已安装存储的清理。
它是 #22 有界发送 worker 的前置能力。公共 Datagram runtime 仍使用同步 `Install`；
本增量不启用 worker、网络发送队列，也不代表吞吐提升。

## 所有权

`Install` 与 `InstallDeferred` 必须且只能配置一个。两者都在已认证的 FINISH/READY
及 credit promotion 之后执行，响应方必须先安装再发送 READY。延后安装器返回
`func(releaseStorage func())` 类型的清理函数。安装成功要求清理函数非 nil，且 credit scope 仍开放。

退役时，引擎关闭 scope、移除协议 attempt 并清理其报文和密钥存储，然后仅调用一次清理函数，
交付一个幂等、支持并发调用的 release 回调。适配器必须停止接纳、取消工作，清理全部保留的
初始存储，最后调用 release。引擎方法仍由原 owner 串行调用；release 仅访问独立 lease owner
及共享 credit ledger，可在引擎 Close 后执行，不会重新进入引擎。

适配器必须在 completion 所需的 owner lock 之外等待 worker 和 carrier 清理完成。
引擎 `Close` 发起延后清理后返回；适配器的公共 `Close` 必须等待清理完成。
handshake 引擎不会创建清理 goroutine、轮询或已退役 Session 集合。

## 额度与失败

延后模式在 HELLO/CHALLENGE 之前额外预留 `DeferredDisposalBytes = 512` 字节及一个
纯字节 lease，覆盖固定 owner、最多十六个 initial lease handle、base receive lease、
metadata lease 和 release 回调。该 owner 不持有 attempt、引擎、报文、方向密钥或适配器引用。
这笔预留独立于公布的 receive 最小值，必须满足同一 Peer/Session 字节和 reservation 上限。
同步模式保持现有接纳额度。

base `Receive`、全部 `Initial`、清理 metadata 及 Session slot 持续计费，直到清理释放它们。
`MarkAccepted` 仍仅释放响应方的 pending-accept 计数。组件清理存储后可以释放自己持有的
initial handle；引擎后续释放保持幂等。关闭共享 ledger 会拒绝新接纳，但不会撤销有效 claim。
退役 Session 只要仍持有 claim，就继续占用其 slot。

安装器同时返回错误和非 nil 清理函数时，使用相同的延后清理流程。返回 nil 清理函数表示
安装器已自行清理部分存储，引擎立即释放 claim。成功结果若缺少清理函数或 scope 已关闭，
仍视为安装失败。尚未安装的 attempt，包括接纳回滚和 handshake 超时，同步释放 metadata。
失败不会发布 established 结果。

release 回调本身不能证明适配器已经清理存储；与同步清理一样，顺序由适配器所有权契约保证。
未来 worker 集成仍需证明 completion 有界可靠投递、initial/receive 额度完整保留、清理等待，
以及公共 Close 返回后无网络活动。

## 验证

确定性测试覆盖发起方和响应方的本地 Session close、引擎 close、已认证远端 close 和
ledger close，检查字节、reservation、Session slot、initial handle 生命周期、pending-accept、
退役期间拒绝替代 Session 接纳、安装错误、已关闭 scope、nil 清理函数、精确接纳上限、
未安装 attempt 超时，以及并发重复释放。现有同步 handshake、fuzz 和公共 runtime 回归继续适用。
