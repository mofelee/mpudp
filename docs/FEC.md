# MPUDP v0.1 FEC 设计与边界

`internal/fec` 实现一个上层 Datagram 对应一个 Reed-Solomon block。它只负责有界的
shard 编码、恢复和短期去重，不感知 Carrier 或网络路径。

## 实现选择

项目固定使用
[`github.com/klauspost/reedsolomon`](https://github.com/klauspost/reedsolomon)
`v1.14.2`。该库使用 MIT License；MPUDP v0.1 使用其标准 GF(2^8) 编码范围，并要求：

- `data_shards = k > 0`；
- `parity_shards = r > 0`；
- `n = k + r <= 256`。

精确模块版本、传递依赖和分发义务见 [依赖与许可证审计](DEPENDENCIES.md)。

参数先由 FEC 层以防整数溢出的方式校验，再交给库构造函数复核。codec 固定使用
`WithMaxGoroutines(1)` 和 `WithInversionCache(false)`：单次编解码不使用库的并行
分片，并且不会让不同缺失组合累积 inversion cache。项目本身要求 Go 1.24。

`RS(5,3)` 在本文中表示 `n=5, k=3, r=2`：五个 shard 中任意三个不同且经过认证的
shard 可以恢复原 Datagram。它不表示必然容忍任意两条 Carrier 故障，因为 FEC 层
不决定 shard 如何映射到 Carrier。

## UDP budget 与大小上限

FEC 层不复制 wire layout 常量。调用方必须把当前 `DATA_SHARD` 完整 wire overhead
作为 `Budget.DataShardWireOverhead` 传入；它包含 MPUDP prefix、DATA_SHARD metadata
和 authentication tag，但不包含 shard payload。当前 wire 实现应传
`wire.DataShardOverhead`。

设：

```text
U = negotiated MaxUDPPayload
H = DataShardWireOverhead
C = ShardCapacity
D = configured MaxDatagramSize

C                  = U - H
FECDatagramLimit   = k * C
EffectiveLimit     = min(FECDatagramLimit, D)
shardSize(length)  = max(1, ceil(length / k))
encoded UDP length = H + shardSize(length) <= U
```

`DeriveLimits` 要求 `H >= 0`、`U > H`、`D > 0`，并要求 `D` 能由 wire 的 `uint32
OriginalLength` 表示。它在计算前检查 `k*C` 和 `n*C` 的 `int` 乘法；后者覆盖编码器
完整 shard backing allocation。无效预算返回可由 `errors.Is` 判断的
`ErrInvalidBudget`。

Datagram 长度可以恰好等于 `EffectiveLimit`。多一个 byte 会在取得 PacketID 或分配
任何 shard storage 之前返回 `ErrMessageTooLarge`。每个 shard 长度相同，并被限制在
`C` 内，因此完整 `DATA_SHARD` UDP payload 不超过协商预算。只有当该预算不高于真实路径
安全 UDP payload 且 Linux DF/PMTU mode 生效时，网络层才能进一步保证不产生本地 IP
fragment；FEC 层本身只保证不会把一个 shard 再做协议内分片。

v0.1 的一个 block 始终使用同一个冻结 Session budget 和等长 shard。由
[#13](https://github.com/mofelee/mpudp/issues/13) 跟踪的未来 PLPMTUD 不改变这一现有
契约；由 [#14](https://github.com/mofelee/mpudp/issues/14) 跟踪的不等长/per-Carrier
shard 必须经过新的 wire、FEC、恢复阈值和互操作设计评审，不能直接套用本编码格式。

## 编码语义

每次 `Encoder.Encode` 独立产生一个 block。编码器复制输入到一块连续、自有的 shard
allocation，不修改或引用调用方的 payload；不足整除的 data shard 尾部补零，并生成
`r` 个 parity shard。

空 Datagram 使用唯一规范形式：`OriginalLength=0`，且全部 data/parity shard 都是
恰好一个值为零的 byte。这样 wire 上仍有 shard payload，同时恢复结果是非 nil、长度为
零的 Datagram。

一个 `Encoder` 只属于一个 Session 的一个发送方向，并可被并发调用。PacketID 从 0
开始，只在成功编码后递增；参数、大小或编码失败不会消耗 ID。`math.MaxUint64` 可以
成功使用一次，之后所有编码都返回 `ErrPacketIDExhausted`，绝不回绕到 0。反方向必须
使用另一个 `Encoder`，因此两个方向各自维护独立序列。

## 解码、超时与去重

调用方只能把完整解析且认证成功的 DATA_SHARD 交给 `AddVerifiedShard`。FEC 包不执行
HMAC、wire 解码或来源认证。调用方必须先确认完整 wire packet 的认证 tag；解码器随后
再检查协商 FEC 参数、shard index、`OriginalLength` 和规范 shard 长度。被保留的
payload 会立即复制，之后调用方可以复用接收 buffer。

聚合 key 是完整的 `[16]byte SessionID + uint64 PacketID`。同一 key 下：

- 相同 index 和相同 payload 是 duplicate，不增加有效 shard 数；
- 相同 index 但 payload 不同返回 `ErrConflictingShard`；
- `OriginalLength` 或 shard size 与首个 shard 不一致返回 `ErrInconsistentBlock`；
- 第 `k` 个不同 shard 到达的同一次调用同步执行 `ReconstructData`、去除 padding，并且
  返回唯一一次 `OutcomeComplete`，不等待慢 shard、timer tick 或较小 PacketID。

完成后立即释放 shard state，只在 completion cache 中保留 key。TTL 从完成时固定计算，
duplicate 不刷新 TTL。cache 达到容量时先移除最早到期的 key；到期时间相同时按
SessionID、PacketID 的字典序确定，因而行为可重复。TTL 到期或容量淘汰后，旧 shard
可以再次形成新 block，甚至再次交付；completion cache 提供的是明确受限的短期去重
窗口，不是永久的 exactly-once 存储。

未完成 block 的 deadline 在第一个 shard 到达时固定，后续 shard 不刷新。调用方应
周期性调用 `Sweep`；每次合法 `AddVerifiedShard` 也会先进行 opportunistic sweep。
deadline 小于或等于当前 clock 时间即过期，block 会直接丢弃，不发送 ACK/NACK 或请求
重传。`DecoderConfig.Clock` 可注入测试 clock，因此测试不需要真实 sleep。

## 内存与生命周期边界

`MaxPendingBlocks` 是未完成 block 的硬上限。达到上限时，新 key 返回
`ErrDecoderFull`，不会驱逐已有未完成 block；同一 key 的 duplicate 不增加占用。每个
未完成 block 最多保留 `k-1` 个 payload，每个 payload 最多 `ShardCapacity` bytes，另有
固定的 `n` 个 slice slot 和 heap/map bookkeeping。

`MaxCompletedBlocks` 和 `CompletionTTL` 同时约束 completion cache；cache 只保存 key 和
deadline，不保存已恢复 Datagram。pending 与 completion 各使用带索引的 expiry heap，
所有 map 和 heap 都有对应容量边界。`Stats` 可查询精确的 pending block、shard、byte
和 completion key 数量。

`Decoder` 不创建后台 goroutine 或 timer。`AddVerifiedShard`、`Sweep`、`Stats` 和
`Close` 可并发调用；`Close` 幂等并立即清空所有状态，之后 `AddVerifiedShard` 始终返回
`ErrClosed`。

## 非目标

本包不实现 Carrier 调度或 shard-to-Carrier 映射、UDP I/O、Session handshake、wire
编码、HMAC/加密、ACK/NACK、DATA 重传、接收/交付队列，或不同 PacketID 间的有序
重排。这些职责必须由其他层实现，并在调用 FEC 前后维持各自的容量和认证边界。

## 测试与 benchmark

单元测试覆盖 RS(5,3) 的全部 0/1/2 shard 丢失组合、并发到达、大小边界、固定超时、
容量和 completion cache 淘汰。代表性 encode/recovery benchmark 会报告吞吐和分配，
但不设置依赖机器的绝对性能门槛：

```bash
go test ./internal/fec -run '^$' -bench 'RS5_3$' -benchmem
```
