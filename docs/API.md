# MPUDP v0.1 公共 API

公共包为 `github.com/mofelee/mpudp`，严格配置模型位于
`github.com/mofelee/mpudp/config`。公共数据面只提供 Datagram Session，不暴露 shard、
Carrier 或上层协议适配。

## 启动与角色

```go
cfg, err := config.Parse(yamlBytes)
if err != nil {
    // errors.Is(err, mpudp.ErrInvalidConfig)
}

ctx, cancel := context.WithCancel(context.Background())
defer cancel()
peer, err := mpudp.NewPeerContext(ctx, cfg)
if err != nil {
    // 构造失败已经回收半初始化资源
}
defer peer.Close()

if cfg.InitiatorEnabled() {
    outbound, err := peer.NewSession()
    // 握手异步进行；建立前 WritePacket 返回 ErrNotReady
}

if cfg.ListenerEnabled() {
    listener, err := peer.Listener()
    inbound, err := listener.Accept(ctx)
}
```

`NewPeer` 先完成无副作用验证，再为 listener/dual 模式绑定 `listen` socket，并启动一个
Peer 级 dispatcher。initiator-only 不绑定 listener；每次 `NewSession` 为每个配置的
Carrier 打开一个长期 UDP socket，并立即发起认证 HELLO。任一 Carrier 打开失败时，本次
调用已经打开的 socket 会全部关闭，且不会保留半初始化 Session。

`NewPeerContext` 提供相同的启动路径，并允许 context 取消 listener bind、Carrier dial 和
运行时网络操作。context 取消会阻止或中止工作，但调用方仍须调用 `Peer.Close`，才能同步
关闭 socket 并等待 dispatcher、receive loop 和在途 callback 全部退出。

CLI `cmd/mpudp` 读取 `-config` 后创建 Peer。initiator/dual 模式还会自动创建一个 outbound
Session，然后持续运行直到 SIGINT 或 SIGTERM；启动失败或收到信号时都会执行受控 Close。

YAML 解析只对真正省略的可选字段应用默认值。Go 调用方直接组装配置时必须先调用
`config.Default()`，再覆盖角色、FEC、PSK 和需要调整的选项；零值 `config.Config` 不会由
`Validate` 或 `NewPeer` 隐式补默认。

`Peer.NewSession()` 只在 initiator/dual 模式可用；`Peer.Listener()` 只在 listener/dual
模式可用，错误角色返回 `ErrModeUnavailable`。一个 dual Peer 的
`limits.max_sessions` 同时约束 outbound 与 accepted inbound Session 总数。

## Datagram 接口

```go
type Session interface {
    WritePacket(payload []byte) error
    ReadPacket() ([]byte, error)
    Close() error
}
```

一次成功的 `WritePacket` 表示一个完整 Datagram，一次成功的 `ReadPacket` 返回一个完整
Datagram。空 Datagram 返回非 nil 的零长度 slice。不同 PacketID 不承诺有序交付；FEC
duplicate/late shard 不会让同一 PacketID 重复交付。MPUDP 不提供可靠、有序或流语义。

`NewSession` 不等待握手完成。握手期间 `WritePacket` 返回 `ErrNotReady`；建立后，payload
先按协商后的有效 Datagram 上限检查，超限返回 `ErrMessageTooLarge`，不会分配 FEC block、
取得 PacketID 或发送任何 shard。其他发送失败通过根包的 `ErrNoAvailablePaths`、
`ErrPartialSend`、`ErrAllSendsFailed` 和 `ErrPathMTUExceeded` 分类，并保留底层
`errors.Is` cause。一个错误可同时匹配发送结果和路径原因，例如 partial send 与 PMTU。

`ReadPacket` 会阻塞到一个完整 Datagram 到达或 Session 关闭。它当前不接受 context；需要
取消等待时，调用方应关闭 Session 或其所属 Peer。Close 后不会交付队列中残留的数据。

```go
type Listener interface {
    Accept(ctx context.Context) (Session, error)
    Close() error
}
```

`Accept` 阻塞到认证且兼容的 HELLO 创建新 Session、context 取消/超时，或 Listener 关闭。
nil context 返回 `ErrInvalidConfig`。每个入站 Session 只因第一次创建入队一次；重复 HELLO
只刷新现有状态。`Listener.Close` 停止入站接收、关闭其 accepted Session，并唤醒全部
Accept；dual Peer 的 outbound Session 不受影响。

## 有界队列与 deadline

Peer 只创建一个 runtime dispatcher goroutine和一个可复用 timer，负责全部 Session 的
HELLO retry、keepalive、Endpoint expiry 和 FEC sweep deadline。每个 UDP Carrier 可有一个
transport receive loop；callback 只把拥有所有权的 packet 非阻塞放入 ingress，不执行认证、
FEC、Close 或 goroutine 创建。

| 资源 | 容量 | 满载策略 |
|---|---:|---|
| Peer packet/recoverable-error ingress | `limits.receive_queue_capacity` | drop newest event |
| Listener terminal failure latch | 1 | retain first terminal failure |
| Listener accept | `limits.receive_queue_capacity` | close/release newest Session |
| 每 Session delivery | `limits.delivery_queue_capacity` | drop newest Datagram |

因此一个慢 `Accept`/`ReadPacket` 消费者不会阻塞 transport callback、其他 Session 或无限增加
内存。drop 策略不会产生 DATA 重传或把 Datagram 降级成字节流。listener 的 terminal socket
error 使用独立的一次性 latch，不会因 packet ingress 已满而丢失；超大 packet、nil remote
和临时网络错误仍是可恢复的单次诊断，不会关闭仍可用的 Listener 或 Carrier。

公共配置到内部 Session 的额外有界映射为：

- completed PacketID cache 容量使用 `limits.max_pending_fec_blocks`；
- completed PacketID TTL 使用 `timers.endpoint_ttl`；
- handshake jitter 未单独暴露配置，使用 retry interval 的四分之一默认值。

## 关闭与并发

- `Peer.NewSession`、`Peer.Listener`、`Peer.Config`、`Peer.Mode` 和 `Peer.Close` 可并发调用。
- 同一 `Session` 的 `WritePacket`、`ReadPacket` 和 `Close` 可并发调用。
- 同一 `Listener` 的多个 `Accept` 和 `Close` 可并发调用。
- 多个并发 `WritePacket` 保持 Datagram 边界，但 PacketID/send 顺序不作保证。
- `Peer.Close`、`Listener.Close` 和 `Session.Close` 均幂等；并发调用共享首次关闭结果。

关闭首先阻止新的 write/Session/accept，再取消 dispatcher 和在途 Session 操作；内部状态机
在 socket 可用时用最长一秒的 context 尝试 CLOSE，随后关闭 Carrier/listener socket、释放
FEC/Endpoint/timer 状态、唤醒阻塞调用，并等待 receive loop、callback 和 dispatcher 退出。
Close 返回后不再有属于该对象的后台网络活动。收到认证的远端 CLOSE 也会释放对应公共
Session 和 initiator Carrier。

`Peer.Errors()` 返回容量为一的异步诊断 channel。运行时生产者不会阻塞；channel 已满时
丢弃最新诊断。错误文本只给出稳定操作类别，底层 cause 仍可用 `errors.Is`/`errors.As`
检查。该 channel 在 `Peer.Close` 时不会关闭，消费者应与自己的 lifecycle context 一起
select。

## SessionID 与诊断

`Peer.Statistics()` 返回可直接 JSON 编码的有界诊断快照；`CapturedAt` 用于相邻采样差分，
例如 `ΔPaths[i].SentPackets / Δseconds` 得到 PPS。累计计数和峰值贯穿 Peer 生命周期，
不因 Session 关闭或 socket rebuild 清零，`Peer.Close()` 后仍可读取。各字段独立原子采样，正在收发时
不是同一瞬间的一致性事务；不可用单次快照中字段之间的细微差异推断丢包。

默认记录 ingress accepted/drop、delivery accepted/drop、成功写入的 Datagram 和实际
`ReadPacket` 返回的 payload 字节，以及 FEC 完成、需要 parity 的恢复 block/缺失 data
shard、pending 超时、decoder-full、pending duplicate 和 completed-cache late shard。
`FEC.LateShards` 包括已经恢复完成后正常抵达的剩余 parity/data shard，不等于网络丢包，
也不覆盖 completed cache 过期或淘汰后的到达。`IngressDrops` 仅统计完整 packet ingress
队列溢出；可恢复错误事件、鉴权拒绝及关闭时未消费的数据不计入这个数值。

`FEC.CompletedCapacityEvictions` 只累计 completed cache 因容量上限产生的淘汰，不包含
TTL 到期。`PendingBlocks`、`PendingShards`、`PendingBytes` 是所有存活 decoder 的当前
占用总和，会在完成、超时和关闭时下降；`PendingBytes` 仅计算 decoder 持有的 shard
payload 字节，不含 map、索引、codec 和重建过程的临时内存。对应的
`PendingBlocksHighWater`、`PendingShardsHighWater`、`PendingBytesHighWater` 保存 Peer
生命周期内各总和的独立峰值，关闭后仍保留；它们不是逐 Session 峰值之和。

容量淘汰计数与 pending 占用不能单独证明某个 completed key 被晚到 shard 重新打开。
以下确定性工作负载先完成 32 个已知 PacketID，再释放各自两个 parity shard；固定
RS(3+2)、1200-byte Datagram、16 个 pending 槽，比较 completed 容量 8/16/32：

```sh
go test ./internal/fec -run TestDelayedParityCapacityDiagnostics -v
go test ./internal/fec -run '^$' -bench BenchmarkDelayedParityCapacity -benchmem
```

| Completed 容量 | 容量淘汰 | 重新打开的 pending block | Pending shard | Pending 字节 | Decoder-full | 新 block 被拒绝 |
|---|---|---|---|---|---|---|
| 8 | 24 | 16 | 32 | 12800 | 17 | 是 |
| 16 | 16 | 16 | 32 | 12800 | 1 | 是 |
| 32 | 0 | 0 | 0 | 0 | 0 | 否 |

Pending 指标在全部晚到 parity 处理后、尝试新 block 前采样；decoder-full 包含随后对新
block 首个 shard 的尝试。测试还验证固定 decode timeout 到期后占用归零。该实验记录
v1 当前行为并为 #18 提供基线，不修复该缺陷，也不代表网络吞吐量或生产容量建议。

`Paths` 只包含配置顺序的 `carrier-N` 和可选的 `listener`：同一 Carrier 索引的多个
Session socket 合并，listener 为单个共享 socket 的汇总，不输出地址、SessionID、PSK
或业务内容。计数是 UDP payload 层，包括 MPUDP 头部、FEC 和控制报文，不含 IP/UDP/L2
头部。接收字节使用 `Read`/`ReadFrom` 返回的长度；超大报文可能已被内核截断至接收
buffer，因此该情况另计 `ReceiveOversizeDrops`。`SentBytes` 只累计 socket write 返回
的已写入字节，`SentPackets` 仅计完整成功的 socket write，`SendErrors` 计 socket write
错误或 short write；写前校验、设置 deadline 失败和内核/qdisc 后续丢弃不在其中。

`ListenerPaths` 单独统计监听端已认证且协议语义接受的 Endpoint 流量，路径名称按首次
接受顺序分配为 `listener-path-N`。Peer 生命周期内最多保留 256 个匿名路径槽，之后
的新路径合并至 `listener-overflow`，不再保留新地址索引。身份包含监听 socket generation
和本地/远端 Endpoint，不含 SessionID；同一路径跨 Session、Endpoint TTL 到期后仍复用
已有槽，计数不会清零，也不回收槽。统计快照不输出地址或身份哈希。

无效认证、未知 Session 非 HELLO、不兼容握手、Endpoint/Session/decoder 容量拒绝和
不匹配的 PONG 不分配槽，也不增加路径接收计数；已接受的 duplicate/late shard 仍计入。
CLOSE 只归入该 Session 已有的源 Endpoint，不为未知源分配槽。发送包含该路径上的
HELLO_ACK、PONG、keepalive、DATA 和 CLOSE 的实际 socket 写入。因接收范围不同，
`ListenerPaths` 总和不必等于包含无效/超大报文的 `Paths` 中 `listener` 汇总；路径行的
`ReceiveOversizeDrops` 为零，超大报文只计入原始 socket 行。

`Peer.SetDiagnosticsEnabled(true)` 打开额外诊断，默认关闭：

- `IngressQueue`：callback enqueue 到 dispatcher 处理之间的队列时间。
- `SendLatency`：公共 Session 写入通过生命周期检查后，内部 Datagram 写入的总耗时，
  包括编码、调度和 socket send；不是接收确认时间。
- 每路径 `WriteQueue`：socket 写锁的实际等待时间，不包含 deadline 等写前准备；
  `SocketWrite` 为实际 socket 调用耗时，不包含写后计数和 deadline 清理。
  listener socket 汇总与匿名路径使用同一次 socket 调用测量，不计上层 `Send` 包装耗时。
- 每路径 `SentPacketSizes` / `ReceivedPacketSizes`：完整 UDP payload 长度的固定分桶，
  `UpperBounds` 为包含上界，`Counts` 为各桶独立计数。

延迟使用固定 24 桶：`<=1us, <=2us, ..., <=4194304us, overflow`，同样为独立计数；
`TotalNS` 和 `MaxNS` 单位为纳秒。关闭诊断保留已有样本；开关切换前启动的在途操作仍可
完成统计。关闭状态保留基本原子计数，但不读取额外时钟或记录包长分布；
`go test -run '^$' -bench BenchmarkIngressDiagnostics -benchmem .` 可比较 ingress 局部
开销，不代替完整负载下的开启/关闭实验。

这些统计不声称在 256 个槽以外逐一覆盖监听端远端、socket receive overflow、qdisc drop、KCP
RTT/RTO/重传、业务 ACK 返回排队、per-Carrier MTU epoch/probe/padding 等指标。它们需要
基准工具的相应内核/上层采样或后续协议实现，不能以零值替代缺失证据。

`SessionID` 是 `[16]byte`。`NewSessionID()` 和公开 `NewPeer()` 始终使用
`crypto/rand.Reader`；全零结果有限重试，随机源失败不会返回可用 ID。配置和公共 API 没有
设置固定 SessionID 的入口。

Peer、Listener 和运行时 Session 的默认格式只包含角色、计数、状态及 SessionID 的短哈希。
错误文本只包含稳定类别或 packet/path 计数。PSK、认证 tag、完整 SessionID、完整 payload
和底层注入错误文本不会进入默认诊断；底层 cause 仍可通过 `errors.Is`/`errors.As` 检查。

## 稳定错误类别

调用方应使用 `errors.Is` 判断：

| Sentinel | 含义 |
|---|---|
| `ErrInvalidConfig` | YAML、配置值或 nil context 无效 |
| `ErrMessageTooLarge` | Datagram 超过配置/协商后的有效上限 |
| `ErrClosed` | Peer、Listener 或 Session 已关闭 |
| `ErrAuthentication` | HMAC/认证失败 |
| `ErrHandshakeIncompatible` | 协议、FEC 或 transport 能力不兼容 |
| `ErrNotReady` | initiator 尚未完成握手或握手已耗尽 |
| `ErrModeUnavailable` | 当前配置未启用请求的 bootstrap 角色 |
| `ErrResourceLimit` | Session、Endpoint 或 FEC block 达到有界上限 |
| `ErrNoAvailablePaths` | Datagram 开始发送时没有健康路径 |
| `ErrPartialSend` | 一个 Datagram 的部分 FEC shard 发送失败 |
| `ErrAllSendsFailed` | 一个 Datagram 的全部 FEC shard 发送失败 |
| `ErrPathMTUExceeded` | UDP packet 超过路径已知 MTU；可与发送结果类别同时匹配 |

认证失败和 malformed 外部 UDP packet 在 dispatcher 内被丢弃，不会创建公共 Session 或通过
`ReadPacket`/`Accept` 暴露；上述类别适用于可观察的公共调用和保留 cause 的错误链。
