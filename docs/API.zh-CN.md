# MPUDP 公共 API

[English](API.md)

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
v2 在打开 Carrier socket 前预留临时启动额度。DNS 和 socket 启动不持有共享 Peer
mutex；取消或失败会释放临时预留和已创建的 socket 资源。

保留既有 v1 Datagram；Linux 还支持显式 `wire.version: v2` 的 Datagram，要求
`transport.mtu_discovery: fixed`、`transport.budget_strategy: session` 和关闭 repair。
`aggregation.enabled` 可为 true 或 false。KCP、repair、PLPMTUD、per-Carrier budget
及非 Linux 平台的 v2 仍返回 `ErrProtocolUnavailable`，Peer 为 nil；这些合法但不支持的
选择在访问 context、随机源、socket/timer 依赖和启动 goroutine 之前拒绝。
非法配置仍先返回 `ErrInvalidConfig`，例如 KCP 配 v1、非零 FEC 或 v2 UDP 上限小于 512。
`Parse` / `Validate` 成功只表示配置合法，不表示运行时可用；没有自动降级或 KCP packet adapter。
两个错误均通过 `errors.Is` 区分，错误不回显协议输入值或 PSK。

`NewPeerContext` 提供相同的启动路径，并允许 context 取消 listener bind、Carrier dial 和
运行时网络操作。context 取消会阻止或中止工作，但调用方仍须调用 `Peer.Close`，才能同步
关闭 socket 并等待 dispatcher、receive loop 和在途 callback 全部退出。

CLI `cmd/mpudp` 读取 `-config` 后创建 Peer。initiator/dual 模式还会自动创建一个 outbound
Session，然后持续运行直到 SIGINT 或 SIGTERM；启动失败或收到信号时都会执行受控 Close。

YAML 解析只对真正省略的可选字段应用默认值。Go 调用方直接组装 v1 配置时先调用
`config.Default()`，再覆盖角色、FEC、PSK 和需要调整的选项；v2 使用
`config.DefaultV2(protocol)` 初始化共享 transport/资源和所选协议的配置默认值；上述 Linux
Datagram 子集可以启动，KCP 等未实现组合仍返回 `ErrProtocolUnavailable`。
零值 `config.Config` 不会由 `Validate` 或 `NewPeer` 隐式补数值
默认；直接 Go literal 必须显式满足全部 v2 校验。`Clone()` 同时深复制方向路径预算和 rate
map。`Config.Protocol` 与 `Config.Wire.Version` 使用 `config.Protocol` /
`config.WireVersion` 类型；`Default()` 填入 `ProtocolDatagram`
与 `WireVersionV1`。为兼容旧 Go struct literal，只有这两个新增字段的空字符串按
datagram/v1 解释，并可通过 `EffectiveProtocol()` / `EffectiveWireVersion()` 查询；原配置
不被改写。显式 YAML 空字符串仍无效。KCP 选择必须显式使用 `WireVersionV2` 并保持 FEC 0/0，
目前构造仍返回 `ErrProtocolUnavailable`。

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
Datagram。空 Datagram 返回非 nil 的零长度 slice。不同 Datagram 不承诺有序交付。
同一存活 Session 接收方向内，FEC duplicate/late shard 不会让同一原 Datagram 重复交付。
接收端使用固定 65536-ID 窗口，尚未接纳且已落到窗口外的 Datagram 会被丢弃；既有 pending
block 保留到原 deadline。`Close` 后重新创建的 Session 不继承窗口。MPUDP 不提供可靠、
有序或流语义；完整范围和 v1 HELLO 重放边界见 [FEC 设计](FEC.md#解码超时与去重)。
v2 为原 DatagramID 和编码 GroupID 分别维护 Completed/Expired 终态；完成的原报文
才进入交付队列，分片不会直接暴露给应用。`ReadPacket` 返回的 slice 归调用者所有，
随后 Session.Close 不会清零或复用它。

`NewSession` 不等待握手完成。握手期间 `WritePacket` 返回 `ErrNotReady`；建立后，payload
先按协商后的有效 Datagram 上限检查，超限返回 `ErrMessageTooLarge`，不会分配 FEC block、
取得 PacketID 或发送任何 shard。其他发送失败通过根包的 `ErrNoAvailablePaths`、
`ErrPartialSend`、`ErrAllSendsFailed` 和 `ErrPathMTUExceeded` 分类，并保留底层
`errors.Is` cause。一个错误可同时匹配发送结果和路径原因，例如 partial send 与 PMTU。

v1 和关闭聚合的 v2 在 `WritePacket` 返回前完成该报文的本地编码/socket 尝试；这不是
远端交付确认。开启 v2 聚合后，成功只表示完整 payload 已复制、额度已预留并在有界队列
中取得 DatagramID，调用者可立即复用输入 slice。容量不足返回 `ErrResourceLimit`，
不保留前缀、不取得 ID、不建立后台等待队列。并发 admission 在队列提交处排序；容量、
descriptor 数、最早 admission 的 `aggregation.max_delay` 或显式 Flush 触发封组。
后续写入不会刷新最早报文的等待期限；操作系统调度及共享 dispatcher 工作仍可能增加延迟。

v2 Datagram Session 还实现可选接口；原有三方法 `Session` 和 v1 行为保持兼容：

```go
type DatagramSession interface {
    Session
    Flush(context.Context) error
    CloseGracefully(context.Context) error
}
```

可通过 `datagram, ok := current.(mpudp.DatagramSession)` 查询支持。
`Flush(ctx)` 捕获调用时已提交的 admission 边界、封闭其尾组，并等待该边界内全部原始
shard 完成本地 socket 尝试或失败；不等待 repair、远端 ACK 或应用读取，也不包括之后
提交的写入。取消 context 只停止该次等待，已经接纳的报文仍可能发送。首个异步发送错误
保存在 Session 中，后续相关 Flush/CloseGracefully 可以观察到，不依赖可能丢弃的
`Peer.Errors()`。nil context 返回 `ErrInvalidConfig`。

`CloseGracefully(ctx)` 停止新的 admission、Flush 已接纳的报文，并在成功、失败或
context 到期后关闭 Session；当前不支持 repair，因此不包含远端修复义务。重复调用返回
首次调用完成后的结果。普通 `Close`
可丢弃已接纳但未发送的内容。两者都不会把本地发送完成解释为远端交付保证。

`ReadPacket` 会阻塞到一个完整 Datagram 到达或 Session 关闭。它当前不接受 context；需要
取消等待时，调用方应关闭 Session 或其所属 Peer。Close 后不会交付队列中残留的数据。

```go
type Listener interface {
    Accept(ctx context.Context) (Session, error)
    Close() error
}
```

`Accept` 阻塞到认证且兼容的 Session 完成入站接纳、context 取消/超时，或 Listener 关闭。
v2 在 FINISH 验证、已预留额度提升和组件安装成功后发送 READY；待接受槽位在握手前
预留，只有公共 Accept 取出 Session 才释放该计数。nil context 返回 `ErrInvalidConfig`。
每个入站 Session 最多入队一次，握手重试不会生成重复的公共 Session。
`Listener.Close` 停止入站接收、关闭已接受及等待接受的入站 Session，并唤醒全部
Accept；dual Peer 的 outbound Session 不受影响。

## 有界队列与 deadline

Peer 只创建一个 runtime dispatcher goroutine 和一个可复用 timer。v1 驱动 HELLO retry、
keepalive、Endpoint expiry 和 FEC sweep；v2 驱动握手/control retry、聚合尾组及 group/original
接收期限。每个 UDP Carrier 可有一个
transport receive loop；callback 只把拥有所有权的 packet 非阻塞放入 ingress，不执行认证、
FEC、Close 或 goroutine 创建。

| 资源 | 容量 | 满载策略 |
|---|---:|---|
| Peer packet/recoverable-error ingress | `limits.receive_queue_capacity` | drop newest event |
| Listener terminal failure latch | 1 | retain first terminal failure |
| v1 Listener accept | `limits.receive_queue_capacity` | close/release newest Session |
| v2 Listener accept | `limits.max_pending_accepts` | 握手接纳时预留；不足时拒绝新接纳 |
| 每 Session delivery | `limits.delivery_queue_capacity` | drop newest Datagram |

因此一个慢 `Accept`/`ReadPacket` 消费者不会阻塞 transport callback 或无限增加
内存。drop 策略不会产生 DATA 重传或把 Datagram 降级成字节流。listener 的 terminal socket
error 使用独立的一次性 latch，不会因 packet ingress 已满而丢失；超大 packet、nil remote
和临时网络错误仍是可恢复的单次诊断，不会关闭仍可用的 Listener 或 Carrier。

公共配置到内部 Session 的额外有界映射为：

- v1 completed PacketID 使用固定 65536-ID / 8 KiB 接收窗口，独立于 pending 容量和 Endpoint TTL；
- v1 handshake jitter 未单独暴露配置，使用 retry interval 的四分之一默认值。
- v2 的原 Datagram 和编码组各用独立有界终态窗口；ring、接收状态、FEC 输出和待交付
  payload 由 Session/Peer 额度计费，失败或关闭先清理存储再释放额度。

额度计量已预留义务和 Peer/Session 拥有的存储，不是进程 RSS。Go allocator/GC 保留的
内存及 codec 共享查找表不包含在这些 ownership 计数中。

v2 当前使用串行 dispatcher 和有界的同步 socket 尝试，每次使用 20ms context。
同一 Peer 的编码或发送工作可能延迟其他 Session；没有每报文 goroutine 或无界等待队列。
`limits.max_send_workers` 和 path queue 配置不是已实现的并行发送池保证。当前路径选择/
速率限制不代表完整 #22 scheduler、快速健康检测或性能验收；尚未交付 repair、MTU 探测/
迁移、KCP 或 smux，也不声称达到 #16 的吞吐目标。

## 关闭与并发

- `Peer.NewSession`、`Peer.Listener`、`Peer.Config`、`Peer.Mode` 和 `Peer.Close` 可并发调用。
- 同一 `Session` 的 `WritePacket`、`ReadPacket` 和 `Close` 可并发调用。
- 同一 `Listener` 的多个 `Accept` 和 `Close` 可并发调用。
- 多个并发 `WritePacket` 保持 Datagram 边界；v2 ID 按 admission 提交排序，不保证物理发送或交付顺序。
- `Peer.Close`、`Listener.Close` 和 `Session.Close` 均幂等；并发调用共享首次关闭结果。

关闭首先阻止新的 write/Session/accept，再取消 dispatcher 和在途 Session 操作；内部状态机
在 socket 可用时有界尝试 CLOSE（v1 最长一秒，v2 每次 socket 尝试使用 20ms context），随后关闭 Carrier/listener socket、释放
FEC/Endpoint/timer 状态、唤醒阻塞调用，并等待 receive loop、callback 和 dispatcher 退出。
Close 返回后不再有属于该对象的后台网络活动。收到认证的远端 CLOSE 也会释放对应公共
Session 和 initiator Carrier。
v2 Session 和 Listener 关闭会取消各自关联的在途发送。

`Peer.Errors()` 返回容量为一的异步诊断 channel。运行时生产者不会阻塞；channel 已满时
丢弃最新诊断。错误文本只给出稳定操作类别，底层 cause 仍可用 `errors.Is`/`errors.As`
检查。该 channel 在 `Peer.Close` 时不会关闭，消费者应与自己的 lifecycle context 一起
select。

## SessionID 与诊断

`Peer.Statistics()` 返回可直接 JSON 编码的有界诊断快照；`CapturedAt` 用于相邻采样差分，
例如 `ΔPaths[i].SentPackets / Δseconds` 得到 PPS。累计计数和峰值贯穿 Peer 生命周期，
不因 Session 关闭或 socket rebuild 清零，`Peer.Close()` 后仍可读取。各字段独立原子采样，正在收发时
不是同一瞬间的一致性事务；不可用单次快照中字段之间的细微差异推断丢包。

以下详细 FEC、认证后的 `ListenerPaths` 和延迟统计以既有 v1 运行时为准。v2 复用 Peer/transport 的
基本 ingress、交付、admission 和 socket 计数，但尚不提供同等的内部 FEC/路径诊断覆盖；
零值不能解释成没有丢包或恢复。v2 `SentDatagrams` 统计成功 admission，并非远端交付。

默认记录 ingress accepted/drop、delivery accepted/drop、成功写入的 Datagram 和实际
`ReadPacket` 返回的 payload 字节，以及 FEC 完成、需要 parity 的恢复 block/缺失 data
shard、pending 超时、decoder-full、pending duplicate、已完成 ID 的 late shard，以及
固定窗口外的 `TooOldShards`。`FEC.LateShards` 包括窗口内已恢复完成后正常抵达的剩余
parity/data shard；`TooOldShards` 包括没有既存 pending state 且低于窗口下界的 ID，不能
区分从未到达的数据与已经完成的数据。两者均不能单独证明网络丢包，旧 ID 丢弃不会增加
decoder-full。`IngressDrops` 仅统计完整 packet ingress
队列溢出；可恢复错误事件、鉴权拒绝及关闭时未消费的数据不计入这个数值。

`FEC.CompletedCapacityEvictions` 保留用于旧内部 decoder 对照，只累计旧 completed cache
因容量上限产生的淘汰，不包含 TTL 到期；生产窗口不使用该缓存，因此该计数为零。
`PendingBlocks`、`PendingShards`、`PendingBytes` 是所有存活 decoder 的当前
占用总和，会在完成、超时和关闭时下降；`PendingBytes` 仅计算 decoder 持有的 shard
payload 字节，不含 map、索引、codec 和重建过程的临时内存。对应的
`PendingBlocksHighWater`、`PendingShardsHighWater`、`PendingBytesHighWater` 保存 Peer
生命周期内各总和的独立峰值，关闭后仍保留；它们不是逐 Session 峰值之和。

容量淘汰计数与 pending 占用不能单独证明某个 completed key 被晚到 shard 重新打开。
以下确定性工作负载先完成 32 个已知 PacketID，再释放各自两个 parity shard；固定
RS(3+2)、1200-byte Datagram、16 个 pending 槽，比较旧 completed 容量 8/16/32 与固定窗口：

```sh
go test ./internal/fec -run 'TestDelayedParity(Capacity|Window)Diagnostics' -v
go test ./internal/fec -run TestReplayWindowHighBlockRateDelayedParityDoesNotReopen -v
go test ./internal/fec -run '^$' -bench BenchmarkDelayedParityCapacity -benchmem
```

| 模式/旧 Completed 容量 | 容量淘汰 | 重新打开的 pending block | Pending shard | Pending 字节 | Decoder-full | 新 block 被拒绝 |
|---|---|---|---|---|---|---|
| 旧缓存 / 8 | 24 | 16 | 32 | 12800 | 17 | 是 |
| 旧缓存 / 16 | 16 | 16 | 32 | 12800 | 1 | 是 |
| 旧缓存 / 32 | 0 | 0 | 0 | 0 | 0 | 否 |
| 65536-ID 窗口 / 任意旧容量 | 0 | 0 | 0 | 0 | 0 | 否 |

Pending 指标在全部晚到 parity 处理后、尝试新 block 前采样；decoder-full 包含随后对新
block 首个 shard 的尝试。窗口模式将全部 64 个已知晚到 parity 计为 `LateShards`。
高 block-rate 对照先完成 65568 个 ID，再释放全部 parity：窗口模式的 `LateShards` 为
131072、`TooOldShards` 为 64、重新打开的 pending 和 decoder-full 均为零，新 block
仍可完成。这些确定性实验验证 #18 的接收状态修复，不代表网络吞吐量或生产容量建议。

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
不匹配的 PONG、窗口外旧 DATA 不分配槽，也不增加路径接收计数；已接受的
duplicate/late shard 仍计入。
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
| `ErrProtocolUnavailable` | 有效配置选择了当前平台未实现的协议或功能组合 |
| `ErrMessageTooLarge` | Datagram 超过配置/协商后的有效上限 |
| `ErrClosed` | Peer、Listener 或 Session 已关闭 |
| `ErrAuthentication` | HMAC/认证失败 |
| `ErrHandshakeIncompatible` | 协议、FEC 或 transport 能力不兼容 |
| `ErrNotReady` | 握手或必要的 v2 context 尚未就绪 |
| `ErrModeUnavailable` | 当前配置未启用请求的 bootstrap 角色 |
| `ErrResourceLimit` | Session、Endpoint、FEC block、admission/accept 队列或 Session/Peer 保留额度不足 |
| `ErrNoAvailablePaths` | Datagram 开始发送时没有健康路径 |
| `ErrPartialSend` | 一个 Datagram 的部分 FEC shard 发送失败 |
| `ErrAllSendsFailed` | 一个 Datagram 的全部 FEC shard 发送失败 |
| `ErrPathMTUExceeded` | UDP packet 超过路径已知 MTU；可与发送结果类别同时匹配 |

认证失败和 malformed 外部 UDP packet 在 dispatcher 内被丢弃，不会创建公共 Session 或通过
`ReadPacket`/`Accept` 暴露；上述类别适用于可观察的公共调用和保留 cause 的错误链。
