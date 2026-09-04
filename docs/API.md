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
