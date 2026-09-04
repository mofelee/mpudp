# MPUDP v0.1 公共 API

公共包为 `github.com/mofelee/mpudp`，严格配置模型位于
`github.com/mofelee/mpudp/config`。

## 生命周期

```go
cfg, err := config.Parse(yamlBytes)
if err != nil {
    // errors.Is(err, mpudp.ErrInvalidConfig)
}

peer, err := mpudp.NewPeer(cfg)
if err != nil {
    // 配置失败；此时没有 socket、goroutine 或 timer
}
defer peer.Close()
```

YAML 解析只对真正省略的可选字段应用默认值。Go 调用方直接组装配置时必须先调用
`config.Default()`，再覆盖模式、FEC、PSK 和需要调整的选项；零值 `config.Config`
不会由 `Validate` 或 `NewPeer` 隐式补默认。

`Peer.NewSession()` 只在 initiator/dual 模式可用；`Peer.Listener()` 只在
listener/dual 模式可用。错误模式返回 `ErrModeUnavailable`。`Peer.Close()` 会关闭
它创建的 Session 和 Listener，且可以重复或并发调用。

当前 Loop 1 的构造函数只复制配置并建立内存中的生命周期句柄。它们不绑定
`listen`、不建立 Carrier、不进行 DNS 查询，不启动 goroutine/timer，也不实现 wire、
FEC 或握手。对未关闭句柄调用 `WritePacket`、`ReadPacket` 或 `Accept` 会立即返回
`ErrNotReady`；后续 loop 将替换这些路径而保持 API 契约。

## Datagram 接口

```go
type Session interface {
    WritePacket(payload []byte) error
    ReadPacket() ([]byte, error)
    Close() error
}
```

一次成功的 `WritePacket` 表示一个完整 Datagram，一次成功的 `ReadPacket` 返回一个
完整 Datagram。API 不暴露 shard、Carrier 或上层协议，不把多个 Datagram 合并，
也不把一个 Datagram 拆成多个上层交付。不同 PacketID 不承诺有序交付。

```go
type Listener interface {
    Accept(ctx context.Context) (Session, error)
    Close() error
}
```

`Accept` 接受 context 以表达取消/超时；nil context 返回 `ErrInvalidConfig`。

## SessionID

`SessionID` 是 `[16]byte`。`NewSessionID()` 和公开的 `NewPeer()` 始终使用
`crypto/rand.Reader`。全零结果会做有限次重试；随机源失败不会返回可用 ID。配置和
公开 API 都没有设置固定 SessionID 的入口。包内测试通过 reader 注入验证读取长度、
错误传播和全零重试，而不把不安全注入暴露给调用方。

## 并发语义

- `Peer.NewSession`、`Peer.Listener`、`Peer.Config`、`Peer.Mode` 和 `Peer.Close` 可并发调用。
- 同一 `Session` 的 `WritePacket`、`ReadPacket` 和 `Close` 可并发调用。
- 同一 `Listener` 的 `Accept` 和 `Close` 可并发调用。
- `Close` 幂等；关闭后的新操作返回 `ErrClosed`。未来阻塞的 `ReadPacket`/`Accept` 必须被唤醒。
- 多个并发 `WritePacket` 仍保持各自 Datagram 边界，但发送顺序不作保证。

## 稳定错误类别

调用方应使用 `errors.Is` 判断：

| Sentinel | 含义 |
|---|---|
| `ErrInvalidConfig` | YAML 或配置值无效 |
| `ErrMessageTooLarge` | Datagram 超过发送前可判断的有效上限 |
| `ErrClosed` | Peer、Listener 或 Session 已关闭 |
| `ErrAuthentication` | HMAC/认证失败 |
| `ErrHandshakeIncompatible` | 协议、FEC 或 transport 能力不兼容 |
| `ErrNotReady` | 当前骨架尚无运行时数据面 |
| `ErrModeUnavailable` | 当前配置未启用请求的 bootstrap 模式 |
| `ErrResourceLimit` | 操作会超过配置的 Session/Endpoint/block 等有界资源 |

错误文本不是兼容接口，也不会包含 PSK、认证 tag 或完整 Payload。
