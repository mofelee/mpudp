# 预付费串行拨号准入

[English](v2-prepared-serial-dial.md)

发起方包装对象及其套接字可能在握手安装前已经存在，因此 `InstallDeferred` 无法覆盖
所有构造及待完成握手失败。预付费串行拨号在构造前保留一个未来 Session 的准入，
在串行回退期间持续持有，并最终移交给已安装对象的清理或待完成对象的终止清理。
公共运行时必须显式启用这一能力。

## API

```go
func (e *Engine) PrepareDial(
    now time.Time, policy Policy,
    disposePending func(releaseStorage func()),
) (*PreparedDial, error)

func (e *Engine) BeginPreparedDial(
    now time.Time, prepared *PreparedDial,
    carriers []Carrier, deadline time.Time,
) (DialID, Result, error)

func (e *Engine) AbortPreparedDial(now time.Time, prepared *PreparedDial) error
```

`PreparedDial` 是不透明句柄。副本共享同一个生命周期，无法直接释放其租约。
三个方法都要求沿用 Engine 的串行调用与不递减时钟约束。`PrepareDial` 要求配置
`InstallDeferred` 和非 nil 的待完成对象清理回调，并在发布前验证、复制策略。
准备失败不会调用清理回调，因为包装对象可能尚未存在。准备成功不会发包，也没有重试计时器。

应在分配包装对象、交付队列存储或打开任何 Carrier 之前调用 `PrepareDial`。
创建每个认证路由/绑定时，同时捕获原生传输代次和地址。构造完成后，使用完整的
配置 Carrier 顺序及可选的绝对截止时间启动拨号。

Carrier 数量、顺序、绑定无效，或截止时间已经到期时，不会消耗准备句柄，调用方可以
修正请求或中止。有效接管只发生一次；此后的标识符、熵或首个尝试失败，会在返回前
调用待完成对象的清理回调，即使尚未发布 DialID。已接管句柄再次启动或中止均返回
`ErrInvalid`，应使用 `CancelDial` 或 `CloseSession` 操作当前所有者。

未接管的准备可以重复中止，包括 Engine.Close 之后。其他 Engine 的句柄无效。
Engine.Close 还会中止全部未启动准备，不依赖后续 Begin 调用才能回收其所有权。

## 准入与回退

准备保留普通 Receive、Initial、握手数据包及延迟清理额度，额外保留
`PreparedDialBytes`（32768 字节），覆盖私有所有者、策略/句柄元数据和最多 256 个
Carrier 的请求。独立的 `DeferredDisposalBytes` 仍为 512 字节。
两项额度均不会改变 Initial 索引或最多 16 项的约束。沿用现有策略规则，只要 Initial
覆盖要求的接收下限，就支持空 Receive 声明。

当 Receive 非空、Initial 有 N 项时，准备使用 N+4 项预留、一个待完成握手名额及
一个未来 Session 名额。Receive 为空时使用 N+3 项预留。字节准入需要覆盖：

```text
Receive.Bytes + sum(Initial.Bytes) + PacketReservationBytes
    + DeferredDisposalBytes + PreparedDialBytes
```

`Snapshot.Prepared` 统计未启动准备，`Snapshot.Pending` 统计已启动的待完成尝试。
两者之和受 MaxPending 限制，普通拨号和监听准入也受此共享限制。
PacketBytes 包含尚未启动的数据包预留。账本的握手、Session、字节和预留数量上限仍然生效。

该 API 固定采用串行尝试。在提升作用域前，失败尝试可将同一待完成作用域、组件租约和
数据包存储复用于下一个 Carrier；旧数据包、密钥、转录及尝试引用会被清除。
每次尝试都有新的 SessionID 和客户端 nonce、独立重试预算及原始生命周期，
同时受未改变的调用方截止时间约束。回退不会调用包装对象清理，也无需第二个 Session
名额，支持 MaxSessions=1 且字节、预留数量恰好满额的情况。

提升作用域或部分安装后的失败，会终止预付费拨号。已提升作用域及已绑定或释放的
Initial 租约不能重新充当待完成准入。普通 `BeginDial`，包括 Concurrent>1 和原有
回退行为，继续使用现有 API。

## 清理所有权

待完成对象中止、回退耗尽、终止失败及 Engine.Close 会先关闭作用域并清除、释放协议
数据包及密钥状态，再调用待完成对象的清理回调。Receive、Initial、两项元数据额度和
Session 名额会一直保留到 `releaseStorage` 执行。回调在串行调用方内同步执行，必须
只标记、取消并安排有界清理，不能等待或重新进入 Engine。完成回调可以并发执行，
重复或复制调用均具有幂等性。

适配层必须等待构造结束后才能最终释放存储。阻塞的 Carrier 打开操作可能在较早的
清理过程检查已挂接 Carrier 之后才返回新套接字。必须保留完成回调，直到构造结束，
全部拥有的套接字、交付存储和包装对象状态均清除。Engine.Close 只启动这一退休过程；
适配层在所有者锁之外等待清理完成。

安装成功会将同一准入移交给 `InstallDeferred`，并禁用待完成清理钩子。
安装失败但返回非 nil 清理函数时，由该函数接管完整清理完成回调；返回 nil 表示部分
安装已自行清理，待完成钩子仍负责此前存在的包装对象。只有一条路径获得完成回调。
旧尝试 ID、过期准备句柄及旧 Dial 取消不能清理获胜的包装对象。

确定性测试覆盖满额回退和安装、跨关闭保留构造、准入回滚、共享待完成上限、复制及
错误 Engine 句柄、接管后的同步失败、提升后的安装失败、并发释放、组件索引保持和
元数据容量。该前置能力没有增加工作线程或网络测量。
