# MPUDP v0.1 Session 状态机

`internal/session` 实现认证驱动的 Session bootstrap、Endpoint learning、keepalive、
方向性 UDP payload budget 和 FEC 数据面编排。它不创建或关闭 UDP socket，也不修改公开
`Peer` API；`peer.go` 的队列、Accept/Read/Write 包装和 transport callback 投递由 #7 集成。

## 分层边界

Session 层接收完整的 `transport.ReceivedPacket`，并保留其中 generation-bound
`ReplyPath`。该路径绑定实际接收 socket、local address 和 remote UDP Endpoint，因此
HELLO_ACK、PONG 和 Listener 反向 DATA 都从同一监听源端口发出。

处理顺序固定为：

1. 使用本地 hard receive limit 调用 `wire.DecodeAuthenticated`；
2. 验证 ReplyPath 当前可用，并取得一次 local/remote address 快照；
3. 查找 Session，验证角色、状态、冻结的 FEC/能力和 negotiated receive budget；
4. 验证 Endpoint 容量以及 DATA/PONG 的 Session 语义；
5. 最后才创建或刷新 Session、Endpoint、probe 或 FEC decode state，并按需响应。

错误 PSK、bit 篡改、截断、未知 Session 的非 HELLO、非法 FEC 和 `0xffff`
capability 都不会创建或刷新 Session/Endpoint/deadline/FEC state，也不会触发响应。错误和
诊断字符串不包含 PSK、packet payload、完整 tag 或完整 SessionID；`Session.String`
只显示 SessionID 的短 SHA-256 fingerprint。

Transport 的 receive callback 是同步的。#7 必须先向有界 ingress queue 做非阻塞投递，
再由 worker 调用 Session；不得在 callback 内执行 FEC 恢复、同步 Close 或 Rebuild，也不
得为每个 packet 启动 goroutine。

## 状态迁移

| 当前状态 | 事件 | 下一状态 | 行为 |
|---|---|---|---|
| `handshaking` | Initiator `Start` | `handshaking` | 每个 Carrier 各发送一次 HELLO |
| `handshaking` | 任一匹配 HELLO_ACK | `established` | 冻结参数和预算，创建方向性 FEC codec |
| `handshaking` | 所有 Carrier 尝试耗尽 | `handshake_failed` | 取消所有 retry deadline，不创建数据面 |
| 不存在 | Listener 收到兼容 HELLO | `established` | 原子创建一次、学习 Endpoint、原路 ACK |
| `established` | 重复匹配 HELLO/ACK | `established` | 刷新/增加 Endpoint；HELLO 再次原路 ACK |
| 任意 live 状态 | 本地 Close | `closed` | 取消工作并对当前路径各尝试一次 CLOSE |
| 任意 live 状态 | 认证 CLOSE | `closed` | 无响应、无新 Endpoint learning，释放状态 |

`closed` 是终态，Close 幂等。并发或重复 Close 共享同一个完成边界和首次 best-effort
发送结果。CLOSE 没有 ACK、重试或可靠关闭语义。

## Initiator bootstrap

Initiator 接收由上层安全生成的非零 16-byte SessionID 和固定顺序的独立 Carrier 路径。
首次 `Start` 在每条路径发送认证 HELLO。每条未确认 Carrier 独立维护 attempt 和 retry
deadline；initial HELLO 是 attempt 1。单路径发送失败不阻止其他路径。

Retry delay 为：

```text
handshake_retry_interval + jitter
0 <= jitter <= handshake_retry_jitter_limit
```

Jitter 由随机 SessionID、PathID 和 attempt 的 SHA-256 确定，因此测试可重复，同时不同
Session 不会统一唤醒。未显式设置上限时使用 interval 的四分之一；上限不得大于 interval。
每 Carrier 最多发送 `max_handshake_attempts` 次 HELLO，最终一次之后再经过一个 bounded
reply window 才标记耗尽。已经建立 Session 中仍未确认且耗尽的 Carrier 会退出 DATA
候选，但 keepalive 仍可探测并恢复它。

任一合法 ACK 立即建立 Session。已确认 Carrier 停止 HELLO retry；其他 Carrier 继续其
有界注册尝试，后续 ACK 加入同一 Session，但其耗尽不会关闭已经建立的 Session。Close
取消全部在途 send context 和未来 deadline。

## Listener registry 与 Endpoint pool

Listener 只允许认证且与本地 FEC 匹配的 HELLO 创建 Session。SessionID map 受
`MaxSessions` 硬限制；达到上限时确定性拒绝新 Session，不驱逐现有 Session。重复 HELLO
只返回既有 Session，Accept 通知由 #7 对 `HandleResult.Created` 恰好投递一次。

Endpoint identity 包含 transport PathID、socket generation、local address 和 remote
address，记录 generation-bound ReplyPath、最后认证活动时间、health 和最近 RTT。发送
候选按完整 identity 排序，绝不依赖 Go map iteration 顺序。

新增 Endpoint 前先清除已经达到 TTL 的记录。若仍达到 `MaxEndpoints`，采用固定的
reject-new 策略；现有 Endpoint 仍可刷新，不因攻击者提供的新来源而被驱逐。Endpoint
达到 TTL 后从调度候选和 probe state 删除，但 SessionID、已建立状态和其他路径不变。
认证 HELLO/ACK、合法 DATA、PING 和匹配 PONG 可以新增或刷新 Endpoint；CLOSE、错误 FEC、
over-budget DATA 和不匹配 PONG 不会刷新。

## FEC 与方向性预算

握手字段必须与本地 `data_shards`/`parity_shards` 完全一致。第一条合法 HELLO/ACK 冻结
peer capability；后续不同 FEC 或 capability 返回 `ErrHandshakeIncompatible`，不会部分
更新 Session。

v0.1 为两个方向分别保存字段，但当前确定性公式相同：

```text
send_max_udp_payload    = min(local_capability, peer_capability)
receive_max_udp_payload = min(local_capability, peer_capability)
```

例如双方声明 1200/1000 时，两个方向均为 1000。所有 HELLO、ACK、PING、PONG、CLOSE 和
DATA_SHARD 编码都受适用预算检查；已认证但超过冻结 receive budget 的 packet 不进入
Endpoint 或 FEC state。

建立时为该方向创建一个 `fec.Encoder` 和一个 `fec.Decoder`：

```text
shard_capacity             = negotiated_max_udp_payload - wire.DataShardOverhead
fec_derived_datagram_limit = data_shards * shard_capacity
effective_datagram_limit   = min(fec_derived_datagram_limit, configured_max_datagram)
```

RS(5,3)、budget 1200、wire overhead 71 时，capacity 为 1129，最大 Datagram 恰好
3387 bytes。3387 可编码为五个 1200-byte DATA_SHARD；3388 在分配 packet 或调用任何
`Path.Send` 前返回 `fec.ErrMessageTooLarge`。

`WritePacket` 取得当前健康路径的稳定快照，生成一个 FEC block，逐 shard 完成 wire/HMAC
编码，再调用 `transport.SendBlock`。每个 DATA shard 只尝试一次；没有 DATA ACK/NACK、重传、
协议内再分片或跨 Datagram 聚合。接收端把认证后的 shard 交给 bounded FEC decoder；达到
任意 k 个不同 shard 的同一次调用立即返回 recovered Datagram，PacketID 完成顺序不受限。

## Path health 与 PMTU

Session 在配置 Carrier 和 learned Endpoint 上保存独立 health bit。`Available()` 与 health
都为 true 才能进入后续 DATA scheduler snapshot。`ErrPathMTUExceeded`（包括 Linux
`EMSGSIZE`/Packet Too Big）只把发生错误的 PathID 标记 unhealthy；同一 block 的其他
shard 继续尝试，其他路径仍可双向发送，Session 保持 `established`。

Unhealthy Carrier 仍收到小型 keepalive PING，以便观察恢复，但不会继续承担 DATA。
匹配的认证流量会恢复该路径；transport rebuild 或外部诊断也可调用
`SetPathHealthy(pathID, true)` 明确恢复。v0.1 不动态改变全 Session shard size，不把一个
shard 切小重发，也不实现 PLPMTUD。

## Keepalive 与 RTT

建立后，每个 keepalive interval 都在每个配置 Carrier 上分别发送 PING，不能只选择一条
路径。Listener 也可以对尚未过期的 learned Endpoint 发送 probe。每路径最多保留一个
outstanding probe；下一轮替换旧值，因此内存上限为 Carrier/Endpoint 上限。

Token 非零且在 Session 内单调变化；timestamp 是对端必须原样回显的 opaque uint64。
收到 PING 后只在完成认证、状态和 Endpoint 检查后通过该 packet 的 ReplyPath 回 PONG。
PONG 必须同时匹配 Session、当前 path identity、token 和 timestamp；错误路径、旧 token、
重复或未来样本不刷新 Endpoint、不消费当前 probe，也不更新 RTT。RTT 只使用本地注入
Clock 的 send/receive time 差，不解释或信任对端时钟。

单 Carrier/Endpoint 超时或失败只移除该路径，不关闭整个 Session。

## Deadline 与资源所有权

Session 不为每 Endpoint 创建 goroutine 或 timer。它只保存以下有界逻辑 deadline：

- 每个配置 Carrier 最多一个 HELLO retry deadline；
- 每 Session 一个 keepalive deadline；
- 每 Session 一个 FEC sweep deadline；
- 每 Endpoint 的固定 `last_activity + TTL` 值。

`NextDeadline` 返回最早唤醒时间，集成层用一个 bounded driver 在注入 Clock 到达该时间时
调用 `Advance`。`Advance` 不补发错过 interval 的 burst：每类 due work 每次最多执行一轮，
然后从当前时间安排下一轮。FEC pending/completion、Session、Endpoint、probe 和 transport
queue 均有显式配置上限。

Close 清除所有 deadline、Endpoint、probe 和 FEC state，取消 in-flight control/DATA
context，并等待已进入的有界操作退出。状态机自身不启动后台 goroutine，因此不存在隐藏
timer 或 worker 需要回收。

## 测试契约

单元测试使用 fake Clock 和 fake ReplyPath，不依赖 sleep 或公网，覆盖：

- 任一 ACK 建立、其他 Carrier 后续加入、重复/不兼容握手；
- 1200/1000 双向协商和 `config == wire == transport` UDP hard limit；
- 未认证输入、非法 `0xffff` capability 的零状态/零响应不变量；
- retry jitter 上限、attempt 耗尽和 Close 取消；
- Endpoint cap/reject-new、TTL 和 Session cap；
- 每 Carrier PING、PONG path/token/timestamp 匹配与 RTT；
- RS(5,3) 3387/+1 边界、乱序 k-shard 恢复和反向 Endpoint 调度；
- PMTU path 隔离/恢复、其他路径继续发送；
- best-effort CLOSE、并发幂等 Close 和高频 Write context 清理。
