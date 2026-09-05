# MPUDP v0.1 Carrier 与 Shard Scheduler

本文档固定 v0.1 的 UDP Carrier 生命周期、Listener 回复路由、Shard 调度和发送错误语义。
本层只收发已经编码并认证的 MPUDP wire packet；它不解析 wire、不生成 FEC shard，也不
重试 DATA。

## 硬边界

| 项目 | v0.1 边界 |
|---|---:|
| FEC shards / block | 1..256 |
| 当前可用 Carrier/Endpoint | 1..256 |
| raw transport 单个 UDP payload | 1..65507 bytes |

`transport` 包自身接受 1..65507 bytes，以便保持通用 UDP 边界；有效 MPUDP 配置的
`transport.max_udp_payload` 范围更窄，为 72..65507 bytes，因为最小 authenticated
控制包需要 72 bytes。

`scheduler.Assign(packetID, shardCount, pathCount)` 在任何内存分配之前检查两个 count。
257、`MaxInt`、零和负数均返回 typed `CountError`。这使遗漏上游 Config 校验时也不会
根据任意整数分配内存。

## 确定性调度

一个 block 调用开始时取得当前可用路径的有序快照。Shard `i` 使用：

```text
start   = PacketID % M
path(i) = (start + i) % M
```

其中 `M` 是该快照中的路径数。结果仅由 `PacketID`、shard 数和有序路径快照决定，
不依赖 goroutine 或 I/O 完成顺序。

- `n > M`：每条路径承载的 shard 数相差不超过 1；
- `n == M`：每条路径恰好一个 shard；
- `n < M`：当前 block 只使用 `n` 条路径，后续 PacketID 轮换起点；
- 可用集合变化只影响后续 block，不重建 Session；
- v0.1 不使用 RTT、带宽、丢包率或随机数加权。

例如，5 shards / 2 paths：

```text
PacketID 100: A B A B A
PacketID 101: B A B A B
```

## Carrier 生命周期

每个配置 Carrier 由独立的 connected UDP socket 实现，语义等价于为每个远端分别执行
一次 `DialUDP("udp", nil, remote)`：本地地址/端口由操作系统选择，并在该 generation
生命周期内复用。发送 packet 时不会重新建 socket，也不会以一个 socket 轮流服务多个
配置 Carrier。

Carrier ID 在 rebuild 前后稳定。每次成功 rebuild 才递增 generation：

1. 创建并配置新 socket；失败时原 generation 保持可用；
2. 原子发布新 generation；
3. cancel 并关闭旧 socket；
4. 等待旧 read loop、回调和在途 write 退出后返回。

旧 packet 携带的 generation-bound ReplyPath 在替换后返回
`ErrGenerationReplaced`，不会误发到新随机源端口。`Close` 幂等，关闭 socket 并等待相同
清理边界；之后 Send/Rebuild 返回 `ErrClosed`。

接收 hook 在 read loop 中同步调用，必须是有界、非阻塞的队列投递，不得在 hook 内同步
调用所属 Carrier/Listener 的 `Close` 或 `Rebuild`。运行时队列容量和满载策略由 Peer
层统一拥有，transport 不创建第二套无界队列。

## Listener ReplyRoute

Listener 对每个收到的 packet 保存：

- 实际接收 packet 的 Listener socket 和 generation；
- 该 socket 的 local address；
- UDP source Endpoint。

生成的 ReplyPath 只调用该 Listener socket 的 `WriteTo(payload, endpoint)`。它不会 dial
新 socket，因此 NAT/conntrack 返回流量保持监听源 IP/port；多个监听 socket 也不会选错
本地入口。Listener 关闭后已保存的 ReplyPath 立即失效。

只有 wire/HMAC 层认证成功后，Session 层才可保存 ReplyPath 或学习 Endpoint。transport
提供路由事实，但不把未认证输入写入长期 Session 状态。

原生 `*net.UDPConn` Listener 使用 `ReadFromUDPAddrPort`，在长度验证后仅创建一次
自有 remote address 快照。IPv4-mapped 表示、IPv6 zone、ReplyPath identity 和原 socket
回复保持不变；每个报文仍拥有独立 payload 与地址快照。注入或包装的 `net.PacketConn`
继续调用其 `ReadFrom` 并沿用原有地址复制规则，不因实现同名 AddrPort 方法而绕过自定义行为。

## Block 发送结果

`SendBlock` 对 block 中每个 shard 恰好调用一次选定路径，即使先前 shard 失败也继续。
它返回每个 shard 的 path ID/index 和 error，但不包含 packet bytes：

| 状态 | 返回错误 |
|---|---|
| 没有可用路径，未尝试 | `ErrNoAvailablePaths` |
| 每个 shard 均成功 | `nil` |
| 至少一个成功、至少一个失败 | `ErrPartialSend` / `BlockSendError` |
| 所有 shard 均失败 | `ErrAllSendsFailed` / `BlockSendError` |

`BlockSendError` 支持 `errors.Is` 检查各路径原因。错误字符串只报告 path、operation、
generation 和计数，不格式化 PSK、认证 tag、payload 或注入 connection 的原始错误文本。
DATA 不在本层重试或拆分。

## PMTU、DF 与 EMSGSIZE

Linux UDP socket 创建时按地址族设置并回读验证：

| 地址族 | socket option | 值 |
|---|---|---|
| IPv4 | `IP_MTU_DISCOVER` | `IP_PMTUDISC_DO` |
| IPv6 | `IPV6_MTU_DISCOVER` | `IPV6_PMTUDISC_DO` |

`AF_INET6` socket 还会读取 `IPV6_V6ONLY`。值为 0 时，同一 dual-stack socket 可以发送
IPv4-mapped datagram，因此实现会同时设置并回读验证 IPv6 和 IPv4 两组 PMTU discovery
option；值为 1 的 IPv6-only socket 只需设置 IPv6 option。

DF/PMTU socket mode 禁止本地 IPv4 fragmentation，并让内核在已知路径能力不足时以
`EMSGSIZE`（或收到 Packet Too Big 后的等价错误）拒绝 datagram；transport 不会再拆
shard。该错误同时分类为 `ErrPathMTUExceeded`，只影响本次 shard 尝试，不会关闭
Carrier 或 Session；同一 block 的其他路径继续发送。

这不是动态 PMTU 探测器。远端 ICMP Packet Too Big 被过滤时，错误可能不会同步返回，
流量会静默黑洞。部署者必须把 `transport.max_udp_payload` 配置为所有 Carrier 的已知安全
最小 UDP payload；v0.1 不实现由 [#13](https://github.com/mofelee/mpudp/issues/13)
跟踪的 PLPMTUD/自适应预算，也不实现由
[#14](https://github.com/mofelee/mpudp/issues/14) 跟踪的 per-Carrier budget 或不等长
shard。

非 Linux build 的 `PMTUDiscoverySupported()` 明确返回 false。默认仍可使用 UDP，但不
声称具备无 IP fragmentation 保证；设置 `RequirePMTU` 会以 `ErrPMTUUnsupported` 拒绝
启动。v0.1 的“0 fragment”端到端验收只在 Linux 网络 namespace/VM 拓扑中成立。

## 测试边界

单元测试使用可注入 dial/packet connection 覆盖 rebuild、旧 read loop 退出、generation
路由、超限、partial/all/no-path 和并发 Close。真实 loopback 测试覆盖每 Carrier 独立源
端口及长期复用、Listener 同源端口回复，并在 Linux IPv6 loopback 上验证 DF 模式产生
真实 `EMSGSIZE` 后小 packet 仍可发送。`TestLinuxDualStackListenerConfiguresIPv4AndIPv6PMTU`
还固定 dual-stack Listener 同时配置 IPv4/IPv6 PMTU option 的要求。

跨 namespace 的小 MTU、1200/1000 capability negotiation、exact-limit/+1、
`ErrPathMTUExceeded` 和 0 IP fragment 证据由 canonical
`mtu-budget-no-fragment` 场景覆盖；本包测试不创建 namespace、nftables 规则或 VM。
