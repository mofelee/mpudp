# MPUDP v0.1 简化需求文档

本文件的每个编号章节都在 [需求追踪矩阵](TRACEABILITY.md) 中映射到实现、自动化验证、
tracking issue 和当前状态。字节级互操作细节以 [Wire Protocol](WIRE_PROTOCOL.md) 为准。

## 1. 目标

使用 Go 实现一个用户态 **Multipath UDP Tunnel**。

MPUDP 对上层提供一条双向 Datagram Tunnel。每个上层 Datagram 经过 Reed-Solomon 编码后形成多个 shard，再由调度器把 shard 分配到一个或多个 UDP Carrier 上传输。

```text
Upper Layer
     │ Datagram
     ▼
   MPUDP
     │ RS(n,k)
     ▼
  n shards
     │
Shard Scheduler
     │
 ┌───┼───┐
 ▼   ▼   ▼
UDP UDP UDP
```

MPUDP 利用不同 Carrier 背后的路径差异，提高对随机丢包、慢路径和链路失效的容忍能力。

## 2. 功能边界

MPUDP 负责：

- 建立和维护逻辑 Session；
- 管理多个 UDP Carrier；
- Reed-Solomon 编码和恢复；
- Shard 调度；
- 对端 UDP Endpoint 学习；
- NAT/conntrack 保活；
- 保留 Datagram 边界。

MPUDP 不负责：

- DATA ACK 或 NACK；
- DATA 重传；
- 可靠传输；
- 有序交付；
- 字节流；
- 拥塞控制；
- 理解任何具体上层协议。

上层 Payload 对 MPUDP 永远只是一个不透明的 `[]byte`。

## 3. 最小配置

主动连接侧：

```yaml
carriers:
  - "192.0.2.11:4000"
  - "192.0.2.12:4000"

fec:
  data_shards: 3
  parity_shards: 2

psk: "development-only-example-key"

transport:
  max_udp_payload: 1200
```

监听侧：

```yaml
listen: "0.0.0.0:9000"

fec:
  data_shards: 3
  parity_shards: 2

psk: "development-only-example-key"

transport:
  max_udp_payload: 1200
```

规则：

- 一个节点至少配置 `carriers` 或 `listen` 之一；
- 同一节点允许同时配置 `carriers` 和 `listen`；
- 配置 `carriers` 表示可以主动建立 Session；
- 配置 `listen` 表示可以接受 Session；
- 双方的 FEC 参数和 PSK 必须一致；
- `transport.max_udp_payload` 是完整 MPUDP UDP payload 的本地能力，合法范围为
  72..65507 bytes，默认 1200；握手后使用双方声明值的较小值；
- v0.1 不需要 `peer.id`；
- Session ID 由程序自动随机生成。

示例中的 `development-only-example-key` 仅供开发测试，生产不得复用。YAML 解析器只接受
scalar `psk`，不支持 `psk_file` 或环境变量展开。生产部署必须生成独立高熵密钥，并通过
secret manager、mode 0600 的受保护模板文件，或嵌入程序的 `config.Secret` 注入；不得把
PSK 写入日志、错误、命令行或诊断 artifact。完整边界见
[PSK 管理](CONFIGURATION.md#psk-管理)。

## 4. Carrier

`carriers` 中的每个值是一个远端 UDP 入口，格式为 `host:port`。

```yaml
carriers:
  - "192.0.2.11:4000"
```

它不表示：

- 本地绑定地址或本地源端口；
- 最终 Peer 的真实地址；
- 中间 UDP 转发节点内部的转发目的地；
- Wire Protocol 中的 Path ID。

对于每个 Carrier，必须创建一个独立并长期存活的 UDP socket：

```go
conn, err := net.DialUDP("udp", nil, carrierAddr)
```

本地 IP 和端口由操作系统选择。不得要求用户配置本地端口，也不得使用一个 UDP socket 轮流向全部 Carrier 发送。

Carrier 可以对应不同端口、地址、网卡、ISP、VPN、策略路由或 UDP NAT 转发节点。MPUDP 不需要理解这些底层差异。

## 5. Peer 和 Session

协议两端统一称为 Peer。

建立 Session 时可以存在：

- Initiator：首先发送 `HELLO`；
- Listener：监听并接受 `HELLO`。

Session 建立后，双方数据面完全对等，任意一端都可以发送 Datagram。

Session ID：

```go
type SessionID [16]byte
```

要求：

- 使用安全随机数生成；
- 不写入配置文件；
- 不依赖 UDP 五元组；
- 对端 IP 或端口变化时，Session 可以继续存在。

## 6. 动态地址和反向通信

必须支持：

```text
Alice：动态地址或 NAT 后
Bob：固定公网地址
```

Alice 通过 Carrier 主动发送 UDP，建立沿途 NAT/conntrack 状态。Bob 从认证成功的数据包中学习返回 Endpoint。

Session 建立后：

- Alice 可以向 Bob 发送；
- Bob 可以向已学习的 Endpoint 发送；
- 中间 NAT/转发节点负责把返回流量送回 Alice 对应的 UDP socket。

Bob 返回数据时必须使用接收 Session 的监听 socket，或至少保持与被转发目标相同的本地源 IP/端口。不能为返回流量另建随机源端口的 UDP socket，否则中间 conntrack 可能无法匹配。

现实限制：Alice 尚未发包、NAT 映射已过期或 Alice 已离线时，Bob 无法凭空冷启动连接到 Alice。双方对等指的是 Session 建立后的数据面。

## 7. Endpoint Learning

接收端记录认证成功数据包的 Source IP 和 Source Port，形成 Remote Endpoint Pool。

只有满足以下条件才能新增或刷新 Endpoint：

- Packet 格式有效；
- Session ID 有效；
- PSK 认证成功。

Endpoint 不需要预先配置。

当对端 IP 或 NAT 端口变化时，新的认证 Endpoint 可以加入 Pool；旧 Endpoint 超时后删除。Endpoint 变化不能直接销毁 Session。

## 8. Keepalive

使用 `PING` / `PONG`：

- 维持 NAT/conntrack 映射；
- 刷新 Endpoint 活跃时间；
- 判断 Carrier/Endpoint 是否可用；
- 记录基本 RTT。

Initiator 必须通过每个 Carrier 分别发送 Keepalive，不能只在其中一个 Carrier 上保活。

单个 Carrier 或 Endpoint 失效不能直接关闭整个 Session。

## 9. Reed-Solomon FEC

配置：

```yaml
fec:
  data_shards: 3
  parity_shards: 2
```

定义：

```text
k = data_shards
r = parity_shards
n = k + r
```

上例为 `RS(5,3)`：共生成 5 个 shard，收到任意 3 个即可恢复原始 Datagram。

要求：

- `data_shards > 0`；
- `parity_shards > 0`；
- 参数必须受所选 Reed-Solomon 库支持；
- 双方在握手时验证 FEC 参数一致。

## 10. 一个 Datagram 一个 FEC Block

每次 `WritePacket(payload)` 都独立形成一个 FEC Block：

```text
Upper Datagram
      │
分成 k 个 data shard
      │
生成 r 个 parity shard
      │
共 n 个 shard
```

v0.1 不等待多个上层 Datagram 再进行聚合编码，以避免额外延迟。

Header 必须包含原始 Datagram 长度，以便恢复后去掉编码填充。

## 11. FEC 与 Carrier 数量独立

禁止假设：

```text
total_shards == carrier_count
```

正确模型：

```text
n shards → M carriers
```

例如 `RS(5,3)` 可以运行在两个 Carrier 上：

```text
shard 0 → A
shard 1 → B
shard 2 → A
shard 3 → B
shard 4 → A
```

也可以运行在五个 Carrier 上：

```text
shard 0 → A
shard 1 → B
shard 2 → C
shard 3 → D
shard 4 → E
```

Carrier 数量发生变化时，Session 不需要重建，Scheduler 重新计算映射即可。

## 12. Shard Scheduler

v0.1 使用简单、均匀的轮询调度。

要求：

- 同一 FEC Block 的 shard 尽量均匀分布；
- Carrier 少于 shard 时，允许一个 Carrier 承载多个 shard；
- Carrier 多于 shard 时，允许只使用其中一部分；
- 每个新 Block 轮换起始 Carrier，避免长期固定偏向某个 Carrier；
- 某个 shard 发送失败时，仍继续尝试发送其他 shard；
- v0.1 不实现 RTT、带宽或丢包率加权。

示例：

```text
Block 100: A B A B A
Block 101: B A B A B
```

## 13. Shard 容错与 Carrier 容错

必须区分：

- Shard Loss Tolerance；
- Carrier Failure Tolerance。

`RS(5,3)` 始终意味着最多容忍 2 个 shard 丢失，但不代表无条件容忍任意 2 个 Carrier 失效。

例如某个 Block 的分配为：

```text
A = 3 shards
B = 2 shards
```

则：

- B 完全失效：剩余 3 个 shard，可以恢复；
- A 完全失效：剩余 2 个 shard，无法恢复。

只有将 shard 分布到真正独立的 Failure Domain，才能获得对应的完整链路容错能力。

## 14. Packet ID 和解码

每个原始 Datagram 分配一个单调递增的 `uint64 PacketID`。两个发送方向分别维护自己的 PacketID。

接收端按以下键聚合 shard：

```text
SessionID + PacketID
```

当收到至少 `data_shards` 个不同索引的有效 shard 后：

1. 立即恢复原始 Datagram；
2. 去掉填充；
3. 向上层交付一次；
4. 删除 Decode State；
5. 短期记住已完成 PacketID，丢弃迟到或重复 shard。

不得等待剩余慢 Carrier。

## 15. Datagram API 和语义

```go
type Session interface {
    WritePacket(payload []byte) error
    ReadPacket() ([]byte, error)
    Close() error
}
```

要求：

- 一次 `WritePacket` 对应一个 Datagram；
- 一次成功的 `ReadPacket` 返回一个完整 Datagram；
- 不合并 Datagram；
- 不拆成多个上层 Datagram；
- FEC 恢复后的同一个 Datagram 只交付一次；
- 不保证不同 PacketID 的交付顺序。

## 16. Packet Types

v0.1 只需要：

```text
HELLO
HELLO_ACK
DATA_SHARD
PING
PONG
CLOSE
```

不需要：

```text
PATH_REGISTER
PATH_ACK
DATA_ACK
DATA_NACK
```

## 17. Wire Header 最低字段

公共字段至少包含：

```text
Magic
Version
Packet Type
Session ID
Authentication Tag
```

`DATA_SHARD` 额外包含：

```text
Packet ID
Data Shards
Parity Shards
Shard Index
Original Datagram Length
Shard Payload
```

Wire Header 不包含 `PathID`、接口名称、ISP 或本地 Carrier 名称。所有多字节整数使用
Network Byte Order。v0.1 的 prefix、type body、长度和 full tag 是固定格式，不是可选
最低集合；精确 offset、范围和拒绝规则见 [Wire Protocol](WIRE_PROTOCOL.md)。

## 18. PSK 认证

双方配置同一个 PSK：

```yaml
psk: "development-only-example-key"
```

要求：

- 所有控制包和数据包都必须认证；
- v0.1 固定使用完整 32-byte HMAC-SHA-256 tag；
- 认证覆盖除认证标签自身外的 24-byte prefix 和完整 type-specific body；
- 认证失败的数据包静默丢弃；
- 认证失败的数据包不能创建 Session 或学习 Endpoint；
- 透明 UDP 转发节点不需要 PSK。

v0.1 不要求加密 Payload。PSK/HMAC 只提供认证和完整性，不提供保密性。精确算法、覆盖
字节和 constant-time comparison 要求见 [Wire Protocol](WIRE_PROTOCOL.md#authentication)；
生产密钥处理遵循 [PSK 管理](CONFIGURATION.md#psk-管理)。上面的固定字符串仍只用于开发
测试。

## 19. Session Bootstrap

Initiator：

1. 生成随机 Session ID；
2. 为每个 Carrier 创建独立 UDP socket；
3. 通过每个 Carrier 发送认证后的 `HELLO`；
4. 收到任意合法 `HELLO_ACK` 后，将 Session 标记为可用；
5. 继续接收其他 Carrier 的 `HELLO_ACK`，扩充可用返回通道。

Listener：

1. 在 `listen` 地址接收 UDP；
2. 验证 Packet、PSK 和 FEC 参数；
3. 创建或查找对应 Session；
4. 学习来源 Endpoint；
5. 使用监听 socket 向同一 Endpoint 返回 `HELLO_ACK`；
6. 同一 Session ID 从其他合法 Endpoint 到达时，加入原 Session，而不是创建新 Session。

HELLO 按每个 Carrier 独立执行有界 timeout retry；PING 是周期性新 probe，CLOSE 不重试，
DATA 不做 ACK 或重传。

HELLO 和 HELLO_ACK 必须在 HMAC 保护的 body 中声明发送方本地 `max_udp_payload`
capability。例如双方声明 1200/1000 时，建立后的 send/receive budget 均冻结为 1000。
HELLO packet 使用发送方本地 budget，HELLO_ACK 使用双方最小值；建立后的 PING、PONG、
DATA_SHARD 和 CLOSE 均使用冻结值。重复 HELLO/ACK 不能静默重协商 live Session。

## 20. Decode Timeout 和资源限制

未收齐足够 shard 的 Block 不得永久占用内存。

实现必须限制：

- FEC Decode Timeout；
- 最大未完成 FEC Block 数；
- 最大接收队列；
- 最大上层 Datagram 大小。

超时或超限时直接丢弃该 Datagram，不请求重传。

大小术语和公式固定为：

```text
Path MTU                  = 完整 IP packet 上限
UDP payload               = UDP header 后的完整 MPUDP packet
DATA_SHARD wire overhead  = 71 bytes
shard_capacity            = negotiated_max_udp_payload - 71
effective_datagram_limit  = min(data_shards * shard_capacity,
                                limits.max_datagram_size)
```

Datagram 可以恰好等于 effective limit；多 1 byte 必须在 FEC allocation、PacketID 消耗和
任何发送前返回 `ErrMessageTooLarge`。Linux socket 使用 DF/PMTU discovery 防止本地 IP
fragmentation，已知过大路径错误分类为 `ErrPathMTUExceeded`，但这不是 PLPMTUD：ICMP
Packet Too Big 被过滤仍可能形成静默黑洞。部署值必须不高于所有 Carrier 的已知安全
UDP payload。

## 21. 并发和关闭

要求：

- 每个 UDP socket 可以有独立接收循环；
- Packet 必须先解析和认证，再进入 Session/FEC 处理；
- `Close` 后所有 goroutine 和 timer 都能退出；
- 不允许无限增长的 map、channel 或队列；
- `go test -race ./...` 必须通过。

## 22. MVP 测试

至少覆盖：

1. **配置解析**：Alice 只配置 `carriers + fec + psk`，Bob 只配置 `listen + fec + psk`。
2. **随机本地端口**：每个 Carrier 使用独立 UDP socket，由系统分配本地端口。
3. **双向通信**：Alice 建立 Session 后，Alice → Bob 和 Bob → Alice 均可发送 Datagram。
4. **RS(5,3) / 5 Carrier**：丢失任意两个 shard 仍能恢复。
5. **RS(5,3) / 2 Carrier**：支持 `5 shards → 2 carriers`，并轮换 3/2 分配。
6. **慢 Carrier**：到达时间为 `10ms, 20ms, 30ms, 500ms, timeout` 时，在第三个 shard 到达后立即恢复。
7. **超出 FEC 能力**：只收到两个 shard 时，超时丢弃且不重传。
8. **Endpoint 变化**：认证后的新 Source IP/Port 可以加入，旧 Endpoint 超时删除，Session 保持。
9. **透明转发**：T 节点不理解 MPUDP；Alice 不知道 Bob 的真实地址；Bob 可以沿 NAT/conntrack 返回 Alice。
10. **重复 shard**：同一 Datagram 只向上层交付一次。

当前 hosted gate 的九个 canonical workflow row 及其完整场景契约见
[集成测试](INTEGRATION.md#scenario-contract)。它们在保留上述基础测试的同时覆盖 public
Peer 双向 API、RS loss/rotation/slow-path、NAT reverse/rebinding/expiry、错误 PSK 与
state-pollution、1200/1000 MTU exact-limit/+1 与零分片证据，以及 SIGTERM/public Close
后的资源清理。Workflow、manifest 和文档中的 canonical 名称必须由自动化合同测试保持
一致。

## 23. v0.1 非目标

第一版不实现：

```text
DATA ACK / NACK
DATA 重传
可靠传输
有序交付
拥塞控制
字节流
TCP 转发
SOCKS
TUN
具体上层协议适配
动态 FEC 调整
复杂加权调度
STUN / ICE / TURN
自有 Relay 协议
Mesh
Payload 加密
PLPMTUD / Session 内自适应 payload budget
per-Carrier payload budget / 不等长 shard
内核模块
eBPF / XDP / DPDK
```

T1–T5 使用 nftables 完成的透明 UDP 转发属于部署环境，不属于 MPUDP 协议。

PLPMTUD 和已建立 Session 的自适应 budget 由
[#13](https://github.com/mofelee/mpudp/issues/13) 跟踪；per-Carrier budget 及其所需的
不等长 shard/wire 设计由 [#14](https://github.com/mofelee/mpudp/issues/14) 跟踪。两者均为
明确的 post-v0.1 工作，不得作为 version 1 的静默行为变化。

## 24. 完成标准

```text
✓ Go 用户态实现
✓ 不需要 peer.id
✓ Session ID 自动随机生成
✓ Initiator / Listener 只用于 Bootstrap
✓ Session 建立后双方数据面对等
✓ carriers 只配置远端 UDP 入口
✓ 每个 Carrier 使用独立随机本地端口
✓ Bob 使用监听 socket 发送返回数据
✓ Session 不绑定 UDP 五元组
✓ Endpoint 自动学习和过期
✓ 每个 Carrier 独立 Keepalive
✓ RS(n,k) 参数可配置
✓ FEC shard 数与 Carrier 数量独立
✓ 一个 Datagram 对应一个 FEC Block
✓ n shards 可以映射到 M carriers
✓ 收到 k 个 shard后立即恢复
✓ 不等待慢 Carrier
✓ 同一 Datagram 只交付一次
✓ 不实现 DATA ACK
✓ 不实现 DATA 重传
✓ 不保证有序交付
✓ 不理解具体上层协议
✓ 所有 Packet 使用 PSK 认证
✓ UDP payload budget 经过认证协商并冻结，exact-limit/+1 行为有单元与集成覆盖
✓ Linux canonical MTU 场景提供 DF/PMTU、EMSGSIZE/ErrPathMTUExceeded 和零 fragment 证据
✓ 错误认证输入不创建 Session、Endpoint 或 FEC state，诊断不泄漏敏感内容
✓ CI 发布稳定 build、race 和九个 canonical integration check 名称
✓ 每个集成 case 都执行 exact teardown 和 clean-resource audit
```

交付完成还要求：格式、module verify、vet、build、unit、race 和全部 hosted checks 在
候选 commit 上通过；变更进入 `main` 后必须在 exact promoted SHA 上重新执行并成功，而
不是沿用 feature branch 结果。最终证据记录 hosted run/SHA、所有 canonical case、清理
结果和敏感信息审计。仓库内的 [追踪矩阵](TRACEABILITY.md) 记录可复用机制与测试映射；
会随 commit 变化的 exact-main run 证据记录在对应 GitHub issue，而不硬编码进本文。

## 25. 核心模型

```text
                  Upper Layer
                       │
                  Datagram API
                       │
                       ▼
                     MPUDP
                       │
                    RS(n,k)
                       │
                    n shards
                       │
                Shard Scheduler
                       │
                  n → M mapping
                       │
        ┌──────────────┼──────────────┐
        ▼              ▼              ▼
     Carrier A      Carrier B      Carrier M
        │              │              │
       UDP            UDP            UDP
```

核心边界：

> MPUDP 对上层只暴露 Datagram Tunnel；FEC、多 Carrier、随机本地端口、Endpoint 学习和实际网络路径全部隐藏在 MPUDP 内部。
