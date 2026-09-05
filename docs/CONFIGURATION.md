# MPUDP 配置参考

配置是单个 YAML 文档。解析器启用 `yaml.v3` 的 `KnownFields` 严格模式：未知字段、
重复键、错误类型、额外 YAML 文档和数值溢出都返回
`config.ErrInvalidConfig`（它与 `mpudp.ErrInvalidConfig` 是同一个 sentinel）。省略可选
字段会应用默认值；显式填写 `0` 不等同于省略，并会按对应范围严格校验。
所有整数参数只接受 YAML integer，拒绝小数、浮点数（包括 `120.0`）、指数浮点表示、
数字字符串和布尔值；不会将 `120.5` 静默截断为 `120`。合法整数的 YAML 十六进制、
八进制、二进制和下划线分隔形式仍按整数解码，再校验对应字段范围。

配置文件最大为 1 MiB（`config.MaxConfigBytes`）。`Parse` 在解析前检查 byte slice
长度；`Decode` 最多读取 1 MiB + 1 byte 来判断超限，不会把任意大的文件无界读入
内存。任何位置的显式 YAML `null`（包括 `~` 和空 mapping value）都是错误，不能借此
把已提供但类型错误的字段伪装成“省略”；只有真正未出现的可选字段会应用默认值。

Go 代码直接构造 v1 配置时应从 `cfg := config.Default()` 开始；v2 使用
`config.DefaultV2(config.ProtocolDatagram)` 或 `config.DefaultV2(config.ProtocolKCP)`，
再设置角色、FEC 和 PSK。v2 helper 仅提供配置默认值，不会启用尚不可用的运行时。
`Config` 的零值不会在 `Validate`/`NewPeer` 中被静默改写为默认值。这样配置文件中的显式
数值零和程序中的数值零具有同样的严格语义。兼容旧版 Go struct literal 时，新增的
`Protocol == ""` 和 `Wire.Version == ""` 分别按 `datagram` 和 `v1` 解释，原对象不被
改写；`EffectiveProtocol()` 和 `EffectiveWireVersion()` 返回这两个有效选择。显式 YAML
空字符串不适用这个兼容规则，会被拒绝。

## 运行模式

至少配置 `carriers` 或 `listen` 之一：

| 配置 | 模式 | 含义 |
|---|---|---|
| 只有 `carriers` | initiator | 可以主动创建 Session |
| 只有 `listen` | listener | 可以接受 Session |
| 两者都有 | dual | 同一 Peer 同时具备两种能力 |

`carriers` 中每一项都是远端 UDP `host:port`，不是本地绑定地址或本地源端口。远端
host 不能为空，也不能是 `0.0.0.0`/`::`；支持 DNS 名、IPv4 和带方括号的 IPv6。
端口范围为 1 到 65535。大小写、IP 文本和端口规范化后重复的 Carrier 会被拒绝，
最多允许 256 项。`listen` 是本地 `host:port`，因此允许省略 host，例如 `:9000`。
地址只做无副作用的语法校验；解析配置时不会进行 DNS 查询或打开 socket。

配置中不存在 `peer.id` 或 `session_id`。这两个名称会作为未知字段被拒绝。SessionID
由运行时使用 `crypto/rand.Reader` 生成 16 个字节，不绑定 UDP 五元组。

## 协议与 wire 版本

`protocol` 与上面的 initiator/listener/dual 角色独立，必须是字符串 `datagram` 或 `kcp`，
省略时为 `datagram`。`wire.version` 必须是字符串 `v1` 或 `v2`，省略时为 `v1`。
大小写变体、空字符串、数字、布尔值、错误容器、未知字段和显式 `null` 均被拒绝。
`config.Default()` 显式设置 `ProtocolDatagram` 和 `WireVersionV1`。

| 配置选择 | `Parse` / `Validate` | `NewPeer` / `NewPeerContext` |
|---|---|---|
| 省略新字段，或 `datagram` + `v1` | 按既有 FEC/资源规则验证 | 既有 v1 Datagram 运行时 |
| `kcp` + 省略版本或 `v1` | `ErrInvalidConfig` | `ErrInvalidConfig` |
| `datagram` + `v2` | 必须显式提供正数 k/r；UDP 上限至少 512 | `ErrProtocolUnavailable` |
| `kcp` + `v2` | 省略 FEC 得到 0/0；显式非零或负数 FEC 无效；UDP 上限至少 512 | 合法配置返回 `ErrProtocolUnavailable` |

当前增量只实现配置边界，尚未实现 v2 握手、数据面或 KCP Session。配置解析成功不表示
该协议可运行。两个构造函数先验证配置，再拒绝尚未实现的 v2，且不访问运行时 context、
随机源、socket 或 timer 依赖，也不启动 goroutine。不会静默回退到 v1。
共享 v2 transport、scheduler、资源上限和接收超时已经支持严格解析；aggregation、repair、
KCP tuning 和 mux 配置字段仍是未知字段。配置上限验证不表示已经分配资源或启用数据面。

### V2 共享配置

以下字段仅在 `wire.version: v2` 下有效；v1 显式提供这些字段，即使是零值或空 map，也会
被拒绝。省略新字段的 v1 默认值保持不变。完整范围与后续协议字段见
[v2 配置设计表](design/v2-configuration-api.md)。

| 字段 | v2 默认值 | 范围 |
|---|---:|---|
| `transport.max_receive_udp_payload` | 最终 `max_udp_payload` | 512..65507 |
| `transport.mtu_discovery` | `fixed` | `fixed` 或 `plpmtud` |
| `transport.budget_strategy` | `session` | `session` 或 `per_carrier` |
| `transport.max_retained_epochs` | 2 | 1..8 |
| `transport.max_epoch_age` | `5s` | `100ms`..`60s` |
| `transport.max_migrations` | 2 | 1..2；配置值不自动启用迁移 |
| `transport.plpmtud.base_udp_payload` | 512 | 必须为 512 |
| `transport.plpmtud.probe_interval` | `1s` | `100ms`..`60s` |
| `transport.plpmtud.max_outstanding_per_path` | 1 | 必须为 1 |
| `limits.max_pending_handshakes` | 256 | 1..4096 |
| `limits.max_pending_accepts` | 256 | 1..65536 |
| `limits.max_peer_retained_bytes` | 268435456 | 1 MiB..1 GiB |
| `limits.max_session_retained_bytes` | 16777216 | 1 MiB..Peer 上限 |
| `limits.max_datagram_reassemblies` | 1024 | 1..65536 |
| `limits.max_fragments_per_datagram` | 256 | 1..4096 |
| `limits.max_migration_transaction_bytes` | 8388608 | 1..8 MiB，且不超过 Session |
| `limits.max_streams_per_session` | 128 | 1..4096 |
| `limits.max_peer_streams` | 4096 | 1..65536 |
| `limits.max_stream_retained_bytes` | 278528 | 正数且不超过 Session；mux 最低窗口约束在后续增量实现 |
| `limits.max_path_queued_packets` | 256 | 1..4096 |
| `limits.max_path_queued_bytes` | 1048576 | 512..Session 上限 |
| `limits.max_send_workers` | 8 | 1..32 |
| `timers.datagram_reassembly_timeout` | `10s` | `100ms`..`60s` |
| `timers.group_decode_timeout` | `10s` | `100ms`..`60s` |

`transport.plpmtud` 只允许在 `plpmtud` 模式出现；fixed 模式下连空配置块也会拒绝。
`outbound_path_budgets` / `inbound_path_budgets` 只用于 fixed/per_carrier，元素形如
`{path_id: 1, max_udp_payload: 1200}`。initiator 的 outbound 列表必须完整覆盖配置的
Carrier 索引；listener 的 inbound 列表独立覆盖连续 1..N 索引，并受 endpoint 上限约束。
双角色的两个列表互不补全，未使用角色必须省略对应列表。索引可按任意顺序列出，但不能
重复、缺失或重编号；每个预算必须在 512..本地发送硬上限之间。

`scheduler.outbound_path_rates_bps` / `inbound_path_rates_bps` 是可省略的 PathID map，
例如 `{1: 100000000, 2: 50000000}`。每个 rate 范围为 1000..1000000000000 bit/s，未列出
的合法 PathID 使用 100000000。键和值均严格要求 YAML integer，`1.5` 键或 `1` 与 `0x1`
形成的重复键会被拒绝。outbound 键必须在 Carrier 范围内；inbound 键受反向静态列表或
`limits.max_endpoints_per_session` 限制。Go `Clone()` 深复制两个列表和两个 rate map。

所有字节上限都是配置约束，不代表最大值同时获得预留。降低 Session 上限时，可能需要
同步显式降低 migration/path/stream 等默认上限；解析器不会暗中裁剪这些配置值。UDP
发送和接收硬上限互相独立，不能把反向接收能力当作本地已验证的路径 MTU。

## 最小示例

```yaml
carriers:
  - "192.0.2.11:4000"
  - "[2001:db8::11]:4000"

fec:
  data_shards: 3
  parity_shards: 2

psk: "development-only-example-key"

transport:
  max_udp_payload: 1200
```

Datagram 的 `fec.data_shards` 和 `fec.parity_shards` 都必须大于 0，总数不得超过 256。这个范围
选择 `github.com/klauspost/reedsolomon` 的标准 GF(2^8) profile；运行时按这组参数为
每个方向创建 encoder/decoder。

## PSK 管理

`psk` 必须是非空 YAML scalar 字符串，UTF-8 编码后最多 4096 bytes。解析器不支持
`psk_file`、环境变量展开或 shell 插值；配置中的 `${NAME}` 只是字面密钥内容。PSK 只用于
HMAC-SHA-256 认证与完整性保护，不加密 Payload。

本文和仓库内其他示例的 `development-only-example-key` 仅供开发测试，不能部署到生产。
生产密钥必须高熵且独立生成。推荐通过 secret manager 或受保护的模板流程创建 mode 0600
配置文件，或者由嵌入程序直接构造 `config.NewSecret`；环境变量可能经进程信息、崩溃转储或
诊断工具泄漏，不应被默认视为安全存储。任何密钥都不得写入日志、错误、命令行参数或诊断
artifact。

`Secret.String`、`GoString`、Config 格式化和 YAML 输出统一显示 `[REDACTED]`；只有显式
调用 `Secret.Bytes()` 才能取得一个副本。校验和运行时错误不包含密钥值。

## UDP payload budget

为避免混淆，本文使用以下四个大小概念：

| 术语 | 定义 |
|---|---|
| Path MTU | 一个完整 IP packet 在路径上的大小上限 |
| UDP payload | UDP header 之后的 bytes，包括完整 MPUDP wire packet |
| shard data capacity | 协商 UDP payload 减去固定 71-byte `DATA_SHARD` wire overhead |
| Datagram 上限 | `min(k * shard data capacity, limits.max_datagram_size)` |

| 字段 | 默认值 | 合法闭区间 | 归属 |
|---|---:|---:|---|
| `transport.max_udp_payload`，v1 | 1200 bytes | 72..65507 bytes | 完整 MPUDP UDP payload |
| `transport.max_udp_payload`，v2 配置 | 1200 bytes | 512..65507 bytes | 配置验证；v2 运行时尚不可用 |

`max_udp_payload` 是 UDP header 之后的完整 MPUDP wire packet 上限，包括 MPUDP
prefix、type-specific body、完整 32-byte HMAC tag 和 packet payload。它不是 IP MTU，
也不是单纯的 RS shard data capacity。72 bytes 是固定 v0.1 layout 中强制控制包
（PING/PONG）的完整最小预算；65507 是保守的 UDP payload 硬上限。1200 为 IPv6
minimum link MTU 留出了 IP/UDP header 空间，但它不是探测出的 Path MTU，也不能保证
穿过管理员配置得更小的下层隧道。部署者必须按所有 Carrier 中已知的最小安全 UDP
payload 向下配置。Linux DF/PMTU socket mode 只阻止本地分片；远端 ICMP Packet Too Big
被过滤时仍可能形成静默黑洞。v0.1 不实现由
[#13](https://github.com/mofelee/mpudp/issues/13) 跟踪的 PLPMTUD/自适应预算。

本字段是 Session 全局声明值。HELLO 字段声明发送方的本地能力，整个 HELLO packet 也
按发送方本地预算编码。HELLO_ACK 字段声明响应方的本地能力，但整个 ACK packet 按双方
声明值的较小值编码。认证握手成功后，每个方向冻结该协商预算；后续 PING、PONG、
`DATA_SHARD` 和 CLOSE 都按它编码。CLOSE 的固定 wire size 是 56 bytes。由
[#14](https://github.com/mofelee/mpudp/issues/14) 跟踪的 per-Carrier budget 和不等长
shard 不属于 v0.1。

## 资源上限

| 字段 | 默认值 | 合法闭区间 |
|---|---:|---:|
| `limits.max_datagram_size` | 65536 bytes | 1..16777216 bytes |
| `limits.max_pending_fec_blocks` | 1024 | 1..65536 |
| `limits.receive_queue_capacity` | 256 | 1..65536 |
| `limits.delivery_queue_capacity` | 256 | 1..65536 |
| `limits.max_sessions` | 1024 | 1..65536 |
| `limits.max_endpoints_per_session` | 256 | 1..256 |
| `limits.max_handshake_attempts` | 8 | 1..64 |

`max_datagram_size` 是进程资源上限，不是 wire 可发送上限。实际 `WritePacket` 上限
还要取 FEC 从协商 UDP budget 推导出的值与该资源上限中的较小值。运行时在任何 FEC
分配、PacketID 消耗或 shard 发送之前执行检查；超过时返回 `mpudp.ErrMessageTooLarge`。

`max_pending_fec_blocks` 同时限制每个 decoder 的未完成 block 数量和 bounded completed
PacketID cache 容量；完成项 TTL 使用 `timers.endpoint_ttl`。`receive_queue_capacity`
约束 transport callback 到 Peer dispatcher 的 pre-auth ingress，满载时非阻塞丢弃最新
event。`delivery_queue_capacity` 约束已恢复 Datagram 的每 Session 交付队列，同样采用
drop-newest。Listener accept queue 也使用 receive 容量；满载时关闭并释放最新创建的
Session。

`max_sessions` 和 `max_endpoints_per_session` 限制认证成功后可创建的运行时状态；未认证
来源不能消费这些配额。`max_handshake_attempts` 为每个 Carrier 的主动 bootstrap 提供硬
重试上限，DATA 不使用该重试能力。握手 jitter 没有独立配置字段，由运行时固定为
`handshake_retry_interval / 4` 的上限。

## 时间参数

YAML 中时间值必须是带单位的 Go duration 字符串，不能写成裸整数。

| 字段 | 默认值 | 合法闭区间 |
|---|---:|---:|
| `timers.decode_timeout` | `3s` | `100ms`..`1m` |
| `timers.endpoint_ttl` | `2m` | `5s`..`24h` |
| `timers.keepalive_interval` | `15s` | `1s`..`5m` |
| `timers.handshake_retry_interval` | `1s` | `100ms`..`1m` |

Peer dispatcher 使用一个可复用 timer 驱动所有 Session 的 handshake retry、keepalive、
Endpoint expiry 和 FEC sweep；不会为每个 packet 或 Endpoint 创建 timer/goroutine。
`decode_timeout` 从首个 shard 到达时固定，`endpoint_ttl` 从最近一次有效认证活动计算，
`keepalive_interval` 按 Carrier/Endpoint 安排 probe。Close 会取消这些 deadline 并等待
dispatcher 退出。

完整覆盖所有可选项的示例：

```yaml
listen: "0.0.0.0:9000"
fec: {data_shards: 3, parity_shards: 2}
psk: "development-only-example-key" # 仅供开发测试；生产环境必须安全注入高熵密钥
transport:
  max_udp_payload: 1200
limits:
  max_datagram_size: 65536
  max_pending_fec_blocks: 1024
  receive_queue_capacity: 256
  delivery_queue_capacity: 256
  max_sessions: 1024
  max_endpoints_per_session: 256
  max_handshake_attempts: 8
timers:
  decode_timeout: "3s"
  endpoint_ttl: "2m"
  keepalive_interval: "15s"
  handshake_retry_interval: "1s"
```

第三方模块、许可证和未来分发义务见 [依赖审计](DEPENDENCIES.md)。
