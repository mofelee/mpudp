# MPUDP v0.1 配置参考

配置是单个 YAML 文档。解析器启用 `yaml.v3` 的 `KnownFields` 严格模式：未知字段、
重复键、错误类型、额外 YAML 文档和数值溢出都返回
`config.ErrInvalidConfig`（它与 `mpudp.ErrInvalidConfig` 是同一个 sentinel）。省略可选
字段会应用默认值；显式填写 `0` 不等同于省略，并会按对应范围严格校验。

配置文件最大为 1 MiB（`config.MaxConfigBytes`）。`Parse` 在解析前检查 byte slice
长度；`Decode` 最多读取 1 MiB + 1 byte 来判断超限，不会把任意大的文件无界读入
内存。任何位置的显式 YAML `null`（包括 `~` 和空 mapping value）都是错误，不能借此
把已提供但类型错误的字段伪装成“省略”；只有真正未出现的可选字段会应用默认值。

Go 代码直接构造配置时应从 `cfg := config.Default()` 开始，再设置模式、FEC 和 PSK；
`Config` 的零值不会在 `Validate`/`NewPeer` 中被静默改写为默认值。这样配置文件中的显式
零和程序中的零具有同样的严格语义。

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

`fec.data_shards` 和 `fec.parity_shards` 都必须大于 0，总数不得超过 256。这个范围
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
| `transport.max_udp_payload` | 1200 bytes | 72..65507 bytes | 完整 MPUDP UDP payload |

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
