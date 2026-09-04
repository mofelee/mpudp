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

psk: "replace-with-a-secret"

transport:
  max_udp_payload: 1200
```

`fec.data_shards` 和 `fec.parity_shards` 都必须大于 0，总数不得超过 256。这个范围
选择 `github.com/klauspost/reedsolomon` 的标准 GF(2^8) profile；配置层只验证构造
能力，不在本 loop 编码或解码数据。

`psk` 必须是非空 YAML 字符串，UTF-8 编码后最多 4096 bytes。PSK 仅用于后续的
HMAC 认证与完整性保护，不提供 Payload 加密。`Secret.String`、`GoString`、Config
格式化和 YAML 输出统一显示 `[REDACTED]`；只有显式调用 `Secret.Bytes()` 才能取得
一个密钥副本。校验错误不包含密钥值。

## UDP payload budget

| 字段 | 默认值 | 合法闭区间 | 归属 |
|---|---:|---:|---|
| `transport.max_udp_payload` | 1200 bytes | 72..65507 bytes | 完整 MPUDP UDP payload |

`max_udp_payload` 是 UDP header 之后的完整 MPUDP wire packet 上限，包括 MPUDP
prefix、type-specific body、完整 32-byte HMAC tag 和 packet payload。它不是 IP MTU，
也不是单纯的 RS shard data capacity。72 bytes 是固定 v0.1 layout 中强制控制包
（PING/PONG）的完整最小预算；65507 是保守的 UDP payload 硬上限。1200 为 IPv6
minimum link MTU 留出了 IP/UDP header 空间，但不能保证穿过管理员配置得更小的
下层隧道。

本字段是 Session 全局声明值；后续握手使用双方声明值的较小值。所有控制包和
`DATA_SHARD` 都受协商值约束。每 Carrier 可变 shard size 不属于 v0.1。

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
还要取 FEC 从协商 UDP budget 推导出的值与该资源上限中的较小值。当前 API 骨架在
任何协议分配之前先执行资源上限检查；超过时返回 `mpudp.ErrMessageTooLarge`。

`max_pending_fec_blocks` 限制每个运行时所有者持有的未完成 decode block 数量；两个
queue 字段分别约束认证后接收工作队列和已恢复 Datagram 的交付队列。具体满载策略
由 Peer 运行时 loop 实现，但容量不会变成无界 channel。`max_sessions` 和
`max_endpoints_per_session` 限制认证成功后可创建的运行时状态；未认证来源不能消费
这些配额。`max_handshake_attempts` 为每轮主动 bootstrap 提供硬重试上限，DATA 不使用
这个重试能力。

## 时间参数

YAML 中时间值必须是带单位的 Go duration 字符串，不能写成裸整数。

| 字段 | 默认值 | 合法闭区间 |
|---|---:|---:|
| `timers.decode_timeout` | `3s` | `100ms`..`1m` |
| `timers.endpoint_ttl` | `2m` | `5s`..`24h` |
| `timers.keepalive_interval` | `15s` | `1s`..`5m` |
| `timers.handshake_retry_interval` | `1s` | `100ms`..`1m` |

这些值目前只属于经过校验的配置。Loop 1 不创建 timer；后续状态机负责 timer 的
所有权、取消和关闭清理。

完整覆盖所有可选项的示例：

```yaml
listen: "0.0.0.0:9000"
fec: {data_shards: 3, parity_shards: 2}
psk: "replace-with-a-secret"
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
