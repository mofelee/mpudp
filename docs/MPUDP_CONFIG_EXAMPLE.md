# MPUDP 配置与部署示例

本文给出一个最小的 Alice / T1–T5 / Bob 部署示例。

- Alice 和 Bob 运行 MPUDP。
- T1–T5 不运行 MPUDP，只使用 nftables 做双向 UDP NAT 转发。
- Alice 通过 5 个 Carrier 向 Bob 建立同一条 MPUDP Session。
- FEC 使用 `RS(5,3)`：3 个数据 shard、2 个校验 shard。
- Alice 不配置本地 UDP 端口。每个 Carrier 建立时创建独立 UDP socket，由操作系统随机分配本地临时端口。
- Alice 只知道 T1–T5 的 UDP 入口，不知道这些转发节点最终把数据送往哪里。

> 下列地址均来自文档保留网段，部署时必须替换为真实地址。

## 1. 拓扑

```text
                         T1  192.0.2.11:4000 ──┐
                         T2  192.0.2.12:4000 ──┤
Alice ── MPUDP/RS(5,3) ─ T3  192.0.2.13:4000 ──┼── Bob 203.0.113.20:9000
                         T4  192.0.2.14:4000 ──┤
                         T5  192.0.2.15:4000 ──┘
```

节点清单：

| 节点 | 地址 | 作用 |
|---|---|---|
| Alice | 动态地址或 NAT 后 | MPUDP Peer，主动建立 Session |
| T1 | `192.0.2.11:4000` | UDP NAT 转发到 Bob |
| T2 | `192.0.2.12:4000` | UDP NAT 转发到 Bob |
| T3 | `192.0.2.13:4000` | UDP NAT 转发到 Bob |
| T4 | `192.0.2.14:4000` | UDP NAT 转发到 Bob |
| T5 | `192.0.2.15:4000` | UDP NAT 转发到 Bob |
| Bob | `203.0.113.20:9000` | MPUDP Peer，监听并接受 Session |

## 2. Alice 配置

文件：`alice.yaml`

```yaml
carriers:
  - "192.0.2.11:4000"
  - "192.0.2.12:4000"
  - "192.0.2.13:4000"
  - "192.0.2.14:4000"
  - "192.0.2.15:4000"

fec:
  data_shards: 3
  parity_shards: 2

psk: "secret"
```

### Alice 的运行时行为

对于 `carriers` 中的每一项，MPUDP 必须分别创建一个长期存活的 UDP socket：

```go
conn, err := net.DialUDP("udp", nil, carrierAddr)
```

`nil` 表示本地地址和端口由操作系统选择。实际运行时可能类似：

```text
Alice:49152 → T1:4000
Alice:53881 → T2:4000
Alice:60421 → T3:4000
Alice:41973 → T4:4000
Alice:55722 → T5:4000
```

这些本地端口：

- 不写入配置文件；
- 不要求固定；
- 在对应 Carrier 存活期间保持不变；
- Carrier 重建时可以重新随机分配。

Alice 不需要配置 Bob 的地址，也不需要知道 T1–T5 的 nftables 转发目标。

## 3. Bob 配置

文件：`bob.yaml`

```yaml
listen: "0.0.0.0:9000"

fec:
  data_shards: 3
  parity_shards: 2

psk: "secret"
```

Bob 只监听 UDP `9000`，不预先配置 Alice 或 T1–T5 的地址。

Bob 从认证成功的 MPUDP 数据包中学习返回 Endpoint。例如，经过 NAT 后，Bob 实际看到的地址可能是：

```text
192.0.2.11:51001
192.0.2.12:43882
192.0.2.13:60219
192.0.2.14:37944
192.0.2.15:55103
```

这里的端口由 T1–T5 的 NAT/conntrack 分配，不保证等于 Alice 的随机源端口，也不保证等于 `4000`。

Bob 向这些已学习 Endpoint 发送 UDP，T1–T5 的 conntrack 会把返回数据还原并转发给 Alice 对应的 UDP socket。

Bob 必须使用监听在 `203.0.113.20:9000` 的同一个 UDP socket 发送返回数据，不能另建随机源端口的 socket；否则 T1–T5 上的 conntrack 可能无法把返回包匹配到原来的转发状态。

## 4. T1 配置

T1 公网地址：`192.0.2.11`

T1 不需要 MPUDP 配置和 PSK。它只负责：

```text
T1:4000 ⇄ Bob:9000
```

开启 IPv4 转发：

```bash
cat >/etc/sysctl.d/99-mpudp-forward.conf <<'EOF_SYSCTL'
net.ipv4.ip_forward=1
EOF_SYSCTL

sysctl --system
```

`/etc/nftables.conf`：

```nftables
table ip mpudp {
    chain prerouting {
        type nat hook prerouting priority dstnat; policy accept;
        udp dport 4000 dnat to 203.0.113.20:9000
    }

    chain postrouting {
        type nat hook postrouting priority srcnat; policy accept;
        ip daddr 203.0.113.20 udp dport 9000 masquerade
    }

    chain forward {
        type filter hook forward priority filter; policy drop;

        ct state established,related accept
        ip daddr 203.0.113.20 udp dport 9000 accept
    }
}
```

加载规则：

```bash
nft -c -f /etc/nftables.conf
nft -f /etc/nftables.conf
systemctl enable --now nftables
```

## 5. T2 配置

T2 公网地址：`192.0.2.12`

T2 使用与 T1 相同的 sysctl 和 nftables 配置：

```text
T2:4000 ⇄ Bob:9000
```

`/etc/nftables.conf`：

```nftables
table ip mpudp {
    chain prerouting {
        type nat hook prerouting priority dstnat; policy accept;
        udp dport 4000 dnat to 203.0.113.20:9000
    }

    chain postrouting {
        type nat hook postrouting priority srcnat; policy accept;
        ip daddr 203.0.113.20 udp dport 9000 masquerade
    }

    chain forward {
        type filter hook forward priority filter; policy drop;

        ct state established,related accept
        ip daddr 203.0.113.20 udp dport 9000 accept
    }
}
```

## 6. T3 配置

T3 公网地址：`192.0.2.13`

```text
T3:4000 ⇄ Bob:9000
```

`/etc/nftables.conf`：

```nftables
table ip mpudp {
    chain prerouting {
        type nat hook prerouting priority dstnat; policy accept;
        udp dport 4000 dnat to 203.0.113.20:9000
    }

    chain postrouting {
        type nat hook postrouting priority srcnat; policy accept;
        ip daddr 203.0.113.20 udp dport 9000 masquerade
    }

    chain forward {
        type filter hook forward priority filter; policy drop;

        ct state established,related accept
        ip daddr 203.0.113.20 udp dport 9000 accept
    }
}
```

## 7. T4 配置

T4 公网地址：`192.0.2.14`

```text
T4:4000 ⇄ Bob:9000
```

`/etc/nftables.conf`：

```nftables
table ip mpudp {
    chain prerouting {
        type nat hook prerouting priority dstnat; policy accept;
        udp dport 4000 dnat to 203.0.113.20:9000
    }

    chain postrouting {
        type nat hook postrouting priority srcnat; policy accept;
        ip daddr 203.0.113.20 udp dport 9000 masquerade
    }

    chain forward {
        type filter hook forward priority filter; policy drop;

        ct state established,related accept
        ip daddr 203.0.113.20 udp dport 9000 accept
    }
}
```

## 8. T5 配置

T5 公网地址：`192.0.2.15`

```text
T5:4000 ⇄ Bob:9000
```

`/etc/nftables.conf`：

```nftables
table ip mpudp {
    chain prerouting {
        type nat hook prerouting priority dstnat; policy accept;
        udp dport 4000 dnat to 203.0.113.20:9000
    }

    chain postrouting {
        type nat hook postrouting priority srcnat; policy accept;
        ip daddr 203.0.113.20 udp dport 9000 masquerade
    }

    chain forward {
        type filter hook forward priority filter; policy drop;

        ct state established,related accept
        ip daddr 203.0.113.20 udp dport 9000 accept
    }
}
```

T2–T5 同样需要：

```text
net.ipv4.ip_forward=1
```

并执行 nftables 规则校验、加载和开机启用。

## 9. Bob 防火墙

Bob 至少需要允许 UDP `9000`：

```nftables
table inet filter {
    chain input {
        type filter hook input priority filter; policy drop;

        ct state established,related accept
        iifname "lo" accept
        udp dport 9000 accept
    }
}
```

该片段仅表示 MPUDP 所需规则。实际部署时应合并到 Bob 现有防火墙，不要直接覆盖已有配置。

## 10. 数据流

Alice 到 Bob：

```text
Upper Datagram
      │
      ▼
   RS(5,3)
      │
      ├── shard 0 → Carrier 0 → T1 ──┐
      ├── shard 1 → Carrier 1 → T2 ──┤
      ├── shard 2 → Carrier 2 → T3 ──┼──→ Bob:9000
      ├── shard 3 → Carrier 3 → T4 ──┤
      └── shard 4 → Carrier 4 → T5 ──┘
```

Bob 收到任意 3 个有效 shard 即可恢复原始 Datagram。

Bob 到 Alice：

```text
Bob
 │
 ├── shard → learned T1 endpoint → conntrack → Alice carrier 0
 ├── shard → learned T2 endpoint → conntrack → Alice carrier 1
 ├── shard → learned T3 endpoint → conntrack → Alice carrier 2
 ├── shard → learned T4 endpoint → conntrack → Alice carrier 3
 └── shard → learned T5 endpoint → conntrack → Alice carrier 4
```

## 11. 配置语义

### `carriers`

每个值都是一个远端 UDP 入口，格式为 `host:port`。

它不是：

- Alice 的本地绑定地址；
- Alice 的本地源端口；
- 最终 MPUDP Peer 的地址声明；
- T 节点内部的转发目的地。

每项必须创建独立 UDP socket，不能用一个 socket 轮流向所有 Carrier 发送。

### `listen`

本地用于接收 MPUDP UDP 数据包的监听地址。

### `fec.data_shards`

恢复原始 Datagram 所需的数据 shard 数量，即 `k`。

### `fec.parity_shards`

额外校验 shard 数量。总 shard 数：

```text
n = data_shards + parity_shards
```

示例配置为 `RS(5,3)`。

### `psk`

Alice 和 Bob 共享的预共享密钥，用于认证 MPUDP 数据包和控制包。

T1–T5 不需要知道 PSK。

## 12. 重要约束

1. Alice 和 Bob 的 FEC 参数必须一致。
2. Alice 和 Bob 的 PSK 必须一致。
3. 每个 Alice Carrier 必须拥有独立 UDP socket 和独立随机本地端口。
4. T1–T5 必须同时执行 DNAT 和 SNAT/masquerade；只有 DNAT 无法保证返回流量继续经过原转发节点。
5. MPUDP 必须通过每个 Carrier 周期性发送认证后的保活包，以维持 Alice 和 T 节点上的 UDP/NAT 状态。
6. Bob 只能向认证成功且尚未过期的 Endpoint 发送返回数据。
7. Alice 冷启动时必须主动向 T1–T5 发包。Bob 无法在 NAT 映射尚未建立时凭空连接 Alice。
8. Session 建立后，Alice 和 Bob 在数据面完全对等，任意一端都可以发送上层 Datagram。
