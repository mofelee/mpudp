# V2 Datagram 诊断，2026-09-05

[English](v2-diagnostics-20260905.md)

首次受控 v2 对照暴露了严重的上传负载问题。开启聚合的下载优于本轮 v1 对照，
但两个方向均未达到性能契约；所有验收标记保持 false。

```text
source: c7d3ab00baf74c84f4864d877f3f5332bcc2f205
probe SHA-256: c0f2823cfd5a0ae26ce886480c57c430b53add580e437ea6abfc9b0976a732fb
run: mpudp-v2-diagnostics-c7d3ab0
source and runner: clean
```

六个测试均使用既有五条每方向 100 Mbit/s 链路、20 ms 单向 netem 延迟、单业务流
和单 Session、RS(3+2)、1200 字节 UDP 上限及 1400 字节原始报文。每报文只有
1360 个已校验 body 字节计入吞吐。每轮预热 3 秒、稳态 15 秒，仅一轮，关闭
时序诊断和 profile。v2 使用 fixed/session 预算、关闭 repair、每路径配置
100 Mbit/s；聚合关闭或使用有界的 250us/32-record 配置。v1 保留原直接写入路径。

## 结果

| 方向 | 配置 | Mbit/s | 最差 5 秒 | RTT P95/P99 ms | 宿主平均 idle % |
| --- | --- | ---: | ---: | --- | ---: |
| 上传 | v1 | 65.971 | 65.291 | 85 / 179 | 7.43 |
| 上传 | v2 | 0.178 | 0.000 | 不可用 | 10.05 |
| 上传 | v2 聚合 | 0.309 | 0.000 | 不可用 | 10.10 |
| 下载 | v1 | 62.960 | 60.029 | 101 / 156 | 8.56 |
| 下载 | v2 | 25.090 | 23.973 | 58 / 67 | 18.67 |
| 下载 | v2 聚合 | 75.654 | 69.882 | 62 / 102 | 12.68 |

两个 v2 上传 case 的 75 次计划 RTT 中均无按期回复；分位数不可用不代表低延迟。
其余行均为 75/75 按期回复。echo 同样是 1400 字节，分桶精度 1 ms，因此这不是
专用的低速小包延迟对照。

报告按时间戳匹配两端每秒快照，覆盖完整 15 秒接收 bucket。最大偏差 247.73 ms，
在声明的 250 ms 容差内；以下比例为近似值。CPU 按核心计数，100% 表示一个核心。
socket PPS 包含原始协议流量。

| 方向/配置 | 发送 CPU % | 接收 CPU % | 正向 socket PPS | IPv4 L3 字节 / 已校验 body 字节 |
| --- | ---: | ---: | ---: | ---: |
| 上传 v1 | 124.14 | 121.32 | 30,647 | 2.105 |
| 上传 v2 | 121.84 | 135.69 | 20,091 | 1106.846 |
| 上传 v2 聚合 | 122.57 | 134.02 | 19,390 | 616.684 |
| 下载 v1 | 124.30 | 115.89 | 28,937 | 2.083 |
| 下载 v2 | 122.48 | 83.01 | 11,539 | 4.528 |
| 下载 v2 聚合 | 121.43 | 111.32 | 16,880 | 2.195 |

字节比只各计一次两端发送量，每包补 28 字节 IPv4/UDP 头，包含控制、校验片、
padding、echo 和未成功交付的流量，不能证明 shaper 的精确计量。上传的极高
成本是失败信号，不能视为有效 FEC 保护开销。

## 失败证据

v2 上传的 initial-to-final 计数分别出现 281,648 次未聚合 ingress drop 和
289,350 次聚合 ingress drop。这些较宽区间包含预热与 drain，不能作为稳态计数。
接收路径在预热时交付数据，随后多个稳态秒没有已校验字节，在接近组超时处短暂
恢复。发送端没有 admission pressure 或 send error；本地发送成功并不证明远端
有效交付。

两个 v2 下载 case 的 initial-to-final ingress drop 均为零。聚合使每个已交付
原始报文对应的正向包数降至约 2.427，v1 则约为五个。这证实该 case 的封装效率
有所改善，但仍远低于 250 Mbit/s。待处理组扫描、接收处理成本、分配和 credit
保留仍需调查；本报告不认定唯一根因，不证明主机余量，也不以单轮结果证明因果。

未计入损坏或重复业务报文。两端本地尾组 drain 通过，但它只证明本地 shard 发送
尝试，不证明远端收到。三轮 300 秒、原生 KCP 和故障/MTU 验收均保持未完成。

## 复现与产物

按[测量指南](v2-measurement.zh-CN.md)使用上述精确源码/二进制、五路径、
`--protocols mpudp --mpudp-profiles v1 v2 v2-aggregation`、双方向、
`--payloads 1400 --flows 1 --rounds 1 --seconds 15 --warmup 3` 和
`--host-diagnostics basic`。实验使用已有 hypervisor Python：
`/nix/store/60m4rxhg2fldqaak400c0lry96ijrzqn-python3-3.13.13/bin/python3`。

[审计后的原始记录及派生报告](v2-diagnostics-20260905.tar.gz)包含 144 个原始公共
文件、原校验索引、`report.json`、`audit.json` 和 `PUBLIC_SHA256SUMS`。
解压后在 `mpudp-v2-diagnostics-c7d3ab0/` 内校验两个索引。审计重新验证六个 case
与主机快照，扫描已知 PSK 和私有材料标记，并验证归档可确定性重建。未包含配置、
PSK、私钥、profile 或 `.lab` 目录；保留实验地址及主机元数据。

全部 60 个自有临时 unit 已停止，两端临时工作目录已删除。未改变 VM 或 hypervisor
配置。后续探索性 profile 测试独立，不属于本归档或上述表格。

```text
archive SHA-256: b1033dcdb48d6b88e0c0254c5e21c12fd5faf6be258129bb4b3f0237259edffd
```
