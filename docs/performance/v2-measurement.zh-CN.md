# V2 Datagram 测量

[English](v2-measurement.md)

首次[受控诊断报告](v2-diagnostics-20260905.zh-CN.md)记录了严重的 v2 上传性能问题；
功能可运行不代表吞吐验收通过。
[截止时间索引对照](v2-deadlines-20260905.zh-CN.md)公开支持该优化的 profile 和微基准，
优化后的网络运行仍发生上传交付失败。

运行器可比较 Linux 公共 v2 Datagram 与 v1，继续按接收端校验后的唯一业务字节
计量，每条业务流使用一个 Session。工具不会宣告产品验收通过；三轮 300 秒、
底层容量校准、主机余量及故障/MTU 矩阵仍按[性能契约](../PERFORMANCE.md)单独验收。

## 配置与前置条件

`--mpudp-profiles v1 v2 v2-aggregation` 只扩展 `mpudp` 与 `kcp-mpudp` 测试。
原生协议按各自布局运行一次。默认仍为 `v1`，原 case ID 不变；v2 使用独立后缀。

v2 运行器需要 `PyYAML==6.0.2`，以保留生成 YAML 中整数类型的路径 ID。
两端使用相同探针二进制。记录包含源码及二进制哈希、非秘密配置、方向硬上限和
路径速率。PSK 与 SSH 私有文件必须保留在发布产物目录之外。

支持的 v2 配置为 `protocol: datagram`、fixed/session MTU、关闭 repair 和显式
方向速率。`kcp-mpudp` 是保留的实验性 KCP-over-Datagram-FEC，对应的配置仍是
Datagram，并非原生产品 KCP。固定版本 kcp-go 的 `--kcp-mtu` 上限仍为 1500，
与应用报文大小独立。原生 `kcp` 则是每进程单 UDP 路径上的直接 kcp-go 对照。

默认每路径配置速率为 100,000,000 bit/s，聚合最大等待 250 微秒、32 个 record，
队列上限 256 个原始报文和 1 MiB。可用 `--v2-path-rate-bps`、
`--v2-aggregation-max-delay-us`、`--v2-aggregation-max-records` 和
`--v2-max-original-bytes` 在边界内覆盖。它们是配置值，并非实测速率或延迟。
v2 原始报文上限同时受 manifest 分片容量限制，不使用 v1 单 block 的公式。

## 受控诊断

从已提交且干净的源码构建：

```sh
python3 -m pip install PyYAML==6.0.2
go build -C integration/perf -trimpath \
  -ldflags "-X main.sourceSHA=$(git rev-parse HEAD)" \
  -o /tmp/mpudp-perfprobe ./cmd/perfprobe
python3 scripts/perf/run-probe.py \
  --topology scripts/perf/topology.example.json \
  --ssh-config /root/mpudp-test/.lab/ssh_config \
  --binary /tmp/mpudp-perfprobe --source-sha "$(git rev-parse HEAD)" \
  --psk-file /private/mpudp-perf.psk --output /tmp/mpudp-v2-diagnostic \
  --protocols mpudp --mpudp-profiles v1 v2 v2-aggregation \
  --paths 5 --directions upload download --payloads 1400 \
  --flows 1 --rounds 1 --seconds 15 --warmup 3 --host-diagnostics basic
```

需要时指定实验环境已有的 `--hypervisor-python` 路径。添加 `--plan` 可仅生成
矩阵，不发起 SSH 或流量。运行器创建并清理自身临时进程和探针目录，不修改 VM
或 hypervisor。另以 64 字节报文、低发送速率比较延迟；现有 RTT 分桶精度为
1 ms，无法精确分辨 250 微秒聚合差异。RTT 包含未获回复的全部计划机会，与
bulk 共用同一 Session。

## 校验后的稳态报告

```sh
python3 scripts/perf/report-probe.py /tmp/mpudp-v2-diagnostic
```

报告校验索引中的输入哈希、完成状态和源码/二进制身份、两端元数据、接收端字节
计量、RTT 与交换的 summary。派生输出应写在原产物树之外，以保留原校验索引。

CPU、分配和 socket PPS 使用每秒累计计数的差值。发送端与接收端的 sample 序号
不代表同一时钟；报告按时间戳在接收端名义稳态窗口内匹配边界，默认允许最多
250 ms 的两端偏差和接收采样延迟，并输出实际时间、区间长度及选中 bucket。
匹配不足时失败，不插值计数。无预热时，第一个 bucket 没有前置采样边界，因此
不进入成本计算；完整窗口的接收吞吐和 RTT 仍单独报告。端点墙上时钟必须同步，
仅匹配时间戳不能证明同步。最大 RSS 是进程生命周期峰值，不是稳态内存差值。
CPU 百分比按核心计数，100% 表示占用一个核心。

原始 MPUDP socket 计数包含数据、控制、校验片、尾组和 echo。IPv4 L3 估算对每个
发送 UDP 报文增加 28 字节；双向成本只各计一次两端发送量，以所选接收区间的
唯一业务字节为分母。由于两端区间只在已报告容差内匹配，这些比例是近似值，
不能证明 HTB 精确计量，也不能分解 padding、认证、控制及校验字节。原生协议
socket PPS 不可用；v2 详细 FEC 和已认证 listener-path 指标仍不可用。

v2 封装公式为 `S = U - 94`、受保护逻辑字节 `L = 4 + 20*m + A`、
padding `k*S - L`、完整组 IPv4 字节 `(k+r)*(94+S+28)`。`A` 包含原报文字节，
也包含每条探针报文的 40 字节校验头；只有校验后的 body 计入业务吞吐。
实测必须包含实际尾组及双向控制流量。封装容量是算术上界，不能代替实测吞吐。
