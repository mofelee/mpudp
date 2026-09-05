# 传输层写入尝试时间，2026-09-05

[English](transport-attempt-20260905.md)

可选的 `transport.SendWithAttempt` 辅助函数返回原生传输路径进入连接写入的时间。
本地测试夹具中，请求时间戳使关闭诊断时的操作耗时中位数增加 78.7-87.9 ns，
没有增加分配：全部 90 个观测值均为 264 B/op、4 allocs/op。
运行时调用方尚未启用这一前置能力；本次变更没有增加工作线程、节流策略或网络流量。

```text
基线源码：5e19e44a6ea0d9e6a084129ebfe2075e4881ae00
实测候选源码：0ffd222f3bdbbfb81abe76ba1ce92e18a756220d
候选源码树：5d574745aea7e6df148545e4925b9058bbed7f9a
```

即使后续文档或合并提交发布本报告，上述标识仍指向实际参与测量的代码。

## 时间戳契约

`SendWithAttempt(ctx, path, payload) (time.Time, error)` 在调用 `Write`、
`WriteTo` 或原生 `WriteMsgUDPAddrPort` 前立即读取时间，此时已获取传输层写互斥锁，
完成截止时间与取消处理设置，以及适用的源地址控制检查和地址转换。
这是调用连接写入的时间，不是调度时间或内核发包时间；非零值不能证明发送成功。

原生路径在写入前拒绝发送时返回零时间。一旦开始调用连接写入，短写、写入失败、
取消和重置截止时间失败都会保留尝试时间。包级辅助函数精确识别内置 Carrier 及捕获的
Carrier/listener 路径类型。未知的自定义 `ReplyPath` 实现调用其普通 `Send`，
返回零时间。即使自定义包装类型嵌入 Carrier 并继承了时间戳方法，也仍会保留它重写的
`Send` 行为。`ReplyPath` 接口没有变化。

普通 `Send` 传递 nil 时间戳存储；新增的时间戳逻辑不会为该路径增加时钟读取或分配。
请求时间戳不会启用诊断。捕获的代次、远端/源地址/OOB 所有权、写入串行化、截止时间
清理、取消回调等待及 Close 行为均得到保留。

## 本地比较

共享夹具使用 `context.Background` 和确定性的空操作连接发送 1200 字节数据包，
执行真实的传输层锁、取消处理设置及可选诊断。它不包含 UDP 系统调用、网络负载、
调用方逐次创建超时上下文或保留模拟数据包副本的成本。`ns/op` 是基准测试的每次操作
耗时，不是采样 CPU 成本或产品吞吐量；原始输出中的 `MB/s` 不是网络带宽。

记录的环境为原生 Linux/amd64、Go 1.26.4、Intel Xeon E3-1245 v5。
可用亲和性 CPU 为 0-5；每个计时进程固定在 CPU 2，使用 `GOMAXPROCS=1` 和
`-test.cpu=1`。基线 Send、候选 Send、候选时间戳辅助函数依次运行，每个场景采集
五次 200 ms 观测。记录的计时窗口内，父任务及其他代理的测试、构建和批量处理均暂停。

下表为 ns/op 中位数，方括号为原始最小值和最大值。保留全部样本，没有剔除异常值，
也不声称差异具有统计显著性。

| 路径 / 诊断 | 基线 Send | 候选 Send | 时间戳辅助函数 |
| --- | ---: | ---: | ---: |
| Carrier / 关闭 | 706.5 [695.7, 752.8] | 695.8 [684.3, 723.3] | 783.7 [764.1, 845.4] |
| 捕获的 Carrier / 关闭 | 713.4 [696.1, 743.7] | 714.3 [701.9, 739.2] | 795.2 [785.6, 809.1] |
| listener / 关闭 | 801.2 [795.4, 835.5] | 813.7 [799.5, 824.1] | 892.4 [878.3, 901.5] |
| Carrier / 开启 | 2193 [1028, 3000] | 970 [967.8, 991.5] | 1189 [1172, 1205] |
| 捕获的 Carrier / 开启 | 956.8 [939.8, 982.8] | 966.9 [955.1, 1004] | 1037 [1004, 1049] |
| listener / 开启 | 1079 [1049, 1131] | 1097 [1068, 1101] | 1164 [1134, 1178] |

关闭诊断时，时间戳辅助函数相对候选 Send 的中位数增量，依 Carrier、捕获的 Carrier、
listener 顺序分别为 87.9 ns、80.9 ns、78.7 ns。普通 Send 中位数变化依次为
-1.5%、+0.1%、+1.6%。所有基线及候选观测值均为相同的 264 B/op、4 allocs/op。

基线中开启诊断的 Carrier 序列噪声明显，范围为 1028-3000 ns/op。
不能将其表面上的改善归因于本次变更。这些短时间的合成测试既不能证明网络吞吐量提升，
也不构成正式性能验收结果。

## 源码与可执行文件

基线只在固定源码上增加共享的普通 Send 基准夹具。候选补丁包含两个基准夹具和全部
生产代码及测试变更。源码补丁、共享夹具及可执行文件的哈希均在基准调用前记录，
运行后再次检查。提交之后，提交对应的补丁和重新构建的候选可执行文件均与实测产物
逐字节一致。测试可执行文件的构建元数据没有嵌入 VCS SHA。

| 产物 | SHA-256 |
| --- | --- |
| 候选源码/测试补丁 | `957b2c37ea2d464e77f841203afd84763cd9be56dd26432c64fcf1a60eed191b` |
| 共享普通 Send 夹具 | `12174cf15cce5f779bee5320b7e929a0cface596a86f65d3ba91137a4286a336` |
| 候选时间戳夹具 | `24ed51b1fccef5179f293d2b5d2a3f1725343dae4fd46b69a954e8fad840d8eb` |
| 基线可执行文件，未打包 | `3657ada5f86e9d488a38fbb3ecdeff2c064e669e2f5c0e3ffe313617884fc818` |
| 候选可执行文件，未打包 | `edf12d44d841a22e89507a5f7f173d33e0556fffd0985c3fd28e584f93bd1d30` |

使用记录的 Go 环境重建源码并编译，将 `evidence_dir` 设置为解压后的证据目录，
并使用尚未占用的工作树路径：

```sh
git worktree add --detach /tmp/mpudp-attempt-before 5e19e44a6ea0d9e6a084129ebfe2075e4881ae00
git worktree add --detach /tmp/mpudp-attempt-after 0ffd222f3bdbbfb81abe76ba1ce92e18a756220d
cp "$evidence_dir/send_legacy_benchmark_test.go.txt" /tmp/mpudp-attempt-before/internal/transport/send_legacy_benchmark_test.go
go -C /tmp/mpudp-attempt-before test -c -o "$evidence_dir/baseline.test" ./internal/transport
go -C /tmp/mpudp-attempt-after test -c -o "$evidence_dir/candidate.test" ./internal/transport
```

也可以向干净基线应用 `candidate.patch`，重建包含两个夹具的完整候选源码树。
公开审计通过临时 Git 索引验证源码树完全一致，不会更改当前工作树。
重新构建的可执行文件哈希可能因构建路径或环境不同而变化；源码树重建不依赖此结果。
在其他负载空闲的主机上，从解压的证据目录依次执行以下命令，按可用 CPU 调整亲和性：

```sh
env GOMAXPROCS=1 taskset -c 2 ./baseline.test -test.run '^$' -test.bench '^BenchmarkTransportSendLegacy$' -test.benchtime=200ms -test.count=5 -test.cpu=1
env GOMAXPROCS=1 taskset -c 2 ./candidate.test -test.run '^$' -test.bench '^BenchmarkTransportSendLegacy$' -test.benchtime=200ms -test.count=5 -test.cpu=1
env GOMAXPROCS=1 taskset -c 2 ./candidate.test -test.run '^$' -test.bench '^BenchmarkTransportSendAttempt$' -test.benchtime=200ms -test.count=5 -test.cpu=1
```

## 验证与归档

产出记录确认 `go test ./internal/transport`、覆盖根模块全部 25 个包的
`go test -race ./...`、`go vet ./...`、格式检查及 `git diff --check` 通过。
归档中的产出 README 记录了这些结果，但不包含完整测试终端输出。
父任务和独立代码审查均未发现阻塞问题。测试覆盖锁和截止时间设置之后的时间戳位置、
写入前错误、写入及清理失败、取消、Close、连续调用重置、适配器回退和原生源地址回复。

[公开归档](transport-attempt-20260905.tar.gz) 包含 24 个普通文本文件：19 个原始记录、
公开说明与审计代码、精确成员清单、源码验证记录及包含 23 项的公开校验索引。
可执行文件、私有文件和 `.lab` 目录均被排除，精确成员允许清单确保它们不会被打包；
二进制哈希仅作为元数据保留。主机元数据及临时源码路径仍然可见。
所有实际解压成员均通过 UTF-8、二进制内容及凭据标记扫描。
归档的类型、权限、所有者和时间戳均已规范化，重新打包得到相同字节。

解压后执行 `sha256sum -c PUBLIC_SHA256SUMS`。保留的 `pre-run-SHA256SUMS`
还列出了未打包的可执行文件，属于历史元数据，不是公开验证索引。
使用包含两个源码提交的仓库执行：

```sh
python3 public-evidence.py audit /path/to/transport-attempt-20260905.tar.gz --repo /path/to/mpudp
python3 compare.py
```

审计验证归档成员及校验值、源码重建、夹具身份，以及全部 90 个原始观测值与保留的
比较 JSON 一致。它不需要可执行文件，也不会重跑测量。

```text
归档字节数：21732
归档 SHA-256：3e5bdf68be30bf2bb3dc85d656191ea79c4323a96b684a2fc3668abef435d5ce
```
