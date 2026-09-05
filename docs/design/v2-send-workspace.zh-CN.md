# V2 预留发送工作区

[English](v2-send-workspace.md)

同步固定预算 Datagram controller 在握手额度提升前，为一个已封闭的 FEC 组和一次
报文组装预留额度。此前，已接纳的原报文可能占满 Session 或 Peer 的全部剩余字节，
而编码需要随后再次申请输出额度。原报文未消费就无法释放额度，重复重试也无法打破
这种停滞。

## 初始所有权

已有 initial claim 索引保持不变，末尾新增 `InitialOutput` 和 `InitialAssembly`，
总数变为六个。输出额度覆盖全部数据及校验分片的 backing、分片 slice 数组，以及
workspace/output 所有者元数据。组装额度继续采用已有的保守上限，即完整 UDP 报文
预算的两倍。两者独立于 controller/control 存储、接收 scratch、原报文及 codec。

额度按协商前的本地固定预算计算。安装时，每个专用 byte-only lease 只绑定一次，
不重复申请额度；实际协商后的报文大小不会超过该预算。构造中途失败时，controller
销毁已成功绑定的所有者，未使用的调用方 lease 留给握手回滚。初始额度不足会在接纳
业务 payload 前拒绝准入。

发送间隙仍保留这些额度。完整原报文可以使用剩余字节额度，而不会阻塞自身的输出或
报文组装。分片原报文即使已消费部分前缀，也保留完整复制 backing 的计费，直到最后
一片完成封组。这个保证针对可用路径和串行 driver 下的本地内存进度，不保证远端交付，
也不保证任意网络故障下的进度。

## 聚合 API

`RequiredOutputWorkspaceBytes(shards, shardBytes)` 无需创建 codec 即可计算输出上限。
`Queue.NewPrepaidOutputWorkspace(lease)` 将专用 lease 绑定到该 queue 的固定尺寸；
失败不会改变传入 lease 的所有权。`Queue.SealWithWorkspace(now, force, workspace)`
只允许该 workspace 同时持有一个有效 output，不再申请额外 ledger lease。工作区忙时
不会消费原报文 ID、payload 或 cursor；codec 失败会归还工作区槽位，不提交队列状态。

已有 `Queue.Seal` 仍分别预留每个 output，允许多个 output 同时存在。工作区是显式
可选功能，不改变普通 queue 的准入或时间语义。每个不可变 output 拥有独立分片 backing
和全新的共享 release 状态。旧 output 的复制句柄在释放后不会因工作区复用而重新有效。

`Output.Release` 先清零分片并清除引用，再归还槽位；常驻预留额度保留供后续复用。
`OutputWorkspace.Close` 禁止新使用，若还有有效 output，则等它释放后再返还 lease；
该方法不阻塞，也不撤销已有 output。调用方同时关闭 Queue 和 workspace。Scope/Peer
关闭本身不会撤销有效 lease。Controller.Close 同步销毁当前组、工作区和组装额度，
随后握手所有者才释放初始句柄的副本。本次变更没有增加发送 worker 或异步销毁流程。

## 验证

测试覆盖 Session 与 Peer 字节上限满载、同一部分消费原报文的连续组、原报文副本释放后
仍受保护的组装额度、工作区复用及旧复制句柄、普通多 output、构造回滚、codec 失败，
以及有存活 output 时的 Close。已有接收 scratch 压力测试也在预留发送组尚未完成时
保持出站队列压力。

```sh
go test ./internal/aggregationv2 ./internal/sessionv2
go test -race ./...
go vet ./...
```

这里计量的是拥有的字节和已预留责任，不是 allocator 开销、进程 RSS、吞吐量结论，
也没有新增公共队列上限。
