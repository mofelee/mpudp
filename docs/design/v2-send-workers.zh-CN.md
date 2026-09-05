# V2 Peer 发送 Worker

[English](v2-send-workers.md)

Linux fixed/session Datagram 使用 Peer 级固定发送池处理已建立的控制包和 FEC 数据包。
协议所有者仍串行处理认证、编码、接纳和状态变更。bootstrap 与尽力发送的 CLOSE
仍使用有界 context 同步发送。本实现交付 worker 执行及所有权，#16/#22 的其余调度、
健康检测和吞吐验收仍未完成。

## 执行与容量

`limits.max_send_workers` 创建固定 1..32 个发送 worker，默认为 8。每个 worker
具有一个工作槽和一个可靠结果槽，直到协议所有者消费结果后才再次空闲。因此完成通知
不会竞争可能丢弃事件的入口或诊断 channel。合并唤醒仅是提示，结果存储保留每个终态。
不会为每个数据包或 Session 创建 worker goroutine。

所有者按 Session 环轮转，仅在 worker 空闲时调用 `TakeSend`。控制器每条逻辑路径最多
接纳一个数据包，每个 Session 最多保留 `min(max_send_workers, 协商路径数)` 个。
等待的 FEC 分片描述符在分组层有界，尚未接纳数据包或路径。实现遵守配置的路径包数/
字节上限，有效完整 UDP 包必须适配 `max_path_queued_bytes`。一个已封闭 FEC 组持续
保留，直到全部分片到达终态。

worker 使用继承 Session 的 20ms context。调用前检查 100ms 排队期限，按时开始的
发送仍享有独立执行超时。原生 Carrier 和 listener 的写锁等待也响应 context，因此取消
等待者无需等待其他写入结束，也不会修改该写入的 deadline。自定义 Send 保留适配器
自身的实现；要保证有界 I/O，适配器必须响应 context。

原生发送路径在创建绑定时捕获 generation 和地址后备存储，rebuild 不能给旧认证路由
选择替换 socket。Listener 回复保留原 socket、目标源地址及 OOB，因此多个入站 worker
仍可能等待同一个 socket 写锁。配置 8 个 worker、单 Session 五条路径时，最多有五个
同时存活的 Session intent，并不代表八次独立 socket 写入。

调用返回或调用前取消后，worker 释放私有数据包所有权，再发布标量完成结果。
原生尝试时间是完成写锁/deadline 设置后进入连接写入的时间。自定义 Send 的时间未知，
但 nil-error 成功语义保留，并按完成时间保守 pacing。入队、分派或 UDP 成功都不证明
远端交付。

所有者先消费完成结果，再分派新工作；先登记返回 intent 的所有权，再处理其他控制器
结果，避免先前失败关闭 Session 时丢失在途记录。全部 Peer 槽忙碌时，驱动抑制发送就绪
期限，仍保留队列过期、重试和接收维护。pacing 等待不占用 worker。

## 接纳与清理

创建 Peer 前从字节上限扣除固定 worker/channel 元数据。控制器初始声明预付有界数据包
槽位和完成元数据。这些是所属存储边界，不是 RSS 或 Go 运行时栈/GC 上限。

发起端构造使用[预付串行拨号](v2-prepared-serial-dial.zh-CN.md)。包装对象和已打开的
全部 socket 在构造与安装前回退期间共享同一预留 scope，没有释放再获取的间隙。
Carrier 构造仍在执行时不能分派清理。scope 提升或部分安装后的失败对该 prepared API
是终态；内部传统并发 BeginDial 保留原有策略。

已安装 Session 使用[延后清理](v2-deferred-disposal.zh-CN.md)。关闭时在所有者锁内
停止接纳、取消发送并唤醒公共等待者。Scope.Close 后仍允许受限的控制器完成处理；
全部数据包和结果返回后才最终释放控制器初始存储。

一个固定清理 worker 在协议锁外关闭退休 Carrier。等待清理仅由已有有界 Session 记录
表示，worker 有一对工作/结果槽。失效路径可以单独清理 Carrier，其余 Session 继续工作。
最终 Session 清理等待构造、在途发送及单个 Carrier 清理全部结束，再清除包装对象/路径
引用并调用保留的存储释放 continuation。

Session.Close 等待最终清理。Listener.Close 先等待入站 Session，再关闭共享 listener
socket；dual Peer 的出站 Session 保持独立。Peer.Close 停止接纳，等待构造，排空发送/
清理结果，等待固定 worker 退出后再关闭账本。父 context 取消也会启动退休，并保持驱动
运行直到清理完成；子对象 Close 和失败的 NewSession 无需并发 Peer.Close 即可完成。
调用 Peer.Close 才会等待空闲固定 worker 退出并关闭账本。重复 Close 和 CloseGracefully 等待同一
最终结果。慢速清理可能保留接纳容量并延迟其他清理，但不持有协议锁；任意自定义 Close
不会被强制超时。

## 验证边界

确定性控制器测试覆盖释放/token 身份、乱序结果、原生/自定义时间、旧路径及控制版本、
满额度和维护期限。runtime 测试覆盖固定 worker 数、阻塞路径、公共接纳/接收推进、
Flush 失败边界、满完成槽，以及保留额度的 Session/Listener/Peer 清理。原生 socket
测试覆盖繁忙写锁上的取消等待。

同一源码下 workers=1 与 workers=8 的对照见[测量指南](../performance/v2-measurement.zh-CN.md)。
两端配置显式记录 worker 数，并与探针进程数分开保存。吞吐/分配和启用计时的观察属于
独立实验。发送池本身不证明主机余量、独立 listener socket 容量、故障收敛或三轮
300 秒性能门槛。
