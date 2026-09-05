# MPUDP

[![CI](https://github.com/mofelee/mpudp/actions/workflows/ci.yml/badge.svg)](https://github.com/mofelee/mpudp/actions/workflows/ci.yml)

MPUDP 是一个用 Go 实现的用户态 Multipath UDP Datagram Tunnel。它把一个应用 Datagram
编码成一个 Reed-Solomon block，把 shard 轮转分配到多个长期 UDP Carrier，并在对端恢复
原始 Datagram。v0.1 已包含严格配置、认证 Wire 协议、FEC、调度、UDP transport、
initiator/listener/dual Session 运行时和可复现的 Linux 网络集成测试。

```text
Application Datagram API
          |
          v
  Peer -> Session -> FEC block -> shard scheduler -> long-lived UDP Carriers
   ^                                                        |
   |                                                        v
Listener <- authenticated HELLO and learned reverse path <- UDP network
```

公共 API 保留 Datagram 边界；它不提供可靠、有序或 stream 语义。当前 CLI 也不是 TUN、
SOCKS、TCP proxy 或通用 stream adapter。

## 支持范围

- Go 1.24 或更高版本；`go.mod` 的最低版本为 1.24。
- v0.1 运行时支持 Linux `amd64` 和 `arm64`。
- 其他平台可能通过部分构建或单元测试，但不属于 v0.1 的运行支持范围。

```bash
go build ./...
go test -count=1 ./...
go test -race -count=1 ./...
go vet ./...
```

## 下载二进制

[GitHub Releases](https://github.com/mofelee/mpudp/releases) 提供 Linux `amd64` 和
`arm64` 的静态二进制归档，以及 `checksums.txt`。归档包含文档、构建信息和第三方许可证。

以 Linux amd64 的首个版本为例（ARM64 请把 `arch` 改为 `arm64`）：

```bash
version=v0.1.0
arch=amd64
archive="mpudp_${version#v}_linux_${arch}"
curl -fLO "https://github.com/mofelee/mpudp/releases/download/${version}/${archive}.tar.gz"
curl -fLO "https://github.com/mofelee/mpudp/releases/download/${version}/checksums.txt"
sha256sum --ignore-missing --check checksums.txt
tar -xzf "${archive}.tar.gz"
"./${archive}/mpudp" --version
sudo install -m 0755 "${archive}/mpudp" /usr/local/bin/mpudp
```

`mpudp --version` 显示版本和源码 commit，无需配置文件，也不会打开网络 socket。
发布方式及本地打包步骤见 [发布流程](docs/RELEASING.md)。CLI 的业务流量边界与源码运行相同，
详见下节；发布二进制不会增加 TUN、SOCKS 或 stream 适配功能。

## 启动 CLI

最小 initiator 配置如下。示例 PSK 仅供本机开发测试；生产环境必须注入高熵密钥，并按
[受保护的 PSK 配置](docs/CONFIGURATION.md#psk-管理)限制文件权限、日志和分发路径。

```yaml
carriers:
  - "127.0.0.1:9000"
fec:
  data_shards: 3
  parity_shards: 2
psk: "development-only-example-key"
```

```bash
go run ./cmd/mpudp -config ./initiator.yaml
```

命令会解析并校验配置、打开所需 socket，然后运行到 SIGINT 或 SIGTERM。initiator 和
dual 配置会自动创建一个 outbound Session；listener 配置接受经过认证的 inbound
Session。CLI 当前不会从 Session 读写业务 Datagram，因此要实际承载应用流量，应嵌入
下面的 Go API 或实现一个显式的上层 adapter。

## 公共 Datagram API

下面省略了启动阶段的错误分支，但展示了双向数据路径。`NewSession` 会立即返回；握手完成
前 `WritePacket` 返回 `mpudp.ErrNotReady`。生产调用方必须处理该状态以及其他稳定错误类别。

```go
listenerPeer, err := mpudp.NewPeerContext(ctx, listenerConfig)
if err != nil {
    return err
}
defer listenerPeer.Close()

listener, err := listenerPeer.Listener()
if err != nil {
    return err
}
type acceptResult struct {
    session mpudp.Session
    err     error
}
accepted := make(chan acceptResult, 1)
go func() {
    session, acceptErr := listener.Accept(ctx)
    accepted <- acceptResult{session: session, err: acceptErr}
}()

initiatorPeer, err := mpudp.NewPeerContext(ctx, initiatorConfig)
if err != nil {
    return err
}
defer initiatorPeer.Close()
outbound, err := initiatorPeer.NewSession()
if err != nil {
    return err
}

for {
    err = outbound.WritePacket(request)
    if !errors.Is(err, mpudp.ErrNotReady) {
        break
    }
    select {
    case <-ctx.Done():
        return ctx.Err()
    case <-time.After(10 * time.Millisecond):
    }
}
if err != nil {
    return err
}
result := <-accepted
if result.err != nil {
    return result.err
}
inbound := result.session
requestCopy, err := inbound.ReadPacket()
if err != nil {
    return err
}
if err := inbound.WritePacket(replyFor(requestCopy)); err != nil {
    return err
}
reply, err := outbound.ReadPacket()
if err != nil {
    return err
}
fmt.Printf("received %d-byte reply\n", len(reply))
return nil
```

完整的角色、并发、关闭、错误和容量契约见 [公共 API](docs/API.md) 与
[Session 运行时](docs/SESSION.md)。

## v0.1 非目标

- 不提供 DATA ACK/NACK、重传、可靠或有序交付、拥塞控制、动态 FEC、加权调度或
  按流公平性。
- 不内置 TUN/TAP、SOCKS、TCP、VPN 路由、stream 适配层或具体上层协议适配。
- 不提供 STUN/ICE/TURN、自有 Relay 或 Mesh；T1-T5 的 nftables 转发只属于测试和部署
  环境，不是 MPUDP 产品能力。
- PSK/HMAC 只认证 packet 并保护完整性，不加密 Payload，也不提供保密性。
- 不在已建立 Session 内执行 PLPMTUD 或自适应调整 payload budget；该扩展由
  [#13](https://github.com/mofelee/mpudp/issues/13) 跟踪。
- 不为每个 Carrier 协商不同 budget，也不生成不等长 shard；该协议扩展由
  [#14](https://github.com/mofelee/mpudp/issues/14) 跟踪。
- 不提供内核模块、eBPF、XDP 或 DPDK 数据面。

## CI 与网络集成

GitHub Actions 在 pull request、`main` push 和手动触发时发布以下 11 个稳定 check 名称；
分支保护应直接使用这些名称。

<!-- mpudp-ci-checks:start -->
- `build-unit`
- `race`
- `integration / direct-single-carrier`
- `integration / rs53-five-carrier-loss`
- `integration / rs53-two-carrier-rotation`
- `integration / slow-path-early-recovery`
- `integration / transparent-nat-reverse-path`
- `integration / endpoint-rebinding-and-expiry`
- `integration / auth-and-state-pollution`
- `integration / mtu-budget-no-fragment`
- `integration / shutdown-cleanup`
<!-- mpudp-ci-checks:end -->

本地一次运行全部九个 canonical case：

```bash
run_id=local-all
sudo env PATH="${PATH}" GOFLAGS=-buildvcs=false \
  MPUDP_IT_REQUIRE_CONNTRACK=1 MPUDP_IT_SEED=local-all \
  scripts/integration/run \
    --run-id "${run_id}" \
    --state "/tmp/mpudp-it-state-${run_id}" \
    --artifacts /tmp/mpudp-it-artifacts \
    --case direct-single-carrier \
    --case rs53-five-carrier-loss \
    --case rs53-two-carrier-rotation \
    --case slow-path-early-recovery \
    --case transparent-nat-reverse-path \
    --case endpoint-rebinding-and-expiry \
    --case auth-and-state-pollution \
    --case mtu-budget-no-fragment \
    --case shutdown-cleanup
```

当前 harness 明确要求以 root 身份运行（EUID 0），并依赖 `conntrack`、`iproute2`、
`nftables`、`tc`、`tcpdump` 等 Linux 工具。拓扑、依赖、可复现参数、诊断和清理边界见
[集成测试](docs/INTEGRATION.md)。

## 文档索引

- [公共 API](docs/API.md)
- [配置参考](docs/CONFIGURATION.md)
- [Session 与握手](docs/SESSION.md)
- [UDP Transport](docs/TRANSPORT.md)
- [Wire 协议](docs/WIRE_PROTOCOL.md)
- [FEC 与调度](docs/FEC.md)
- [集成测试](docs/INTEGRATION.md)
- [单流性能基准与证据](docs/PERFORMANCE.md)
- [完整配置示例](docs/MPUDP_CONFIG_EXAMPLE.md)
- [v0.1 需求](docs/MPUDP_REQUIREMENTS.md)
- [需求追踪矩阵](docs/TRACEABILITY.md)
- [依赖与许可证审计](docs/DEPENDENCIES.md)
- [发布流程](docs/RELEASING.md)
