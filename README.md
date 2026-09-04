# MPUDP

MPUDP 是一个用 Go 实现的用户态 Multipath UDP Datagram Tunnel。当前仓库已完成
v0.1 Loop 1：严格配置模型、公共 Datagram API 和无网络副作用的生命周期骨架。
Wire codec、Reed-Solomon 数据面、UDP Carrier 和握手将在后续 loop 实现；当前命令
只校验配置，不会打开 socket 或启动后台任务。

## 开发环境

- Go 1.24 或更高版本；`go.mod` 的最低版本为 1.24。
- v0.1 运行时支持目标为 Linux `amd64` 和 `arm64`。
- 配置与 API 骨架使用可移植 Go；能在其他平台构建不代表该平台进入 v0.1 支持范围。

```bash
go build ./...
go test ./...
go test -race ./...
go vet ./...
```

## 配置校验

```bash
go run ./cmd/mpudp -config ./alice.yaml
```

成功时命令输出配置模式并退出。配置错误可通过库中的
`errors.Is(err, mpudp.ErrInvalidConfig)` 判断；错误和格式化输出不会包含 PSK。

配置字段、默认值及严格校验规则见 [配置参考](docs/CONFIGURATION.md)，公共类型和
并发契约见 [API 参考](docs/API.md)。完整产品语义见
[需求文档](docs/MPUDP_REQUIREMENTS.md)。
