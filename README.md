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

## CI 与经典网络场景

GitHub Actions 在 pull request、`main` push 和手动触发时运行以下 11 个稳定
check 名称；分支保护应直接使用这些名称：

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

`build-unit` 的本地等价门禁为：

```bash
test -z "$(gofmt -l .)"
go mod verify
go vet ./...
go build ./...
go test -count=1 ./...
go test ./internal/wire -run '^$' -fuzz '^FuzzDecodeArbitrary$' -fuzztime 3s -timeout 1m
go test ./internal/wire -run '^$' -fuzz '^FuzzRoundTripBounded$' -fuzztime 3s -timeout 1m
go test ./internal/wire -run '^$' -fuzz '^FuzzSingleBitTamper$' -fuzztime 3s -timeout 1m
```

`race` 的本地等价命令是 `go test -race -count=1 ./...`。集成场景仅支持
Linux，并需要 root 或等价的网络管理权限。Debian/Ubuntu 环境安装与 CI 相同的
依赖：

```bash
sudo apt-get update
sudo apt-get install --yes --no-install-recommends \
  conntrack diffutils iproute2 iputils-ping nftables procps tcpdump
```

使用固定 case、run ID 和 seed 可以重放 CI 失败。state 与诊断目录必须分离；
seed 是 1..128 个字母、数字、点、下划线、冒号、加号或连字符组成的文本；未指定时
使用 run ID。harness 会校验并把它写入 state、case-start 事件和失败诊断。
以下命令无论成功或失败都会执行精确 teardown 和残留审计：

```bash
case_name=direct-single-carrier
run_id=local-direct-single-carrier
seed=local-1001
sudo env PATH="${PATH}" GOFLAGS=-buildvcs=false \
  MPUDP_IT_REQUIRE_CONNTRACK=1 MPUDP_IT_SEED="${seed}" \
  scripts/integration/run \
    --run-id "${run_id}" \
    --state "/tmp/mpudp-it-state-${run_id}" \
    --artifacts /tmp/mpudp-it-artifacts \
    --case "${case_name}"
```

失败时，脱敏诊断位于 `/tmp/mpudp-it-artifacts/<run-id>`；CI 的短期 failure
artifact 还包含 commit、case、run ID 和 seed。成功运行不上传 artifact。拓扑结构、
场景列表、诊断内容和分阶段排障命令见 [集成测试参考](docs/INTEGRATION.md)。

## 配置校验

```bash
go run ./cmd/mpudp -config ./alice.yaml
```

成功时命令输出配置模式并退出。配置错误可通过库中的
`errors.Is(err, mpudp.ErrInvalidConfig)` 判断；错误和格式化输出不会包含 PSK。

配置字段、默认值及严格校验规则见 [配置参考](docs/CONFIGURATION.md)，公共类型和
并发契约见 [API 参考](docs/API.md)。完整产品语义见
[需求文档](docs/MPUDP_REQUIREMENTS.md)。
