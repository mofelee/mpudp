# 发布流程

GitHub Actions 的 `Release` workflow 在推送 `v*` tag 时自动构建并发布。
版本使用 `vMAJOR.MINOR.PATCH`，也支持 `v0.2.0-rc.1` 这样的预发布版本。
预发布版本会标记为 GitHub prerelease，不替换 latest release。

## 产物与校验

支持的发布目标为 Linux `amd64` 和 `arm64`。每个版本包含三个附件：

- `mpudp_<version>_linux_amd64.tar.gz`
- `mpudp_<version>_linux_arm64.tar.gz`
- `checksums.txt`，记录两个归档的 SHA-256。

每个归档有同名顶层目录，包含 `mpudp`、`README.md`、`docs/`、`BUILDINFO.txt` 和
`licenses/`，以及仓库存在时的项目 `LICENSE`。第三方许可证按实际编译的生产依赖收集，
包含 YAML 的原始 `NOTICE`、Go runtime/标准库及 Go vendored 依赖的许可证材料。

发布工具链固定为 Go `1.26.4`，使用 `CGO_ENABLED=0`、`-mod=readonly`、`-trimpath`、
`-buildvcs=false` 和 `-s -w`，并通过 `-X` 写入版本和完整源码 commit。
amd64 使用 `GOAMD64=v1`，arm64 使用 `GOARM64=v8.0`。不依赖主机的动态 C 库。
归档的顺序、时间戳、所有者和 gzip header 固定；相同源码、版本及工具链可复现构建。

`BUILDINFO.txt` 和 `mpudp --version` 可用于确认版本与源码提交，`go version -m mpudp`
可检查链接的模块和编译选项。校验和用于验证下载完整性，不是独立的数字签名。

## 发布步骤

1. 在 `docs/releases/<tag>.md` 编写发行说明，说明支持范围、兼容性和已知限制。
   如果没有该文件，workflow 使用 GitHub 自动生成的发行说明。
2. 将修改合并到 `main`，确认 CI 通过；可在 Actions 手动运行 `Release` 并指定版本做
   dry run。手动运行和相关 pull request 只构建及上传 Actions artifacts，不创建 Release。
3. 在已验证的提交上创建并推送版本 tag：

   ```bash
   git switch main
   git pull --ff-only
   git tag -a v0.1.0 -m "MPUDP v0.1.0"
   git push origin v0.1.0
   ```

4. Tag workflow 在该 tag 的源码上复用完整 CI（包括 race 和九个网络集成场景），同时在
   原生 amd64/arm64 runner 上运行单元测试、打包并执行归档内二进制的版本检查。
5. 所有检查通过后，workflow 创建 draft Release、上传全部附件、重新下载并核对校验和，
   最后公开 Release。无需额外配置 PAT，发布 job 使用 GitHub 内置 token 的
   `contents: write` 权限；其他 job 只有只读仓库权限。

如果上传或下载验证失败，Release 保持 draft。检查失败原因和 draft 附件后再恢复发布；
workflow 不覆盖已经存在的 Release。已经公开的 tag 和附件不应移动或覆盖，修正应发布新版本。

## 本地打包

在仓库根目录执行（需要 Go、Git、Bash 和 GNU coreutils/tar/gzip）：

```bash
output_dir=$(mktemp -d)
MPUDP_RELEASE_VERSION=v0.1.0 MPUDP_RELEASE_ARCH=amd64 \
  bash scripts/release/package "$output_dir"
MPUDP_RELEASE_VERSION=v0.1.0 MPUDP_RELEASE_ARCH=arm64 \
  bash scripts/release/package "$output_dir"
```

本地可以交叉编译；执行二进制的 smoke test 需要对应架构。使用干净且已提交的工作树，
保证嵌入的 commit 与归档内容相符。生产发布以 tag workflow 的源码、测试和产物为准。
