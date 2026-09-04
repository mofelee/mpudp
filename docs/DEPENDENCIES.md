# Dependency and License Audit

This document records the dependency surface selected by `go.mod` and
`go.sum`, the packages compiled by the repository, and the tooling invoked by
CI. It is an engineering inventory, not legal advice.

This audit **does not select or grant a license for MPUDP**. The repository has
no root `LICENSE`, `LICENSE.md`, or `COPYING` file. Choosing the project's
license remains a separate owner decision. The licenses below apply only to
the identified third-party works and do not license MPUDP's own source.

## Audit method

Run the following from the repository root after every module, toolchain, build
mode, or release packaging change:

```bash
go list -deps -f '{{if .Module}}{{.Module.Path}} {{.Module.Version}}{{else}}stdlib{{end}}' ./... | sort -u
go list -deps -test -f '{{if .Module}}{{.Module.Path}} {{.Module.Version}}{{else}}stdlib{{end}}' ./... | sort -u
go list -m all
go mod graph
go mod verify
go mod tidy -diff
```

The first command identifies modules compiled by non-test packages. The second
adds test variants and test-only imports. `go list -m all` and `go mod graph`
show the selected module graph, which is deliberately broader than either
compiled set. An empty `go mod tidy -diff` result confirms that the checked-in
module files already describe that graph.

This snapshot was checked against the license and notice files in the selected
module versions. The compiled module sets were also compared with `GOOS=linux`
for both `GOARCH=amd64` and `GOARCH=arm64`; they were identical. CI declares Go
1.24.x; a release audit must repeat the checks with the exact toolchain, target,
and build flags used for the released artifact.

## Compiled production dependencies

`go list -deps ./...` identifies the following external dependency surface.
The Go runtime and relevant standard-library packages are also linked into a
normal Go executable even though they are not modules in `go.mod`.

| Component | Version | Use | Upstream license |
|---|---:|---|---|
| Go runtime and standard library | release toolchain | Runtime and standard-library support | [BSD-3-Clause](https://go.dev/LICENSE) |
| [`github.com/klauspost/reedsolomon`](https://github.com/klauspost/reedsolomon) | v1.14.2 | Reed-Solomon encoding and recovery | [MIT](https://github.com/klauspost/reedsolomon/blob/v1.14.2/LICENSE) |
| [`github.com/klauspost/cpuid/v2`](https://github.com/klauspost/cpuid) | v2.3.0 | Indirect CPU feature detection used by `reedsolomon` | [MIT](https://github.com/klauspost/cpuid/blob/v2.3.0/LICENSE) |
| [`gopkg.in/yaml.v3`](https://github.com/go-yaml/yaml) | v3.0.1 | YAML configuration parsing | [MIT and Apache-2.0, by file](https://github.com/go-yaml/yaml/blob/v3.0.1/LICENSE); includes an upstream [NOTICE](https://github.com/go-yaml/yaml/blob/v3.0.1/NOTICE) |
| [`golang.org/x/sys`](https://pkg.go.dev/golang.org/x/sys) | v0.30.0 | Linux socket and system-call support | [BSD-3-Clause](https://github.com/golang/sys/blob/v0.30.0/LICENSE) |

The YAML license file identifies eight libyaml-derived files as MIT and the
remaining files as Apache-2.0. Treat the module as a mixed-license component;
do not collapse it to a single permissive label in release notices.

## Compiled test-only dependency

`go list -deps -test ./...` adds one selected module that is not imported by
the production package graph:

| Component | Version | Use | Upstream license |
|---|---:|---|---|
| [`go.uber.org/goleak`](https://github.com/uber-go/goleak) | v1.3.0 | Goroutine-leak assertions in tests | [MIT](https://github.com/uber-go/goleak/blob/v1.3.0/LICENSE) |

Test-only classification describes the current repository build. If test
binaries or test source bundles are distributed, their exact contents still
need a distribution audit.

## Module-graph-only dependencies

The following versions are selected by `go list -m all` and appear in
`go mod graph`, but neither `go list -deps ./...` nor
`go list -deps -test ./...` reports them as compiled by the current production
packages or repository tests. They remain recorded because graph selection can
change when imports, tags, or build constraints change.

| Component | Version | Upstream license |
|---|---:|---|
| [`github.com/creack/pty`](https://github.com/creack/pty) | v1.1.9 | [MIT](https://github.com/creack/pty/blob/v1.1.9/LICENSE) |
| [`github.com/davecgh/go-spew`](https://github.com/davecgh/go-spew) | v1.1.1 | [ISC](https://github.com/davecgh/go-spew/blob/v1.1.1/LICENSE) |
| [`github.com/kr/pretty`](https://github.com/kr/pretty) | v0.1.0 | [MIT](https://github.com/kr/pretty/blob/v0.1.0/License) |
| [`github.com/kr/text`](https://github.com/kr/text) | v0.2.0 | [MIT](https://github.com/kr/text/blob/v0.2.0/License) |
| [`github.com/pmezard/go-difflib`](https://github.com/pmezard/go-difflib) | v1.0.0 | [BSD-3-Clause](https://github.com/pmezard/go-difflib/blob/v1.0.0/LICENSE) |
| [`github.com/stretchr/testify`](https://github.com/stretchr/testify) | v1.8.0 | [MIT](https://github.com/stretchr/testify/blob/v1.8.0/LICENSE) |
| [`gopkg.in/check.v1`](https://github.com/go-check/check) | v1.0.0-20180628173108-788fd7840127 | [BSD-2-Clause](https://github.com/go-check/check/blob/788fd7840127/LICENSE) |

## CI and system tooling

GitHub Actions are executed build tooling, not shipped MPUDP dependencies. The
workflow pins each action to a full commit and the license at each checked pin
is MIT:

| Action | Pinned commit | Declared version | License |
|---|---|---:|---|
| `actions/checkout` | `3d3c42e5aac5ba805825da76410c181273ba90b1` | v7.0.1 | [MIT](https://github.com/actions/checkout/blob/3d3c42e5aac5ba805825da76410c181273ba90b1/LICENSE) |
| `actions/setup-go` | `b7ad1dad31e06c5925ef5d2fc7ad053ef454303e` | v7.0.0 | [MIT](https://github.com/actions/setup-go/blob/b7ad1dad31e06c5925ef5d2fc7ad053ef454303e/LICENSE) |
| `actions/upload-artifact` | `043fb46d1a93c77aae656e7c1c64a875d1fc6a0a` | v7.0.1 | [MIT](https://github.com/actions/upload-artifact/blob/043fb46d1a93c77aae656e7c1c64a875d1fc6a0a/LICENSE) |

The Linux integration workflow installs `conntrack`, `diffutils`, `iproute2`,
`iputils-ping`, `nftables`, `procps`, and `tcpdump`. Those packages, the hosted
runner image, the shell utilities used by the harness, and the Linux kernel are
environment prerequisites. MPUDP does not redistribute them in the current
workflow, so they are outside the product dependency tables above. Reassess
them if a future image, appliance, container, or bundle includes them.

## Distribution obligations

Before any source or binary distribution, audit the exact artifact rather than
relying only on the module graph:

- choose and publish a root MPUDP license separately;
- reproduce the copyright, permission, and disclaimer text required by every
  included MIT, ISC, BSD-2-Clause, and BSD-3-Clause component, including the
  applicable no-endorsement clauses;
- include the YAML module's MIT and Apache-2.0 terms and its upstream `NOTICE`
  verbatim, and satisfy Apache-2.0 notice and modification requirements;
- include the Go runtime and standard-library license materials required for
  binary distribution;
- record the exact Go version, module versions, build tags, target, and flags,
  and inspect the built artifact for embedded modules;
- audit CGO and any statically or dynamically linked system libraries for the
  actual build mode and target; and
- repeat the audit for vendored code, generated code, copied assets, packaging
  metadata, container layers, and any newly introduced release tooling.

The current CI workflow runs `go build ./...` but publishes no binaries. Its
only upload is short-lived integration failure diagnostics. Issue
[#10](https://github.com/mofelee/mpudp/issues/10) covers this documentation
audit; it does not authorize a release, tag, artifact publication, or root
license choice.
