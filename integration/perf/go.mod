module github.com/mofelee/mpudp/integration/perf

go 1.24.0

require (
	github.com/mofelee/mpudp v0.1.0
	github.com/xtaci/kcp-go/v5 v5.6.72
	golang.org/x/net v0.47.0
	golang.org/x/sys v0.38.0
)

require (
	github.com/klauspost/cpuid/v2 v2.3.0 // indirect
	github.com/klauspost/reedsolomon v1.14.2 // indirect
	github.com/pkg/errors v0.9.1 // indirect
	github.com/tjfoc/gmsm v1.4.1 // indirect
	golang.org/x/crypto v0.45.0 // indirect
	golang.org/x/time v0.14.0 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)

replace github.com/mofelee/mpudp => ../..
