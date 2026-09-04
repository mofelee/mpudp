//go:build !linux

package transport

import "syscall"

// PMTUDiscoverySupported is false because v0.1 only specifies and verifies the
// Linux PMTU/DF socket contract. Other platforms can be used only when callers
// do not require that guarantee.
func PMTUDiscoverySupported() bool { return false }

func configurePMTU(syscall.Conn, string) (bool, error) { return false, nil }

func isPathMTUError(error) bool { return false }
