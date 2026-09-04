package config

import (
	"fmt"
	"net"
	"net/netip"
	"strconv"
	"strings"
)

func validateAddress(value string, listener bool) (string, error) {
	if value == "" {
		return "", fmt.Errorf("address must not be empty")
	}
	if strings.TrimSpace(value) != value {
		return "", fmt.Errorf("address must not contain surrounding whitespace")
	}
	host, portText, err := net.SplitHostPort(value)
	if err != nil {
		return "", fmt.Errorf("must use host:port syntax: %w", err)
	}
	port, err := strconv.ParseUint(portText, 10, 16)
	if err != nil || port == 0 {
		return "", fmt.Errorf("port must be an integer in [1, 65535]")
	}
	if host == "" {
		if !listener {
			return "", fmt.Errorf("remote host must not be empty")
		}
		return net.JoinHostPort("", strconv.FormatUint(port, 10)), nil
	}

	canonicalHost, err := canonicalHost(host)
	if err != nil {
		return "", err
	}
	if !listener {
		if addr, parseErr := netip.ParseAddr(canonicalHost); parseErr == nil && addr.IsUnspecified() {
			return "", fmt.Errorf("remote host must not be an unspecified IP address")
		}
	}
	return net.JoinHostPort(canonicalHost, strconv.FormatUint(port, 10)), nil
}

func canonicalHost(host string) (string, error) {
	if addr, err := netip.ParseAddr(host); err == nil {
		return addr.String(), nil
	}

	name := strings.TrimSuffix(host, ".")
	if name == "" || len(name) > 253 {
		return "", fmt.Errorf("host is not a valid IP address or DNS name")
	}
	for _, label := range strings.Split(name, ".") {
		if len(label) == 0 || len(label) > 63 || label[0] == '-' || label[len(label)-1] == '-' {
			return "", fmt.Errorf("host is not a valid IP address or DNS name")
		}
		for i := 0; i < len(label); i++ {
			b := label[i]
			if (b < 'a' || b > 'z') && (b < 'A' || b > 'Z') && (b < '0' || b > '9') && b != '-' {
				return "", fmt.Errorf("host is not a valid IP address or DNS name")
			}
		}
	}
	return strings.ToLower(name), nil
}
