package main

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/mofelee/mpudp/config"
)

func TestRunValidatesConfiguration(t *testing.T) {
	t.Parallel()
	path := writeConfig(t, `carriers: ["example.com:4000"]
fec: {data_shards: 3, parity_shards: 2}
psk: "test-key"
`)
	var stdout, stderr bytes.Buffer
	if code := run([]string{"-config", path}, &stdout, &stderr); code != 0 {
		t.Fatalf("run() code = %d, stderr = %q", code, stderr.String())
	}
	if got, want := stdout.String(), "configuration valid: mode=initiator\n"; got != want {
		t.Fatalf("stdout = %q, want %q", got, want)
	}
	if stderr.Len() != 0 {
		t.Fatalf("stderr = %q, want empty", stderr.String())
	}
}

func TestRunRequiresConfigPath(t *testing.T) {
	t.Parallel()
	var stdout, stderr bytes.Buffer
	if code := run(nil, &stdout, &stderr); code != 2 {
		t.Fatalf("run() code = %d, want 2", code)
	}
	if stdout.Len() != 0 || !strings.Contains(stderr.String(), "-config is required") {
		t.Fatalf("stdout = %q, stderr = %q", stdout.String(), stderr.String())
	}
}

func TestRunDoesNotLeakPSKOnConfigurationFailure(t *testing.T) {
	t.Parallel()
	const marker = "cli-PSK-DO-NOT-LEAK-a44b"
	path := writeConfig(t, `carriers: ["not-an-address"]
fec: {data_shards: 3, parity_shards: 2}
psk: "cli-PSK-DO-NOT-LEAK-a44b"
`)
	var stdout, stderr bytes.Buffer
	if code := run([]string{"-config", path}, &stdout, &stderr); code != 1 {
		t.Fatalf("run() code = %d, want 1", code)
	}
	if strings.Contains(stdout.String(), marker) || strings.Contains(stderr.String(), marker) {
		t.Fatalf("PSK leaked: stdout = %q, stderr = %q", stdout.String(), stderr.String())
	}
}

func TestRunBoundsOversizeConfigWithoutLeakingPSK(t *testing.T) {
	t.Parallel()
	const marker = "oversize-cli-PSK-DO-NOT-LEAK-c913"
	base := `carriers: ["example.com:4000"]
fec: {data_shards: 3, parity_shards: 2}
psk: "oversize-cli-PSK-DO-NOT-LEAK-c913"
`
	wantSize := config.MaxConfigBytes + 1
	contents := base + "#" + strings.Repeat("x", wantSize-len(base)-1)
	if len(contents) != wantSize {
		t.Fatalf("oversize fixture length = %d, want %d", len(contents), wantSize)
	}
	path := writeConfig(t, contents)
	var stdout, stderr bytes.Buffer
	if code := run([]string{"-config", path}, &stdout, &stderr); code != 1 {
		t.Fatalf("run() code = %d, want 1", code)
	}
	if strings.Contains(stdout.String(), marker) || strings.Contains(stderr.String(), marker) {
		t.Fatalf("PSK leaked: stdout = %q, stderr = %q", stdout.String(), stderr.String())
	}
	if !strings.Contains(stderr.String(), "maximum size") {
		t.Fatalf("stderr = %q, want size-limit diagnosis", stderr.String())
	}
}

func writeConfig(t *testing.T, contents string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "mpudp.yaml")
	if err := os.WriteFile(path, []byte(contents), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}
	return path
}
