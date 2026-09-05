package integration_test

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"testing"
)

func TestShutdownSignalWaitsForListenerBeforeClosingInitiator(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("shutdown harness checks Linux process state")
	}
	repository, err := filepath.Abs("..")
	if err != nil {
		t.Fatal(err)
	}
	probe := `
source "$1"
MPUDP_IT_STATE_DIR=$2
fail_listener=$3
# These fixture IDs exceed Linux's PID limit; kill is replaced below.
shutdown_listener_pid=2147483646
shutdown_initiator_pid=2147483647
shutdown_listener_log=${MPUDP_IT_STATE_DIR}/bob.log
shutdown_initiator_log=${MPUDP_IT_STATE_DIR}/alice.log
shutdown_listener_events=${MPUDP_IT_STATE_DIR}/bob.ndjson
shutdown_initiator_events=${MPUDP_IT_STATE_DIR}/alice.ndjson
kill() {
	printf 'signal %s\n' "$*"
}
wait_shutdown_helper() {
	printf 'wait %s\n' "$1"
	if [[ $1 == "${shutdown_listener_pid}" && ${fail_listener} == true ]]; then
		return 23
	fi
}
finish_shutdown_pair decode-incomplete signal
`
	for _, test := range []struct {
		name         string
		failListener bool
		wantTrace    string
	}{
		{
			name:      "listener exits before initiator signal",
			wantTrace: "signal -TERM 2147483646\nwait 2147483646\nsignal -TERM 2147483647\nwait 2147483647\n",
		},
		{
			name:         "listener failure stops shutdown sequence",
			failListener: true,
			wantTrace:    "signal -TERM 2147483646\nwait 2147483646\n",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			state := t.TempDir()
			const events = "{\"event\":\"phase_ready\",\"phase\":\"decode-incomplete\"}\n" +
				"{\"event\":\"shutdown_trigger\",\"phase\":\"decode-incomplete\",\"trigger\":\"signal\"}\n" +
				"{\"event\":\"peer_close_complete\",\"phase\":\"decode-incomplete\"}\n"
			for _, role := range []string{"bob", "alice"} {
				if err := os.WriteFile(filepath.Join(state, role+".ndjson"), []byte(events), 0o600); err != nil {
					t.Fatal(err)
				}
			}
			command := exec.Command("bash", "-c", probe,
				filepath.Join(repository, "scripts", "integration", "run-case-worker"),
				filepath.Join(repository, "scripts", "integration", "run-case"), state,
				fmt.Sprint(test.failListener))
			output, runErr := command.CombinedOutput()
			if test.failListener {
				if failure, ok := runErr.(*exec.ExitError); !ok || failure.ExitCode() != 23 {
					t.Fatalf("listener failure was not propagated: %v\n%s", runErr, output)
				}
			} else if runErr != nil {
				t.Fatalf("finish shutdown pair: %v\n%s", runErr, output)
			}
			if string(output) != test.wantTrace {
				t.Fatalf("shutdown order = %q, want %q", output, test.wantTrace)
			}
			if !test.failListener {
				aggregated, err := os.ReadFile(filepath.Join(state, "events.ndjson"))
				if err != nil || string(aggregated) != events+events {
					t.Fatalf("shutdown events = %q, error %v", aggregated, err)
				}
			}
		})
	}
}
