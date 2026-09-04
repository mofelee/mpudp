package integration_test

import (
	"bufio"
	"crypto/sha256"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
)

func TestHarnessShellSyntaxAndPublicArguments(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("bash harness is Linux-only")
	}
	repository, err := filepath.Abs("..")
	if err != nil {
		t.Fatal(err)
	}
	scripts, err := filepath.Glob(filepath.Join(repository, "scripts", "integration", "*"))
	if err != nil {
		t.Fatal(err)
	}
	var shellScripts []string
	for _, script := range scripts {
		info, statErr := os.Stat(script)
		if statErr != nil {
			t.Fatal(statErr)
		}
		if !info.IsDir() {
			shellScripts = append(shellScripts, script)
		}
	}
	if len(shellScripts) == 0 {
		t.Fatal("no integration shell scripts found")
	}
	arguments := append([]string{"-n"}, shellScripts...)
	if output, err := exec.Command("bash", arguments...).CombinedOutput(); err != nil {
		t.Fatalf("bash -n failed: %v\n%s", err, output)
	}
	runCaseContents, err := os.ReadFile(filepath.Join(repository, "scripts", "integration", "run-case"))
	if err != nil {
		t.Fatal(err)
	}
	for _, unsafePipeline := range []string{"| grep -q", "| grep -Fq", "| grep -Eq"} {
		if strings.Contains(string(runCaseContents), unsafePipeline) {
			t.Fatalf("run-case contains %q; an early grep exit can SIGPIPE the live producer under pipefail", unsafePipeline)
		}
	}

	controlHelp := commandOutput(t, repository, "scripts/integration/control", "--help")
	if !strings.Contains(controlHelp, "--link-state up|down") || strings.Contains(controlHelp, "--state up|down") {
		t.Fatalf("control help has ambiguous link-state syntax:\n%s", controlHelp)
	}
	cases := commandOutput(t, repository, "scripts/integration/run-case", "--list")
	for _, name := range []string{
		"transparent-nat-v4", "transparent-nat-v6", "path-controls-v4", "peer-smoke-v4", "peer-smoke-v6",
		"peer-payload-mtu-v4", "peer-payload-mtu-v6", "peer-nat-rebinding-v4", "peer-nat-rebinding-v6",
		"peer-endpoint-expiry-v4", "peer-endpoint-expiry-v6",
	} {
		if !strings.Contains(cases, name) {
			t.Fatalf("case manifest output is missing %q:\n%s", name, cases)
		}
	}

	parsedFailure := commandFailureOutput(t, repository, "scripts/integration/control",
		"--state", "/does/not/exist/mpudp-it-state", "link", "--path", "1", "--family", "4", "--link-state", "up")
	if strings.Contains(parsedFailure, "unknown control argument") {
		t.Fatalf("--link-state was not parsed:\n%s", parsedFailure)
	}
	legacyFailure := commandFailureOutput(t, repository, "scripts/integration/control", "--state-value", "up")
	if !strings.Contains(legacyFailure, "unknown control argument") {
		t.Fatalf("ambiguous legacy argument unexpectedly accepted:\n%s", legacyFailure)
	}
	invalidDirection := commandFailureOutput(t, repository, "scripts/integration/control",
		"--state", "/does/not/exist/mpudp-it-state", "netem", "--path", "1", "--family", "4",
		"--direction", "sideways", "--delay", "1ms")
	if !strings.Contains(invalidDirection, "direction must be forward, reverse, or both") {
		t.Fatalf("invalid netem direction was not rejected before mutation:\n%s", invalidDirection)
	}
	invalidRunID := commandFailureOutput(t, repository, "scripts/integration/teardown", "--run-id", "../../outside")
	if !strings.Contains(invalidRunID, "invalid run ID") {
		t.Fatalf("teardown did not reject a path-like run ID before resolution:\n%s", invalidRunID)
	}
}

func TestInheritedCaseChildMarkerDoesNotBypassTimeoutWrapper(t *testing.T) {
	if runtime.GOOS != "linux" || os.Geteuid() != 0 {
		t.Skip("run-case timeout wrapper probe requires Linux root")
	}
	repository, err := filepath.Abs("..")
	if err != nil {
		t.Fatal(err)
	}
	state := t.TempDir()
	runID := "timeout-wrapper-unit"
	token := fmt.Sprintf("%x", sha256.Sum256([]byte(runID)))[:8]
	files := map[string]string{
		".mpudp-integration-state": "mpudp integration state v1\n",
		"run-id":                   runID + "\n", "token": token + "\n",
		"owner-token": strings.Repeat("a", 32) + "\n", "ready": "ready\n",
	}
	for name, contents := range files {
		if err := os.WriteFile(filepath.Join(state, name), []byte(contents), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	fakeBin := filepath.Join(state, "fake-bin")
	if err := os.Mkdir(fakeBin, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(filepath.Join(state, "bin"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(state, "bin", "netprobe"), []byte("#!/usr/bin/env bash\nexit 0\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	timeoutProbe := filepath.Join(state, "timeout-worker-entered")
	fakeIP := filepath.Join(fakeBin, "ip")
	if err := os.WriteFile(fakeIP, []byte("#!/usr/bin/env bash\n: >\"${MPUDP_IT_TIMEOUT_PROBE:?}\"\nexec sleep 6\n"), 0o700); err != nil {
		t.Fatal(err)
	}

	command := exec.Command(filepath.Join(repository, "scripts/integration/run-case"),
		"--state", state, "--timeout", "1", "transparent-nat-v4")
	command.Dir = repository
	for _, variable := range os.Environ() {
		if !strings.HasPrefix(variable, "PATH=") && !strings.HasPrefix(variable, "MPUDP_IT_CASE_CHILD=") &&
			!strings.HasPrefix(variable, "MPUDP_IT_CASE_TOKEN=") &&
			!strings.HasPrefix(variable, "MPUDP_IT_TIMEOUT_PROBE=") {
			command.Env = append(command.Env, variable)
		}
	}
	command.Env = append(command.Env, "PATH="+fakeBin+":"+os.Getenv("PATH"), "MPUDP_IT_CASE_CHILD=1",
		"MPUDP_IT_TIMEOUT_PROBE="+timeoutProbe)
	started := time.Now()
	output, runErr := command.CombinedOutput()
	elapsed := time.Since(started)
	exitErr, ok := runErr.(*exec.ExitError)
	if !ok || exitErr.ExitCode() != 124 || elapsed < 750*time.Millisecond || elapsed >= 4*time.Second {
		t.Fatalf("inherited child marker bypassed real timeout: elapsed=%s err=%v output=%s", elapsed, runErr, output)
	}
	if _, err := os.Stat(timeoutProbe); err != nil {
		t.Fatalf("timeout worker did not invoke the blocking ip probe: %v", err)
	}

	privateToken := strings.Repeat("b", 32)
	forged := exec.Command(filepath.Join(repository, "scripts/integration/run-case"),
		"--state", state, "--timeout", "1", "--internal-child-token", privateToken, "path-controls-v4")
	forged.Dir = repository
	for _, variable := range os.Environ() {
		if !strings.HasPrefix(variable, "MPUDP_IT_CASE_TOKEN=") {
			forged.Env = append(forged.Env, variable)
		}
	}
	forged.Env = append(forged.Env, "MPUDP_IT_CASE_TOKEN="+privateToken)
	started = time.Now()
	forgedOutput, forgedErr := forged.CombinedOutput()
	if forgedErr == nil || time.Since(started) >= time.Second ||
		!strings.Contains(string(forgedOutput), "unknown run-case argument") {
		t.Fatalf("caller-controlled internal token was accepted: elapsed=%s err=%v output=%s",
			time.Since(started), forgedErr, forgedOutput)
	}
}

func TestRunRejectsArtifactStateOverlapBeforeSetup(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("integration runner is Linux-only")
	}
	repository, err := filepath.Abs("..")
	if err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	realParent := filepath.Join(root, "real")
	if err := os.Mkdir(realParent, 0o700); err != nil {
		t.Fatal(err)
	}
	aliasParent := filepath.Join(root, "alias")
	if err := os.Symlink(realParent, aliasParent); err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name      string
		runID     string
		state     string
		artifacts string
	}{
		{
			name:      "equal through symlink",
			runID:     "artifact-overlap-equal",
			state:     filepath.Join(aliasParent, "equal-state"),
			artifacts: filepath.Join(realParent, "equal-state", "."),
		},
		{
			name:      "descendant with lexical parent",
			runID:     "artifact-overlap-child",
			state:     filepath.Join(aliasParent, "child-state"),
			artifacts: filepath.Join(realParent, "child-state", "missing", "..", "artifacts"),
		},
		{
			name:      "diagnostic target contains state",
			runID:     "artifact-overlap-parent",
			state:     filepath.Join(aliasParent, "artifact-overlap-parent", "state"),
			artifacts: realParent,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			output := commandFailureOutput(t, repository, "scripts/integration/run",
				"--run-id", test.runID, "--state", test.state, "--artifacts", test.artifacts)
			if !strings.Contains(output, "artifact output overlaps run state") {
				t.Fatalf("overlap rejection was not reported:\n%s", output)
			}
			canonicalState := filepath.Join(realParent, strings.TrimPrefix(test.state, aliasParent+string(os.PathSeparator)))
			if _, err := os.Lstat(canonicalState); !os.IsNotExist(err) {
				t.Fatalf("runner mutated rejected state path: %v", err)
			}
		})
	}
}

func TestCaseWorkerExitStopsOnlyRecordedOwnedHelpers(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("case helper ownership uses Linux /proc")
	}
	repository, err := filepath.Abs("..")
	if err != nil {
		t.Fatal(err)
	}
	state := t.TempDir()
	runID := "case-cleanup-unit"
	token := fmt.Sprintf("%x", sha256.Sum256([]byte(runID)))[:8]
	files := map[string]string{
		".mpudp-integration-state": "mpudp integration state v1\n",
		"run-id":                   runID + "\n",
		"token":                    token + "\n",
		"owner-token":              strings.Repeat("a", 32) + "\n",
	}
	for name, contents := range files {
		if err := os.WriteFile(filepath.Join(state, name), []byte(contents), 0o600); err != nil {
			t.Fatal(err)
		}
	}

	helper := exec.Command("bash", "-c", `
child=
trap 'if [[ -n $child ]]; then kill "$child" 2>/dev/null || true; fi; exit 0' TERM
while :; do sleep 10 & child=$!; wait "$child" || true; done
`, "/test-only/capture-udp", "--run-id", runID)
	if err := helper.Start(); err != nil {
		t.Fatal(err)
	}
	helperWaited := false
	t.Cleanup(func() {
		if helperWaited {
			return
		}
		_ = helper.Process.Signal(syscall.SIGKILL)
		_ = helper.Wait()
	})

	startCommand := exec.Command("bash", "-c", `source "$1"; printf '%s ' "$2"; mpudp_it_process_start_time "$2"`, "start-time",
		filepath.Join(repository, "scripts", "integration", "lib.sh"), strconv.Itoa(helper.Process.Pid))
	startOutput, err := startCommand.CombinedOutput()
	if err != nil {
		t.Fatalf("read helper start time: %v\n%s", err, startOutput)
	}
	if err := os.WriteFile(filepath.Join(state, "pids"), startOutput, 0o600); err != nil {
		t.Fatal(err)
	}

	probe := `source "$1"; mpudp_it_load_state "$2"; trap cleanup_case_worker EXIT; exit 17`
	command := exec.Command("bash", "-c", probe, filepath.Join(repository, "scripts", "integration", "run-case-worker"),
		filepath.Join(repository, "scripts", "integration", "run-case"), state)
	output, runErr := command.CombinedOutput()
	exitErr, ok := runErr.(*exec.ExitError)
	if !ok || exitErr.ExitCode() != 17 {
		t.Fatalf("worker cleanup did not preserve failure status: err=%v output=%s", runErr, output)
	}

	done := make(chan error, 1)
	go func() { done <- helper.Wait() }()
	select {
	case err := <-done:
		helperWaited = true
		if err != nil {
			t.Fatalf("recorded helper exit: %v", err)
		}
	case <-time.After(2 * time.Second):
		_ = helper.Process.Signal(syscall.SIGKILL)
		_ = <-done
		helperWaited = true
		t.Fatal("recorded owned helper remained alive after worker exit")
	}
}

func TestStateOutputPathContainment(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("bash harness is Linux-only")
	}
	repository, err := filepath.Abs("..")
	if err != nil {
		t.Fatal(err)
	}
	state := t.TempDir()
	direct := filepath.Join(state, "capture.log")
	escaped := filepath.Join(state, "..", "outside.log")
	checker := `
source "$1"
MPUDP_IT_STATE_DIR=$(CDPATH= cd -- "$2" && pwd -P)
mpudp_it_assert_state_child_path "$3"
`
	for name, test := range map[string]struct {
		path    string
		wantErr bool
	}{
		"direct child":     {path: direct},
		"parent traversal": {path: escaped, wantErr: true},
		"nested child":     {path: filepath.Join(state, "nested", "capture.log"), wantErr: true},
	} {
		t.Run(name, func(t *testing.T) {
			command := exec.Command("bash", "-c", checker, "check",
				filepath.Join(repository, "scripts", "integration", "lib.sh"), state, test.path)
			output, runErr := command.CombinedOutput()
			if test.wantErr && runErr == nil {
				t.Fatalf("escaped output unexpectedly accepted: %s", output)
			}
			if !test.wantErr && runErr != nil {
				t.Fatalf("direct output rejected: %v\n%s", runErr, output)
			}
		})
	}
}

func TestTeardownRunIDResolvesDefaultState(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("bash harness is Linux-only")
	}
	if os.Geteuid() != 0 {
		t.Skip("teardown ownership test requires root")
	}
	repository, err := filepath.Abs("..")
	if err != nil {
		t.Fatal(err)
	}
	runID := fmt.Sprintf("teardown-unit-%d", os.Getpid())
	state := filepath.Join("/tmp", "mpudp-it-"+runID)
	if err := os.Mkdir(state, 0o700); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(state) })
	token := fmt.Sprintf("%x", sha256.Sum256([]byte(runID)))[:8]
	files := map[string]string{
		".mpudp-integration-state": "mpudp integration state v1\n",
		"run-id":                   runID + "\n", "token": token + "\n",
		"owner-token": strings.Repeat("a", 32) + "\n",
		"namespaces":  "", "host-links": "", "pids": "", "events.ndjson": "",
	}
	for name, contents := range files {
		if err := os.WriteFile(filepath.Join(state, name), []byte(contents), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	commandOutput(t, repository, "scripts/integration/teardown", "--run-id", runID)
	if _, err := os.Stat(state); !os.IsNotExist(err) {
		t.Fatalf("default state remains after --run-id teardown: %v", err)
	}
}

func TestCaseManifestIsBoundedAndUnique(t *testing.T) {
	manifest, err := os.Open(filepath.Join("scenarios", "cases.tsv"))
	if err != nil {
		t.Fatal(err)
	}
	defer manifest.Close()

	seen := make(map[string]bool)
	scanner := bufio.NewScanner(manifest)
	for scanner.Scan() {
		line := scanner.Text()
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		fields := strings.Split(line, "\t")
		if len(fields) != 5 {
			t.Fatalf("manifest row has %d fields, want 5: %q", len(fields), line)
		}
		if seen[fields[0]] {
			t.Fatalf("duplicate case %q", fields[0])
		}
		seen[fields[0]] = true
		timeout, parseErr := strconv.Atoi(fields[3])
		if parseErr != nil || timeout < 1 || timeout > 120 {
			t.Fatalf("case %q timeout = %q", fields[0], fields[3])
		}
		if fields[4] != "true" && fields[4] != "false" {
			t.Fatalf("case %q runtime flag = %q", fields[0], fields[4])
		}
	}
	if err := scanner.Err(); err != nil {
		t.Fatal(err)
	}
	if len(seen) < 4 {
		t.Fatalf("manifest has only %d cases", len(seen))
	}
}

func TestProcessRunIDMatchingIsExact(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("process ownership uses Linux /proc")
	}
	repository, err := filepath.Abs("..")
	if err != nil {
		t.Fatal(err)
	}
	checker := `
source "$1"
MPUDP_IT_RUN_ID=$2
if mpudp_it_process_matches_run "$4"; then exit 10; fi
MPUDP_IT_RUN_ID=$3
mpudp_it_process_matches_run "$4"
`
	for _, helper := range []string{"netprobe", "peerprobe", "capture-fragments", "capture-udp"} {
		t.Run(helper, func(t *testing.T) {
			const exactRunID = "argv-exact-long"
			decoy := exec.Command("bash", "-c", `trap 'exit 0' TERM; while :; do sleep 1 & wait $!; done`,
				"/test-only/"+helper, "--run-id", exactRunID)
			if err := decoy.Start(); err != nil {
				t.Fatal(err)
			}
			defer func() {
				_ = decoy.Process.Signal(syscall.SIGTERM)
				_ = decoy.Wait()
			}()

			output, err := exec.Command("bash", "-c", checker, "check",
				filepath.Join(repository, "scripts", "integration", "lib.sh"),
				"argv-exact", exactRunID, strconv.Itoa(decoy.Process.Pid)).CombinedOutput()
			if err != nil {
				t.Fatalf("exact argv run-ID matching failed: %v\n%s", err, output)
			}
		})
	}
}

func TestSetupRecordsOnlySuccessfulKernelMutation(t *testing.T) {
	repository, err := filepath.Abs("..")
	if err != nil {
		t.Fatal(err)
	}
	contents, err := os.ReadFile(filepath.Join(repository, "scripts", "integration", "setup"))
	if err != nil {
		t.Fatal(err)
	}
	setup := string(contents)
	assertOrdered := func(first, second string) {
		t.Helper()
		firstIndex := strings.Index(setup, first)
		secondIndex := strings.Index(setup, second)
		if firstIndex < 0 || secondIndex < 0 || firstIndex >= secondIndex {
			t.Fatalf("setup transaction order %q before %q was not preserved", first, second)
		}
	}
	assertOrdered(`mpudp_it_run_signal_shielded ip netns add "${namespace}"`, `mpudp_it_record_namespace "${namespace}"`)
	assertOrdered(`mpudp_it_run_signal_shielded ip link add "${host_left}"`, `mpudp_it_record_host_link "${host_left}"`)
}

func TestSignalShieldedMutationCompletesBeforePendingSignal(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("bash harness is Linux-only")
	}
	repository, err := filepath.Abs("..")
	if err != nil {
		t.Fatal(err)
	}
	marker := filepath.Join(t.TempDir(), "created")
	probe := `
source "$1"
parent=$BASHPID
pending=0
trap 'pending=1' TERM
(sleep 0.05; kill -TERM "$parent") &
sender=$!
mpudp_it_run_signal_shielded bash -c 'touch "$1"; sleep 0.2' mutation "$2"
status=$?
wait "$sender"
[[ $status == 0 && $pending == 1 && -f $2 ]]
`
	output, err := exec.Command("bash", "-c", probe, "probe",
		filepath.Join(repository, "scripts", "integration", "lib.sh"), marker).CombinedOutput()
	if err != nil {
		t.Fatalf("signal-shielded mutation probe failed: %v\n%s", err, output)
	}
}

func TestNoStateTeardownPreservesForeignResources(t *testing.T) {
	if runtime.GOOS != "linux" || os.Geteuid() != 0 {
		t.Skip("foreign-resource preservation probe requires Linux root")
	}
	if _, err := exec.LookPath("ip"); err != nil {
		t.Skip("ip command is unavailable")
	}
	repository, err := filepath.Abs("..")
	if err != nil {
		t.Fatal(err)
	}
	runID := fmt.Sprintf("foreign-%d-%d", os.Getpid(), time.Now().UnixNano()%1_000_000)
	token := fmt.Sprintf("%x", sha256.Sum256([]byte(runID)))[:8]
	namespace := "mpudp-it-" + runID + "-alice"
	left := "mi" + token + "fa"
	right := "mi" + token + "fb"
	state := filepath.Join("/tmp", "mpudp-it-"+runID)
	if _, err := os.Lstat(state); !os.IsNotExist(err) {
		t.Fatalf("test state path is not absent: %v", err)
	}
	if output, err := exec.Command("ip", "netns", "add", namespace).CombinedOutput(); err != nil {
		t.Fatalf("create foreign namespace: %v\n%s", err, output)
	}
	namespaceLive := true
	defer func() {
		if namespaceLive {
			_ = exec.Command("ip", "netns", "del", namespace).Run()
		}
	}()
	if output, err := exec.Command("ip", "link", "add", left, "type", "veth", "peer", "name", right).CombinedOutput(); err != nil {
		t.Fatalf("create foreign veth: %v\n%s", err, output)
	}
	linkLive := true
	defer func() {
		if linkLive {
			_ = exec.Command("ip", "link", "del", "dev", left).Run()
		}
	}()

	commandOutput(t, repository, "scripts/integration/teardown", "--run-id", runID)
	if err := exec.Command("ip", "netns", "exec", namespace, "true").Run(); err != nil {
		t.Fatal("no-state teardown deleted the foreign namespace")
	}
	if err := exec.Command("ip", "link", "show", "dev", left).Run(); err != nil {
		t.Fatal("no-state teardown deleted the foreign veth")
	}
	if err := exec.Command("ip", "link", "del", "dev", left).Run(); err != nil {
		t.Fatal(err)
	}
	linkLive = false
	if err := exec.Command("ip", "netns", "del", namespace).Run(); err != nil {
		t.Fatal(err)
	}
	namespaceLive = false
}

func commandOutput(t *testing.T, directory, name string, arguments ...string) string {
	t.Helper()
	command := exec.Command(filepath.Join(directory, name), arguments...)
	command.Dir = directory
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("%s %v failed: %v\n%s", name, arguments, err, output)
	}
	return string(output)
}

func commandFailureOutput(t *testing.T, directory, name string, arguments ...string) string {
	t.Helper()
	command := exec.Command(filepath.Join(directory, name), arguments...)
	command.Dir = directory
	output, err := command.CombinedOutput()
	if err == nil {
		t.Fatalf("%s %v unexpectedly succeeded:\n%s", name, arguments, output)
	}
	return string(output)
}
