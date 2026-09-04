package integration_test

import (
	"bufio"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

type ciWorkflow struct {
	On          map[string]ciWorkflowTrigger `yaml:"on"`
	Permissions map[string]string            `yaml:"permissions"`
	Concurrency struct {
		Group            string `yaml:"group"`
		CancelInProgress bool   `yaml:"cancel-in-progress"`
	} `yaml:"concurrency"`
	Jobs map[string]struct {
		Name     string           `yaml:"name"`
		Steps    []ciWorkflowStep `yaml:"steps"`
		Strategy struct {
			FailFast bool `yaml:"fail-fast"`
			Matrix   struct {
				Cases []string `yaml:"case"`
			} `yaml:"matrix"`
		} `yaml:"strategy"`
	} `yaml:"jobs"`
}

type ciWorkflowTrigger struct {
	Branches []string `yaml:"branches"`
}

type ciWorkflowStep struct {
	If  string `yaml:"if"`
	Run string `yaml:"run"`
}

type canonicalCaseContract struct {
	Name    string
	Runner  string
	Family  string
	Timeout int
}

var canonicalCaseContracts = []canonicalCaseContract{
	{Name: "direct-single-carrier", Runner: "direct-single-carrier", Family: "4", Timeout: 30},
	{Name: "rs53-five-carrier-loss", Runner: "rs-scenario", Family: "protocol", Timeout: 20},
	{Name: "rs53-two-carrier-rotation", Runner: "rs-scenario", Family: "protocol", Timeout: 20},
	{Name: "slow-path-early-recovery", Runner: "rs-scenario", Family: "protocol", Timeout: 20},
	{Name: "transparent-nat-reverse-path", Runner: "transparent-nat-reverse-path", Family: "4", Timeout: 35},
	{Name: "endpoint-rebinding-and-expiry", Runner: "endpoint-rebinding-and-expiry", Family: "4", Timeout: 40},
	{Name: "auth-and-state-pollution", Runner: "peer-auth-pollution", Family: "4", Timeout: 45},
	{Name: "mtu-budget-no-fragment", Runner: "mtu-budget-no-fragment", Family: "dual", Timeout: 70},
	{Name: "shutdown-cleanup", Runner: "peer-shutdown-cleanup", Family: "4", Timeout: 90},
}

func TestCIWorkflowSecurityAndCleanupContract(t *testing.T) {
	repository, err := filepath.Abs("..")
	if err != nil {
		t.Fatal(err)
	}
	contents, err := os.ReadFile(filepath.Join(repository, ".github", "workflows", "ci.yml"))
	if err != nil {
		t.Fatal(err)
	}
	var workflow ciWorkflow
	if err := yaml.Unmarshal(contents, &workflow); err != nil {
		t.Fatalf("invalid CI workflow YAML: %v", err)
	}

	for _, trigger := range []string{"pull_request", "push", "workflow_dispatch"} {
		if _, ok := workflow.On[trigger]; !ok {
			t.Errorf("CI workflow is missing %q trigger", trigger)
		}
	}
	if len(workflow.On) != 3 {
		t.Errorf("CI workflow has unexpected triggers: %v", workflow.On)
	}
	if !reflect.DeepEqual(workflow.On["push"].Branches, []string{"main"}) {
		t.Errorf("CI push branches = %v, want only main", workflow.On["push"].Branches)
	}
	if !reflect.DeepEqual(workflow.Permissions, map[string]string{"contents": "read"}) {
		t.Errorf("CI workflow permissions = %v, want only contents: read", workflow.Permissions)
	}
	if !workflow.Concurrency.CancelInProgress ||
		!strings.Contains(workflow.Concurrency.Group, "github.event.pull_request.number") ||
		!strings.Contains(workflow.Concurrency.Group, "github.ref") {
		t.Errorf("CI concurrency does not cancel prior runs for the same PR/ref: %+v", workflow.Concurrency)
	}

	wantJobs := map[string]string{
		"build-unit":  "build-unit",
		"race":        "race",
		"integration": "integration / ${{ matrix.case }}",
	}
	if len(workflow.Jobs) != len(wantJobs) {
		t.Fatalf("CI jobs = %v, want exactly build-unit, race, and integration", workflow.Jobs)
	}
	for id, name := range wantJobs {
		job, ok := workflow.Jobs[id]
		if !ok || job.Name != name {
			t.Errorf("job %q name = %q, want %q", id, job.Name, name)
		}
	}

	wantCases := make([]string, 0, len(canonicalCaseContracts))
	for _, contract := range canonicalCaseContracts {
		wantCases = append(wantCases, contract.Name)
	}
	integrationJob := workflow.Jobs["integration"]
	if integrationJob.Strategy.FailFast {
		t.Error("integration matrix must keep fail-fast disabled")
	}
	if !reflect.DeepEqual(integrationJob.Strategy.Matrix.Cases, wantCases) {
		t.Errorf("integration cases = %v, want %v", integrationJob.Strategy.Matrix.Cases, wantCases)
	}
	alwaysCleanup := map[string]bool{
		"scripts/integration/teardown": false,
		"scripts/integration/audit":    false,
	}
	for _, step := range integrationJob.Steps {
		for command := range alwaysCleanup {
			if strings.Contains(step.Run, command) && step.If == "${{ always() }}" {
				alwaysCleanup[command] = true
			}
		}
	}
	for command, found := range alwaysCleanup {
		if !found {
			t.Errorf("integration %s step is not guarded by exact always() cleanup", command)
		}
	}

	text := string(contents)
	usesPattern := regexp.MustCompile(`(?m)^\s*uses:\s*[^\s]+@([^\s#]+)`)
	uses := usesPattern.FindAllStringSubmatch(text, -1)
	if len(uses) == 0 {
		t.Fatal("CI workflow contains no actions")
	}
	immutableSHA := regexp.MustCompile(`^[0-9a-f]{40}$`)
	for _, match := range uses {
		if !immutableSHA.MatchString(match[1]) {
			t.Errorf("action ref %q is not an immutable commit SHA", match[1])
		}
	}

	requiredFragments := []string{
		"actions/checkout@3d3c42e5aac5ba805825da76410c181273ba90b1 # v7.0.1",
		"actions/setup-go@b7ad1dad31e06c5925ef5d2fc7ad053ef454303e # v7.0.0",
		"actions/upload-artifact@043fb46d1a93c77aae656e7c1c64a875d1fc6a0a # v7.0.1",
		"persist-credentials: false",
		"GOFLAGS=-buildvcs=false",
		"conntrack diffutils iproute2 iputils-ping nftables procps tcpdump",
		"MPUDP_IT_REQUIRE_CONNTRACK=1",
		"MPUDP_IT_SEED=${MPUDP_IT_SEED}",
		"--run-id \"${MPUDP_IT_RUN_ID}\"",
		"--expect-clean",
		"--no-same-owner",
		"retention-days: 3",
	}
	for _, fragment := range requiredFragments {
		if !strings.Contains(text, fragment) {
			t.Errorf("CI workflow is missing required fragment %q", fragment)
		}
	}
	for _, forbidden := range []string{"pull_request_target", "secrets.", "ssh ", "virsh", "hypervisor"} {
		if strings.Contains(strings.ToLower(text), forbidden) {
			t.Errorf("CI workflow contains forbidden privileged/external input %q", forbidden)
		}
	}
}

func TestCICanonicalCasesMatchRunnableManifest(t *testing.T) {
	repository, err := filepath.Abs("..")
	if err != nil {
		t.Fatal(err)
	}
	manifest, err := os.Open(filepath.Join(repository, "integration", "scenarios", "cases.tsv"))
	if err != nil {
		t.Fatal(err)
	}
	defer manifest.Close()

	type manifestCase struct {
		runner          string
		family          string
		timeout         int
		requiresRuntime string
	}
	rows := make(map[string]manifestCase)
	scanner := bufio.NewScanner(manifest)
	for lineNumber := 1; scanner.Scan(); lineNumber++ {
		line := scanner.Text()
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		fields := strings.Split(line, "\t")
		if len(fields) != 5 {
			t.Fatalf("manifest line %d has %d fields, want 5", lineNumber, len(fields))
		}
		timeout, parseErr := strconv.Atoi(fields[3])
		if parseErr != nil || timeout < 1 || timeout > 120 {
			t.Fatalf("manifest line %d has invalid timeout %q", lineNumber, fields[3])
		}
		if _, duplicate := rows[fields[0]]; duplicate {
			t.Fatalf("manifest case %q is duplicated", fields[0])
		}
		rows[fields[0]] = manifestCase{
			runner: fields[1], family: fields[2], timeout: timeout, requiresRuntime: fields[4],
		}
	}
	if err := scanner.Err(); err != nil {
		t.Fatal(err)
	}

	runnerContents, err := os.ReadFile(filepath.Join(repository, "scripts", "integration", "run-case"))
	if err != nil {
		t.Fatal(err)
	}
	runnerText := string(runnerContents)
	foundationRunners := map[string]bool{
		"raw-nat-smoke": true, "path-controls": true, "nat-rebinding-trigger": true,
		"peer-smoke": true, "peer-payload-mtu": true, "peer-nat-rebinding": true,
		"peer-endpoint-expiry": true,
	}
	seenRunners := make(map[string]bool)
	for _, contract := range canonicalCaseContracts {
		row, ok := rows[contract.Name]
		if !ok {
			t.Errorf("canonical CI case %q is absent from the manifest", contract.Name)
			continue
		}
		if row.runner != contract.Runner || row.family != contract.Family || row.timeout != contract.Timeout {
			t.Errorf("canonical case %q manifest = runner %q, family %q, timeout %d; want %q, %q, %d",
				contract.Name, row.runner, row.family, row.timeout,
				contract.Runner, contract.Family, contract.Timeout)
		}
		if row.requiresRuntime != "false" {
			t.Errorf("canonical CI case %q is not runnable: requires_peer_runtime=%q", contract.Name, row.requiresRuntime)
		}
		if foundationRunners[row.runner] {
			t.Errorf("canonical CI case %q silently aliases foundation runner %q", contract.Name, row.runner)
		}
		seenRunners[row.runner] = true
	}
	for runner := range seenRunners {
		dispatch := runner + `) `
		if strings.Count(runnerText, dispatch) != 1 {
			t.Errorf("canonical runner %q dispatch count = %d, want 1", runner, strings.Count(runnerText, dispatch))
		}
	}
}
