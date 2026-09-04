package integration_test

import (
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

func TestRequirementsTraceabilityContract(t *testing.T) {
	repository := documentationRepository(t)
	requirements := readDocumentationFile(t, repository, "docs/MPUDP_REQUIREMENTS.md")
	traceability := readDocumentationFile(t, repository, "docs/TRACEABILITY.md")

	headingPattern := regexp.MustCompile(`^## ([0-9]+)\. (.+)$`)
	var headings []string
	for _, line := range markdownLinesOutsideFences(requirements) {
		match := headingPattern.FindStringSubmatch(line)
		if match == nil {
			continue
		}
		number, err := strconv.Atoi(match[1])
		if err != nil {
			t.Fatalf("invalid requirement number %q: %v", match[1], err)
		}
		if number != len(headings)+1 {
			t.Fatalf("requirement heading %q has number %d, want %d", line, number, len(headings)+1)
		}
		headings = append(headings, match[2])
	}
	if len(headings) != 25 {
		t.Fatalf("numbered requirement headings = %d, want 25", len(headings))
	}

	rows := parseTraceabilityRows(t, traceability)
	if len(rows) != len(headings) {
		t.Fatalf("traceability rows = %d, want %d", len(rows), len(headings))
	}
	allowedStatus := map[string]bool{"implemented": true, "verified": true, "deferred": true}
	declaredTests := repositoryTestDeclarations(t, repository)
	testReferencePattern := regexp.MustCompile("`((?:Test|Fuzz)[A-Za-z0-9_]+)`")
	issueReferencePattern := regexp.MustCompile(`#[0-9]+`)
	for index, row := range rows {
		wantSection := strconv.Itoa(index + 1)
		if row["Section"] != wantSection {
			t.Errorf("traceability row %d section = %q, want %q", index+1, row["Section"], wantSection)
		}
		if row["Requirement"] != headings[index] {
			t.Errorf("traceability section %s requirement = %q, want %q", wantSection, row["Requirement"], headings[index])
		}
		for _, field := range []string{"Implementation", "Verification", "Tracking"} {
			if strings.TrimSpace(row[field]) == "" {
				t.Errorf("traceability section %s has empty %s", wantSection, field)
			}
		}
		if !allowedStatus[row["Status"]] {
			t.Errorf("traceability section %s has invalid status %q", wantSection, row["Status"])
		}
		if !issueReferencePattern.MatchString(row["Tracking"]) {
			t.Errorf("traceability section %s has no issue reference in Tracking", wantSection)
		}
		references := testReferencePattern.FindAllStringSubmatch(row["Verification"], -1)
		if row["Status"] != "deferred" && len(references) == 0 {
			t.Errorf("%s traceability section %s names no Test or Fuzz declaration", row["Status"], wantSection)
		}
		for _, reference := range references {
			if !declaredTests[reference[1]] {
				t.Errorf("traceability section %s cites unknown test %q", wantSection, reference[1])
			}
		}
	}
	for _, section := range append(integerRange(1, 22), 25) {
		if got := rows[section-1]["Status"]; got != "verified" {
			t.Errorf("traceability section %d status = %q, want verified", section, got)
		}
	}
	if row := rows[22]; row["Status"] != "deferred" || !strings.Contains(row["Tracking"], "#13") || !strings.Contains(row["Tracking"], "#14") {
		t.Errorf("traceability section 23 must be deferred to both #13 and #14: %+v", row)
	}
	if got := rows[23]["Status"]; got != "implemented" {
		t.Errorf("traceability section 24 status = %q, want implemented for event-based delivery evidence", got)
	}
}

func TestDocumentedCIChecksAndCanonicalCasesMatchWorkflow(t *testing.T) {
	repository := documentationRepository(t)
	workflowContents := readDocumentationFile(t, repository, ".github/workflows/ci.yml")
	var workflow ciWorkflow
	if err := yaml.Unmarshal([]byte(workflowContents), &workflow); err != nil {
		t.Fatalf("invalid CI workflow YAML: %v", err)
	}

	build, buildOK := workflow.Jobs["build-unit"]
	race, raceOK := workflow.Jobs["race"]
	integration, integrationOK := workflow.Jobs["integration"]
	if !buildOK || !raceOK || !integrationOK {
		t.Fatalf("CI jobs must include build-unit, race, and integration")
	}
	wantChecks := []string{build.Name, race.Name}
	for _, caseName := range integration.Strategy.Matrix.Cases {
		wantChecks = append(wantChecks, strings.ReplaceAll(integration.Name, "${{ matrix.case }}", caseName))
	}
	readme := readDocumentationFile(t, repository, "README.md")
	gotChecks := parseCodeBulletInventory(t, readme, "mpudp-ci-checks")
	if !reflect.DeepEqual(gotChecks, wantChecks) {
		t.Errorf("README CI checks = %v, want workflow-derived %v", gotChecks, wantChecks)
	}

	wantCases := make([]string, 0, len(canonicalCaseContracts))
	for _, contract := range canonicalCaseContracts {
		wantCases = append(wantCases, contract.Name)
	}
	if !reflect.DeepEqual(integration.Strategy.Matrix.Cases, wantCases) {
		t.Fatalf("workflow integration cases = %v, want canonical contract %v", integration.Strategy.Matrix.Cases, wantCases)
	}
	integrationDoc := readDocumentationFile(t, repository, "docs/INTEGRATION.md")
	gotCases := parseCodeBulletInventory(t, integrationDoc, "mpudp-canonical-cases")
	if !reflect.DeepEqual(gotCases, wantCases) {
		t.Errorf("integration documentation cases = %v, want workflow-derived %v", gotCases, wantCases)
	}
}

func TestDocumentationIndexAndHygiene(t *testing.T) {
	repository := documentationRepository(t)
	readme := readDocumentationFile(t, repository, "README.md")
	requiredDocs := []string{
		"docs/API.md",
		"docs/CONFIGURATION.md",
		"docs/DEPENDENCIES.md",
		"docs/FEC.md",
		"docs/INTEGRATION.md",
		"docs/MPUDP_CONFIG_EXAMPLE.md",
		"docs/MPUDP_REQUIREMENTS.md",
		"docs/SESSION.md",
		"docs/TRACEABILITY.md",
		"docs/TRANSPORT.md",
		"docs/WIRE_PROTOCOL.md",
	}
	for _, path := range requiredDocs {
		if !strings.Contains(readme, "]("+path+")") {
			t.Errorf("README documentation index is missing %s", path)
		}
		if _, err := os.Stat(filepath.Join(repository, filepath.FromSlash(path))); err != nil {
			t.Errorf("indexed documentation %s is not readable: %v", path, err)
		}
	}

	published := append([]string{"README.md"}, requiredDocs...)
	for _, path := range published {
		contents := readDocumentationFile(t, repository, path)
		for _, stale := range []string{
			"Loop 1",
			"本 loop",
			"由 #7 集成",
			"harness for issue #8",
			"final full #8 gate",
			"intended for #9",
			"Issue #9 adds",
			`psk: "secret"`,
			`psk: "replace-with-a-secret"`,
		} {
			if strings.Contains(contents, stale) {
				t.Errorf("%s contains stale or unsafe documentation fragment %q", path, stale)
			}
		}
	}

	for _, required := range []string{
		"DATA ACK/NACK",
		"动态 FEC",
		"加权调度",
		"STUN/ICE/TURN",
		"自有 Relay",
		"Mesh",
		"不加密 Payload",
		"T1-T5 的 nftables 转发",
		"eBPF",
		"XDP",
		"DPDK",
		"issues/13",
		"issues/14",
	} {
		if !strings.Contains(readme, required) {
			t.Errorf("README v0.1 boundary is missing %q", required)
		}
	}
}

func documentationRepository(t *testing.T) string {
	t.Helper()
	repository, err := filepath.Abs("..")
	if err != nil {
		t.Fatal(err)
	}
	return repository
}

func readDocumentationFile(t *testing.T, repository, path string) string {
	t.Helper()
	contents, err := os.ReadFile(filepath.Join(repository, filepath.FromSlash(path)))
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	return string(contents)
}

func repositoryTestDeclarations(t *testing.T, repository string) map[string]bool {
	t.Helper()
	declarationPattern := regexp.MustCompile(`(?m)^func ((?:Test|Fuzz)[A-Za-z0-9_]+)\(`)
	declarations := make(map[string]bool)
	err := filepath.WalkDir(repository, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), "_test.go") {
			return nil
		}
		contents, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		for _, match := range declarationPattern.FindAllStringSubmatch(string(contents), -1) {
			declarations[match[1]] = true
		}
		return nil
	})
	if err != nil {
		t.Fatalf("enumerate repository tests: %v", err)
	}
	return declarations
}

func markdownLinesOutsideFences(contents string) []string {
	var lines []string
	inFence := false
	for _, line := range strings.Split(strings.ReplaceAll(contents, "\r\n", "\n"), "\n") {
		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(trimmed, "```") || strings.HasPrefix(trimmed, "~~~") {
			inFence = !inFence
			continue
		}
		if !inFence {
			lines = append(lines, line)
		}
	}
	return lines
}

func parseTraceabilityRows(t *testing.T, contents string) []map[string]string {
	t.Helper()
	block := markerBlock(t, contents, "mpudp-traceability")
	var tableLines []string
	for _, line := range strings.Split(block, "\n") {
		if strings.HasPrefix(strings.TrimSpace(line), "|") {
			tableLines = append(tableLines, line)
		}
	}
	if len(tableLines) < 3 {
		t.Fatal("traceability marker does not contain a Markdown table")
	}
	headings := splitMarkdownRow(tableLines[0])
	wantHeadings := []string{"Section", "Requirement", "Implementation", "Verification", "Tracking", "Status"}
	if !reflect.DeepEqual(headings, wantHeadings) {
		t.Fatalf("traceability headings = %v, want %v", headings, wantHeadings)
	}
	if len(splitMarkdownRow(tableLines[1])) != len(wantHeadings) {
		t.Fatal("traceability separator has the wrong number of columns")
	}
	rows := make([]map[string]string, 0, len(tableLines)-2)
	for lineNumber, line := range tableLines[2:] {
		fields := splitMarkdownRow(line)
		if len(fields) != len(headings) {
			t.Fatalf("traceability data row %d has %d columns, want %d", lineNumber+1, len(fields), len(headings))
		}
		row := make(map[string]string, len(fields))
		for index, heading := range headings {
			row[heading] = fields[index]
		}
		rows = append(rows, row)
	}
	return rows
}

func splitMarkdownRow(line string) []string {
	line = strings.TrimSpace(line)
	line = strings.TrimPrefix(line, "|")
	line = strings.TrimSuffix(line, "|")
	parts := strings.Split(line, "|")
	for index := range parts {
		parts[index] = strings.TrimSpace(parts[index])
	}
	return parts
}

func parseCodeBulletInventory(t *testing.T, contents, marker string) []string {
	t.Helper()
	block := markerBlock(t, contents, marker)
	pattern := regexp.MustCompile(`^- ` + "`" + `([^` + "`" + `]+)` + "`" + `$`)
	var values []string
	for _, line := range strings.Split(strings.TrimSpace(block), "\n") {
		if strings.TrimSpace(line) == "" {
			continue
		}
		match := pattern.FindStringSubmatch(strings.TrimSpace(line))
		if match == nil {
			t.Fatalf("%s inventory has invalid line %q", marker, line)
		}
		values = append(values, match[1])
	}
	if len(values) == 0 {
		t.Fatalf("%s inventory is empty", marker)
	}
	return values
}

func markerBlock(t *testing.T, contents, marker string) string {
	t.Helper()
	start := "<!-- " + marker + ":start -->"
	end := "<!-- " + marker + ":end -->"
	if strings.Count(contents, start) != 1 || strings.Count(contents, end) != 1 {
		t.Fatalf("%s markers must each appear exactly once", marker)
	}
	afterStart := strings.SplitN(contents, start, 2)[1]
	parts := strings.SplitN(afterStart, end, 2)
	if len(parts) != 2 {
		t.Fatalf("%s end marker precedes start marker", marker)
	}
	return parts[0]
}

func integerRange(first, last int) []int {
	values := make([]int, 0, last-first+1)
	for value := first; value <= last; value++ {
		values = append(values, value)
	}
	return values
}
