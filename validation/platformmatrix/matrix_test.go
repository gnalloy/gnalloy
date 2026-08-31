package platformmatrix

import (
	"errors"
	"os"
	"strings"
	"testing"
)

func TestLoadDefaultMatrix(t *testing.T) {
	file, err := os.Open("../../scripts/platform-matrix.json")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()

	matrix, err := Load(file)
	if err != nil {
		t.Fatal(err)
	}
	if len(matrix.Targets) < 4 {
		t.Fatalf("targets=%d, want cross-platform matrix", len(matrix.Targets))
	}
	if _, ok := matrix.Target("linux", "amd64"); !ok {
		t.Fatal("missing linux/amd64 target")
	}
	if _, ok := matrix.Target("windows", "amd64"); !ok {
		t.Fatal("missing windows/amd64 target")
	}
	linux, _ := matrix.Target("linux", "amd64")
	if !contains(linux.L2Drivers, "af_packet") {
		t.Fatalf("linux l2Drivers=%v, want af_packet", linux.L2Drivers)
	}
	windows, _ := matrix.Target("windows", "amd64")
	if !contains(windows.L2Drivers, "npcap") {
		t.Fatalf("windows l2Drivers=%v, want npcap", windows.L2Drivers)
	}
	if !contains(windows.Unsupported, "resolver-dns-native-macos") {
		t.Fatalf("windows unsupported=%v, want resolver-dns-native-macos", windows.Unsupported)
	}
	if !hasGate(windows.Gates, "protocol-gate") {
		t.Fatalf("windows gates=%v, want protocol-gate", windows.Gates)
	}
	darwin, _ := matrix.Target("darwin", "arm64")
	if !contains(darwin.L2Drivers, "bpf") {
		t.Fatalf("darwin l2Drivers=%v, want bpf", darwin.L2Drivers)
	}
	if contains(darwin.Unsupported, "resolver-dns-native-macos") {
		t.Fatalf("darwin unsupported=%v, must not contain resolver-dns-native-macos", darwin.Unsupported)
	}
	if !hasGate(darwin.Gates, "protocol-gate") {
		t.Fatalf("darwin gates=%v, want protocol-gate", darwin.Gates)
	}
}

func TestMatrixRejectsDuplicateTargets(t *testing.T) {
	_, err := Load(strings.NewReader(`{
		"version": 1,
		"targets": [
			{"name": "linux-amd64", "goos": "linux", "goarch": "amd64", "gates": [{"name": "compile", "command": ["go", "test"]}]},
			{"name": "linux-amd64", "goos": "linux", "goarch": "amd64", "gates": [{"name": "compile", "command": ["go", "test"]}]}
		]
	}`))
	if !errors.Is(err, ErrInvalidMatrix) {
		t.Fatalf("err=%v, want ErrInvalidMatrix", err)
	}
}

func TestMatrixRejectsEmptyGateCommand(t *testing.T) {
	_, err := Load(strings.NewReader(`{
		"version": 1,
		"targets": [
			{"name": "linux-amd64", "goos": "linux", "goarch": "amd64", "gates": [{"name": "compile"}]}
		]
	}`))
	if !errors.Is(err, ErrInvalidMatrix) {
		t.Fatalf("err=%v, want ErrInvalidMatrix", err)
	}
}

func contains(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}

func hasGate(gates []Gate, want string) bool {
	for _, gate := range gates {
		if gate.Name == want {
			return true
		}
	}
	return false
}
