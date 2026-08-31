package platformmatrix

import (
	"encoding/json"
	"fmt"
	"io"
	"strings"
)

// Matrix 是跨平台验证矩阵的机器可读事实源。
type Matrix struct {
	Version int      `json:"version"`
	Targets []Target `json:"targets"`
}

// Target 描述一个 GOOS/GOARCH 组合的验证边界。
type Target struct {
	Name        string   `json:"name"`
	GOOS        string   `json:"goos"`
	GOARCH      string   `json:"goarch"`
	Native      bool     `json:"native,omitempty"`
	Backends    Backends `json:"backends"`
	L2Drivers   []string `json:"l2Drivers,omitempty"`
	Gates       []Gate   `json:"gates"`
	Unsupported []string `json:"unsupported,omitempty"`
}

// Backends 描述 readiness/completion 后端在该平台的可用集合。
type Backends struct {
	Readiness  []string `json:"readiness,omitempty"`
	Completion []string `json:"completion,omitempty"`
}

// Gate 描述目标平台上的一个验证动作。
type Gate struct {
	Name       string   `json:"name"`
	Command    []string `json:"command"`
	NativeOnly bool     `json:"nativeOnly,omitempty"`
}

// Load 读取并校验跨平台验证矩阵。
func Load(r io.Reader) (Matrix, error) {
	if r == nil {
		return Matrix{}, fmt.Errorf("%w: nil reader", ErrInvalidMatrix)
	}
	var matrix Matrix
	decoder := json.NewDecoder(r)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&matrix); err != nil {
		return Matrix{}, err
	}
	if err := matrix.Validate(); err != nil {
		return Matrix{}, err
	}
	return matrix, nil
}

// Validate 校验矩阵的基础结构和重复目标。
func (m Matrix) Validate() error {
	if m.Version <= 0 {
		return fmt.Errorf("%w: invalid version", ErrInvalidMatrix)
	}
	if len(m.Targets) == 0 {
		return fmt.Errorf("%w: no targets", ErrInvalidMatrix)
	}
	seen := make(map[string]struct{}, len(m.Targets))
	for i, target := range m.Targets {
		if err := target.Validate(); err != nil {
			return fmt.Errorf("%w target %d: %v", ErrInvalidMatrix, i, err)
		}
		key := target.GOOS + "/" + target.GOARCH
		if _, ok := seen[key]; ok {
			return fmt.Errorf("%w: duplicate target %s", ErrInvalidMatrix, key)
		}
		seen[key] = struct{}{}
	}
	return nil
}

// Target 查找指定 GOOS/GOARCH 的验证目标。
func (m Matrix) Target(goos string, goarch string) (Target, bool) {
	for _, target := range m.Targets {
		if target.GOOS == goos && target.GOARCH == goarch {
			return target, true
		}
	}
	return Target{}, false
}

// Validate 校验单个目标。
func (t Target) Validate() error {
	if strings.TrimSpace(t.Name) == "" {
		return fmt.Errorf("empty name")
	}
	if strings.TrimSpace(t.GOOS) == "" || strings.TrimSpace(t.GOARCH) == "" {
		return fmt.Errorf("empty goos/goarch")
	}
	if len(t.Gates) == 0 {
		return fmt.Errorf("no gates")
	}
	for i, gate := range t.Gates {
		if err := gate.Validate(); err != nil {
			return fmt.Errorf("gate %d: %v", i, err)
		}
	}
	return nil
}

// Validate 校验单个门禁命令。
func (g Gate) Validate() error {
	if strings.TrimSpace(g.Name) == "" {
		return fmt.Errorf("empty name")
	}
	if len(g.Command) == 0 || strings.TrimSpace(g.Command[0]) == "" {
		return fmt.Errorf("empty command")
	}
	return nil
}
