package platformmatrix

import "errors"

var (
	// ErrInvalidMatrix 表示跨平台验证矩阵结构不完整或字段非法。
	ErrInvalidMatrix = errors.New("gnalloy/validation/platformmatrix: invalid matrix")
)
