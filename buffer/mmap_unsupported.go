//go:build !linux

package buffer

// NewMmapAllocator 在非 Linux 平台返回不支持错误，避免引入外部依赖。
func NewMmapAllocator(MmapAllocatorConfig) (Allocator, error) {
	return nil, ErrUnsupportedMmap
}
