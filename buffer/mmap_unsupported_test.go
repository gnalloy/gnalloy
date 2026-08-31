//go:build !linux

package buffer

import (
	"errors"
	"testing"
)

func TestMmapAllocatorUnsupportedPlatform(t *testing.T) {
	_, err := NewMmapAllocator(MmapAllocatorConfig{BlockSize: 1024, Blocks: 1})
	if !errors.Is(err, ErrUnsupportedMmap) {
		t.Fatalf("err=%v, want %v", err, ErrUnsupportedMmap)
	}
}
