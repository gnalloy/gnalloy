//go:build linux

package epoll

import (
	"testing"

	"golang.org/x/sys/unix"
)

func TestEpollWaitBufferCapsToDestination(t *testing.T) {
	events := make([]unix.EpollEvent, 8)
	got := epollWaitBuffer(events, 3)
	if len(got) != 3 {
		t.Fatalf("len=%d, want 3", len(got))
	}
	if cap(got) != cap(events) {
		t.Fatalf("cap=%d, want %d", cap(got), cap(events))
	}
}

func TestEpollWaitBufferKeepsStorageWhenDestinationIsLarger(t *testing.T) {
	events := make([]unix.EpollEvent, 8)
	got := epollWaitBuffer(events, 16)
	if len(got) != len(events) {
		t.Fatalf("len=%d, want %d", len(got), len(events))
	}
}
