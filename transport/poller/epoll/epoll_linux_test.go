//go:build linux

package epoll

import (
	"testing"

	"gnalloy.org/gnalloy/transport/poller"
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

func TestEpollEventsDefaultsToEdgeTriggered(t *testing.T) {
	events := epollEvents(poller.ReadyRead)
	if events&unix.EPOLLET == 0 {
		t.Fatalf("events=%#x, want EPOLLET", events)
	}
}

func TestEpollEventsSupportsLevelTriggeredInterest(t *testing.T) {
	events := epollEvents(poller.ReadyRead | poller.ReadyLevelTriggered)
	if events&unix.EPOLLET != 0 {
		t.Fatalf("events=%#x, do not want EPOLLET", events)
	}
	if events&unix.EPOLLIN == 0 {
		t.Fatalf("events=%#x, want EPOLLIN", events)
	}
}
