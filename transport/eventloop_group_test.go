package transport

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"gnalloy.org/gnalloy/transport/poller/memory"
)

func newTestEventLoopGroup(t *testing.T, size int) *EventLoopGroup {
	t.Helper()
	group, err := NewEventLoopGroup(EventLoopGroupConfig{
		Size:        size,
		StartMillis: 0,
		Clock:       func() int64 { return 0 },
		PollerFactory: func(int) (Poller, error) {
			return memory.New(), nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := group.Shutdown(ctx); err != nil {
			t.Fatal(err)
		}
	})
	return group
}

func TestEventLoopGroupRoundRobin(t *testing.T) {
	group := newTestEventLoopGroup(t, 3)

	var got []EventLoopID
	for i := 0; i < 5; i++ {
		loop, err := group.Next()
		if err != nil {
			t.Fatal(err)
		}
		got = append(got, loop.ID())
	}

	want := []EventLoopID{0, 1, 2, 0, 1}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("round-robin=%v, want %v", got, want)
	}
}

func TestEventLoopGroupCPUAffinityMapping(t *testing.T) {
	group, err := NewEventLoopGroup(EventLoopGroupConfig{
		Size:        3,
		CPUAffinity: []int{2, 4},
		StartMillis: 0,
		Clock:       func() int64 { return 0 },
		PollerFactory: func(int) (Poller, error) {
			return memory.New(), nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := group.Close(); err != nil {
			t.Fatal(err)
		}
	})

	want := []int{2, 4, 2}
	for i, loop := range group.loops {
		if !loop.pinCPU || loop.cpuAffinity != want[i] {
			t.Fatalf("loop %d pin=%v cpu=%d, want cpu %d", i, loop.pinCPU, loop.cpuAffinity, want[i])
		}
	}
}

func TestEventLoopGroupRejectsNegativeCPUAffinity(t *testing.T) {
	_, err := NewEventLoopGroup(EventLoopGroupConfig{
		Size:        1,
		CPUAffinity: []int{-1},
		PollerFactory: func(int) (Poller, error) {
			return memory.New(), nil
		},
	})
	if !errors.Is(err, ErrInvalidEventLoopGroup) {
		t.Fatalf("err=%v, want %v", err, ErrInvalidEventLoopGroup)
	}
}

func TestEventLoopGroupInvoke(t *testing.T) {
	group := newTestEventLoopGroup(t, 1)
	if err := group.Start(context.Background()); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	ran := false
	if _, err := group.Invoke(ctx, func() error {
		ran = true
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if !ran {
		t.Fatal("invoke task was not executed")
	}
}

func TestEventLoopGroupRegisterNext(t *testing.T) {
	group := newTestEventLoopGroup(t, 1)
	if err := group.Start(context.Background()); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	handler := &testEventHandler{id: 9, fd: FDRef{FD: 99}}
	called := false
	loop, err := group.RegisterNext(ctx, handler, ReadyRead, func(gotLoop *EventLoop, gotHandler EventHandler) error {
		called = gotLoop.ID() == 0 && gotHandler == handler
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if loop.ID() != 0 || !called {
		t.Fatalf("loop=%d called=%v", loop.ID(), called)
	}
}
