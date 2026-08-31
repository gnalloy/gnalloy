package transport

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"
)

type testLocalResource struct {
	loopID   EventLoopID
	closeErr error
	closed   atomic.Int32
}

func (r *testLocalResource) Close() error {
	r.closed.Add(1)
	return r.closeErr
}

func TestEventLoopLocalReturnsOneValuePerLoop(t *testing.T) {
	group := newTestEventLoopGroup(t, 2)
	loops := group.Loops()

	var creates atomic.Int32
	local, err := NewEventLoopLocal(func(loop *EventLoop) (*testLocalResource, error) {
		creates.Add(1)
		return &testLocalResource{loopID: loop.ID()}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	defer local.Close()

	first, err := local.Get(loops[0])
	if err != nil {
		t.Fatal(err)
	}
	again, err := local.Get(loops[0])
	if err != nil {
		t.Fatal(err)
	}
	second, err := local.Get(loops[1])
	if err != nil {
		t.Fatal(err)
	}

	if first != again {
		t.Fatal("same EventLoop returned different local values")
	}
	if first == second {
		t.Fatal("different EventLoops shared one local value")
	}
	if first.loopID != 0 || second.loopID != 1 {
		t.Fatalf("loop ids=(%d,%d), want (0,1)", first.loopID, second.loopID)
	}
	if creates.Load() != 2 {
		t.Fatalf("factory calls=%d, want 2", creates.Load())
	}

	snapshot := local.Snapshot()
	if len(snapshot) != 2 {
		t.Fatalf("snapshot len=%d, want 2", len(snapshot))
	}
	if snapshot[0].EventLoopID != 0 || snapshot[0].Value != first {
		t.Fatalf("snapshot[0]=%+v, want loop 0", snapshot[0])
	}
	if snapshot[1].EventLoopID != 1 || snapshot[1].Value != second {
		t.Fatalf("snapshot[1]=%+v, want loop 1", snapshot[1])
	}
}

func TestEventLoopLocalRejectsInvalidInputsAndClosedLocal(t *testing.T) {
	if _, err := NewEventLoopLocal[*testLocalResource](nil); !errors.Is(err, ErrInvalidEventLoopLocal) {
		t.Fatalf("new err=%v, want %v", err, ErrInvalidEventLoopLocal)
	}

	local, err := NewEventLoopLocal(func(loop *EventLoop) (*testLocalResource, error) {
		return &testLocalResource{loopID: loop.ID()}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := local.Get(nil); !errors.Is(err, ErrNoEventLoop) {
		t.Fatalf("nil loop err=%v, want %v", err, ErrNoEventLoop)
	}
	if err := local.Close(); err != nil {
		t.Fatal(err)
	}

	group := newTestEventLoopGroup(t, 1)
	if _, err := local.Get(group.loops[0]); !errors.Is(err, ErrEventLoopLocalClosed) {
		t.Fatalf("closed get err=%v, want %v", err, ErrEventLoopLocalClosed)
	}
}

func TestEventLoopLocalCreatesOnceUnderConcurrency(t *testing.T) {
	group := newTestEventLoopGroup(t, 1)
	loop := group.loops[0]

	var creates atomic.Int32
	local, err := NewEventLoopLocal(func(loop *EventLoop) (*testLocalResource, error) {
		creates.Add(1)
		return &testLocalResource{loopID: loop.ID()}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	defer local.Close()

	const workers = 32
	results := make(chan *testLocalResource, workers)
	var wg sync.WaitGroup
	wg.Add(workers)
	for i := 0; i < workers; i++ {
		go func() {
			defer wg.Done()
			value, err := local.Get(loop)
			if err != nil {
				t.Errorf("get local: %v", err)
				return
			}
			results <- value
		}()
	}
	wg.Wait()
	close(results)

	var first *testLocalResource
	for value := range results {
		if first == nil {
			first = value
			continue
		}
		if value != first {
			t.Fatal("concurrent Get returned different values")
		}
	}
	if creates.Load() != 1 {
		t.Fatalf("factory calls=%d, want 1", creates.Load())
	}
}

func TestEventLoopLocalCloseClosesValuesOnce(t *testing.T) {
	group := newTestEventLoopGroup(t, 2)
	loops := group.Loops()
	closeErr := errors.New("close failed")

	local, err := NewEventLoopLocal(func(loop *EventLoop) (*testLocalResource, error) {
		resource := &testLocalResource{loopID: loop.ID()}
		if loop.ID() == 1 {
			resource.closeErr = closeErr
		}
		return resource, nil
	})
	if err != nil {
		t.Fatal(err)
	}

	first, err := local.Get(loops[0])
	if err != nil {
		t.Fatal(err)
	}
	second, err := local.Get(loops[1])
	if err != nil {
		t.Fatal(err)
	}

	if err := local.Close(); !errors.Is(err, closeErr) {
		t.Fatalf("close err=%v, want %v", err, closeErr)
	}
	if err := local.Close(); err != nil {
		t.Fatalf("second close err=%v, want nil", err)
	}
	if first.closed.Load() != 1 || second.closed.Load() != 1 {
		t.Fatalf("closed counts=(%d,%d), want (1,1)", first.closed.Load(), second.closed.Load())
	}
	if got := local.Snapshot(); len(got) != 0 {
		t.Fatalf("snapshot after close=%+v, want empty", got)
	}
}

func TestEventLoopCloseClosesEventLoopLocalValues(t *testing.T) {
	group := newTestEventLoopGroup(t, 1)
	loop := group.loops[0]

	local, err := NewEventLoopLocal(func(loop *EventLoop) (*testLocalResource, error) {
		return &testLocalResource{loopID: loop.ID()}, nil
	})
	if err != nil {
		t.Fatal(err)
	}

	value, err := local.Get(loop)
	if err != nil {
		t.Fatal(err)
	}
	if err := loop.Close(); err != nil {
		t.Fatal(err)
	}
	if value.closed.Load() != 1 {
		t.Fatalf("closed count=%d, want 1", value.closed.Load())
	}
	if got := local.Snapshot(); len(got) != 0 {
		t.Fatalf("snapshot after loop close=%+v, want empty", got)
	}
}
