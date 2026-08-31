package channel

import (
	"testing"
	"time"

	"gnalloy.org/gnalloy/buffer"
	"gnalloy.org/gnalloy/transport"
)

type lifecycleRecorder struct {
	events []string
}

func (r *lifecycleRecorder) HandlerAdded(*HandlerContext) error {
	r.events = append(r.events, "added")
	return nil
}

func (r *lifecycleRecorder) HandlerRemoved(*HandlerContext) error {
	r.events = append(r.events, "removed")
	return nil
}

func (r *lifecycleRecorder) ChannelRegistered(ctx *HandlerContext) {
	r.events = append(r.events, "registered")
	ctx.FireChannelRegistered()
}

func (r *lifecycleRecorder) ChannelUnregistered(ctx *HandlerContext) {
	r.events = append(r.events, "unregistered")
	ctx.FireChannelUnregistered()
}

func (r *lifecycleRecorder) ChannelReadComplete(ctx *HandlerContext) {
	r.events = append(r.events, "readComplete")
	ctx.FireChannelReadComplete()
}

func (r *lifecycleRecorder) FlushComplete(ctx *HandlerContext) {
	r.events = append(r.events, "flushComplete")
	ctx.FireFlushComplete()
}

func TestPipelineLifecycleHandlers(t *testing.T) {
	recorder := &lifecycleRecorder{}
	ch := NewLocalChannel(1, buffer.NewHeapAllocator(), &captureSink{})
	if err := ch.Pipeline().AddLast("life", recorder); err != nil {
		t.Fatal(err)
	}
	if len(recorder.events) != 1 || recorder.events[0] != "added" {
		t.Fatalf("events=%v", recorder.events)
	}
	ch.Pipeline().FireChannelRegistered()
	ch.Pipeline().FireChannelReadComplete()
	ch.Pipeline().FireFlushComplete()
	ch.Pipeline().FireChannelUnregistered()
	if err := ch.Pipeline().Remove("life"); err != nil {
		t.Fatal(err)
	}
	want := []string{"added", "registered", "readComplete", "flushComplete", "unregistered", "removed"}
	if len(recorder.events) != len(want) {
		t.Fatalf("events=%v", recorder.events)
	}
	for i := range want {
		if recorder.events[i] != want[i] {
			t.Fatalf("events=%v want=%v", recorder.events, want)
		}
	}
}

func TestPromiseListenersRunBeforeAndAfterCompletion(t *testing.T) {
	promise := NewPromise()
	before := make(chan error, 1)
	after := make(chan error, 1)
	promise.AddListener(func(f Future) {
		before <- f.Err()
	})
	if !promise.SetSuccess() {
		t.Fatal("first completion should succeed")
	}
	promise.AddListener(func(f Future) {
		after <- f.Err()
	})
	select {
	case err := <-before:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("listener registered before completion did not run")
	}
	select {
	case err := <-after:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("listener registered after completion did not run")
	}
	if promise.SetFailure(ErrPromiseFailed) {
		t.Fatal("second completion should be ignored")
	}
}

func TestPromiseTimeoutSuccessAndRemoveListener(t *testing.T) {
	promise := NewPromise()
	called := false
	handle := promise.AddListenerHandle(func(Future) {
		called = true
	})
	if !promise.RemoveListener(handle) {
		t.Fatal("listener was not removed")
	}
	done, err := promise.AwaitTimeout(time.Nanosecond)
	if done || err != nil {
		t.Fatalf("done=%v err=%v, want timeout without error", done, err)
	}
	if !promise.SetSuccess() {
		t.Fatal("completion failed")
	}
	done, err = promise.AwaitTimeout(time.Second)
	if !done || err != nil {
		t.Fatalf("done=%v err=%v, want success", done, err)
	}
	if !promise.IsSuccess() || promise.Cause() != nil {
		t.Fatalf("success=%v cause=%v", promise.IsSuccess(), promise.Cause())
	}
	if called {
		t.Fatal("removed listener was called")
	}
}

type recordingFutureExecutor struct {
	tasks []transport.Task
}

func (e *recordingFutureExecutor) Submit(task transport.Task) error {
	e.tasks = append(e.tasks, task)
	return nil
}

func TestPromiseListenersUseConfiguredExecutor(t *testing.T) {
	executor := &recordingFutureExecutor{}
	promise := NewPromiseWithExecutor(executor)
	called := false
	promise.AddListener(func(Future) {
		called = true
	})
	if !promise.SetSuccess() {
		t.Fatal("completion failed")
	}
	if called {
		t.Fatal("listener ran inline before executor drained")
	}
	if len(executor.tasks) != 1 {
		t.Fatalf("tasks=%d, want 1", len(executor.tasks))
	}
	executor.tasks[0]()
	if !called {
		t.Fatal("listener was not dispatched through executor")
	}
}

func TestCompletedPromiseListenerUsesConfiguredExecutor(t *testing.T) {
	executor := &recordingFutureExecutor{}
	promise := NewPromiseWithExecutor(executor)
	if !promise.SetSuccess() {
		t.Fatal("completion failed")
	}
	called := false
	promise.AddListener(func(Future) {
		called = true
	})
	if called {
		t.Fatal("post-completion listener ran inline before executor drained")
	}
	if len(executor.tasks) != 1 {
		t.Fatalf("tasks=%d, want 1", len(executor.tasks))
	}
	executor.tasks[0]()
	if !called {
		t.Fatal("post-completion listener was not dispatched through executor")
	}
}

func TestUnsafeWriteFutureCompletesAfterDrain(t *testing.T) {
	rw := &partialWriteRW{steps: []writeStep{{n: 2, again: true}, {n: 2}}}
	ch, unsafeCh := NewUnsafeChannel(UnsafeConfig{
		ID:         1,
		FD:         transport.FDRef{FD: 1},
		Allocator:  buffer.NewHeapAllocator(),
		Poller:     &fakeReadyPoller{},
		ReadWriter: rw,
	})
	buf := buffer.NewHeapBuffer(4)
	_, _ = buf.WriteBytes([]byte("ping"))
	future := ch.WriteFuture(buf)
	if future.IsDone() {
		t.Fatal("write future should wait for drain")
	}
	flushFuture := ch.FlushFuture()
	if flushFuture.IsDone() {
		t.Fatal("flush future should wait for drain")
	}
	unsafeCh.HandleEvent(transport.PollEvent{Model: transport.PollerReadiness, Ready: transport.ReadyWrite})
	if err := future.Await(); err != nil {
		t.Fatal(err)
	}
	if err := flushFuture.Await(); err != nil {
		t.Fatal(err)
	}
}

func TestUnsafeCloseFutureCompletesOnInactive(t *testing.T) {
	ch, _ := NewUnsafeChannel(UnsafeConfig{
		ID:         1,
		FD:         transport.FDRef{FD: 1},
		Allocator:  buffer.NewHeapAllocator(),
		Poller:     &fakeReadyPoller{},
		ReadWriter: &partialWriteRW{},
	})
	future := ch.CloseFuture()
	if err := future.Await(); err != nil {
		t.Fatal(err)
	}
}
