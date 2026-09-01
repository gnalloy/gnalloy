package channel

import (
	"errors"
	"strings"
	"sync"
	"testing"

	"gnalloy.org/gnalloy/buffer"
	"gnalloy.org/gnalloy/transport"
	"gnalloy.org/gnalloy/transport/poller/memory"
)

type ownerLoopSink struct {
	id      transport.ChannelID
	fd      transport.FDRef
	ch      *LocalChannel
	writes  []any
	flushes int
	closed  bool
}

type recordingOwnerExecutor struct {
	tasks []transport.Task
	err   error
}

func (e *recordingOwnerExecutor) Submit(task transport.Task) error {
	if e.err != nil {
		return e.err
	}
	e.tasks = append(e.tasks, task)
	return nil
}

func (e *recordingOwnerExecutor) drain() {
	for len(e.tasks) > 0 {
		task := e.tasks[0]
		copy(e.tasks, e.tasks[1:])
		e.tasks = e.tasks[:len(e.tasks)-1]
		task()
	}
}

func (s *ownerLoopSink) ID() transport.ChannelID {
	return s.id
}

func (s *ownerLoopSink) FD() transport.FDRef {
	return s.fd
}

func (s *ownerLoopSink) HandleEvent(transport.PollEvent) {}

func (s *ownerLoopSink) BindEventExecutor(executor interface{ Submit(transport.Task) error }) {
	s.ch.BindEventExecutor(executor)
}

func (s *ownerLoopSink) Write(msg any) error {
	s.writes = append(s.writes, msg)
	return nil
}

func (s *ownerLoopSink) Flush() error {
	s.flushes++
	return nil
}

func (s *ownerLoopSink) Close() error {
	s.closed = true
	return nil
}

func TestLocalChannelWriteAndFlushFutureRunsOnBoundEventLoop(t *testing.T) {
	poller := memory.New()
	loop, err := transport.NewEventLoop(transport.EventLoopConfig{ID: 1, Poller: poller, StartMillis: 0})
	if err != nil {
		t.Fatal(err)
	}
	defer loop.Close()

	sink := &ownerLoopSink{id: 7, fd: transport.FDRef{FD: 77}}
	ch := NewLocalChannelWithTimer(sink.id, buffer.NewHeapAllocator(), sink, loop.Timer())
	sink.ch = ch
	if err := loop.Register(sink, transport.ReadyRead); err != nil {
		t.Fatal(err)
	}

	future := ch.WriteAndFlushFuture("payload")
	if future.IsDone() {
		t.Fatalf("future completed before owner loop ran: %v", future.Err())
	}
	if len(sink.writes) != 0 || sink.flushes != 0 {
		t.Fatalf("write executed outside owner loop: writes=%v flushes=%d", sink.writes, sink.flushes)
	}

	if err := loop.RunOnce(0); err != nil {
		t.Fatal(err)
	}
	if err := future.Await(); err != nil {
		t.Fatal(err)
	}
	if len(sink.writes) != 1 || sink.writes[0] != "payload" || sink.flushes != 1 {
		t.Fatalf("sink writes=%v flushes=%d, want one write and one flush", sink.writes, sink.flushes)
	}
}

func TestLocalChannelWriteAndFlushCoalescesOwnerTasks(t *testing.T) {
	executor := &recordingOwnerExecutor{}
	sink := &ownerLoopSink{id: 10, fd: transport.FDRef{FD: 100}}
	ch := NewLocalChannel(sink.id, buffer.NewHeapAllocator(), sink)
	ch.BindEventExecutor(executor)

	if err := ch.WriteAndFlush("first"); err != nil {
		t.Fatal(err)
	}
	if err := ch.WriteAndFlush("second"); err != nil {
		t.Fatal(err)
	}
	if len(executor.tasks) != 1 {
		t.Fatalf("tasks=%d, want 1", len(executor.tasks))
	}
	executor.drain()
	if len(sink.writes) != 2 || sink.writes[0] != "first" || sink.writes[1] != "second" {
		t.Fatalf("writes=%v, want [first second]", sink.writes)
	}
	if sink.flushes != 2 {
		t.Fatalf("flushes=%d, want 2", sink.flushes)
	}
}

func TestLocalChannelWriteAndFlushReleasesMessageWhenOwnerRejectsTask(t *testing.T) {
	wantErr := errors.New("executor rejected task")
	executor := &recordingOwnerExecutor{err: wantErr}
	sink := &ownerLoopSink{id: 11, fd: transport.FDRef{FD: 101}}
	ch := NewLocalChannel(sink.id, buffer.NewHeapAllocator(), sink)
	ch.BindEventExecutor(executor)
	buf := buffer.NewHeapBuffer(4)
	if _, err := buf.WriteBytes([]byte("ping")); err != nil {
		t.Fatal(err)
	}

	if err := ch.WriteAndFlush(buf); !errors.Is(err, wantErr) {
		t.Fatalf("err=%v, want %v", err, wantErr)
	}
	if buf.RefCnt() != 0 {
		t.Fatalf("ref=%d, want released buffer", buf.RefCnt())
	}
}

func TestLocalChannelWriteAndFlushAcceptsConcurrentProducers(t *testing.T) {
	executor := &recordingOwnerExecutor{}
	sink := &ownerLoopSink{id: 12, fd: transport.FDRef{FD: 102}}
	ch := NewLocalChannel(sink.id, buffer.NewHeapAllocator(), sink)
	ch.BindEventExecutor(executor)
	const producers = 8
	const messages = 100
	var wg sync.WaitGroup
	wg.Add(producers)
	for producer := 0; producer < producers; producer++ {
		go func() {
			defer wg.Done()
			for i := 0; i < messages; i++ {
				if err := ch.WriteAndFlush("payload"); err != nil {
					t.Errorf("write: %v", err)
					return
				}
			}
		}()
	}
	wg.Wait()
	if len(executor.tasks) != 1 {
		t.Fatalf("tasks=%d, want 1", len(executor.tasks))
	}
	executor.drain()
	if len(sink.writes) != producers*messages {
		t.Fatalf("writes=%d, want %d", len(sink.writes), producers*messages)
	}
}

func BenchmarkLocalChannelWriteAndFlushOwnerQueue(b *testing.B) {
	executor := &recordingOwnerExecutor{}
	sink := &ownerLoopSink{id: 13, fd: transport.FDRef{FD: 103}}
	ch := NewLocalChannel(sink.id, buffer.NewHeapAllocator(), sink)
	ch.BindEventExecutor(executor)
	payload := &struct{}{}
	if err := ch.WriteAndFlush(payload); err != nil {
		b.Fatal(err)
	}
	executor.drain()
	sink.writes = make([]any, 0, 1)
	sink.flushes = 0
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := ch.WriteAndFlush(payload); err != nil {
			b.Fatal(err)
		}
		executor.drain()
		sink.writes = sink.writes[:0]
	}
}

func TestLocalChannelWriteFutureReleasesMessageWhenOwnerLoopRejectsTask(t *testing.T) {
	poller := memory.New()
	loop, err := transport.NewEventLoop(transport.EventLoopConfig{ID: 1, Poller: poller, StartMillis: 0})
	if err != nil {
		t.Fatal(err)
	}
	sink := &ownerLoopSink{id: 8, fd: transport.FDRef{FD: 88}}
	ch := NewLocalChannelWithTimer(sink.id, buffer.NewHeapAllocator(), sink, loop.Timer())
	sink.ch = ch
	ch.BindEventExecutor(loop)
	if err := loop.Close(); err != nil {
		t.Fatal(err)
	}

	buf := buffer.NewHeapBuffer(4)
	if _, err := buf.WriteBytes([]byte("ping")); err != nil {
		t.Fatal(err)
	}
	future := ch.WriteFuture(buf)
	if !errors.Is(future.Err(), transport.ErrEventLoopClosed) {
		t.Fatalf("err=%v, want %v", future.Err(), transport.ErrEventLoopClosed)
	}
	if buf.RefCnt() != 0 {
		t.Fatalf("ref=%d, want released buffer", buf.RefCnt())
	}
}

func TestLocalChannelWriteFutureReleasesFileRegionWhenOwnerLoopRejectsTask(t *testing.T) {
	poller := memory.New()
	loop, err := transport.NewEventLoop(transport.EventLoopConfig{ID: 1, Poller: poller, StartMillis: 0})
	if err != nil {
		t.Fatal(err)
	}
	sink := &ownerLoopSink{id: 9, fd: transport.FDRef{FD: 99}}
	ch := NewLocalChannelWithTimer(sink.id, buffer.NewHeapAllocator(), sink, loop.Timer())
	sink.ch = ch
	ch.BindEventExecutor(loop)
	if err := loop.Close(); err != nil {
		t.Fatal(err)
	}

	region, err := NewFileRegion(strings.NewReader("payload"), 0, 7)
	if err != nil {
		t.Fatal(err)
	}
	future := ch.WriteFuture(region)
	if !errors.Is(future.Err(), transport.ErrEventLoopClosed) {
		t.Fatalf("err=%v, want %v", future.Err(), transport.ErrEventLoopClosed)
	}
	if _, err := region.Read(make([]byte, 1)); !errors.Is(err, ErrFileRegionClosed) {
		t.Fatalf("err=%v, want closed region", err)
	}
}
