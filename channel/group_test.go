package channel

import (
	"errors"
	"testing"

	"gnalloy.org/gnalloy/buffer"
	"gnalloy.org/gnalloy/transport"
)

func TestGroupAddRemoveAndSnapshot(t *testing.T) {
	group := NewGroup()
	ch1, _ := newGroupTestChannel(1)
	ch2, _ := newGroupTestChannel(2)

	if !group.Add(ch1) || !group.Add(ch2) {
		t.Fatal("channels should be added")
	}
	if group.Add(ch1) {
		t.Fatal("duplicate channel should not be added")
	}
	if group.Len() != 2 {
		t.Fatalf("len=%d, want 2", group.Len())
	}
	if _, ok := group.Get(ch1.ID()); !ok {
		t.Fatal("channel should be found")
	}
	snapshot := group.Snapshot()
	if len(snapshot) != 2 {
		t.Fatalf("snapshot=%d, want 2", len(snapshot))
	}
	if removed, ok := group.Remove(ch1.ID()); !ok || removed != ch1 {
		t.Fatalf("removed=%v ok=%v", removed, ok)
	}
	if group.Len() != 1 {
		t.Fatalf("len=%d, want 1", group.Len())
	}
}

func TestGroupWriteEachFlushAndClose(t *testing.T) {
	group := NewGroup()
	ch1, sink1 := newGroupTestChannel(1)
	ch2, sink2 := newGroupTestChannel(2)
	group.Add(ch1)
	group.Add(ch2)

	writeFuture := group.WriteEach(func(ch Channel) any {
		return ch.ID()
	})
	if err := writeFuture.Await(); err != nil {
		t.Fatal(err)
	}
	if len(writeFuture.Results()) != 2 {
		t.Fatalf("results=%d, want 2", len(writeFuture.Results()))
	}
	if len(sink1.writes) != 1 || sink1.writes[0] != transport.ChannelID(1) {
		t.Fatalf("sink1 writes=%v", sink1.writes)
	}
	if len(sink2.writes) != 1 || sink2.writes[0] != transport.ChannelID(2) {
		t.Fatalf("sink2 writes=%v", sink2.writes)
	}
	if err := group.Flush().Await(); err != nil {
		t.Fatal(err)
	}
	if sink1.flushes != 1 || sink2.flushes != 1 {
		t.Fatalf("flushes=%d/%d", sink1.flushes, sink2.flushes)
	}
	if err := group.Close().Await(); err != nil {
		t.Fatal(err)
	}
	if sink1.closes != 1 || sink2.closes != 1 {
		t.Fatalf("closes=%d/%d", sink1.closes, sink2.closes)
	}
}

func TestGroupFutureKeepsPerChannelErrors(t *testing.T) {
	group := NewGroup()
	ch1, _ := newGroupTestChannel(1)
	ch2, sink2 := newGroupTestChannel(2)
	sink2.err = ErrNoOutboundSink
	group.Add(ch1)
	group.Add(ch2)

	future := group.WriteEach(func(Channel) any { return "x" })
	if !errors.Is(future.Await(), ErrNoOutboundSink) {
		t.Fatalf("err=%v, want %v", future.Err(), ErrNoOutboundSink)
	}
	results := future.Results()
	if len(results) != 2 {
		t.Fatalf("results=%d, want 2", len(results))
	}
	found := false
	for _, result := range results {
		if result.ID == ch2.ID() && errors.Is(result.Err, ErrNoOutboundSink) {
			found = true
		}
	}
	if !found {
		t.Fatalf("results=%+v", results)
	}
}

func TestGroupHandlerTracksLifecycle(t *testing.T) {
	group := NewGroup()
	ch, _ := newGroupTestChannel(1)
	if err := ch.Pipeline().AddLast("group", NewGroupHandler(group)); err != nil {
		t.Fatal(err)
	}
	ch.Pipeline().FireChannelActive()
	if group.Len() != 1 {
		t.Fatalf("len=%d, want 1", group.Len())
	}
	ch.Pipeline().FireChannelInactive()
	if group.Len() != 0 {
		t.Fatalf("len=%d, want 0", group.Len())
	}
}

type groupTestSink struct {
	writes  []any
	flushes int
	closes  int
	err     error
}

func (s *groupTestSink) Write(msg any) error {
	if s.err != nil {
		return s.err
	}
	s.writes = append(s.writes, msg)
	return nil
}

func (s *groupTestSink) Flush() error {
	if s.err != nil {
		return s.err
	}
	s.flushes++
	return nil
}

func (s *groupTestSink) Close() error {
	if s.err != nil {
		return s.err
	}
	s.closes++
	return nil
}

func newGroupTestChannel(id transport.ChannelID) (*LocalChannel, *groupTestSink) {
	sink := &groupTestSink{}
	ch := NewLocalChannel(id, buffer.NewHeapAllocator(), sink)
	return ch, sink
}
