package std

import (
	"testing"

	"gnalloy.org/gnalloy/transport/poller"
)

func TestPollReturnsRegisteredReadiness(t *testing.T) {
	p := New()
	defer p.Close()
	fd := poller.FDRef{FD: 10}
	if err := p.Register(fd, 7, poller.ReadyRead); err != nil {
		t.Fatal(err)
	}
	events := make([]poller.Event, 4)
	n, err := p.Poll(events, 0)
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 || events[0].Model != poller.Readiness || events[0].Ready != poller.ReadyRead || events[0].ChannelID != 7 {
		t.Fatalf("n=%d events=%+v", n, events[:n])
	}
}

func TestPollRespectsModifyAndDeregister(t *testing.T) {
	p := New()
	defer p.Close()
	fd := poller.FDRef{FD: 10}
	if err := p.Register(fd, 7, poller.ReadyRead); err != nil {
		t.Fatal(err)
	}
	if err := p.Modify(fd, 0); err != nil {
		t.Fatal(err)
	}
	events := make([]poller.Event, 4)
	n, err := p.Poll(events, 0)
	if err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Fatalf("n=%d, want 0", n)
	}
	if err := p.Modify(fd, poller.ReadyWrite); err != nil {
		t.Fatal(err)
	}
	n, err = p.Poll(events, 0)
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 || events[0].Ready != poller.ReadyWrite {
		t.Fatalf("n=%d events=%+v", n, events[:n])
	}
	if err := p.Deregister(fd); err != nil {
		t.Fatal(err)
	}
	n, err = p.Poll(events, 0)
	if err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Fatalf("n=%d, want 0 after deregister", n)
	}
}

func TestWakeupProducesWakeupEvent(t *testing.T) {
	p := New()
	defer p.Close()
	if err := p.Wakeup(); err != nil {
		t.Fatal(err)
	}
	events := make([]poller.Event, 1)
	n, err := p.Poll(events, -1)
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 || events[0].Op != poller.OpWakeup {
		t.Fatalf("n=%d events=%+v", n, events[:n])
	}
}
