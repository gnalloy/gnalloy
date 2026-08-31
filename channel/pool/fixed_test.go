package pool

import (
	"context"
	"errors"
	"testing"
	"time"

	"gnalloy.org/gnalloy/channel"
	"gnalloy.org/gnalloy/transport"
)

func TestFixedPoolQueuesAcquireUntilChannelReturned(t *testing.T) {
	var created int
	p, err := NewFixed(FixedConfig{
		MaxConnections:    1,
		MaxIdle:           1,
		MaxPendingAcquire: 1,
		Factory: func(context.Context) (channel.Channel, error) {
			created++
			ch, _ := newPoolTestChannel(transport.ChannelID(created))
			return ch, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()

	first, err := p.Get(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	result := make(chan channel.Channel, 1)
	errs := make(chan error, 1)
	go func() {
		ch, err := p.Get(context.Background())
		if err != nil {
			errs <- err
			return
		}
		result <- ch
	}()
	select {
	case ch := <-result:
		t.Fatalf("unexpected immediate acquire: %v", ch)
	case err := <-errs:
		t.Fatalf("unexpected acquire error: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	if err := p.Put(first); err != nil {
		t.Fatal(err)
	}
	select {
	case ch := <-result:
		if ch != first {
			t.Fatalf("channel=%v, want returned channel", ch)
		}
	case err := <-errs:
		t.Fatalf("acquire error=%v", err)
	case <-time.After(time.Second):
		t.Fatal("queued acquire did not receive returned channel")
	}
	if created != 1 {
		t.Fatalf("created=%d, want 1", created)
	}
}

func TestFixedPoolRejectsExcessPendingAcquire(t *testing.T) {
	p, err := NewFixed(FixedConfig{
		MaxConnections:    1,
		MaxPendingAcquire: 1,
		Factory: func(context.Context) (channel.Channel, error) {
			ch, _ := newPoolTestChannel(1)
			return ch, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()

	first, err := p.Get(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()

	waitCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	pending := make(chan error, 1)
	go func() {
		_, err := p.Get(waitCtx)
		pending <- err
	}()
	select {
	case err := <-pending:
		t.Fatalf("first pending acquire returned early: %v", err)
	case <-time.After(20 * time.Millisecond):
	}

	_, err = p.Get(context.Background())
	if !errors.Is(err, ErrAcquireQueueFull) {
		t.Fatalf("err=%v, want %v", err, ErrAcquireQueueFull)
	}
	cancel()
	<-pending
}

func TestFixedPoolLifecycleHandler(t *testing.T) {
	lifecycle := &poolLifecycleRecorder{}
	p, err := NewFixed(FixedConfig{
		MaxConnections: 1,
		MaxIdle:        1,
		Handler:        lifecycle,
		Factory: func(context.Context) (channel.Channel, error) {
			ch, _ := newPoolTestChannel(1)
			return ch, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()

	ch, err := p.Get(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if err := p.Put(ch); err != nil {
		t.Fatal(err)
	}
	again, err := p.Get(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if ch != again {
		t.Fatal("fixed pool should reuse released channel")
	}
	if lifecycle.created != 1 || lifecycle.acquired != 2 || lifecycle.released != 1 {
		t.Fatalf("lifecycle=%+v, want created=1 acquired=2 released=1", lifecycle)
	}
	if err := p.Put(again); err != nil {
		t.Fatal(err)
	}
}
