package bootstrap

import (
	"context"
	"errors"
	"testing"

	"gnalloy.org/gnalloy/buffer"
	"gnalloy.org/gnalloy/channel"
	"gnalloy.org/gnalloy/transport"
)

func TestDialerValidate(t *testing.T) {
	_, err := NewDialer().Dial("")
	if !errors.Is(err, ErrEmptyAddress) {
		t.Fatalf("err=%v, want %v", err, ErrEmptyAddress)
	}

	_, err = NewDialer().Dial("127.0.0.1:9000")
	if !errors.Is(err, ErrMissingGroup) {
		t.Fatalf("err=%v, want %v", err, ErrMissingGroup)
	}
}

func TestDialerDialInitializesChannel(t *testing.T) {
	group := newDialerGroup(t)
	var initialized bool
	clientKey := channel.NewAttributeKey[string]("client")
	ch, err := NewDialer().
		Group(group).
		Transport(fakeClientTransport{}).
		Option(channel.OptionAutoRead.Assignment(false)).
		Attr(clientKey.Assignment("bench")).
		Initializer(func(ch channel.Channel) error {
			initialized = true
			return ch.Pipeline().AddLast("discard", discardClientHandler{})
		}).
		DialContext(context.Background(), "127.0.0.1:9000")
	if err != nil {
		t.Fatal(err)
	}
	if !initialized {
		t.Fatal("initializer was not called")
	}
	if ch == nil {
		t.Fatal("channel is nil")
	}
	if channel.OptionAutoRead.Get(ch.Options()) {
		t.Fatal("client option was not applied")
	}
	if got, ok := clientKey.Get(ch.Attributes()); !ok || got != "bench" {
		t.Fatalf("client attr=%q ok=%v", got, ok)
	}
}

func newDialerGroup(t *testing.T) *transport.EventLoopGroup {
	t.Helper()
	group, err := transport.NewEventLoopGroup(transport.EventLoopGroupConfig{
		Size:         1,
		PollerConfig: transport.Config{Backend: transport.BackendMemory},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = group.Close() })
	return group
}

type fakeClientTransport struct{}

func (fakeClientTransport) Dial(_ context.Context, cfg ClientConfig) (channel.Channel, error) {
	ch := channel.NewLocalChannel(1, buffer.NewHeapAllocator(), nil)
	cfg.Apply(ch)
	if err := cfg.Initializer(ch); err != nil {
		return nil, err
	}
	return ch, nil
}

type discardClientHandler struct{}

func (discardClientHandler) ChannelRead(_ *channel.HandlerContext, msg any) {
	if buf, ok := msg.(buffer.ByteBuf); ok {
		buf.Release()
	}
}
