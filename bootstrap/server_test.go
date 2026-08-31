package bootstrap

import (
	"context"
	"errors"
	"testing"
	"time"

	"gnalloy.org/gnalloy/buffer"
	"gnalloy.org/gnalloy/channel"
	"gnalloy.org/gnalloy/transport"
	"gnalloy.org/gnalloy/transport/poller/memory"
)

type fakeServer struct {
	addr string
}

func (s fakeServer) Addr() string {
	return s.addr
}

func (s fakeServer) Close() error {
	return nil
}

type fakeTransport struct {
	cfg ServerConfig
	ch  channel.Channel
	err error
}

func (t *fakeTransport) Bind(_ context.Context, cfg ServerConfig) (Server, error) {
	t.cfg = cfg
	if t.err != nil {
		return nil, t.err
	}
	ch := channel.NewLocalChannel(11, buffer.NewHeapAllocator(), nil)
	cfg.ApplyChild(ch)
	if err := cfg.ChildInitializer(ch); err != nil {
		return nil, err
	}
	t.ch = ch
	return fakeServer{addr: cfg.Address}, nil
}

func newBootstrapGroup(t *testing.T) *transport.EventLoopGroup {
	t.Helper()
	group, err := transport.NewEventLoopGroup(transport.EventLoopGroupConfig{
		Size:        1,
		StartMillis: 0,
		Clock:       func() int64 { return 0 },
		PollerFactory: func(int) (transport.Poller, error) {
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

func TestServerBootstrapValidate(t *testing.T) {
	_, err := NewServerBootstrap().Bind("")
	if !errors.Is(err, ErrEmptyAddress) {
		t.Fatalf("err=%v, want %v", err, ErrEmptyAddress)
	}

	_, err = NewServerBootstrap().Bind("127.0.0.1:9000")
	if !errors.Is(err, ErrMissingGroup) {
		t.Fatalf("err=%v, want %v", err, ErrMissingGroup)
	}
}

func TestServerBootstrapBindInitializesChild(t *testing.T) {
	boss := newBootstrapGroup(t)
	worker := newBootstrapGroup(t)
	ft := &fakeTransport{}
	tenantKey := channel.NewAttributeKey[string]("tenant")

	server, err := NewServerBootstrap().
		Group(boss, worker).
		Transport(ft).
		ChildOption(channel.OptionAutoRead.Assignment(false)).
		ChildAttr(tenantKey.Assignment("goark")).
		ChildHandler(func(ch channel.Channel) {
			_ = ch.Pipeline().AddLast("capture", &captureHandler{})
		}).
		Bind("127.0.0.1:9000")
	if err != nil {
		t.Fatal(err)
	}
	if server.Addr() != "127.0.0.1:9000" {
		t.Fatalf("addr=%q", server.Addr())
	}
	if !boss.IsRunning() || !worker.IsRunning() {
		t.Fatal("event loop groups were not started")
	}
	if ft.cfg.BossGroup != boss || ft.cfg.WorkerGroup != worker {
		t.Fatal("server config did not receive configured groups")
	}
	if _, ok := ft.ch.Pipeline().Context("capture"); !ok {
		t.Fatal("child handler did not initialize pipeline")
	}
	if channel.OptionAutoRead.Get(ft.ch.Options()) {
		t.Fatal("child option was not applied")
	}
	if got, ok := tenantKey.Get(ft.ch.Attributes()); !ok || got != "goark" {
		t.Fatalf("tenant=%q ok=%v", got, ok)
	}
}

type captureHandler struct{}

func (captureHandler) ChannelRead(ctx *channel.HandlerContext, msg any) {
	ctx.FireChannelRead(msg)
}
