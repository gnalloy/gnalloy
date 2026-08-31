package channel

import (
	"sync"
	"time"

	"gnalloy.org/gnalloy/transport"
)

// Group 管理一组 Channel，并提供批量写出、刷新和关闭能力。
type Group struct {
	mu       sync.RWMutex
	channels map[transport.ChannelID]Channel
}

func NewGroup() *Group {
	return &Group{channels: make(map[transport.ChannelID]Channel, 16)}
}

func (g *Group) Add(ch Channel) bool {
	if g == nil || ch == nil {
		return false
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.channels == nil {
		g.channels = make(map[transport.ChannelID]Channel, 16)
	}
	id := ch.ID()
	if _, exists := g.channels[id]; exists {
		return false
	}
	g.channels[id] = ch
	return true
}

func (g *Group) Remove(id transport.ChannelID) (Channel, bool) {
	if g == nil {
		return nil, false
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	ch, ok := g.channels[id]
	if ok {
		delete(g.channels, id)
	}
	return ch, ok
}

func (g *Group) Get(id transport.ChannelID) (Channel, bool) {
	if g == nil {
		return nil, false
	}
	g.mu.RLock()
	defer g.mu.RUnlock()
	ch, ok := g.channels[id]
	return ch, ok
}

func (g *Group) Len() int {
	if g == nil {
		return 0
	}
	g.mu.RLock()
	defer g.mu.RUnlock()
	return len(g.channels)
}

func (g *Group) Snapshot() []Channel {
	if g == nil {
		return nil
	}
	g.mu.RLock()
	defer g.mu.RUnlock()
	out := make([]Channel, 0, len(g.channels))
	for _, ch := range g.channels {
		out = append(out, ch)
	}
	return out
}

// WriteEach 为每个 Channel 生成独立出站消息，避免共享引用计数消息。
func (g *Group) WriteEach(newMessage func(Channel) any) *GroupFuture {
	if newMessage == nil {
		return newGroupFuture(nil, ErrInvalidMessage)
	}
	channels := g.Snapshot()
	results := make([]GroupResult, 0, len(channels))
	var first error
	for _, ch := range channels {
		msg := newMessage(ch)
		err := ch.Write(msg)
		if err != nil && first == nil {
			first = err
		}
		results = append(results, GroupResult{ID: ch.ID(), Err: err})
	}
	return newGroupFuture(results, first)
}

func (g *Group) Flush() *GroupFuture {
	channels := g.Snapshot()
	results := make([]GroupResult, 0, len(channels))
	var first error
	for _, ch := range channels {
		err := ch.Flush()
		if err != nil && first == nil {
			first = err
		}
		results = append(results, GroupResult{ID: ch.ID(), Err: err})
	}
	return newGroupFuture(results, first)
}

func (g *Group) Close() *GroupFuture {
	channels := g.Snapshot()
	results := make([]GroupResult, 0, len(channels))
	var first error
	for _, ch := range channels {
		err := ch.Close()
		if err != nil && first == nil {
			first = err
		}
		results = append(results, GroupResult{ID: ch.ID(), Err: err})
	}
	return newGroupFuture(results, first)
}

// GroupResult 保存单个 Channel 批量操作的结果。
type GroupResult struct {
	ID  transport.ChannelID
	Err error
}

// GroupFuture 是批量 Channel 操作的结果视图。
type GroupFuture struct {
	future  Future
	results []GroupResult
}

func (f *GroupFuture) Done() <-chan struct{} {
	return f.future.Done()
}

func (f *GroupFuture) IsDone() bool {
	return f.future.IsDone()
}

func (f *GroupFuture) IsSuccess() bool {
	return f.future.IsSuccess()
}

func (f *GroupFuture) Err() error {
	return f.future.Err()
}

func (f *GroupFuture) Cause() error {
	return f.future.Cause()
}

func (f *GroupFuture) Await() error {
	return f.future.Await()
}

func (f *GroupFuture) AwaitTimeout(timeout time.Duration) (bool, error) {
	return f.future.AwaitTimeout(timeout)
}

func (f *GroupFuture) AddListener(listener func(Future)) Future {
	return f.future.AddListener(listener)
}

func (f *GroupFuture) AddListenerHandle(listener func(Future)) FutureListenerHandle {
	return f.future.AddListenerHandle(listener)
}

func (f *GroupFuture) RemoveListener(handle FutureListenerHandle) bool {
	return f.future.RemoveListener(handle)
}

func (f *GroupFuture) Results() []GroupResult {
	out := make([]GroupResult, len(f.results))
	copy(out, f.results)
	return out
}

func newGroupFuture(results []GroupResult, err error) *GroupFuture {
	promise := NewPromise()
	if err != nil {
		promise.SetFailure(err)
	} else {
		promise.SetSuccess()
	}
	return &GroupFuture{future: promise, results: results}
}

// GroupHandler 按 Channel 生命周期自动维护 Group 成员。
type GroupHandler struct {
	group *Group
}

func NewGroupHandler(group *Group) *GroupHandler {
	return &GroupHandler{group: group}
}

func (h *GroupHandler) ChannelActive(ctx *HandlerContext) {
	if h.group != nil {
		h.group.Add(ctx.Channel())
	}
	ctx.FireChannelActive()
}

func (h *GroupHandler) ChannelInactive(ctx *HandlerContext) {
	if h.group != nil {
		h.group.Remove(ctx.Channel().ID())
	}
	ctx.FireChannelInactive()
}
