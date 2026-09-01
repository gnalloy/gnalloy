package channel

import (
	"sync/atomic"

	"gnalloy.org/gnalloy/buffer"
	"gnalloy.org/gnalloy/timer"
	"gnalloy.org/gnalloy/transport"
)

const defaultReadBufferSize = 4096

const (
	maxGatheringBuffers = 16
)

// FDReadWriter 抽象 readiness 模型下的非阻塞 fd 读写。
// again 表示底层返回 EAGAIN/EWOULDBLOCK，调用方必须停止本轮读写。
type FDReadWriter interface {
	Read(fd transport.FDRef, dst []byte) (n int, again bool, err error)
	Write(fd transport.FDRef, src []byte) (n int, again bool, err error)
	Close(fd transport.FDRef) error
}

// FDVectorWriter 是支持 gathering write 的 readiness 后端可选能力。
type FDVectorWriter interface {
	Writev(fd transport.FDRef, src [][]byte) (n int, again bool, err error)
}

// UnsafeConfig 描述底层 I/O 分界线创建参数。
type UnsafeConfig struct {
	ID                   transport.ChannelID
	FD                   transport.FDRef
	Allocator            buffer.Allocator
	Poller               transport.Poller
	ReadWriter           FDReadWriter
	FileRegionWriter     FileRegionWriter
	CloseHook            func(*Unsafe)
	ReadBufferSize       int
	WriteBufferWatermark transport.WriteBufferWatermark
	WriteHighWatermark   int
	WriteLowWatermark    int
	FixedBuffers         bool
	Timer                *timer.Wheel
}

type eventLoopTailSubmitter interface {
	SubmitAfterBatch(transport.Task) error
}

// Unsafe 是底层 I/O 事件与业务 Pipeline 的分界线。
type Unsafe struct {
	ch               *LocalChannel
	fd               transport.FDRef
	poller           transport.Poller
	rw               FDReadWriter
	fileRegionWriter FileRegionWriter
	closeHook        func(*Unsafe)
	readBufferSize   int
	readBufferSizer  readBufferSizer
	fixedBuffers     bool
	registered       atomic.Bool
	closed           atomic.Bool
	inactiveFired    atomic.Bool

	outHead        *outboundEntry
	outTail        *outboundEntry
	outFree        *outboundEntry
	outboundBytes  atomic.Int64
	eventExecutor  atomic.Value
	readPending    bool
	writePending   bool
	writeInterest  bool
	readCallback   bool
	deferredFlush  bool
	flushPending   bool
	flushScheduled bool
	flushScheduler eventLoopTailSubmitter
	vectorWriter   FDVectorWriter
	writeBatch     []buffer.ByteBuf
	writeSlices    [][]byte
	flushWaiters   []*DefaultPromise
	closePromise   *DefaultPromise

	autoRead                 atomic.Bool
	writeSpinCount           atomic.Int64
	cachedMaxMessagesPerRead atomic.Int64
	cachedFlushStrategy      atomic.Int64
	writeHighWatermark       int64
	writeLowWatermark        int64
	writable                 atomic.Bool
}

type outboundEntry struct {
	buf     buffer.ByteBuf
	region  FileRegion
	promise Promise
	next    *outboundEntry
}

func NewUnsafeChannel(cfg UnsafeConfig) (*LocalChannel, *Unsafe) {
	readBufferSize := cfg.ReadBufferSize
	if readBufferSize <= 0 {
		readBufferSize = defaultReadBufferSize
	}
	watermark := cfg.WriteBufferWatermark
	if watermark.High == 0 && watermark.Low == 0 && (cfg.WriteHighWatermark != 0 || cfg.WriteLowWatermark != 0) {
		watermark = transport.WriteBufferWatermark{Low: cfg.WriteLowWatermark, High: cfg.WriteHighWatermark}
	}
	watermark = transport.NormalizeWriteBufferWatermark(watermark)
	u := &Unsafe{
		fd:                 cfg.FD,
		poller:             cfg.Poller,
		rw:                 cfg.ReadWriter,
		fileRegionWriter:   cfg.FileRegionWriter,
		closeHook:          cfg.CloseHook,
		readBufferSize:     readBufferSize,
		readBufferSizer:    newReadBufferSizer(readBufferSize),
		fixedBuffers:       cfg.FixedBuffers,
		vectorWriter:       vectorWriterOf(cfg.ReadWriter),
		closePromise:       NewPromise(),
		writeHighWatermark: int64(watermark.High),
		writeLowWatermark:  int64(watermark.Low),
	}
	u.writable.Store(true)
	u.ch = NewLocalChannelWithTimer(cfg.ID, cfg.Allocator, u, cfg.Timer)
	u.bindOptionCache()
	OptionReadBufferSize.Set(u.ch.Options(), readBufferSize)
	OptionWriteBufferWatermark.Set(u.ch.Options(), watermark)
	OptionWriteSpinCount.Set(u.ch.Options(), OptionWriteSpinCount.Get(u.ch.Options()))
	OptionMaxMessagesPerRead.Set(u.ch.Options(), OptionMaxMessagesPerRead.Get(u.ch.Options()))
	OptionFlushStrategy.Set(u.ch.Options(), OptionFlushStrategy.Get(u.ch.Options()))
	return u.ch, u
}

func (u *Unsafe) ID() transport.ChannelID {
	return u.ch.ID()
}

func (u *Unsafe) FD() transport.FDRef {
	return u.fd
}

func (u *Unsafe) Channel() *LocalChannel {
	return u.ch
}

func (u *Unsafe) IsWritable() bool {
	return u.writable.Load()
}

func (u *Unsafe) PendingOutboundBytes() int64 {
	return u.outboundBytes.Load()
}

func (u *Unsafe) WriteBufferWatermark() transport.WriteBufferWatermark {
	return transport.WriteBufferWatermark{Low: int(u.writeLowWatermark), High: int(u.writeHighWatermark)}
}

func (u *Unsafe) bindOptionCache() {
	if u == nil || u.ch == nil {
		return
	}
	options := u.ch.Options()
	options.observe(u.applyOptionUpdate)
	u.cacheAutoRead(OptionAutoRead.Get(options), true)
	u.cacheWriteSpinCount(OptionWriteSpinCount.Get(options), true)
	u.cacheMaxMessagesPerRead(OptionMaxMessagesPerRead.Get(options), true)
}

func (u *Unsafe) applyOptionUpdate(key optionKeyID, value any, present bool) {
	switch key {
	case OptionReadBufferSize.name:
		u.cacheReadBufferSize(value, present)
	case OptionAutoRead.name:
		u.cacheAutoRead(value, present)
	case OptionWriteSpinCount.name:
		u.cacheWriteSpinCount(value, present)
	case OptionMaxMessagesPerRead.name:
		u.cacheMaxMessagesPerRead(value, present)
	case OptionFlushStrategy.name:
		u.cacheFlushStrategy(value, present)
	}
}

func (u *Unsafe) cacheReadBufferSize(value any, present bool) {
	v := defaultReadBufferSize
	if present {
		if typed, ok := value.(int); ok && typed > 0 {
			v = typed
		}
	}
	u.readBufferSize = v
	u.readBufferSizer.reset(v)
}

func (u *Unsafe) cacheAutoRead(value any, present bool) {
	if !present {
		u.autoRead.Store(OptionAutoRead.Default())
		return
	}
	v, ok := value.(bool)
	if !ok {
		v = OptionAutoRead.Default()
	}
	u.autoRead.Store(v)
}

func (u *Unsafe) cacheWriteSpinCount(value any, present bool) {
	v := OptionWriteSpinCount.Default()
	if present {
		if typed, ok := value.(int); ok {
			v = typed
		}
	}
	if v <= 0 {
		v = 1
	}
	u.writeSpinCount.Store(int64(v))
}

func (u *Unsafe) cacheMaxMessagesPerRead(value any, present bool) {
	v := OptionMaxMessagesPerRead.Default()
	if present {
		if typed, ok := value.(int); ok {
			v = typed
		}
	}
	if v <= 0 {
		v = 1
	}
	u.cachedMaxMessagesPerRead.Store(int64(v))
}

func (u *Unsafe) cacheFlushStrategy(value any, present bool) {
	v := OptionFlushStrategy.Default()
	if present {
		if typed, ok := value.(FlushStrategy); ok {
			v = typed
		}
	}
	switch v {
	case FlushImmediate, FlushOnReadComplete, FlushOnEventLoopBatch:
	default:
		v = OptionFlushStrategy.Default()
	}
	u.cachedFlushStrategy.Store(int64(v))
}

func vectorWriterOf(rw FDReadWriter) FDVectorWriter {
	vector, _ := rw.(FDVectorWriter)
	return vector
}
