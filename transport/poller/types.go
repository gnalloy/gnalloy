package poller

import (
	"errors"

	"gnalloy.org/gnalloy/buffer"
)

var (
	ErrUnsupportedPoller       = errors.New("gnalloy/transport/poller: unsupported poller")
	ErrClosedPoller            = errors.New("gnalloy/transport/poller: poller closed")
	ErrInvalidFD               = errors.New("gnalloy/transport/poller: invalid file descriptor")
	ErrInvalidIORequest        = errors.New("gnalloy/transport/poller: invalid io request")
	ErrSubmissionQueueFull     = errors.New("gnalloy/transport/poller: submission queue full")
	ErrCompletionQueueOverflow = errors.New("gnalloy/transport/poller: completion queue overflow")
)

type EventLoopID uint32
type EventLoopGroupID uint32
type ChannelID uint64
type OpID uint64
type TaskID uint64

// FDRef 用 generation 区分被操作系统复用的描述符。
// FD 在 Unix 表示 fd，在 Windows 表示 SOCKET/HANDLE 的整数值。
type FDRef struct {
	FD         int
	Generation uint32
}

func (f FDRef) Valid() bool {
	return f.FD >= 0
}

type Model uint8

const (
	Readiness Model = iota
	Completion
)

type IOOp uint8

const (
	OpAccept IOOp = iota
	OpRead
	OpWrite
	OpClose
	OpWakeup
)

type SocketFamily uint8

const (
	SocketFamilyIPv4 SocketFamily = iota + 1
	SocketFamilyIPv6
)

// SocketAddress 是 completion poller 使用的无分配 socket 地址。
type SocketAddress struct {
	Family SocketFamily
	IP     [16]byte
	Port   int
	ZoneID uint32
}

func (a SocketAddress) Valid() bool {
	return a.Family == SocketFamilyIPv4 || a.Family == SocketFamilyIPv6
}

type ReadyMask uint32

const (
	ReadyRead ReadyMask = 1 << iota
	ReadyWrite
	ReadyHangup
	ReadyError
	// ReadyLevelTriggered 请求 readiness 后端使用水平触发；不支持的后端可忽略。
	ReadyLevelTriggered
)

type BackendKind uint8

const (
	BackendMemory BackendKind = iota
	BackendStd
	BackendEpoll
	BackendKqueue
	BackendIOUring
	BackendIOCP
)

type Event struct {
	Model      Model
	Op         IOOp
	Ready      ReadyMask
	FD         FDRef
	AcceptedFD FDRef
	ChannelID  ChannelID
	OpID       OpID
	Buf        buffer.ByteBuf
	Bufs       []buffer.ByteBuf
	Addr       SocketAddress
	N          int
	Err        error
	More       bool
}

type IORequest struct {
	Op         IOOp
	FD         FDRef
	AcceptedFD FDRef
	ChannelID  ChannelID
	OpID       OpID
	Buf        buffer.ByteBuf
	Bufs       []buffer.ByteBuf
	// TransferBufferOwnership 表示 Submit 成功后 Poller 接管 Buf/Bufs 当前引用。
	// 默认 false 会保持额外 Retain，兼容调用方继续持有原引用的路径。
	TransferBufferOwnership bool
	Ready                   ReadyMask
	Addr                    SocketAddress
	Datagram                bool

	// UseFixedBuffer 仅对支持 registered buffers 的 completion 后端生效。
	UseFixedBuffer   bool
	FixedBufferIndex uint16
}

// Poller 是所有平台 I/O 后端必须实现的最小契约。
// readiness 后端返回可读可写状态，completion 后端返回已完成的 I/O 请求。
type Poller interface {
	Model() Model
	Backend() BackendKind
	Register(fd FDRef, ch ChannelID, interest ReadyMask) error
	Modify(fd FDRef, interest ReadyMask) error
	Deregister(fd FDRef) error
	Submit(req IORequest) error
	Poll(dst []Event, timeoutMillis int) (int, error)
	Wakeup() error
	Close() error
}

// BatchSubmitter 是 completion poller 的可选批量提交能力，不接收 OpWakeup。
// SubmitBatch 成功后才接管 TransferBufferOwnership 标记的 ByteBuf；失败时调用方仍持有原引用。
type BatchSubmitter interface {
	SubmitBatch(reqs []IORequest) error
}

// BufferRegistrar 是支持 registered buffers 的 completion poller 可选能力。
type BufferRegistrar interface {
	RegisterBuffers(buffers [][]byte) error
	UnregisterBuffers() error
}

type Config struct {
	Backend BackendKind

	// Entries 控制 completion 后端的 ring 深度，0 表示使用后端默认值。
	Entries uint32

	// SQPoll 仅对 Linux io_uring 生效，会让内核线程轮询提交队列。
	SQPoll bool

	// SQPollAffinity 为 true 时把 SQPOLL 内核线程绑定到 SQPollCPU。
	SQPollAffinity bool
	SQPollCPU      int

	// SQPollIdleMillis 控制 SQPOLL 空闲退出时间，0 表示后端默认值。
	SQPollIdleMillis uint32

	// MultishotAccept 仅对 Linux io_uring 生效，一个 accept SQE 可产生多个 CQE。
	MultishotAccept bool
}

func CompletionReady(op IOOp) ReadyMask {
	switch op {
	case OpRead, OpAccept:
		return ReadyRead
	case OpWrite:
		return ReadyWrite
	default:
		return 0
	}
}

func ReadinessOp(ready ReadyMask) IOOp {
	if ready&(ReadyHangup|ReadyError) != 0 {
		return OpClose
	}
	if ready&ReadyRead != 0 {
		return OpRead
	}
	if ready&ReadyWrite != 0 {
		return OpWrite
	}
	return OpWakeup
}
