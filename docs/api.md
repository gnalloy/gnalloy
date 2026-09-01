# API Reference

[简体中文](api.zh-CN.md) | [Docs Index](README.md)

This inventory is generated from `go doc -short` for the packages in this repository. It is a quick public-surface map; source files and tests remain the authority for exact semantics.

## Packages

### `gnalloy.org/gnalloy`

Package name: `gnalloy`

```text
(no exported API reported by go doc)
```

### `gnalloy.org/gnalloy/bootstrap`

Package name: `bootstrap`

```text
var ErrMissingGroup = errors.New("gnalloy/bootstrap: boss group and worker group are required") ...
func DefaultChannelFactory(cfg channel.UnsafeConfig) (channel.Channel, *channel.Unsafe, error)
type ChannelFactory func(cfg channel.UnsafeConfig) (channel.Channel, *channel.Unsafe, error)
type ChannelInitializer = ChildInitializer
type ChildInitializer func(ch channel.Channel) error
type ClientConfig struct{ ... }
type ClientTransport interface{ ... }
type Dialer struct{ ... }
    func NewDialer() *Dialer
type Server interface{ ... }
type ServerBootstrap struct{ ... }
    func NewServerBootstrap() *ServerBootstrap
type ServerConfig struct{ ... }
type ServerTransport interface{ ... }
```

### `gnalloy.org/gnalloy/buffer`

Package name: `buffer`

```text
var ErrReleasedBuffer = errors.New("gnalloy/buffer: buffer already released") ...
func ActiveLeakCount() int
func ByteBufCompare(a ByteBuf, b ByteBuf) int
func ByteBufEqual(a ByteBuf, b ByteBuf) bool
func ByteBufHashCode(buf ByteBuf) uint32
func BytesBefore(buf ByteBuf, fromIndex int, toIndex int, needle []byte) int
func ContiguousReadableBytes(src ByteBuf) ([]byte, bool)
func CopyReadableBytes(dst []byte, src ByteBuf) int
func EnableLeakDetection(enabled bool)
func FixedBufferIndex(buf ByteBuf) (uint16, bool)
func ForEachReadableSlice(src ByteBuf, fn func([]byte) bool) bool
func HexDump(buf ByteBuf) string
func HexDumpRange(buf ByteBuf, index int, length int) string
func IndexOfByte(buf ByteBuf, fromIndex int, toIndex int, value byte) int
func IndexOfBytes(buf ByteBuf, fromIndex int, toIndex int, needle []byte) int
func ReadableString(src ByteBuf) string
func ReadableStringAt(src *CompositeByteBuf, index int, length int) (string, error)
func ResetLeakDetection()
func WriteReadableBytes(dst ByteBuf, src ByteBuf) error
type Allocator interface{ ... }
    func NewMmapAllocator(MmapAllocatorConfig) (Allocator, error)
type AllocatorStats struct{ ... }
type ByteBuf interface{ ... }
    func NewOwnedBuffer(data []byte, release func([]byte)) ByteBuf
    func NewSharedBuffer(data []byte) ByteBuf
type ByteOrder uint8
    const BigEndian ByteOrder = iota ...
type CompositeByteBuf struct{ ... }
    func NewCompositeByteBuf() *CompositeByteBuf
type DirectByteBuf struct{ ... }
    func NewHeapBuffer(size int) *DirectByteBuf
type FixedBuffer interface{ ... }
type FixedBufferProvider interface{ ... }
type HeapAllocator struct{ ... }
    func NewHeapAllocator() *HeapAllocator
type LeakDetector struct{ ... }
type LeakRecord struct{ ... }
    func ActiveLeaks() []LeakRecord
type MmapAllocatorConfig struct{ ... }
type PooledAllocator struct{ ... }
    func NewPooledAllocator(cfg PooledAllocatorConfig) (*PooledAllocator, error)
type PooledAllocatorConfig struct{ ... }
type PooledAllocatorStats struct{ ... }
type PooledSizeClassStats struct{ ... }
type StatAllocator interface{ ... }
```

### `gnalloy.org/gnalloy/channel`

Package name: `channel`

```text
var ErrDuplicateHandler = errors.New("gnalloy/channel: duplicate handler name") ...
var OptionAutoRead = NewChannelOption("AUTO_READ", true) ...
func NewUnsafeChannel(cfg UnsafeConfig) (*LocalChannel, *Unsafe)
type AttributeAssignment struct{ ... }
type AttributeKey[T any] struct{ ... }
    func NewAttributeKey[T any](name string) AttributeKey[T]
type AttributeMap struct{ ... }
    func NewAttributeMap() *AttributeMap
type Channel interface{ ... }
type ChannelActiveHandler interface{ ... }
type ChannelInactiveHandler interface{ ... }
type ChannelOption[T any] struct{ ... }
    func NewChannelOption[T any](name string, defaultValue T) ChannelOption[T]
type ChannelOptionAssignment struct{ ... }
type ChannelOptions struct{ ... }
    func NewChannelOptions() *ChannelOptions
type ChannelReadCompleteHandler interface{ ... }
type ChannelReadHandler interface{ ... }
type ChannelRegisteredHandler interface{ ... }
type ChannelUnregisteredHandler interface{ ... }
type ChannelWritabilityChangedHandler interface{ ... }
type CloseHandler interface{ ... }
type DefaultFileRegion struct{ ... }
    func NewFileRegion(reader io.ReaderAt, offset int64, count int64) (*DefaultFileRegion, error)
type DefaultPromise struct{ ... }
    func NewPromise() *DefaultPromise
    func NewPromiseWithExecutor(executor FutureListenerExecutor) *DefaultPromise
type ExceptionCaughtHandler interface{ ... }
type FDReadWriter interface{ ... }
type FDVectorWriter interface{ ... }
type FileRegion interface{ ... }
type FileRegionEncoder struct{ ... }
    func NewFileRegionEncoder(chunkSize int) (*FileRegionEncoder, error)
type FileRegionWriter interface{ ... }
type FlushCompleteHandler interface{ ... }
type FlushHandler interface{ ... }
type Future interface{ ... }
    func FailedFuture(err error) Future
    func SucceededFuture() Future
type FutureListenerExecutor interface{ ... }
type FutureListenerHandle struct{ ... }
type FutureOutboundSink interface{ ... }
type Group struct{ ... }
    func NewGroup() *Group
type GroupFuture struct{ ... }
type GroupHandler struct{ ... }
    func NewGroupHandler(group *Group) *GroupHandler
type GroupResult struct{ ... }
type Handler any
type HandlerAddedHandler interface{ ... }
type HandlerContext struct{ ... }
type HandlerRemovedHandler interface{ ... }
type LocalChannel struct{ ... }
    func NewLocalChannel(id transport.ChannelID, alloc buffer.Allocator, sink OutboundSink) *LocalChannel
    func NewLocalChannelWithTimer(id transport.ChannelID, alloc buffer.Allocator, sink OutboundSink, ...) *LocalChannel
type OutboundBufferSink interface{ ... }
type OutboundSink interface{ ... }
type Pipeline struct{ ... }
    func NewPipeline(ch Channel, sink OutboundSink) *Pipeline
type Promise interface{ ... }
type ReadSink interface{ ... }
type Unsafe struct{ ... }
type UnsafeConfig struct{ ... }
type UserEventTriggeredHandler interface{ ... }
type WritabilitySink interface{ ... }
type WriteHandler interface{ ... }
```

### `gnalloy.org/gnalloy/channel/embedded`

Package name: `embedded`

```text
var ErrClosed = errors.New("gnalloy/channel/embedded: channel closed")
type Config struct{ ... }
type EmbeddedChannel struct{ ... }
    func New(handlers ...channel.Handler) (*EmbeddedChannel, error)
    func NewWithConfig(cfg Config) (*EmbeddedChannel, error)
type HandlerSpec struct{ ... }
```

### `gnalloy.org/gnalloy/channel/pool`

Package name: `pool`

```text
var ErrInvalidConfig = errors.New("gnalloy/channel/pool: invalid config") ...
type ChannelAcquiredHandler interface{ ... }
type ChannelCreatedHandler interface{ ... }
type ChannelPool interface{ ... }
type ChannelReleasedHandler interface{ ... }
type Config struct{ ... }
type Factory func(context.Context) (channel.Channel, error)
type FixedConfig struct{ ... }
type FixedPool struct{ ... }
    func NewFixed(cfg FixedConfig) (*FixedPool, error)
type FixedStats struct{ ... }
type HealthCheck func(channel.Channel) bool
type LifecycleHandler interface{ ... }
type Map[K comparable] struct{ ... }
    func NewMap[K comparable](factory MapFactory[K]) (*Map[K], error)
type MapFactory[K comparable] func(K) (ChannelPool, error)
type Pool struct{ ... }
    func New(cfg Config) (*Pool, error)
type SimpleConfig = Config
type SimplePool = Pool
    func NewSimple(cfg SimpleConfig) (*SimplePool, error)
```

### `gnalloy.org/gnalloy/codec`

Package name: `codec`

```text
var Base64StandardDialect = Base64Dialect{ ... } ...
var ErrInvalidLengthField = errors.New("gnalloy/codec: invalid length field") ...
var LineSeparatorUnix = LineSeparator{ ... } ...
func LineDelimiters() [][]byte
func NulDelimiter() [][]byte
func WriteOutboundBuffer(ctx *channel.HandlerContext, out buffer.ByteBuf) error
type Base64Decoder struct{ ... }
    func NewBase64Decoder() *Base64Decoder
    func NewBase64DecoderWithDialect(dialect Base64Dialect) *Base64Decoder
type Base64Dialect struct{ ... }
type Base64Encoder struct{ ... }
    func NewBase64Encoder() *Base64Encoder
    func NewBase64EncoderWithDialect(dialect Base64Dialect) *Base64Encoder
type ByteDecoder interface{ ... }
type ByteEncoder interface{ ... }
type ByteListDecoder interface{ ... }
type ByteSliceDecoder struct{}
    func NewByteSliceDecoder() *ByteSliceDecoder
type ByteSliceEncoder struct{}
    func NewByteSliceEncoder() *ByteSliceEncoder
type ByteToMessageCodec struct{ ... }
    func NewByteToMessageCodec(decoder ByteDecoder, encoder ByteEncoder) *ByteToMessageCodec
type ByteToMessageDecoder struct{ ... }
    func NewByteToMessageDecoder(decoder ByteDecoder) *ByteToMessageDecoder
    func NewByteToMessageListDecoder(decoder ByteListDecoder) *ByteToMessageDecoder
type ChunkedByteBufInput struct{ ... }
    func NewChunkedByteBufInput(buf buffer.ByteBuf, chunkSize int) (*ChunkedByteBufInput, error)
type ChunkedInput interface{ ... }
type ChunkedWriteHandler struct{}
    func NewChunkedWriteHandler() *ChunkedWriteHandler
type CombinedChannelDuplexHandler struct{ ... }
    func NewCombinedChannelDuplexHandler(inbound channel.Handler, outbound channel.Handler) *CombinedChannelDuplexHandler
type Cumulator interface{ ... }
    var CompositeCumulator Cumulator = CumulatorFunc(compositeCumulate) ...
type CumulatorFunc func(ctx *channel.HandlerContext, cumulation *buffer.CompositeByteBuf, ...) (*buffer.CompositeByteBuf, error)
type DelimiterBasedFrameDecoder struct{ ... }
    func NewDelimiterBasedFrameDecoder(maxFrameLength int, stripDelimiter bool, failFast bool, delimiters ...[]byte) (*DelimiterBasedFrameDecoder, error)
type DelimiterBasedFrameEncoder struct{ ... }
    func NewDelimiterBasedFrameEncoder(delimiter []byte) (*DelimiterBasedFrameEncoder, error)
type FixedLengthFrameDecoder struct{ ... }
    func NewFixedLengthFrameDecoder(frameLength int) (*FixedLengthFrameDecoder, error)
type FixedLengthFrameEncoder struct{ ... }
    func NewFixedLengthFrameEncoder(frameLength int) (*FixedLengthFrameEncoder, error)
type JsonObjectDecoder struct{ ... }
    func NewJsonObjectDecoder(maxObjectLength int) (*JsonObjectDecoder, error)
type LengthFieldBasedFrameDecoder struct{ ... }
    func NewLengthFieldBasedFrameDecoder(maxFrameLength int, lengthFieldOffset int, lengthFieldLength int, ...) (*LengthFieldBasedFrameDecoder, error)
    func NewLengthFieldBasedFrameDecoderWithOptions(maxFrameLength int, lengthFieldOffset int, lengthFieldLength int, ...) (*LengthFieldBasedFrameDecoder, error)
type LengthFieldPrepender struct{ ... }
    func NewLengthFieldPrepender(lengthFieldLength int, order buffer.ByteOrder) (*LengthFieldPrepender, error)
    func NewLengthFieldPrependerWithOptions(lengthFieldLength int, lengthAdjustment int, ...) (*LengthFieldPrepender, error)
type LineBasedFrameDecoder struct{ ... }
    func NewLineBasedFrameDecoder(maxLength int) (*LineBasedFrameDecoder, error)
    func NewLineBasedFrameDecoderWithOptions(maxLength int, stripDelimiter bool, failFast bool) (*LineBasedFrameDecoder, error)
type LineEncoder struct{ ... }
    func NewLineEncoder(separator LineSeparator) *LineEncoder
type LineSeparator struct{ ... }
    func NewLineSeparator(delimiter []byte) (LineSeparator, error)
type MessageDecoder interface{ ... }
type MessageEncoder interface{ ... }
type MessageList struct{ ... }
type MessageToByteEncoder struct{ ... }
    func NewMessageToByteEncoder(encoder ByteEncoder) *MessageToByteEncoder
    func NewMessageToByteEncoderFunc(accept func(any) bool, estimate func(*channel.HandlerContext, any) int, ...) *MessageToByteEncoder
type MessageToMessageCodec struct{ ... }
    func NewMessageToMessageCodec(decoder MessageDecoder, encoder MessageEncoder) *MessageToMessageCodec
type MessageToMessageDecoder struct{ ... }
    func NewMessageToMessageDecoder(decoder MessageDecoder) *MessageToMessageDecoder
    func NewMessageToMessageDecoderFunc(accept func(any) bool, ...) *MessageToMessageDecoder
type MessageToMessageEncoder struct{ ... }
    func NewMessageToMessageEncoder(encoder MessageEncoder) *MessageToMessageEncoder
    func NewMessageToMessageEncoderFunc(accept func(any) bool, ...) *MessageToMessageEncoder
type ReplayBuffer struct{ ... }
type ReplayDecoder interface{ ... }
type ReplayingDecoder struct{ ... }
    func NewReplayingDecoder(decoder ReplayDecoder) *ReplayingDecoder
type StringDecoder struct{}
    func NewStringDecoder() *StringDecoder
type StringEncoder struct{}
    func NewStringEncoder() *StringEncoder
type TooLongFrameError struct{ ... }
    func NewTooLongFrameError(frameLength int, maxFrameLength int, discarded int) TooLongFrameError
```

### `gnalloy.org/gnalloy/message`

Package name: `message`

```text
func Release(msg any)
func ReleaseAll(messages []any)
```

### `gnalloy.org/gnalloy/queue`

Package name: `queue`

```text
type MPSC[T any] struct{ ... }
    func NewMPSC[T any](capacity uint64) *MPSC[T]
```

### `gnalloy.org/gnalloy/timer`

Package name: `timer`

```text
var ErrInvalidTick = errors.New("gnalloy/timer: invalid tick duration") ...
type Callback interface{ ... }
type CallbackFunc func(ctx Context, task *Task)
type Context interface{ ... }
type State uint8
    const Pending State = iota ...
type Task struct{ ... }
type Wheel struct{ ... }
    func NewWheel(tickMillis int64, wheelSize uint64, startMillis int64) (*Wheel, error)
```

### `gnalloy.org/gnalloy/transport`

Package name: `transport`

```text
const PollerReadiness = poller.Readiness ...
const DefaultWriteHighWatermark = 64 * 1024 ...
var ErrEventLoopClosed = errors.New("gnalloy/transport: event loop closed") ...
var ErrInvalidEventLoopGroup = errors.New("gnalloy/transport: invalid event loop group") ...
var ErrInvalidEventLoopLocal = errors.New("gnalloy/transport: invalid event loop local") ...
var ErrUnsupportedPoller = poller.ErrUnsupportedPoller ...
func AllocatorStatsForEventLoop(id EventLoopID, alloc buffer.Allocator) buffer.AllocatorStats
type BackendKind = poller.BackendKind
    func DefaultBackend() BackendKind
type BatchSubmitter = poller.BatchSubmitter
type BufferRegistrar = poller.BufferRegistrar
type ChannelID = poller.ChannelID
type ClockMillis func() int64
type Config = poller.Config
type EventHandler interface{ ... }
type EventLoop struct{ ... }
    func NewEventLoop(cfg EventLoopConfig) (*EventLoop, error)
type EventLoopConfig struct{ ... }
type EventLoopGroup struct{ ... }
    func NewEventLoopGroup(cfg EventLoopGroupConfig) (*EventLoopGroup, error)
type EventLoopGroupConfig struct{ ... }
type EventLoopGroupID = poller.EventLoopGroupID
type EventLoopID = poller.EventLoopID
type EventLoopLocal[T any] struct{ ... }
    func NewEventLoopLocal[T any](factory EventLoopLocalFactory[T]) (*EventLoopLocal[T], error)
type EventLoopLocalFactory[T any] func(loop *EventLoop) (T, error)
type EventLoopLocalSnapshot[T any] struct{ ... }
type FDRef = poller.FDRef
type IOOp = poller.IOOp
type IORequest = poller.IORequest
type OpID = poller.OpID
type PollEvent = poller.Event
type Poller = poller.Poller
    func NewPoller(cfg Config) (Poller, error)
type PollerFactory func(index int) (Poller, error)
type PollerModel = poller.Model
type ReadyMask = poller.ReadyMask
type RegisterErrorHandler func(loop *EventLoop, handler EventHandler, err error)
type RegisterHook func(loop *EventLoop, handler EventHandler) error
type SocketAddress = poller.SocketAddress
type SocketFamily = poller.SocketFamily
type Task func()
type TaskID = poller.TaskID
type WriteBufferWatermark struct{ ... }
    func DefaultWriteBufferWatermark() WriteBufferWatermark
    func NormalizeWriteBufferWatermark(w WriteBufferWatermark) WriteBufferWatermark
```

### `gnalloy.org/gnalloy/transport/poller`

Package name: `poller`

```text
var ErrUnsupportedPoller = errors.New("gnalloy/transport/poller: unsupported poller") ...
type BackendKind uint8
    const BackendMemory BackendKind = iota ...
type BatchSubmitter interface{ ... }
type BufferRegistrar interface{ ... }
type ChannelID uint64
type Config struct{ ... }
type Event struct{ ... }
type EventLoopGroupID uint32
type EventLoopID uint32
type FDRef struct{ ... }
type IOOp uint8
    const OpAccept IOOp = iota ...
    func ReadinessOp(ready ReadyMask) IOOp
type IORequest struct{ ... }
type Model uint8
    const Readiness Model = iota ...
type OpID uint64
type Poller interface{ ... }
type ReadyMask uint32
    const ReadyRead ReadyMask = 1 << iota ...
    func CompletionReady(op IOOp) ReadyMask
type SocketAddress struct{ ... }
type SocketFamily uint8
    const SocketFamilyIPv4 SocketFamily = iota + 1 ...
type TaskID uint64
```

### `gnalloy.org/gnalloy/transport/poller/epoll`

Package name: `epoll`

```text
(no exported API reported by go doc)
```

### `gnalloy.org/gnalloy/transport/poller/iocp`

Package name: `iocp`

```text
func New() (poller.Poller, error)
type Poller struct{ ... }
```

### `gnalloy.org/gnalloy/transport/poller/iouring`

Package name: `iouring`

```text
(no exported API reported by go doc)
```

### `gnalloy.org/gnalloy/transport/poller/kqueue`

Package name: `kqueue`

```text
(no exported API reported by go doc)
```

### `gnalloy.org/gnalloy/transport/poller/memory`

Package name: `memory`

```text
type Poller struct{ ... }
    func New() *Poller
```

### `gnalloy.org/gnalloy/transport/poller/std`

Package name: `std`

```text
type Poller struct{ ... }
    func New() *Poller
```

### `gnalloy.org/gnalloy/validation/platformmatrix`

Package name: `platformmatrix`

```text
var ErrInvalidMatrix = errors.New("gnalloy/validation/platformmatrix: invalid matrix")
type Backends struct{ ... }
type Gate struct{ ... }
type Matrix struct{ ... }
    func Load(r io.Reader) (Matrix, error)
type Target struct{ ... }
```
