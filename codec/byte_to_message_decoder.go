package codec

import (
	"gnalloy.org/gnalloy/buffer"
	"gnalloy.org/gnalloy/channel"
	"gnalloy.org/gnalloy/message"
)

// ByteDecoder 是 ByteToMessageDecoder 的实际解码策略。
// Decode 必须在产出消息时推进 reader index，否则会触发 ErrDecoderNoProgress。
type ByteDecoder interface {
	Decode(ctx *channel.HandlerContext, in *buffer.CompositeByteBuf) (any, error)
}

// ByteListDecoder 是支持一次解码产出多个消息的字节流解码策略。
type ByteListDecoder interface {
	DecodeBytes(ctx *channel.HandlerContext, in *buffer.CompositeByteBuf, out *MessageList) error
}

// Cumulator 决定入站 ByteBuf 如何并入半包累积区。
type Cumulator interface {
	Cumulate(ctx *channel.HandlerContext, cumulation *buffer.CompositeByteBuf, in buffer.ByteBuf) (*buffer.CompositeByteBuf, error)
}

// CumulatorFunc 允许用函数实现 Cumulator。
type CumulatorFunc func(ctx *channel.HandlerContext, cumulation *buffer.CompositeByteBuf, in buffer.ByteBuf) (*buffer.CompositeByteBuf, error)

func (f CumulatorFunc) Cumulate(ctx *channel.HandlerContext, cumulation *buffer.CompositeByteBuf, in buffer.ByteBuf) (*buffer.CompositeByteBuf, error) {
	return f(ctx, cumulation, in)
}

var (
	// CompositeCumulator 默认使用 CompositeByteBuf 累积半包，跨 buffer 切帧不复制 payload。
	CompositeCumulator Cumulator = CumulatorFunc(compositeCumulate)
	// MergeCumulator 将未读半包和新输入合并到连续 ByteBuf，供必须连续内存的 codec 使用。
	MergeCumulator Cumulator = CumulatorFunc(mergeCumulate)
)

// ByteToMessageDecoder 对齐 Netty 的流式解码基类，负责 TCP 半包累积、循环解码和生命周期释放。
type ByteToMessageDecoder struct {
	decoder     ByteDecoder
	listDecoder ByteListDecoder
	cumulator   Cumulator
	cumulation  *buffer.CompositeByteBuf
	out         MessageList
}

func NewByteToMessageDecoder(decoder ByteDecoder) *ByteToMessageDecoder {
	return &ByteToMessageDecoder{decoder: decoder, cumulator: CompositeCumulator}
}

func NewByteToMessageListDecoder(decoder ByteListDecoder) *ByteToMessageDecoder {
	return &ByteToMessageDecoder{listDecoder: decoder, cumulator: CompositeCumulator}
}

// SetCumulator 切换半包累积策略；nil 会被忽略，避免运行期误配置打断链路。
func (d *ByteToMessageDecoder) SetCumulator(cumulator Cumulator) *ByteToMessageDecoder {
	if cumulator != nil {
		d.cumulator = cumulator
	}
	return d
}

func (d *ByteToMessageDecoder) ChannelRead(ctx *channel.HandlerContext, msg any) {
	in, ok := msg.(buffer.ByteBuf)
	if !ok {
		ctx.FireChannelRead(msg)
		return
	}
	if d.decoder == nil && d.listDecoder == nil {
		in.Release()
		ctx.FireExceptionCaught(ErrInvalidDecoder)
		return
	}
	cumulator := d.cumulator
	if cumulator == nil {
		cumulator = CompositeCumulator
	}
	cumulation, err := cumulator.Cumulate(ctx, d.cumulation, in)
	if err != nil {
		ctx.FireExceptionCaught(err)
		d.releaseCumulation()
		return
	}
	d.cumulation = cumulation
	d.decodeLoop(ctx)
}

func (d *ByteToMessageDecoder) ChannelInactive(ctx *channel.HandlerContext) {
	d.releaseCumulation()
	ctx.FireChannelInactive()
}

func (d *ByteToMessageDecoder) decodeLoop(ctx *channel.HandlerContext) {
	for d.cumulation != nil && d.cumulation.ReadableBytes() > 0 {
		readerBefore := d.cumulation.ReaderIndex()
		readableBefore := d.cumulation.ReadableBytes()

		d.out.Reset()
		if d.listDecoder != nil {
			if err := d.listDecoder.DecodeBytes(ctx, d.cumulation, &d.out); err != nil {
				d.out.ReleaseAll()
				ctx.FireExceptionCaught(err)
				d.releaseCumulation()
				return
			}
		} else {
			out, err := d.decoder.Decode(ctx, d.cumulation)
			if err != nil {
				releaseMessage(out)
				ctx.FireExceptionCaught(err)
				d.releaseCumulation()
				return
			}
			d.out.Add(out)
		}

		progressed := d.cumulation.ReaderIndex() != readerBefore || d.cumulation.ReadableBytes() != readableBefore
		if progressed {
			d.cumulation.DiscardReadComponents()
		}

		if d.out.Len() == 0 {
			if progressed {
				continue
			}
			return
		}
		if !progressed {
			d.out.ReleaseAll()
			ctx.FireExceptionCaught(ErrDecoderNoProgress)
			d.releaseCumulation()
			return
		}
		for i := 0; i < d.out.Len(); i++ {
			ctx.FireChannelRead(d.out.At(i))
		}
		d.out.Reset()
	}
}

func (d *ByteToMessageDecoder) releaseCumulation() {
	if d.cumulation == nil {
		return
	}
	d.cumulation.Release()
	d.cumulation = nil
	d.out.ReleaseAll()
}

func releaseMessage(msg any) {
	message.Release(msg)
}

func compositeCumulate(_ *channel.HandlerContext, cumulation *buffer.CompositeByteBuf, in buffer.ByteBuf) (*buffer.CompositeByteBuf, error) {
	if cumulation == nil {
		cumulation = buffer.NewCompositeByteBuf()
	}
	cumulation.Append(in)
	return cumulation, nil
}

func mergeCumulate(ctx *channel.HandlerContext, cumulation *buffer.CompositeByteBuf, in buffer.ByteBuf) (*buffer.CompositeByteBuf, error) {
	if cumulation == nil {
		cumulation = buffer.NewCompositeByteBuf()
		cumulation.Append(in)
		return cumulation, nil
	}
	if cumulation.ReadableBytes() == 0 {
		cumulation.Clear()
		cumulation.Append(in)
		return cumulation, nil
	}
	total := cumulation.ReadableBytes() + in.ReadableBytes()
	merged, err := ctx.Channel().Allocator().Acquire(total)
	if err != nil {
		in.Release()
		return nil, err
	}
	var scratch [8][]byte
	for _, part := range cumulation.ReadableSlices(scratch[:0]) {
		if _, err := merged.WriteBytes(part); err != nil {
			merged.Release()
			in.Release()
			return nil, err
		}
	}
	slices := in.ReadableSlices(scratch[:0])
	for _, part := range slices {
		if _, err := merged.WriteBytes(part); err != nil {
			merged.Release()
			in.Release()
			return nil, err
		}
	}
	if len(slices) == 0 && in.ReadableBytes() > 0 {
		merged.Release()
		in.Release()
		return nil, buffer.ErrInvalidIndex
	}
	in.Release()
	cumulation.Release()
	next := buffer.NewCompositeByteBuf()
	next.Append(merged)
	return next, nil
}
