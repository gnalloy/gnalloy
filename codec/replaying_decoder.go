package codec

import (
	"errors"

	"gnalloy.org/gnalloy/buffer"
	"gnalloy.org/gnalloy/channel"
)

type ReplayDecoder interface {
	DecodeReplay(ctx *channel.HandlerContext, in *ReplayBuffer) (any, error)
}

// ReplayingDecoder 是 Netty ReplayingDecoder 的 Go 化替代：读取不足时返回 nil 等待后续数据。
type ReplayingDecoder struct {
	*ByteToMessageDecoder
	decoder ReplayDecoder
}

func NewReplayingDecoder(decoder ReplayDecoder) *ReplayingDecoder {
	d := &ReplayingDecoder{decoder: decoder}
	d.ByteToMessageDecoder = NewByteToMessageDecoder(d)
	return d
}

func (d *ReplayingDecoder) Decode(ctx *channel.HandlerContext, in *buffer.CompositeByteBuf) (any, error) {
	if d.decoder == nil {
		return nil, ErrInvalidDecoder
	}
	replay := ReplayBuffer{in: in, readerIndex: in.ReaderIndex()}
	out, err := d.decoder.DecodeReplay(ctx, &replay)
	if errors.Is(err, ErrReplayNeedMore) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if replay.readerIndex != in.ReaderIndex() {
		if err := in.SetReaderIndex(replay.readerIndex); err != nil {
			releaseMessage(out)
			return nil, err
		}
	}
	return out, nil
}

type ReplayBuffer struct {
	in          *buffer.CompositeByteBuf
	readerIndex int
}

func (b *ReplayBuffer) ReadableBytes() int {
	return b.in.WriterIndex() - b.readerIndex
}

func (b *ReplayBuffer) ReaderIndex() int {
	return b.readerIndex
}

func (b *ReplayBuffer) ReadByte() (byte, error) {
	if b.ReadableBytes() < 1 {
		return 0, ErrReplayNeedMore
	}
	value, ok := b.in.GetByte(b.readerIndex)
	if !ok {
		return 0, ErrReplayNeedMore
	}
	b.readerIndex++
	return value, nil
}

func (b *ReplayBuffer) ReadUnsigned(length int, order buffer.ByteOrder) (uint64, error) {
	if length <= 0 || length > 8 {
		return 0, buffer.ErrInvalidIndex
	}
	if b.ReadableBytes() < length {
		return 0, ErrReplayNeedMore
	}
	value, err := b.in.ReadUnsigned(b.readerIndex, length, order)
	if err != nil {
		return 0, err
	}
	b.readerIndex += length
	return value, nil
}

func (b *ReplayBuffer) ReadSlice(length int) (buffer.ByteBuf, error) {
	if length < 0 {
		return nil, buffer.ErrInvalidIndex
	}
	if b.ReadableBytes() < length {
		return nil, ErrReplayNeedMore
	}
	out, err := b.in.Slice(b.readerIndex, length)
	if err != nil {
		return nil, err
	}
	b.readerIndex += length
	return out, nil
}

func (b *ReplayBuffer) SkipBytes(length int) error {
	if length < 0 {
		return buffer.ErrInvalidIndex
	}
	if b.ReadableBytes() < length {
		return ErrReplayNeedMore
	}
	b.readerIndex += length
	return nil
}
