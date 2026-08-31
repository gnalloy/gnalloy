package codec

import (
	"gnalloy.org/gnalloy/buffer"
	"gnalloy.org/gnalloy/channel"
)

// LengthFieldPrepender 为出站 ByteBuf 添加二进制长度字段。
// 为避免复制 payload，编码器会先写 header ByteBuf，再把原始 payload 继续向前传播。
type LengthFieldPrepender struct {
	lengthFieldLength              int
	lengthAdjustment               int
	lengthIncludesLengthFieldWidth bool
	byteOrder                      buffer.ByteOrder
}

func NewLengthFieldPrepender(lengthFieldLength int, order buffer.ByteOrder) (*LengthFieldPrepender, error) {
	return NewLengthFieldPrependerWithOptions(lengthFieldLength, 0, false, order)
}

func NewLengthFieldPrependerWithOptions(lengthFieldLength int, lengthAdjustment int, lengthIncludesLengthFieldWidth bool, order buffer.ByteOrder) (*LengthFieldPrepender, error) {
	switch lengthFieldLength {
	case 1, 2, 3, 4, 8:
	default:
		return nil, ErrInvalidLengthField
	}
	return &LengthFieldPrepender{
		lengthFieldLength:              lengthFieldLength,
		lengthAdjustment:               lengthAdjustment,
		lengthIncludesLengthFieldWidth: lengthIncludesLengthFieldWidth,
		byteOrder:                      order,
	}, nil
}

func (p *LengthFieldPrepender) Write(ctx *channel.HandlerContext, msg any) error {
	in, ok := msg.(buffer.ByteBuf)
	if !ok {
		return ctx.Write(msg)
	}
	length := int64(in.ReadableBytes()) + int64(p.lengthAdjustment)
	if p.lengthIncludesLengthFieldWidth {
		length += int64(p.lengthFieldLength)
	}
	if length < 0 || !fitsLengthField(uint64(length), p.lengthFieldLength) {
		in.Release()
		return ErrEncodedLengthRange
	}
	header, err := ctx.Channel().Allocator().Acquire(p.lengthFieldLength)
	if err != nil {
		in.Release()
		return err
	}
	if err := writeLengthField(header, uint64(length), p.lengthFieldLength, p.byteOrder); err != nil {
		header.Release()
		in.Release()
		return err
	}
	if err := ctx.Write(header); err != nil {
		header.Release()
		in.Release()
		return err
	}
	if err := ctx.Write(in); err != nil {
		in.Release()
		return err
	}
	return nil
}

func fitsLengthField(length uint64, fieldLength int) bool {
	switch fieldLength {
	case 1:
		return length <= 0xff
	case 2:
		return length <= 0xffff
	case 3:
		return length <= 0xffffff
	case 4:
		return length <= 0xffffffff
	case 8:
		return true
	default:
		return false
	}
}

func writeLengthField(buf buffer.ByteBuf, length uint64, fieldLength int, order buffer.ByteOrder) error {
	var tmp [8]byte
	if order == buffer.LittleEndian {
		for i := 0; i < fieldLength; i++ {
			tmp[i] = byte(length >> (8 * i))
		}
		_, err := buf.WriteBytes(tmp[:fieldLength])
		return err
	}
	for i := 0; i < fieldLength; i++ {
		shift := 8 * (fieldLength - 1 - i)
		tmp[i] = byte(length >> shift)
	}
	_, err := buf.WriteBytes(tmp[:fieldLength])
	return err
}
