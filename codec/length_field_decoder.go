package codec

import (
	"gnalloy.org/gnalloy/buffer"
	"gnalloy.org/gnalloy/channel"
)

type LengthFieldBasedFrameDecoder struct {
	*ByteToMessageDecoder

	maxFrameLength     int
	lengthFieldOffset  int
	lengthFieldLength  int
	lengthAdjustment   int
	initialBytesToSkip int
	byteOrder          buffer.ByteOrder
	failFast           bool
	discarding         bool
	tooLongFrameLength int64
	bytesToDiscard     int64
}

func NewLengthFieldBasedFrameDecoder(maxFrameLength int, lengthFieldOffset int, lengthFieldLength int, lengthAdjustment int, initialBytesToSkip int, order buffer.ByteOrder) (*LengthFieldBasedFrameDecoder, error) {
	return NewLengthFieldBasedFrameDecoderWithOptions(maxFrameLength, lengthFieldOffset, lengthFieldLength, lengthAdjustment, initialBytesToSkip, order, true)
}

func NewLengthFieldBasedFrameDecoderWithOptions(maxFrameLength int, lengthFieldOffset int, lengthFieldLength int, lengthAdjustment int, initialBytesToSkip int, order buffer.ByteOrder, failFast bool) (*LengthFieldBasedFrameDecoder, error) {
	if maxFrameLength <= 0 || lengthFieldOffset < 0 || initialBytesToSkip < 0 {
		return nil, ErrInvalidLengthField
	}
	switch lengthFieldLength {
	case 1, 2, 3, 4, 8:
	default:
		return nil, ErrInvalidLengthField
	}
	d := &LengthFieldBasedFrameDecoder{
		maxFrameLength:     maxFrameLength,
		lengthFieldOffset:  lengthFieldOffset,
		lengthFieldLength:  lengthFieldLength,
		lengthAdjustment:   lengthAdjustment,
		initialBytesToSkip: initialBytesToSkip,
		byteOrder:          order,
		failFast:           failFast,
	}
	d.ByteToMessageDecoder = NewByteToMessageDecoder(d)
	return d, nil
}

func (d *LengthFieldBasedFrameDecoder) Decode(_ *channel.HandlerContext, cumulation *buffer.CompositeByteBuf) (any, error) {
	if cumulation == nil {
		return nil, nil
	}
	if d.discarding {
		return d.discardTooLongFrame(cumulation)
	}
	readable := cumulation.ReadableBytes()
	minFrameLength := d.lengthFieldOffset + d.lengthFieldLength
	if readable < minFrameLength {
		if cumulation.ComponentCount() > 1 {
			return nil, compactFragmentedCumulation(cumulation)
		}
		return nil, nil
	}

	readerIndex := cumulation.ReaderIndex()
	lengthFieldIndex := readerIndex + d.lengthFieldOffset
	rawLength, err := cumulation.ReadUnsigned(lengthFieldIndex, d.lengthFieldLength, d.byteOrder)
	if err != nil {
		return nil, err
	}
	if rawLength > uint64(maxInt64Value()) {
		return d.exceededFrameLength(cumulation, int64(d.maxFrameLength)+1)
	}
	frameLength64 := int64(rawLength) + int64(d.lengthAdjustment) + int64(minFrameLength)
	if frameLength64 < int64(minFrameLength) || frameLength64 < int64(d.initialBytesToSkip) {
		return nil, ErrInvalidLengthField
	}
	if frameLength64 > int64(d.maxFrameLength) {
		return d.exceededFrameLength(cumulation, frameLength64)
	}
	frameLength := int(frameLength64)
	if readable < frameLength {
		if cumulation.ComponentCount() > 1 {
			return nil, compactFragmentedCumulation(cumulation)
		}
		return nil, nil
	}
	if cumulation.ComponentCount() > 1 {
		if err := compactFragmentedCumulation(cumulation); err != nil {
			return nil, err
		}
		readerIndex = cumulation.ReaderIndex()
	}

	frameIndex := readerIndex + d.initialBytesToSkip
	frameBytes := frameLength - d.initialBytesToSkip
	frame, err := cumulation.Slice(frameIndex, frameBytes)
	if err != nil {
		return nil, err
	}
	if err := cumulation.SkipBytes(frameLength); err != nil {
		frame.Release()
		return nil, err
	}
	return frame, nil
}

func compactFragmentedCumulation(in *buffer.CompositeByteBuf) error {
	if in == nil || in.ComponentCount() <= 1 || in.ReadableBytes() == 0 {
		return nil
	}
	merged := buffer.NewHeapBuffer(in.ReadableBytes())
	var scratch [8][]byte
	for _, part := range in.ReadableSlices(scratch[:0]) {
		if _, err := merged.WriteBytes(part); err != nil {
			merged.Release()
			return err
		}
	}
	in.Clear()
	in.Append(merged)
	return nil
}

func (d *LengthFieldBasedFrameDecoder) exceededFrameLength(in *buffer.CompositeByteBuf, frameLength int64) (any, error) {
	readable := int64(in.ReadableBytes())
	if readable < frameLength {
		if err := in.SkipBytes(int(readable)); err != nil {
			return nil, err
		}
		d.discarding = true
		d.tooLongFrameLength = frameLength
		d.bytesToDiscard = frameLength - readable
		if d.failFast {
			return nil, NewTooLongFrameError(clampInt(frameLength), d.maxFrameLength, clampInt(readable))
		}
		return nil, nil
	}
	if err := in.SkipBytes(clampInt(frameLength)); err != nil {
		return nil, err
	}
	return nil, NewTooLongFrameError(clampInt(frameLength), d.maxFrameLength, clampInt(frameLength))
}

func (d *LengthFieldBasedFrameDecoder) discardTooLongFrame(in *buffer.CompositeByteBuf) (any, error) {
	readable := int64(in.ReadableBytes())
	if readable == 0 {
		return nil, nil
	}
	discard := readable
	if discard > d.bytesToDiscard {
		discard = d.bytesToDiscard
	}
	if err := in.SkipBytes(int(discard)); err != nil {
		return nil, err
	}
	d.bytesToDiscard -= discard
	if d.bytesToDiscard > 0 {
		return nil, nil
	}
	frameLength := d.tooLongFrameLength
	d.discarding = false
	d.tooLongFrameLength = 0
	if d.failFast {
		return nil, nil
	}
	return nil, NewTooLongFrameError(clampInt(frameLength), d.maxFrameLength, clampInt(frameLength))
}

func maxInt64Value() int64 {
	return int64(^uint64(0) >> 1)
}

func clampInt(v int64) int {
	max := int64(int(^uint(0) >> 1))
	if v > max {
		return int(max)
	}
	if v < 0 {
		return 0
	}
	return int(v)
}
