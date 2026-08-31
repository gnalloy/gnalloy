package buffer

import "unsafe"

// ContiguousReadableBytes 返回当前可读区的连续底层视图。
//
// 返回的切片不转移所有权，调用方不得在 ByteBuf 释放后继续持有，也不得在需要不可变
// 语义的位置直接保存该视图。
func ContiguousReadableBytes(src ByteBuf) ([]byte, bool) {
	if src == nil {
		return nil, false
	}
	switch b := src.(type) {
	case *DirectByteBuf:
		if b.refs.Load() <= 0 {
			return nil, false
		}
		if b.readerIndex == b.writerIndex {
			return nil, true
		}
		return b.data[b.readerIndex:b.writerIndex], true
	case *slicedByteBuf:
		if b.refs.Load() <= 0 {
			return nil, false
		}
		if b.readerIndex == b.writerIndex {
			return nil, true
		}
		return b.data[b.readerIndex:b.writerIndex], true
	case *sharedByteBuf:
		if b.refs.Load() <= 0 {
			return nil, false
		}
		if b.readerIndex == b.writerIndex {
			return nil, true
		}
		return b.data[b.readerIndex:b.writerIndex], true
	case *CompositeByteBuf:
		if b.refs.Load() <= 0 {
			return nil, false
		}
		if b.readerIndex == b.writerIndex {
			return nil, true
		}
		return b.readableSpan(b.readerIndex, b.ReadableBytes())
	default:
		if src.ReadableBytes() == 0 {
			return nil, true
		}
		var stack [2][]byte
		slices := src.ReadableSlices(stack[:0])
		if len(slices) == 1 && len(slices[0]) == src.ReadableBytes() {
			return slices[0], true
		}
		return nil, false
	}
}

// ReadableString 将 ByteBuf 可读区转换为 string，不推进 readerIndex。
//
// 连续 ByteBuf 按 Go string 不可变语义复制；非连续 ByteBuf 先复制到私有字节数组，
// 再构造 string，避免 CompositeByteBuf.Bytes 的中间 ByteBuf 分配。
func ReadableString(src ByteBuf) string {
	if src == nil || src.ReadableBytes() == 0 {
		return ""
	}
	if data, ok := ContiguousReadableBytes(src); ok {
		return string(data)
	}
	data := make([]byte, src.ReadableBytes())
	if CopyReadableBytes(data, src) != len(data) {
		return ""
	}
	return unsafe.String(unsafe.SliceData(data), len(data))
}

// ReadableStringAt 将 CompositeByteBuf 指定可读区间转换为 string，不推进 readerIndex。
//
// 连续区间保持 Go string 不可变语义复制；跨组件区间直接从组件复制到私有字节数组，
// 避免为临时 Slice 构造中间 ByteBuf。
func ReadableStringAt(src *CompositeByteBuf, index int, length int) (string, error) {
	if src == nil {
		if length == 0 {
			return "", nil
		}
		return "", ErrInvalidIndex
	}
	if length < 0 {
		return "", ErrInvalidIndex
	}
	if length == 0 {
		if src.refs.Load() <= 0 {
			return "", ErrReleasedBuffer
		}
		if index < src.readerIndex || index > src.writerIndex {
			return "", ErrInvalidIndex
		}
		return "", nil
	}
	if data, ok := src.ReadableSpan(index, length); ok {
		return string(data), nil
	}
	data := make([]byte, length)
	if n, err := src.copyReadableRange(data, index, length); err != nil {
		return "", err
	} else if n != length {
		return "", ErrNotEnoughBytes
	}
	return unsafe.String(unsafe.SliceData(data), len(data)), nil
}

// CopyReadableBytes 将 src 的可读区复制到 dst，不推进 readerIndex。
//
// 对内置 ByteBuf 类型直接访问已校验的内部布局，避免热路径为 ReadableSlices
// 生成临时切片列表；外部实现仍回退到公开接口。
func CopyReadableBytes(dst []byte, src ByteBuf) int {
	if len(dst) == 0 || src == nil {
		return 0
	}
	switch b := src.(type) {
	case *DirectByteBuf:
		if b.refs.Load() <= 0 {
			return 0
		}
		return copy(dst, b.data[b.readerIndex:b.writerIndex])
	case *slicedByteBuf:
		if b.refs.Load() <= 0 {
			return 0
		}
		return copy(dst, b.data[b.readerIndex:b.writerIndex])
	case *sharedByteBuf:
		if b.refs.Load() <= 0 {
			return 0
		}
		return copy(dst, b.data[b.readerIndex:b.writerIndex])
	case *CompositeByteBuf:
		if b.refs.Load() <= 0 {
			return 0
		}
		return b.copyReadableTo(dst)
	default:
		return copyReadableBytesBySlices(dst, src)
	}
}

// WriteReadableBytes 将 src 的可读区写入 dst，不推进 src 的 readerIndex。
func WriteReadableBytes(dst ByteBuf, src ByteBuf) error {
	if src == nil || src.ReadableBytes() == 0 {
		return nil
	}
	if dst == nil {
		return ErrNoWritableBytes
	}
	n := src.ReadableBytes()
	if dst.WritableBytes() < n {
		return ErrNoWritableBytes
	}
	switch b := dst.(type) {
	case *DirectByteBuf:
		if err := b.checkAlive(); err != nil {
			return err
		}
		copied := CopyReadableBytes(b.data[b.writerIndex:b.writerIndex+n], src)
		if copied != n {
			return ErrNotEnoughBytes
		}
		b.writerIndex += n
		return nil
	case *slicedByteBuf:
		if err := b.checkAlive(); err != nil {
			return err
		}
		copied := CopyReadableBytes(b.data[b.writerIndex:b.writerIndex+n], src)
		if copied != n {
			return ErrNotEnoughBytes
		}
		b.writerIndex += n
		return nil
	default:
		view := dst.WritableBytesView()
		if len(view) >= n {
			copied := CopyReadableBytes(view[:n], src)
			if copied != n {
				return ErrNotEnoughBytes
			}
			return dst.AdvanceWriter(n)
		}
		if data, ok := ContiguousReadableBytes(src); ok {
			_, err := dst.WriteBytes(data)
			return err
		}
		data := make([]byte, n)
		if CopyReadableBytes(data, src) != n {
			return ErrNotEnoughBytes
		}
		_, err := dst.WriteBytes(data)
		return err
	}
}

func copyReadableBytesBySlices(dst []byte, src ByteBuf) int {
	var slices [8][]byte
	written := 0
	for _, part := range src.ReadableSlices(slices[:0]) {
		written += copy(dst[written:], part)
		if written == len(dst) {
			break
		}
	}
	return written
}
