package buffer

import (
	"io"
	"sync"
	"sync/atomic"
)

var sharedByteBufPool sync.Pool

// NewSharedBuffer 把不可变字节切片包装为轻量只读 ByteBuf。
//
// 调用方必须保证 data 在所有返回的 ByteBuf 释放前不被修改；该构造不复制底层字节，
// 适合固定响应体、协议常量帧等高频只读写出路径。
func NewSharedBuffer(data []byte) ByteBuf {
	buf, _ := sharedByteBufPool.Get().(*sharedByteBuf)
	if buf == nil {
		buf = &sharedByteBuf{}
	}
	buf.data = data
	buf.readerIndex = 0
	buf.writerIndex = len(data)
	buf.refs.Store(1)
	buf.leakID = trackLeak("shared")
	return buf
}

type sharedByteBuf struct {
	data []byte

	readerIndex int
	writerIndex int

	refs   atomic.Int32
	leakID uint64
}

func (b *sharedByteBuf) checkAlive() error {
	if b.refs.Load() <= 0 {
		return ErrReleasedBuffer
	}
	return nil
}

func (b *sharedByteBuf) ReadableBytes() int { return b.writerIndex - b.readerIndex }
func (b *sharedByteBuf) WritableBytes() int { return 0 }
func (b *sharedByteBuf) Capacity() int      { return len(b.data) }
func (b *sharedByteBuf) ReaderIndex() int   { return b.readerIndex }
func (b *sharedByteBuf) WriterIndex() int   { return b.writerIndex }

func (b *sharedByteBuf) SetReaderIndex(index int) error {
	if err := b.checkAlive(); err != nil {
		return err
	}
	if index < 0 || index > b.writerIndex {
		return ErrInvalidIndex
	}
	b.readerIndex = index
	return nil
}

func (b *sharedByteBuf) SetWriterIndex(index int) error {
	if err := b.checkAlive(); err != nil {
		return err
	}
	if index < b.readerIndex || index > len(b.data) {
		return ErrInvalidIndex
	}
	b.writerIndex = index
	return nil
}

func (b *sharedByteBuf) Clear() {
	b.readerIndex = 0
	b.writerIndex = 0
}

func (b *sharedByteBuf) GetByte(index int) (byte, bool) {
	if b.refs.Load() <= 0 || index < b.readerIndex || index >= b.writerIndex {
		return 0, false
	}
	return b.data[index], true
}

func (b *sharedByteBuf) ReadByte() (byte, error) {
	if err := b.checkAlive(); err != nil {
		return 0, err
	}
	if b.ReadableBytes() <= 0 {
		return 0, ErrNotEnoughBytes
	}
	v := b.data[b.readerIndex]
	b.readerIndex++
	return v, nil
}

func (b *sharedByteBuf) ReadUnsigned(offset int, length int, order ByteOrder) (uint64, error) {
	if err := b.checkAlive(); err != nil {
		return 0, err
	}
	if length <= 0 || length > 8 || offset < b.readerIndex || offset+length > b.writerIndex {
		return 0, ErrInvalidIndex
	}
	return readUnsignedFrom(b.data[offset:offset+length], order), nil
}

func (b *sharedByteBuf) Read(p []byte) (int, error) {
	if err := b.checkAlive(); err != nil {
		return 0, err
	}
	if b.ReadableBytes() == 0 {
		return 0, io.EOF
	}
	n := copy(p, b.data[b.readerIndex:b.writerIndex])
	b.readerIndex += n
	return n, nil
}

func (b *sharedByteBuf) Write(p []byte) (int, error) {
	return b.WriteBytes(p)
}

func (b *sharedByteBuf) WriteBytes([]byte) (int, error) {
	if err := b.checkAlive(); err != nil {
		return 0, err
	}
	return 0, ErrNoWritableBytes
}

func (b *sharedByteBuf) WriteTo(w io.Writer) (int64, error) {
	if err := b.checkAlive(); err != nil {
		return 0, err
	}
	n, err := w.Write(b.data[b.readerIndex:b.writerIndex])
	b.readerIndex += n
	return int64(n), err
}

func (b *sharedByteBuf) ReadFrom(io.Reader) (int64, error) {
	if err := b.checkAlive(); err != nil {
		return 0, err
	}
	return 0, ErrNoWritableBytes
}

func (b *sharedByteBuf) Bytes() []byte {
	if b.refs.Load() <= 0 {
		return nil
	}
	return b.data[b.readerIndex:b.writerIndex]
}

func (b *sharedByteBuf) ReadableSlices(dst [][]byte) [][]byte {
	if b.refs.Load() <= 0 || b.readerIndex == b.writerIndex {
		return dst
	}
	return append(dst, b.data[b.readerIndex:b.writerIndex])
}

func (b *sharedByteBuf) WritableBytesView() []byte {
	return nil
}

func (b *sharedByteBuf) AdvanceWriter(n int) error {
	if err := b.checkAlive(); err != nil {
		return err
	}
	if n != 0 {
		return ErrInvalidIndex
	}
	return nil
}

func (b *sharedByteBuf) SkipBytes(n int) error {
	if err := b.checkAlive(); err != nil {
		return err
	}
	if n < 0 || n > b.ReadableBytes() {
		return ErrNotEnoughBytes
	}
	b.readerIndex += n
	return nil
}

func (b *sharedByteBuf) Slice(index int, length int) (ByteBuf, error) {
	if err := b.checkAlive(); err != nil {
		return nil, err
	}
	if index < b.readerIndex || length < 0 || index+length > b.writerIndex {
		return nil, ErrInvalidIndex
	}
	return NewSharedBuffer(b.data[index : index+length]), nil
}

func (b *sharedByteBuf) Copy() (ByteBuf, error) {
	if err := b.checkAlive(); err != nil {
		return nil, err
	}
	out := NewHeapBuffer(b.ReadableBytes())
	_, err := out.WriteBytes(b.Bytes())
	if err != nil {
		out.Release()
		return nil, err
	}
	return out, nil
}

func (b *sharedByteBuf) Retain() ByteBuf {
	for {
		refs := b.refs.Load()
		if refs <= 0 {
			panic(ErrReleasedBuffer)
		}
		if b.refs.CompareAndSwap(refs, refs+1) {
			return b
		}
	}
}

func (b *sharedByteBuf) Release() bool {
	refs := b.refs.Add(-1)
	if refs > 0 {
		return false
	}
	if refs < 0 {
		panic(ErrReleasedBuffer)
	}
	untrackLeak(b.leakID)
	b.leakID = 0
	b.data = nil
	b.readerIndex = 0
	b.writerIndex = 0
	sharedByteBufPool.Put(b)
	return true
}

func (b *sharedByteBuf) RefCnt() int32 { return b.refs.Load() }
