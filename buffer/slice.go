package buffer

import (
	"io"
	"sync"
	"sync/atomic"
)

var slicedByteBufPool sync.Pool

type slicedByteBuf struct {
	parent ByteBuf
	data   []byte

	readerIndex int
	writerIndex int

	refs   atomic.Int32
	leakID uint64
}

func newSlicedByteBuf(parent ByteBuf, data []byte) *slicedByteBuf {
	s, _ := slicedByteBufPool.Get().(*slicedByteBuf)
	if s == nil {
		s = &slicedByteBuf{}
	}
	s.parent = parent
	s.data = data
	s.readerIndex = 0
	s.writerIndex = len(data)
	s.refs.Store(1)
	s.leakID = trackLeak("slice")
	return s
}

func (b *slicedByteBuf) checkAlive() error {
	if b.refs.Load() <= 0 {
		return ErrReleasedBuffer
	}
	return nil
}

func (b *slicedByteBuf) ReadableBytes() int { return b.writerIndex - b.readerIndex }
func (b *slicedByteBuf) WritableBytes() int { return len(b.data) - b.writerIndex }
func (b *slicedByteBuf) Capacity() int      { return len(b.data) }
func (b *slicedByteBuf) ReaderIndex() int   { return b.readerIndex }
func (b *slicedByteBuf) WriterIndex() int   { return b.writerIndex }

func (b *slicedByteBuf) SetReaderIndex(index int) error {
	if err := b.checkAlive(); err != nil {
		return err
	}
	if index < 0 || index > b.writerIndex {
		return ErrInvalidIndex
	}
	b.readerIndex = index
	return nil
}

func (b *slicedByteBuf) SetWriterIndex(index int) error {
	if err := b.checkAlive(); err != nil {
		return err
	}
	if index < b.readerIndex || index > len(b.data) {
		return ErrInvalidIndex
	}
	b.writerIndex = index
	return nil
}

func (b *slicedByteBuf) Clear() {
	b.readerIndex = 0
	b.writerIndex = 0
}

func (b *slicedByteBuf) GetByte(index int) (byte, bool) {
	if b.refs.Load() <= 0 || index < b.readerIndex || index >= b.writerIndex {
		return 0, false
	}
	return b.data[index], true
}

func (b *slicedByteBuf) ReadByte() (byte, error) {
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

func (b *slicedByteBuf) ReadUnsigned(offset int, length int, order ByteOrder) (uint64, error) {
	if err := b.checkAlive(); err != nil {
		return 0, err
	}
	if length <= 0 || length > 8 || offset < b.readerIndex || offset+length > b.writerIndex {
		return 0, ErrInvalidIndex
	}
	return readUnsignedFrom(b.data[offset:offset+length], order), nil
}

func (b *slicedByteBuf) Read(p []byte) (int, error) {
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

func (b *slicedByteBuf) Write(p []byte) (int, error) {
	return b.WriteBytes(p)
}

func (b *slicedByteBuf) WriteBytes(src []byte) (int, error) {
	if err := b.checkAlive(); err != nil {
		return 0, err
	}
	if len(src) > b.WritableBytes() {
		return 0, ErrNoWritableBytes
	}
	n := copy(b.data[b.writerIndex:], src)
	b.writerIndex += n
	return n, nil
}

func (b *slicedByteBuf) WriteTo(w io.Writer) (int64, error) {
	if err := b.checkAlive(); err != nil {
		return 0, err
	}
	n, err := w.Write(b.data[b.readerIndex:b.writerIndex])
	b.readerIndex += n
	return int64(n), err
}

func (b *slicedByteBuf) ReadFrom(r io.Reader) (int64, error) {
	if err := b.checkAlive(); err != nil {
		return 0, err
	}
	total := int64(0)
	for b.WritableBytes() > 0 {
		n, err := r.Read(b.data[b.writerIndex:])
		if n > 0 {
			b.writerIndex += n
			total += int64(n)
		}
		if err != nil {
			if err == io.EOF {
				return total, nil
			}
			return total, err
		}
		if n == 0 {
			return total, nil
		}
	}
	return total, nil
}

func (b *slicedByteBuf) Bytes() []byte {
	if b.refs.Load() <= 0 {
		return nil
	}
	return b.data[b.readerIndex:b.writerIndex]
}

func (b *slicedByteBuf) ReadableSlices(dst [][]byte) [][]byte {
	if b.refs.Load() <= 0 || b.readerIndex == b.writerIndex {
		return dst
	}
	return append(dst, b.data[b.readerIndex:b.writerIndex])
}

func (b *slicedByteBuf) WritableBytesView() []byte {
	if b.refs.Load() <= 0 {
		return nil
	}
	return b.data[b.writerIndex:]
}

func (b *slicedByteBuf) AdvanceWriter(n int) error {
	if err := b.checkAlive(); err != nil {
		return err
	}
	if n < 0 || n > b.WritableBytes() {
		return ErrInvalidIndex
	}
	b.writerIndex += n
	return nil
}

func (b *slicedByteBuf) SkipBytes(n int) error {
	if err := b.checkAlive(); err != nil {
		return err
	}
	if n < 0 || n > b.ReadableBytes() {
		return ErrNotEnoughBytes
	}
	b.readerIndex += n
	return nil
}

func (b *slicedByteBuf) Slice(index int, length int) (ByteBuf, error) {
	if err := b.checkAlive(); err != nil {
		return nil, err
	}
	if index < b.readerIndex || length < 0 || index+length > b.writerIndex {
		return nil, ErrInvalidIndex
	}
	b.Retain()
	return newSlicedByteBuf(b, b.data[index:index+length]), nil
}

func (b *slicedByteBuf) Copy() (ByteBuf, error) {
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

func (b *slicedByteBuf) Retain() ByteBuf {
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

func (b *slicedByteBuf) Release() bool {
	refs := b.refs.Add(-1)
	if refs > 0 {
		return false
	}
	if refs < 0 {
		panic(ErrReleasedBuffer)
	}
	untrackLeak(b.leakID)
	b.leakID = 0
	b.parent.Release()
	b.parent = nil
	b.data = nil
	b.readerIndex = 0
	b.writerIndex = 0
	slicedByteBufPool.Put(b)
	return true
}

func (b *slicedByteBuf) RefCnt() int32 { return b.refs.Load() }

func (b *slicedByteBuf) FixedBufferIndex() (uint16, bool) {
	if b.refs.Load() <= 0 || b.parent == nil {
		return 0, false
	}
	return FixedBufferIndex(b.parent)
}
