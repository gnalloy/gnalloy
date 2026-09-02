package buffer

import "sync"

// OwnedBufferReleaser 接收 OwnedBuffer 最后一个引用释放后的底层字节切片。
//
// 实现不得在 Release 返回后继续访问 data，并且必须支持被不同缓冲区并发调用。
type OwnedBufferReleaser interface {
	Release(data []byte)
}

var ownedBufferPool sync.Pool

// NewOwnedBuffer 把外部拥有的字节切片包装为只读 ByteBuf。
//
// 返回的 ByteBuf 初始可读区为整个 data。最后一个引用释放时会调用 release(data)，
// 用于把 TLS、压缩等上游池化切片零拷贝交给 transport。
func NewOwnedBuffer(data []byte, release func([]byte)) ByteBuf {
	owned := acquireOwnedBuffer(data)
	owned.release = release
	return &owned.buf
}

// NewOwnedBufferWithReleaser 把外部拥有的字节切片包装为只读 ByteBuf。
//
// 与 NewOwnedBuffer 相比，该入口直接保存接口，不会为捕获释放函数创建闭包。
func NewOwnedBufferWithReleaser(data []byte, releaser OwnedBufferReleaser) ByteBuf {
	owned := acquireOwnedBuffer(data)
	owned.releaser = releaser
	return &owned.buf
}

type ownedBuffer struct {
	buf      DirectByteBuf
	release  func([]byte)
	releaser OwnedBufferReleaser
}

func acquireOwnedBuffer(data []byte) *ownedBuffer {
	value := ownedBufferPool.Get()
	var owned *ownedBuffer
	if value == nil {
		owned = new(ownedBuffer)
	} else {
		owned = value.(*ownedBuffer)
	}
	owned.buf.reset(data, owned)
	owned.buf.writerIndex = len(data)
	return owned
}

func (o *ownedBuffer) releaseDirect(buf *DirectByteBuf) {
	if o == nil || buf == nil {
		return
	}
	data := buf.data
	release := o.release
	releaser := o.releaser
	buf.data = nil
	buf.readerIndex = 0
	buf.writerIndex = 0
	buf.owner = nil
	o.release = nil
	o.releaser = nil
	if release != nil {
		release(data)
	} else if releaser != nil {
		releaser.Release(data)
	}
	ownedBufferPool.Put(o)
}
