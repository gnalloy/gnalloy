package channel

import (
	"errors"
	"io"
)

const defaultFileRegionChunkSize = 32 << 10

// FileRegion 表示可按区域传输的文件或文件状数据。
type FileRegion interface {
	Count() int64
	Transferred() int64
	Read([]byte) (int, error)
	Close() error
}

// DefaultFileRegion 基于 io.ReaderAt 提供确定偏移和长度的文件区域。
type DefaultFileRegion struct {
	reader      io.ReaderAt
	offset      int64
	count       int64
	transferred int64
	closed      bool
}

func NewFileRegion(reader io.ReaderAt, offset int64, count int64) (*DefaultFileRegion, error) {
	if reader == nil || offset < 0 || count < 0 {
		return nil, ErrInvalidFileRegion
	}
	return &DefaultFileRegion{reader: reader, offset: offset, count: count}, nil
}

func (r *DefaultFileRegion) Count() int64 {
	if r == nil {
		return 0
	}
	return r.count
}

func (r *DefaultFileRegion) Transferred() int64 {
	if r == nil {
		return 0
	}
	return r.transferred
}

// Offset 返回该区域在底层 reader 中的起始偏移。
func (r *DefaultFileRegion) Offset() int64 {
	if r == nil {
		return 0
	}
	return r.offset
}

// ReaderAt 返回底层随机读源，zero-copy 传输会按需识别 *os.File。
func (r *DefaultFileRegion) ReaderAt() io.ReaderAt {
	if r == nil {
		return nil
	}
	return r.reader
}

// Advance 在外部原生传输完成后推进进度。
func (r *DefaultFileRegion) Advance(n int64) error {
	if r == nil || n < 0 || r.transferred+n > r.count {
		return ErrInvalidFileRegion
	}
	if r.closed {
		return ErrFileRegionClosed
	}
	r.transferred += n
	return nil
}

func (r *DefaultFileRegion) Read(dst []byte) (int, error) {
	if r == nil || r.reader == nil {
		return 0, ErrInvalidFileRegion
	}
	if r.closed {
		return 0, ErrFileRegionClosed
	}
	remaining := r.count - r.transferred
	if remaining <= 0 {
		return 0, io.EOF
	}
	if int64(len(dst)) > remaining {
		dst = dst[:remaining]
	}
	n, err := r.reader.ReadAt(dst, r.offset+r.transferred)
	if n > 0 {
		r.transferred += int64(n)
	}
	if errors.Is(err, io.EOF) && r.transferred >= r.count {
		return n, io.EOF
	}
	return n, err
}

func (r *DefaultFileRegion) Close() error {
	if r == nil {
		return nil
	}
	r.closed = true
	return nil
}

// Release 兼容通用消息释放路径。
func (r *DefaultFileRegion) Release() bool {
	return r.Close() == nil
}

// FileRegionEncoder 是 sendfile 不可用时的跨平台 fallback。
type FileRegionEncoder struct {
	chunkSize int
}

func NewFileRegionEncoder(chunkSize int) (*FileRegionEncoder, error) {
	if chunkSize < 0 {
		return nil, ErrInvalidFileRegion
	}
	if chunkSize == 0 {
		chunkSize = defaultFileRegionChunkSize
	}
	return &FileRegionEncoder{chunkSize: chunkSize}, nil
}

func (e *FileRegionEncoder) Write(ctx *HandlerContext, msg any) error {
	region, ok := msg.(FileRegion)
	if !ok {
		return ctx.Write(msg)
	}
	defer region.Close()
	for region.Transferred() < region.Count() {
		remaining := region.Count() - region.Transferred()
		size := e.chunkSize
		if int64(size) > remaining {
			size = int(remaining)
		}
		out, err := ctx.Channel().Allocator().Acquire(size)
		if err != nil {
			return err
		}
		n, readErr := region.Read(out.WritableBytesView())
		if n > 0 {
			if err := out.AdvanceWriter(n); err != nil {
				out.Release()
				return err
			}
			if err := ctx.Write(out); err != nil {
				out.Release()
				return err
			}
		} else {
			out.Release()
		}
		if readErr != nil {
			if errors.Is(readErr, io.EOF) && region.Transferred() >= region.Count() {
				break
			}
			return readErr
		}
	}
	return nil
}
