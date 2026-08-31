package transport

const (
	DefaultWriteHighWatermark = 64 * 1024
	DefaultWriteLowWatermark  = 32 * 1024
)

// WriteBufferWatermark 描述出站缓冲区反压水位线，单位是字节。
type WriteBufferWatermark struct {
	Low  int
	High int
}

func DefaultWriteBufferWatermark() WriteBufferWatermark {
	return WriteBufferWatermark{Low: DefaultWriteLowWatermark, High: DefaultWriteHighWatermark}
}

// NormalizeWriteBufferWatermark 生成可安全使用的水位线配置。
func NormalizeWriteBufferWatermark(w WriteBufferWatermark) WriteBufferWatermark {
	def := DefaultWriteBufferWatermark()
	if w.High <= 0 {
		w.High = def.High
	}
	if w.Low <= 0 || w.Low >= w.High {
		w.Low = w.High / 2
	}
	return w
}
