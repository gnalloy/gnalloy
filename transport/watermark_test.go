package transport

import "testing"

func TestNormalizeWriteBufferWatermark(t *testing.T) {
	def := NormalizeWriteBufferWatermark(WriteBufferWatermark{})
	if def.High != DefaultWriteHighWatermark || def.Low != DefaultWriteLowWatermark {
		t.Fatalf("default watermark=%+v", def)
	}

	normalized := NormalizeWriteBufferWatermark(WriteBufferWatermark{Low: 8, High: 4})
	if normalized.High != 4 || normalized.Low != 2 {
		t.Fatalf("normalized=%+v, want low half of high", normalized)
	}
}
