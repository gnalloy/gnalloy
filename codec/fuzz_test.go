package codec

import (
	"testing"

	"gnalloy.org/gnalloy/buffer"
	"gnalloy.org/gnalloy/channel"
)

const maxCodecFuzzInput = 4096

func FuzzLengthFieldBasedFrameDecoder(f *testing.F) {
	f.Add([]byte{0, 0, 0, 5, 'h', 'e', 'l', 'l', 'o'})
	f.Add([]byte{0, 0, 4, 0})
	f.Add([]byte{0xff, 0xff, 0xff, 0xff})
	f.Fuzz(func(t *testing.T, data []byte) {
		decoder, err := NewLengthFieldBasedFrameDecoder(1024, 0, 4, 0, 4, buffer.BigEndian)
		if err != nil {
			t.Fatal(err)
		}
		fuzzByteDecoder(t, decoder, data)
	})
}

func FuzzLineBasedFrameDecoder(f *testing.F) {
	f.Add([]byte("hello\nworld\r\n"))
	f.Add([]byte("unterminated-line"))
	f.Add([]byte("oversized-line-without-newline"))
	f.Fuzz(func(t *testing.T, data []byte) {
		decoder, err := NewLineBasedFrameDecoderWithOptions(1024, true, false)
		if err != nil {
			t.Fatal(err)
		}
		fuzzByteDecoder(t, decoder, data)
	})
}

func FuzzDelimiterBasedFrameDecoder(f *testing.F) {
	f.Add([]byte("alpha|beta<END>"))
	f.Add([]byte("<END>"))
	f.Add([]byte("payload-without-delimiter"))
	f.Fuzz(func(t *testing.T, data []byte) {
		decoder, err := NewDelimiterBasedFrameDecoder(1024, true, true, []byte("|"), []byte("<END>"))
		if err != nil {
			t.Fatal(err)
		}
		fuzzByteDecoder(t, decoder, data)
	})
}

func fuzzByteDecoder(t *testing.T, decoder channel.Handler, data []byte) {
	t.Helper()
	if len(data) > maxCodecFuzzInput {
		return
	}
	ch := channel.NewLocalChannel(1, buffer.NewHeapAllocator(), nil)
	if err := ch.Pipeline().AddLast("decoder", decoder); err != nil {
		t.Fatal(err)
	}
	fireFuzzBytes(ch, data)
	ch.Pipeline().FireChannelInactive()
}

func fireFuzzBytes(ch channel.Channel, data []byte) {
	if len(data) == 0 {
		ch.Pipeline().FireChannelRead(fuzzByteBuf(nil))
		return
	}
	mid := len(data) / 2
	if mid > 0 {
		ch.Pipeline().FireChannelRead(fuzzByteBuf(data[:mid]))
	}
	ch.Pipeline().FireChannelRead(fuzzByteBuf(data[mid:]))
}

func fuzzByteBuf(data []byte) buffer.ByteBuf {
	buf := buffer.NewHeapBuffer(len(data))
	_, _ = buf.WriteBytes(data)
	return buf
}
