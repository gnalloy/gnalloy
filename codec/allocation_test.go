package codec

import (
	"testing"

	"gnalloy.org/gnalloy/buffer"
	"gnalloy.org/gnalloy/channel"
)

func TestFrameDecodersStayAllocationFreeOnHotPath(t *testing.T) {
	cases := []struct {
		name    string
		decoder channel.Handler
		payload []byte
	}{
		{name: "fixed", decoder: mustFixedLengthDecoder(t, 4), payload: []byte("ping")},
		{name: "line", decoder: mustLineDecoder(t, 64), payload: []byte("ping\n")},
		{name: "delimiter", decoder: mustDelimiterDecoder(t, 64), payload: []byte("ping<END>")},
		{name: "length-field", decoder: mustLengthFieldDecoder(t), payload: []byte{0, 0, 0, 4, 'p', 'i', 'n', 'g'}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			collector := &frameCollector{}
			ch := channel.NewLocalChannel(1, buffer.NewHeapAllocator(), nil)
			_ = ch.Pipeline().AddLast("decoder", tc.decoder)
			_ = ch.Pipeline().AddLast("collector", collector)
			alloc := buffer.NewHeapAllocator()
			for i := 0; i < 128; i++ {
				fireAllocationBudgetFrame(t, ch, collector, alloc, tc.payload)
			}
			allocs := testing.AllocsPerRun(1000, func() {
				fireAllocationBudgetFrame(t, ch, collector, alloc, tc.payload)
			})
			if allocs != 0 {
				t.Fatalf("allocs/run=%f, want 0", allocs)
			}
		})
	}
}

func fireAllocationBudgetFrame(t *testing.T, ch channel.Channel, collector *frameCollector, alloc buffer.Allocator, payload []byte) {
	t.Helper()
	buf, err := alloc.Acquire(len(payload))
	if err != nil {
		t.Fatal(err)
	}
	_, _ = buf.WriteBytes(payload)
	ch.Pipeline().FireChannelRead(buf)
	collector.release()
}

func mustFixedLengthDecoder(t *testing.T, size int) channel.Handler {
	t.Helper()
	decoder, err := NewFixedLengthFrameDecoder(size)
	if err != nil {
		t.Fatal(err)
	}
	return decoder
}

func mustLineDecoder(t *testing.T, size int) channel.Handler {
	t.Helper()
	decoder, err := NewLineBasedFrameDecoder(size)
	if err != nil {
		t.Fatal(err)
	}
	return decoder
}

func mustDelimiterDecoder(t *testing.T, size int) channel.Handler {
	t.Helper()
	decoder, err := NewDelimiterBasedFrameDecoder(size, true, true, []byte("<END>"))
	if err != nil {
		t.Fatal(err)
	}
	return decoder
}

func mustLengthFieldDecoder(t *testing.T) channel.Handler {
	t.Helper()
	decoder, err := NewLengthFieldBasedFrameDecoder(1024, 0, 4, 0, 4, buffer.BigEndian)
	if err != nil {
		t.Fatal(err)
	}
	return decoder
}
