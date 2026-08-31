package transport

import (
	"testing"

	"gnalloy.org/gnalloy/transport/poller/memory"
)

func BenchmarkEventLoopSubmitBurst(b *testing.B) {
	p := memory.New()
	loop, err := NewEventLoop(EventLoopConfig{
		ID:             1,
		Poller:         p,
		TaskQueueSize:  512,
		EventBatchSize: 512,
		StartMillis:    0,
	})
	if err != nil {
		b.Fatal(err)
	}
	defer loop.Close()

	task := func() {}
	b.ReportAllocs()
	for submitted := 0; submitted < b.N; {
		batch := min(256, b.N-submitted)
		for i := 0; i < batch; i++ {
			if err := loop.Submit(task); err != nil {
				b.Fatal(err)
			}
		}
		if err := loop.RunOnce(0); err != nil {
			b.Fatal(err)
		}
		submitted += batch
	}
}
