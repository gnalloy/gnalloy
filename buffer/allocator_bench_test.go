package buffer

import "testing"

func BenchmarkHeapAllocatorAcquireRelease(b *testing.B) {
	alloc := NewHeapAllocator()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		buf, err := alloc.Acquire(4096)
		if err != nil {
			b.Fatal(err)
		}
		buf.Release()
	}
}

func BenchmarkPooledAllocatorAcquireRelease(b *testing.B) {
	alloc, err := NewPooledAllocator(PooledAllocatorConfig{SizeClasses: []int{4096}})
	if err != nil {
		b.Fatal(err)
	}
	warm, err := alloc.Acquire(4096)
	if err != nil {
		b.Fatal(err)
	}
	warm.Release()

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		buf, err := alloc.Acquire(4096)
		if err != nil {
			b.Fatal(err)
		}
		buf.Release()
	}
}

func BenchmarkPooledAllocatorParallelAcquireRelease(b *testing.B) {
	alloc, err := NewPooledAllocator(PooledAllocatorConfig{
		SizeClasses:       []int{4096},
		MaxCachedPerClass: 4096,
	})
	if err != nil {
		b.Fatal(err)
	}
	warm, err := alloc.Acquire(4096)
	if err != nil {
		b.Fatal(err)
	}
	warm.Release()

	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			buf, err := alloc.Acquire(4096)
			if err != nil {
				b.Fatal(err)
			}
			buf.Release()
		}
	})
}

func BenchmarkMmapAllocatorAcquireRelease(b *testing.B) {
	alloc, err := NewMmapAllocator(MmapAllocatorConfig{
		BlockSize: 4096,
		Blocks:    4096,
	})
	if err != nil {
		b.Skip(err)
	}
	defer alloc.Close()

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		buf, err := alloc.Acquire(4096)
		if err != nil {
			b.Fatal(err)
		}
		buf.Release()
	}
}
