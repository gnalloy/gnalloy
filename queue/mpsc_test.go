package queue

import (
	"sync"
	"testing"
)

func TestMPSCOfferPoll(t *testing.T) {
	q := NewMPSC[int](4)
	for i := 0; i < 4; i++ {
		if !q.Offer(i) {
			t.Fatalf("offer %d failed", i)
		}
	}
	if q.Offer(5) {
		t.Fatal("offer should fail when queue is full")
	}
	for i := 0; i < 4; i++ {
		got, ok := q.Poll()
		if !ok || got != i {
			t.Fatalf("poll got (%d,%v), want (%d,true)", got, ok, i)
		}
	}
	if _, ok := q.Poll(); ok {
		t.Fatal("empty queue returned value")
	}
}

func TestMPSCMultipleProducers(t *testing.T) {
	q := NewMPSC[int](1024)
	const producers = 4
	const perProducer = 128

	var wg sync.WaitGroup
	for p := 0; p < producers; p++ {
		wg.Add(1)
		go func(base int) {
			defer wg.Done()
			for i := 0; i < perProducer; i++ {
				for !q.Offer(base + i) {
				}
			}
		}(p * perProducer)
	}
	wg.Wait()

	count := 0
	seen := make(map[int]bool)
	for {
		v, ok := q.Poll()
		if !ok {
			break
		}
		seen[v] = true
		count++
	}
	if count != producers*perProducer {
		t.Fatalf("count=%d", count)
	}
	for i := 0; i < producers*perProducer; i++ {
		if !seen[i] {
			t.Fatalf("missing value %d", i)
		}
	}
}

func BenchmarkMPSCOfferPoll(b *testing.B) {
	q := NewMPSC[int](1024)
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if !q.Offer(i) {
			b.Fatal("offer failed")
		}
		if got, ok := q.Poll(); !ok || got != i {
			b.Fatalf("poll got (%d,%v), want (%d,true)", got, ok, i)
		}
	}
}
