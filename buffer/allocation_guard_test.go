package buffer

import "testing"

func TestHeapAllocatorAcquireReleaseAllocationBudget(t *testing.T) {
	alloc := NewHeapAllocator()
	warm, err := alloc.Acquire(4096)
	if err != nil {
		t.Fatal(err)
	}
	warm.Release()

	var runErr error
	allocs := testing.AllocsPerRun(1000, func() {
		buf, err := alloc.Acquire(4096)
		if err != nil {
			runErr = err
			return
		}
		buf.Release()
	})
	if runErr != nil {
		t.Fatal(runErr)
	}
	if allocs != 0 {
		t.Fatalf("allocs/run=%f, want 0", allocs)
	}
}
