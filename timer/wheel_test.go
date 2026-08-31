package timer

import "testing"

func TestWheelScheduleAdvance(t *testing.T) {
	w, err := NewWheel(10, 64, 1000)
	if err != nil {
		t.Fatal(err)
	}
	calls := 0
	_, err = w.Schedule(25, CallbackFunc(func(Context, *Task) {
		calls++
	}))
	if err != nil {
		t.Fatal(err)
	}
	w.Advance(1019, 0)
	if calls != 0 {
		t.Fatalf("calls=%d before deadline", calls)
	}
	w.Advance(1030, 0)
	if calls != 1 {
		t.Fatalf("calls=%d after deadline", calls)
	}
}

func TestWheelCancel(t *testing.T) {
	w, err := NewWheel(10, 64, 0)
	if err != nil {
		t.Fatal(err)
	}
	calls := 0
	task, err := w.Schedule(10, CallbackFunc(func(Context, *Task) {
		calls++
	}))
	if err != nil {
		t.Fatal(err)
	}
	if !w.Cancel(task) {
		t.Fatal("cancel failed")
	}
	w.Advance(20, 0)
	if calls != 0 {
		t.Fatalf("cancelled timer fired")
	}
}

func BenchmarkWheelScheduleAdvance(b *testing.B) {
	w, err := NewWheel(10, 1024, 0)
	if err != nil {
		b.Fatal(err)
	}
	cb := CallbackFunc(func(Context, *Task) {})
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, err := w.Schedule(10, cb); err != nil {
			b.Fatal(err)
		}
		w.Advance(int64((i+1)*10), 0)
	}
}
