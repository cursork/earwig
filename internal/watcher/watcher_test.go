package watcher

import (
	"sync/atomic"
	"testing"
	"time"
)

func TestDebouncerFiresOnce(t *testing.T) {
	var count atomic.Int32
	d := NewDebouncer(50 * time.Millisecond)
	defer d.Stop()

	cb := func() { count.Add(1) }

	// Rapid triggers
	for i := 0; i < 10; i++ {
		d.Trigger(cb)
	}

	time.Sleep(100 * time.Millisecond)
	if got := count.Load(); got != 1 {
		t.Fatalf("expected 1 callback, got %d", got)
	}
}

func TestDebouncerResetsAfterFire(t *testing.T) {
	var count atomic.Int32
	d := NewDebouncer(50 * time.Millisecond)
	defer d.Stop()

	cb := func() { count.Add(1) }

	d.Trigger(cb)
	time.Sleep(100 * time.Millisecond) // First fire

	d.Trigger(cb)
	time.Sleep(100 * time.Millisecond) // Second fire

	if got := count.Load(); got != 2 {
		t.Fatalf("expected 2 callbacks, got %d", got)
	}
}

func TestDebouncerStop(t *testing.T) {
	var count atomic.Int32
	d := NewDebouncer(100 * time.Millisecond)

	d.Trigger(func() { count.Add(1) })
	d.Stop()

	time.Sleep(150 * time.Millisecond)
	if got := count.Load(); got != 0 {
		t.Fatalf("expected 0 callbacks after stop, got %d", got)
	}
}
