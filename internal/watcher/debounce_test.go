package watcher

import (
	"sync"
	"testing"
	"time"
)

func TestDebouncerBasicTrigger(t *testing.T) {
	done := make(chan struct{})
	d := NewDebouncer(10 * time.Millisecond)
	defer d.Stop()

	d.Trigger(func() {
		close(done)
	})

	select {
	case <-done:
		// pass
	case <-time.After(time.Second):
		t.Fatal("callback never fired")
	}
}

func TestDebouncerCoalescesRapidTriggers(t *testing.T) {
	var mu sync.Mutex
	count := 0
	done := make(chan struct{})

	d := NewDebouncer(50 * time.Millisecond)
	defer d.Stop()

	cb := func() {
		mu.Lock()
		count++
		mu.Unlock()
		// Signal after first callback completes
		select {
		case done <- struct{}{}:
		default:
		}
	}

	// Trigger rapidly — only the first should register (rest ignored while pending)
	for i := 0; i < 10; i++ {
		d.Trigger(cb)
	}

	// Wait for callback
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("callback never fired")
	}

	// Give a bit of time for any extra callbacks to fire
	time.Sleep(100 * time.Millisecond)

	mu.Lock()
	c := count
	mu.Unlock()
	if c != 1 {
		t.Fatalf("expected 1 callback, got %d", c)
	}
}

func TestDebouncerNonOverlapping(t *testing.T) {
	// Verify that a new trigger during a callback doesn't cause overlap.
	// The callback should block new triggers until it returns.
	var mu sync.Mutex
	var concurrent int
	maxConcurrent := 0

	firstDone := make(chan struct{})
	secondDone := make(chan struct{})

	d := NewDebouncer(10 * time.Millisecond)
	defer d.Stop()

	callCount := 0
	d.Trigger(func() {
		mu.Lock()
		concurrent++
		if concurrent > maxConcurrent {
			maxConcurrent = concurrent
		}
		callCount++
		n := callCount
		mu.Unlock()

		// Simulate slow callback
		time.Sleep(50 * time.Millisecond)

		mu.Lock()
		concurrent--
		mu.Unlock()

		if n == 1 {
			close(firstDone)
		} else if n == 2 {
			close(secondDone)
		}
	})

	// Wait for first callback to start, then trigger again
	<-firstDone

	d.Trigger(func() {
		mu.Lock()
		concurrent++
		if concurrent > maxConcurrent {
			maxConcurrent = concurrent
		}
		callCount++
		mu.Unlock()

		mu.Lock()
		concurrent--
		mu.Unlock()
		close(secondDone)
	})

	select {
	case <-secondDone:
	case <-time.After(time.Second):
		t.Fatal("second callback never fired")
	}

	mu.Lock()
	mc := maxConcurrent
	mu.Unlock()
	if mc > 1 {
		t.Fatalf("callbacks overlapped: max concurrent = %d", mc)
	}
}

func TestDebouncerStopPreventsCallback(t *testing.T) {
	called := make(chan struct{}, 1)

	d := NewDebouncer(50 * time.Millisecond)
	d.Trigger(func() {
		called <- struct{}{}
	})
	d.Stop()

	// Wait longer than the debounce interval
	time.Sleep(100 * time.Millisecond)

	select {
	case <-called:
		t.Fatal("callback fired after Stop()")
	default:
		// pass — callback was prevented
	}
}

func TestDebouncerRetriggersAfterCallback(t *testing.T) {
	results := make(chan int, 2)

	d := NewDebouncer(10 * time.Millisecond)
	defer d.Stop()

	d.Trigger(func() {
		results <- 1
	})

	// Wait for first
	select {
	case v := <-results:
		if v != 1 {
			t.Fatalf("expected 1, got %d", v)
		}
	case <-time.After(time.Second):
		t.Fatal("first callback never fired")
	}

	// Now trigger again — should work since previous callback completed
	d.Trigger(func() {
		results <- 2
	})

	select {
	case v := <-results:
		if v != 2 {
			t.Fatalf("expected 2, got %d", v)
		}
	case <-time.After(time.Second):
		t.Fatal("second callback never fired")
	}
}
