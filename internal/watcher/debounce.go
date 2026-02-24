package watcher

import (
	"sync"
	"time"
)

// Debouncer implements leading-edge debounce: the first event starts a timer,
// subsequent events are ignored until the timer fires.
type Debouncer struct {
	interval time.Duration
	mu       sync.Mutex
	timer    *time.Timer
	pending  bool
}

func NewDebouncer(interval time.Duration) *Debouncer {
	return &Debouncer{interval: interval}
}

func (d *Debouncer) Trigger(callback func()) {
	d.mu.Lock()
	defer d.mu.Unlock()

	if d.pending {
		return
	}

	d.pending = true
	d.timer = time.AfterFunc(d.interval, func() {
		d.mu.Lock()
		d.pending = false
		d.mu.Unlock()
		callback()
	})
}

func (d *Debouncer) Stop() {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.timer != nil {
		d.timer.Stop()
	}
}
