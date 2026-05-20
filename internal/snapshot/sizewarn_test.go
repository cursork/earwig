package snapshot

import (
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/nk/earwig/internal/config"
	"github.com/nk/earwig/internal/ignore"
)

// captureStderr runs fn with os.Stderr redirected and returns whatever was
// written. Tests use this to assert on size-warning output.
func captureStderr(t *testing.T, fn func()) string {
	t.Helper()
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	orig := os.Stderr
	os.Stderr = w
	done := make(chan string, 1)
	go func() {
		buf, _ := io.ReadAll(r)
		done <- string(buf)
	}()
	fn()
	w.Close()
	os.Stderr = orig
	return <-done
}

// configWithThreshold builds a Config via JSON so we go through the same parse
// path as production. Caller controls the default and overrides.
func configWithThreshold(t *testing.T, dir, json string) *config.Config {
	t.Helper()
	path := filepath.Join(dir, "config.json")
	if err := os.WriteFile(path, []byte(json), 0644); err != nil {
		t.Fatal(err)
	}
	cfg, err := config.Load(path)
	if err != nil {
		t.Fatal(err)
	}
	return cfg
}

func TestSizeWarn_EmitsAboveThreshold(t *testing.T) {
	s, dir := setup(t)
	writeFile(t, dir, "big.bin", strings.Repeat("x", 200))

	ig, _ := ignore.New(nil)
	cfg := configWithThreshold(t, t.TempDir(), `{"sizeWarn":{"default":"100B"}}`)
	c := NewCreator(s, dir, ig, cfg)

	out := captureStderr(t, func() {
		if _, err := c.TakeSnapshot(nil, "test"); err != nil {
			t.Fatal(err)
		}
	})
	if !strings.Contains(out, "big.bin") {
		t.Errorf("expected warning to mention big.bin, got: %q", out)
	}
	if !strings.Contains(out, "threshold") {
		t.Errorf("expected warning to mention threshold, got: %q", out)
	}
}

func TestSizeWarn_DedupesSameSize(t *testing.T) {
	s, dir := setup(t)
	writeFile(t, dir, "big.bin", strings.Repeat("x", 200))

	ig, _ := ignore.New(nil)
	cfg := configWithThreshold(t, t.TempDir(), `{"sizeWarn":{"default":"100B"}}`)
	c := NewCreator(s, dir, ig, cfg)

	// First snapshot warns.
	captureStderr(t, func() { c.TakeSnapshot(nil, "first") })

	// Touch file mtime but keep size — should be a no-op snapshot, but even a
	// re-read should not emit a duplicate warning.
	out := captureStderr(t, func() {
		// Force a re-walk by writing identical content (same size).
		writeFile(t, dir, "big.bin", strings.Repeat("x", 200))
		parentID := int64(1)
		c.TakeSnapshot(&parentID, "second")
	})
	if strings.Contains(out, "big.bin") {
		t.Errorf("expected no duplicate warning, got: %q", out)
	}
}

func TestSizeWarn_ReWarnsOnGrowth(t *testing.T) {
	s, dir := setup(t)
	writeFile(t, dir, "grows.bin", strings.Repeat("x", 200))

	ig, _ := ignore.New(nil)
	cfg := configWithThreshold(t, t.TempDir(), `{"sizeWarn":{"default":"100B"}}`)
	c := NewCreator(s, dir, ig, cfg)

	captureStderr(t, func() { c.TakeSnapshot(nil, "first") })

	// Grow the file.
	writeFile(t, dir, "grows.bin", strings.Repeat("x", 500))
	out := captureStderr(t, func() {
		parentID := int64(1)
		c.TakeSnapshot(&parentID, "second")
	})
	if !strings.Contains(out, "grows.bin") {
		t.Errorf("expected warning after growth, got: %q", out)
	}
}

func TestSizeWarn_OffOverrideSilences(t *testing.T) {
	s, dir := setup(t)
	writeFile(t, dir, "noisy.bin", strings.Repeat("x", 200))

	ig, _ := ignore.New(nil)
	cfg := configWithThreshold(t, t.TempDir(),
		`{"sizeWarn":{"default":"100B","overrides":{"noisy.bin":"off"}}}`)
	c := NewCreator(s, dir, ig, cfg)

	out := captureStderr(t, func() { c.TakeSnapshot(nil, "test") })
	if strings.Contains(out, "noisy.bin") {
		t.Errorf("expected 'off' override to silence warning, got: %q", out)
	}
}

func TestSizeWarn_BelowThresholdNoWarn(t *testing.T) {
	s, dir := setup(t)
	writeFile(t, dir, "small.txt", "tiny")

	ig, _ := ignore.New(nil)
	cfg := configWithThreshold(t, t.TempDir(), `{"sizeWarn":{"default":"100B"}}`)
	c := NewCreator(s, dir, ig, cfg)

	out := captureStderr(t, func() { c.TakeSnapshot(nil, "test") })
	if strings.Contains(out, "small.txt") {
		t.Errorf("expected no warning below threshold, got: %q", out)
	}
}

func TestSizeWarn_NilConfigUsesDefault(t *testing.T) {
	s, dir := setup(t)
	// 1KB file — way under the 100MB default, must not warn.
	writeFile(t, dir, "modest.bin", strings.Repeat("x", 1024))

	ig, _ := ignore.New(nil)
	c := NewCreator(s, dir, ig, nil)

	out := captureStderr(t, func() { c.TakeSnapshot(nil, "test") })
	if strings.Contains(out, "modest.bin") {
		t.Errorf("expected no warning under default threshold, got: %q", out)
	}
}
