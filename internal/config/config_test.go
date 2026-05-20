package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestParseSize(t *testing.T) {
	cases := []struct {
		in   string
		want int64
		err  bool
	}{
		{"100MB", 100 * 1024 * 1024, false},
		{"1GB", 1024 * 1024 * 1024, false},
		{"500KB", 500 * 1024, false},
		{"1024B", 1024, false},
		{"2048", 2048, false},
		{"0.5MB", 512 * 1024, false},
		{"off", offThreshold, false},
		{"OFF", offThreshold, false},
		{"", offThreshold, false},
		{"  100MB  ", 100 * 1024 * 1024, false},
		{"100mb", 100 * 1024 * 1024, false},
		{"-1MB", 0, true},
		{"banana", 0, true},
	}
	for _, c := range cases {
		got, err := parseSize(c.in)
		if c.err {
			if err == nil {
				t.Errorf("parseSize(%q): expected error, got %d", c.in, got)
			}
			continue
		}
		if err != nil {
			t.Errorf("parseSize(%q): unexpected error: %v", c.in, err)
			continue
		}
		if got != c.want {
			t.Errorf("parseSize(%q): got %d, want %d", c.in, got, c.want)
		}
	}
}

func TestLoad_MissingFile(t *testing.T) {
	c, err := Load(filepath.Join(t.TempDir(), "missing.json"))
	if err != nil {
		t.Fatalf("missing file should return default, got error: %v", err)
	}
	if got := c.SizeWarnThreshold("anything"); got != DefaultSizeWarnBytes {
		t.Errorf("default threshold: got %d, want %d", got, DefaultSizeWarnBytes)
	}
}

func TestLoad_DefaultAndOverrides(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.json")
	cfg := `{
		"sizeWarn": {
			"default": "50MB",
			"overrides": {
				"test-reports/": "500MB",
				"vendor/": "off",
				"aplcore": "off"
			}
		}
	}`
	if err := os.WriteFile(path, []byte(cfg), 0644); err != nil {
		t.Fatal(err)
	}
	c, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}

	cases := []struct {
		path string
		want int64
	}{
		{"main.go", 50 * 1024 * 1024},
		{"test-reports/foo.html", 500 * 1024 * 1024},
		{"vendor/big.tar", offThreshold},
		{"aplcore", offThreshold},
		{"sub/aplcore", offThreshold},
	}
	for _, tc := range cases {
		if got := c.SizeWarnThreshold(tc.path); got != tc.want {
			t.Errorf("threshold(%q): got %d, want %d", tc.path, got, tc.want)
		}
	}
}

func TestLoad_InvalidSize(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.json")
	if err := os.WriteFile(path, []byte(`{"sizeWarn":{"default":"banana"}}`), 0644); err != nil {
		t.Fatal(err)
	}
	if _, err := Load(path); err == nil {
		t.Fatal("expected error for invalid size")
	}
}

func TestLoad_InvalidJSON(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.json")
	if err := os.WriteFile(path, []byte(`not json`), 0644); err != nil {
		t.Fatal(err)
	}
	if _, err := Load(path); err == nil {
		t.Fatal("expected error for invalid JSON")
	}
}

func TestDefault(t *testing.T) {
	c := Default()
	if got := c.SizeWarnThreshold("x"); got != DefaultSizeWarnBytes {
		t.Errorf("default: got %d, want %d", got, DefaultSizeWarnBytes)
	}
}

func TestNilSafe(t *testing.T) {
	var c *Config
	if got := c.SizeWarnThreshold("x"); got != DefaultSizeWarnBytes {
		t.Errorf("nil: got %d, want %d", got, DefaultSizeWarnBytes)
	}
}
