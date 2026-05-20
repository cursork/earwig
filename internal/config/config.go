// Package config parses .earwig/config.json. Currently supports a per-path
// size-warning threshold; sized to expand for future configurable behaviour.
package config

import (
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"

	gitignore "github.com/sabhiram/go-gitignore"
)

// DefaultSizeWarnBytes is the threshold used when no config is present.
const DefaultSizeWarnBytes int64 = 100 * 1024 * 1024

// sentinel value meaning "no warning"
const offThreshold int64 = -1

type Config struct {
	sizeWarnDefault   int64
	sizeWarnOverrides []sizeOverride
}

type sizeOverride struct {
	pattern   string
	matcher   *gitignore.GitIgnore
	threshold int64
}

type rawConfig struct {
	SizeWarn *rawSizeWarn `json:"sizeWarn,omitempty"`
}

type rawSizeWarn struct {
	Default   *string           `json:"default,omitempty"`
	Overrides map[string]string `json:"overrides,omitempty"`
}

// Load reads .earwig/config.json. Missing file returns Default().
func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return Default(), nil
		}
		return nil, fmt.Errorf("reading config %s: %w", path, err)
	}

	var raw rawConfig
	if err := json.Unmarshal(data, &raw); err != nil {
		return nil, fmt.Errorf("parsing config %s: %w", path, err)
	}

	c := Default()
	if raw.SizeWarn != nil {
		if raw.SizeWarn.Default != nil {
			n, err := parseSize(*raw.SizeWarn.Default)
			if err != nil {
				return nil, fmt.Errorf("config sizeWarn.default: %w", err)
			}
			c.sizeWarnDefault = n
		}
		for pattern, sz := range raw.SizeWarn.Overrides {
			n, err := parseSize(sz)
			if err != nil {
				return nil, fmt.Errorf("config sizeWarn.overrides[%q]: %w", pattern, err)
			}
			c.sizeWarnOverrides = append(c.sizeWarnOverrides, sizeOverride{
				pattern:   pattern,
				matcher:   gitignore.CompileIgnoreLines(pattern),
				threshold: n,
			})
		}
	}
	return c, nil
}

func Default() *Config {
	return &Config{sizeWarnDefault: DefaultSizeWarnBytes}
}

// SizeWarnThreshold returns the byte threshold for relPath. The most recently
// declared matching override wins; otherwise the default applies. A threshold
// of -1 means warnings are disabled for this path.
func (c *Config) SizeWarnThreshold(relPath string) int64 {
	if c == nil {
		return DefaultSizeWarnBytes
	}
	for i := len(c.sizeWarnOverrides) - 1; i >= 0; i-- {
		o := c.sizeWarnOverrides[i]
		if o.matcher.MatchesPath(relPath) {
			return o.threshold
		}
	}
	return c.sizeWarnDefault
}

// parseSize accepts "100MB", "1GB", "500KB", "1024" (bytes), or "off"/"" for disabled.
func parseSize(s string) (int64, error) {
	s = strings.TrimSpace(s)
	if s == "" || strings.EqualFold(s, "off") {
		return offThreshold, nil
	}
	upper := strings.ToUpper(s)
	var mult int64 = 1
	var numPart string
	switch {
	case strings.HasSuffix(upper, "GB"):
		mult, numPart = 1024*1024*1024, upper[:len(upper)-2]
	case strings.HasSuffix(upper, "MB"):
		mult, numPart = 1024*1024, upper[:len(upper)-2]
	case strings.HasSuffix(upper, "KB"):
		mult, numPart = 1024, upper[:len(upper)-2]
	case strings.HasSuffix(upper, "B"):
		mult, numPart = 1, upper[:len(upper)-1]
	default:
		numPart = upper
	}
	numPart = strings.TrimSpace(numPart)
	n, err := strconv.ParseFloat(numPart, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid size %q (expected e.g. '100MB' or 'off')", s)
	}
	if n < 0 {
		return 0, fmt.Errorf("invalid size %q (negative)", s)
	}
	return int64(n * float64(mult)), nil
}
