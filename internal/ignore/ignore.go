package ignore

import (
	"os"
	"strings"

	gitignore "github.com/sabhiram/go-gitignore"
)

type Matcher struct {
	patterns []*gitignore.GitIgnore
}

func New(ignoreFiles []string) (*Matcher, error) {
	m := &Matcher{}

	// Always ignore .earwig/ and .git/
	builtins := gitignore.CompileIgnoreLines(".earwig", ".git")
	m.patterns = append(m.patterns, builtins)

	for _, path := range ignoreFiles {
		data, err := os.ReadFile(path)
		if err != nil {
			continue // Skip unreadable files
		}
		lines := strings.Split(string(data), "\n")
		ig := gitignore.CompileIgnoreLines(lines...)
		m.patterns = append(m.patterns, ig)
	}

	return m, nil
}

func (m *Matcher) Match(relPath string) bool {
	for _, p := range m.patterns {
		if p.MatchesPath(relPath) {
			return true
		}
	}
	return false
}
