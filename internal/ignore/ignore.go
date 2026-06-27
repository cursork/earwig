package ignore

import (
	"fmt"
	"os"
	"strings"

	gitignore "github.com/sabhiram/go-gitignore"
)

// Matcher decides whether a relative path should be ignored.
//
// Patterns come from several sources: the builtins (.earwig, .git) plus each
// ignore file passed to New (e.g. .earwig/ignore, .gitignore). A path matching
// any exclude is ignored — UNLESS it also matches a keep, which always wins.
//
// A line beginning with '!' is a keep. Unlike gitignore's own '!' (which only
// re-includes within the single file that declares it), an earwig keep wins
// across every source, so `!runs/` in .earwig/ignore overrides `runs/` in
// .gitignore. Keeps beat excludes regardless of source or declaration order —
// mnemonically, '!' reads as "don't ignore".
//
// Limitation: a keep cannot resurrect a path beneath a directory that is
// excluded by a bare-name pattern (e.g. `secrets` with no trailing slash),
// because the tree walk hard-prunes such a directory before descending. Keep
// the directory itself (or exclude it with a trailing-slash pattern, which does
// not prune) to reach files inside it. This mirrors git's own behaviour.
//
// The builtin ignores (.earwig, .git) are absolute: a keep can NOT override
// them. Otherwise `!.earwig` would let earwig snapshot its own database, and
// `!.git` would put git internals under restore's management — which deletes
// untracked files. Builtins are therefore checked before keeps.
type Matcher struct {
	builtins *gitignore.GitIgnore   // .earwig, .git — always ignored, never kept.
	excludes []*gitignore.GitIgnore // a path matching any of these is ignored…
	keeps    []*gitignore.GitIgnore // …unless it matches a keep, which wins.
}

func New(ignoreFiles []string) (*Matcher, error) {
	m := &Matcher{
		// .earwig/ and .git/ are always ignored, regardless of ignore files,
		// and cannot be re-included by a keep (see type doc).
		builtins: gitignore.CompileIgnoreLines(".earwig", ".git"),
	}

	for _, path := range ignoreFiles {
		data, err := os.ReadFile(path)
		if err != nil {
			if os.IsNotExist(err) {
				continue
			}
			return nil, fmt.Errorf("reading ignore file %s: %w", path, err)
		}
		lines := strings.Split(string(data), "\n")

		// The file as a whole retains standard gitignore semantics (including
		// intra-file '!' negation), so existing .gitignore handling is unchanged.
		m.excludes = append(m.excludes, gitignore.CompileIgnoreLines(lines...))

		// '!' lines are additionally promoted to cross-source keeps, compiled
		// with the leading '!' stripped so the pattern matches the paths it
		// protects (e.g. `!runs/` becomes the keep pattern `runs/`).
		if keepLines := extractKeeps(lines); len(keepLines) > 0 {
			m.keeps = append(m.keeps, gitignore.CompileIgnoreLines(keepLines...))
		}
	}

	return m, nil
}

// extractKeeps returns the keep patterns (lines beginning with '!') with the
// leading '!' removed. Blank lines and comments are skipped.
func extractKeeps(lines []string) []string {
	var keeps []string
	for _, line := range lines {
		trimmed := strings.TrimSpace(line)
		if trimmed == "" || strings.HasPrefix(trimmed, "#") {
			continue
		}
		if strings.HasPrefix(trimmed, "!") {
			keeps = append(keeps, trimmed[1:])
		}
	}
	return keeps
}

// Match reports whether relPath should be ignored. Builtins (.earwig, .git)
// are always ignored and take precedence over keeps. Otherwise a keep wins: if
// any keep pattern matches, the path is tracked even when an exclude matches.
func (m *Matcher) Match(relPath string) bool {
	if m.builtins.MatchesPath(relPath) {
		return true
	}
	for _, k := range m.keeps {
		if k.MatchesPath(relPath) {
			return false
		}
	}
	for _, e := range m.excludes {
		if e.MatchesPath(relPath) {
			return true
		}
	}
	return false
}
