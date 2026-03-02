package snapshot

import (
	"fmt"
	"path/filepath"
	"strings"
)

// safePath validates that relPath resolves to a location within rootDir.
// Returns the cleaned absolute path or an error if the path escapes the root.
// @ ensures err == nil ==> len(result) > 0
// @ ensures err == nil ==> strings.HasPrefix(result, filepath.Clean(rootDir) + string(filepath.Separator))
// @ ensures err != nil ==> result == ""
// @ ensures err != nil ==> err.ErrorMem()
func safePath(rootDir, relPath string) (result string, err error) {
	if relPath == "" {
		return "", fmt.Errorf("empty path")
	}
	if strings.ContainsRune(relPath, 0) {
		return "", fmt.Errorf("path contains NUL byte")
	}
	if filepath.IsAbs(relPath) {
		return "", fmt.Errorf("path is absolute: %s", relPath)
	}
	abs := filepath.Clean(filepath.Join(rootDir, filepath.FromSlash(relPath)))
	root := filepath.Clean(rootDir)
	if abs == root || !strings.HasPrefix(abs, root+string(filepath.Separator)) {
		return "", fmt.Errorf("path escapes root: %s", relPath)
	}
	return abs, nil
}
