package snapshot

import (
	"path/filepath"
	"strings"
)

// hasChanges returns true if any of the four change categories is non-empty.
// This is the core predicate for deciding whether a restore would modify the
// filesystem at all.
//
// @ ensures result == (nDelete > 0 || nWrite > 0 || nModify > 0 || nChmod > 0)
func hasChanges(nDelete, nWrite, nModify, nChmod int) (result bool) {
	return nDelete > 0 || nWrite > 0 || nModify > 0 || nChmod > 0
}

// isUnsafeSymlinkTarget returns true if a symlink target is potentially unsafe:
// either an absolute path or containing ".." which could escape the root.
// Extracting this from restore.go allows formal verification of exactly what
// "unsafe" means.
//
// @ ensures result == (filepath.IsAbs(target) || strings.Contains(target, ".."))
// @ decreases
func isUnsafeSymlinkTarget(target string) (result bool) {
	return filepath.IsAbs(target) || strings.Contains(target, "..")
}

// readFileType returns the file type string for a regular file.
// Always returns "file" — this is the type tag stored in snapshot_files.
//
// @ ensures result == "file"
func readFileType() (result string) {
	return "file"
}

// readSymlinkType returns the file type string for a symlink.
// Always returns "symlink" — this is the type tag stored in snapshot_files.
//
// @ ensures result == "symlink"
func readSymlinkType() (result string) {
	return "symlink"
}
