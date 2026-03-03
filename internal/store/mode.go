package store

import "os"

// MaskMode strips all mode bits above the permission bits (0o777).
// This prevents a crafted database from setting setuid, setgid, or sticky
// bits on restored files. The postcondition guarantees that only the low
// 9 bits (rwxrwxrwx) survive.
//
// @ ensures result == os.FileMode(mode & 0x1FF)
// @ decreases
func MaskMode(mode uint32) (result os.FileMode) {
	return os.FileMode(mode).Perm()
}
