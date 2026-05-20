package main

import "golang.org/x/sys/unix"

// makeCBreak puts fd into cbreak mode: line buffering and echo are disabled
// so we can read single keystrokes, but OPOST (newline translation) and ISIG
// (Ctrl+C → SIGINT) stay on. Returns the previous termios for restoration.
func makeCBreak(fd int) (*unix.Termios, error) {
	t, err := unix.IoctlGetTermios(fd, ioctlGetTermios)
	if err != nil {
		return nil, err
	}
	old := *t
	mod := *t
	mod.Lflag &^= unix.ICANON | unix.ECHO | unix.ECHOE | unix.ECHOK | unix.ECHONL
	mod.Cc[unix.VMIN] = 1
	mod.Cc[unix.VTIME] = 0
	if err := unix.IoctlSetTermios(fd, ioctlSetTermios, &mod); err != nil {
		return nil, err
	}
	return &old, nil
}

func restoreTermios(fd int, t *unix.Termios) error {
	return unix.IoctlSetTermios(fd, ioctlSetTermios, t)
}

func isTerminal(fd int) bool {
	_, err := unix.IoctlGetTermios(fd, ioctlGetTermios)
	return err == nil
}
