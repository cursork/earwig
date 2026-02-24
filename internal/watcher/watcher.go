package watcher

import (
	"context"
	"log"
	"os"
	"path/filepath"

	"github.com/fsnotify/fsnotify"
	"github.com/nk/earwig/internal/ignore"
)

type Watcher struct {
	fsw     *fsnotify.Watcher
	root    string
	ignore  *ignore.Matcher
	OnEvent func()
}

func New(root string, ig *ignore.Matcher) (*Watcher, error) {
	fsw, err := fsnotify.NewWatcher()
	if err != nil {
		return nil, err
	}
	return &Watcher{
		fsw:    fsw,
		root:   root,
		ignore: ig,
	}, nil
}

func (w *Watcher) addRecursive(dir string) error {
	return filepath.WalkDir(dir, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return nil
		}
		if !d.IsDir() {
			return nil
		}
		relPath, _ := filepath.Rel(w.root, path)
		relPath = filepath.ToSlash(relPath)
		if relPath != "." && w.ignore.Match(relPath) {
			return filepath.SkipDir
		}
		return w.fsw.Add(path)
	})
}

func (w *Watcher) Run(ctx context.Context) error {
	if err := w.addRecursive(w.root); err != nil {
		return err
	}

	for {
		select {
		case <-ctx.Done():
			return w.fsw.Close()

		case event, ok := <-w.fsw.Events:
			if !ok {
				return nil
			}

			// Skip chmod events (noise from Spotlight, etc.)
			if event.Has(fsnotify.Chmod) && !event.Has(fsnotify.Create) && !event.Has(fsnotify.Write) && !event.Has(fsnotify.Remove) && !event.Has(fsnotify.Rename) {
				continue
			}

			relPath, _ := filepath.Rel(w.root, event.Name)
			relPath = filepath.ToSlash(relPath)

			if w.ignore.Match(relPath) {
				continue
			}

			// If a new directory was created, watch it recursively
			if event.Has(fsnotify.Create) {
				if info, err := os.Stat(event.Name); err == nil && info.IsDir() {
					w.addRecursive(event.Name)
				}
			}

			if w.OnEvent != nil {
				w.OnEvent()
			}

		case err, ok := <-w.fsw.Errors:
			if !ok {
				return nil
			}
			log.Printf("watcher error: %v", err)
		}
	}
}
