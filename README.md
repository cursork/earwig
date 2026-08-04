# earwig

Filesystem snapshot tool with time-travel. Tracks file changes between git commits, recovering work that never made it into version control.

earwig watches a directory, takes periodic snapshots to a local SQLite database, and lets you browse, diff, search, and restore any past state — including mid-session edits that were reverted, code written then lost during churn, or changes between commits that git never saw.

## Install

```bash
go build -o earwig .
```

Requires Go 1.24+. No CGO — cross-compiles cleanly.

## Quick start

```bash
cd ~/my-project
earwig init                  # create .earwig/ database
earwig watch -detach         # background watcher, snapshots on change

# ... work normally ...

earwig log                   # see snapshot history
earwig show abc123           # what changed in a snapshot
earwig diff abc123           # what would restore change (read-only)
earwig restore abc123        # restore filesystem to that point
earwig tui                   # interactive browser
```

## Commands

| Command | Purpose |
|---------|---------|
| `init` | Create `.earwig/` and database |
| `watch [-detach]` | Watch for changes and auto-snapshot (foreground or background) |
| `snapshot` | Take a manual snapshot |
| `log [file]` | Show snapshot history as git-style ASCII graph. Optional file filter. |
| `show <ref> [file...]` | Show A/M/D changes vs parent, or print file contents from a snapshot |
| `diff <ref>` | Show what a restore would change vs current filesystem (read-only) |
| `restore [-y] <ref>` | Restore filesystem to a snapshot (previews changes, prompts for confirmation) |
| `grep <pattern> [glob]` | Search file contents across all snapshots |
| `check [name] [hash]` | Create a named checkpoint (random name if omitted) |
| `check -d <name>` | Delete a checkpoint |
| `check -u <name> [hash]` | Move a checkpoint to a different snapshot |
| `checks` | List all checkpoints |
| `forget <ref>` | Delete a snapshot (re-parents children, cascade-deletes checkpoints, runs GC) |
| `trim [-y] <age\|ref>` | Delete snapshots older than a duration (`7d`, `2w`, `0s`) or a ref, then reclaim disk |
| `gc` | Remove orphaned blobs |
| `tui` | Interactive split-pane snapshot browser |
| `processes` | List running earwig watchers |
| `db [sql]` | Open SQLite shell, or run a query |

A `<ref>` is a snapshot hash prefix (8–12 chars) or a checkpoint name.

## Watcher

The watcher debounces at 1-minute intervals (trailing edge, non-overlapping). It tracks changed paths via fsnotify and only re-hashes those files. Every 10th snapshot does a full filesystem walk as a safety net.

```bash
earwig watch           # foreground
earwig watch -detach   # background daemon
earwig processes       # see what's running
```

When running in the foreground on a TTY, `earwig watch` accepts single-key commands without Enter:

| Key | Action |
|-----|--------|
| `c` | Checkpoint (auto-snapshots first, random name) |
| `s` | Manual snapshot now |
| `l` | Print `earwig log` inline |
| `t` | Launch the TUI (returns to watch on quit) |
| `/` | Prompt for a grep pattern and run `earwig grep` |
| `?` | Key help |
| `q` / Ctrl+C | Quit |

The watcher uses cbreak mode (line buffering and echo off, but signals and newline translation on), so background snapshot prints and warnings still render normally. When stdin is not a TTY (e.g. `-detach`, piped, redirected), the interactive layer is bypassed and the watcher behaves as before.

**One watcher per directory.** Starting `earwig watch` (or `-detach`) when another watcher is already watching the same directory is refused — earwig prints the running watcher's PID and exits non-zero rather than running a redundant second watcher. Use `earwig processes` to see what's already running.

## Snapshots

Each snapshot stores a **full file manifest** — not diffs. Content-addressable blob storage (SHA-256) means unchanged files cost nothing. Blobs >= 128KB are zstd-compressed when it helps.

Branching happens naturally: restoring to an old snapshot sets HEAD, and the next snapshot parents off it.

## Restore

Restore shows a categorized preview of what will change (Delete / Write / Modify / Chmod / Unchanged) and prompts for confirmation before touching the filesystem. It auto-snapshots the current state first, so you can always undo.

```bash
earwig diff abc123       # see what would change (safe, read-only)
earwig restore abc123    # preview + confirm
earwig restore -y abc123 # skip confirmation (scripted use)
```

## Checkpoints

Named references to snapshots. Useful as bookmarks for known-good states.

```bash
earwig check                    # random name (e.g. "bold-fox"), snapshots current state
earwig check release-v2         # named checkpoint, snapshots current state
earwig check release-v2 abc123  # checkpoint a specific snapshot
earwig checks                   # list all
earwig restore release-v2       # restore by name
earwig check -d release-v2      # delete
earwig check -u release-v2      # move to current state
```

## Trimming history

Auto-snapshots accumulate. `trim` deletes old ones to reclaim disk, keeping a
safe floor so it can never wipe out everything.

```bash
earwig trim 7d              # delete snapshots older than 7 days ago
earwig trim 2w              # ... older than 2 weeks
earwig trim 0s              # collapse to just the latest snapshot
earwig trim release-v2      # delete snapshots older than a checkpoint/hash
earwig trim -y 7d           # skip the confirmation prompt
```

The cutoff is a duration ago (`s`, `m`, `h`, `d`, `w`) or a ref's timestamp.
Trim keeps the **newest snapshot older than the cutoff** (the retained floor),
everything newer, and the current `HEAD` — and deletes the rest. So `trim 0s`
leaves exactly one snapshot, and a lone month-old snapshot survives `trim 7d`.

## Grep

Search file contents across all snapshots. Each unique blob is searched once (deduped).

```bash
earwig grep "TODO"               # basic search
earwig grep -i "error" "*.go"    # case-insensitive, file glob filter
earwig grep -l "func main"       # list matching files only
earwig grep -n 5 "pattern"       # limit to 5 most recent snapshots
earwig grep -max-size 1 "data"   # skip files > 1 MB
```

## TUI

Interactive split-pane browser. Top pane shows the snapshot list, bottom pane shows the diff for the selected snapshot.

| Key | Action |
|-----|--------|
| `j` / `k` | Navigate snapshots |
| `Enter` / `Tab` | Focus diff pane |
| `Esc` | Return to snapshot list / clear filter |
| `t` | Toggle diff mode (vs-filesystem / vs-parent) |
| `/` | Search by filename |
| `?` | Search file contents |
| `r` | Restore the selected snapshot (exits TUI, runs the usual preview + confirm) |
| `g` / `G` | Jump to top / bottom |
| `q` | Quit |

## Storage

All data lives in `.earwig/` within the watched directory:

- `earwig.db` — SQLite database (WAL mode)
- `HEAD` — current snapshot ID
- `flock` — file lock for mutual exclusion between watcher and restore
- `ignore` — custom ignore patterns (gitignore syntax, plus `!` keeps)
- `config.json` — optional JSON config (see below)

earwig respects `.gitignore` and always ignores `.earwig/` and `.git/`.

### Ignore files and keeps

Patterns come from `.earwig/ignore` and `.gitignore` (gitignore syntax). A path
matching any pattern is excluded from snapshots.

A line beginning with `!` is a **keep**: it forces a path to be tracked even when
another source would exclude it. Unlike gitignore's own `!` — which only
re-includes within the single file that declares it — an earwig keep wins
*across* sources, so it can override `.gitignore`:

```
# .gitignore  (e.g. maintained by another tool, or to keep generated output
#              out of git)
runs/

# .earwig/ignore
!runs/        # ...but DO snapshot runs/ in earwig
```

Mnemonically, `!` reads as "don't ignore". Keeps always beat excludes,
regardless of source or declaration order. Two caveats:

- The builtins (`.earwig/`, `.git/`) are absolute — a keep cannot re-include
  them (otherwise earwig could snapshot its own database, or place git internals
  under restore's management).
- A keep cannot resurrect a path beneath a directory excluded by a *bare-name*
  pattern (e.g. `secret`, no trailing slash), because the tree walk prunes that
  directory before descending. Keep the directory itself, or write the exclude
  with a trailing slash (`secret/`, which does not prune). This mirrors git.

### `config.json`

Optional. Currently controls per-path size warnings: earwig prints a stderr
warning when a snapshotted file exceeds the threshold (the file is still
included). Default is 100MB.

```json
{
  "sizeWarn": {
    "default": "100MB",
    "overrides": {
      "test-reports/": "500MB",
      "vendor/": "off",
      "aplcore": "off"
    }
  }
}
```

Sizes accept `B`, `KB`, `MB`, `GB` (or a bare number for bytes). `"off"` disables
the warning for that pattern. Override keys use gitignore-style patterns; the
last matching override wins. Warnings dedupe per-path within a watcher process
and re-fire only when the file has grown past the previously-warned size.

## Safety

earwig is designed with the assumption that the database could be tampered with,
even just by a well-meaning user trying to get a 'fix' in - this includes earwig's
author. Key protections:

- **Path traversal prevention** — all paths validated to resolve within the root directory (formally verified)
- **Blob integrity** — SHA-256 verified on every read
- **Decompression bomb protection** — 512MB size limit, verified after decompression
- **Symlink safety** — never follows symlinks during restore; warnings for unsafe targets
- **Mode bit masking** — strips setuid/setgid/sticky from database values
- **Path conflict detection** — rejects manifests where one path is a prefix of another
- **Interactive confirmation** — restore previews all changes and prompts before acting
- **Mutual exclusion** — `syscall.Flock` prevents watcher/restore races
- **Crash recovery** — `RESTORING` marker enables detection and recovery

Easy to verify functions are [verified using Gobra](GOBRA.md) (ETH Zurich, Z3 SMT solver).

## Testing

```bash
# Unit tests
go test ./...

# E2E tests (Docker)
docker build -t earwig-test -f test/e2e/Dockerfile . && docker run --rm earwig-test

# Generative/property-based tests (Docker)
docker run --rm earwig-test earwig-gen 10000

# Formal verification (Docker)
docker build -t earwig-gobra -f test/gobra/Dockerfile . && docker run --rm earwig-gobra test/gobra/verify.sh

# Fuzz testing
go test ./internal/snapshot/ -fuzz FuzzSafePath -fuzztime 60s
```

## License

MIT
