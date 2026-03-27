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
| `g` / `G` | Jump to top / bottom |
| `q` | Quit |

## Storage

All data lives in `.earwig/` within the watched directory:

- `earwig.db` — SQLite database (WAL mode)
- `HEAD` — current snapshot ID
- `flock` — file lock for mutual exclusion between watcher and restore
- `ignore` — custom ignore patterns (gitignore syntax)

earwig respects `.gitignore` and always ignores `.earwig/` and `.git/`.

## Safety

earwig is designed with the assumption that the database could be tampered with. Key protections:

- **Path traversal prevention** — all paths validated to resolve within the root directory (formally verified)
- **Blob integrity** — SHA-256 verified on every read
- **Decompression bomb protection** — 512MB size limit, verified after decompression
- **Symlink safety** — never follows symlinks during restore; warnings for unsafe targets
- **Mode bit masking** — strips setuid/setgid/sticky from database values
- **Path conflict detection** — rejects manifests where one path is a prefix of another
- **Interactive confirmation** — restore previews all changes and prompts before acting
- **Mutual exclusion** — `syscall.Flock` prevents watcher/restore races
- **Crash recovery** — `RESTORING` marker enables detection and recovery

Twelve security-critical functions are [formally verified using Gobra](GOBRA.md) (ETH Zurich, Z3 SMT solver).

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

Private.
