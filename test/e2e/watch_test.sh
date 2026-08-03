#!/usr/bin/env bash
# ---------------------------------------------------------
# earwig watch interactive E2E tests (tmux-based)
#
# Launches `earwig watch` in a tmux session, sends single-
# key commands (c, s, l, t, /, ?, q) and asserts on
# captured pane output.
# ---------------------------------------------------------
set -euo pipefail
source "$(dirname "$0")/dsl.sh"

SESSION="earwig-watch-test"
WIDTH=120
HEIGHT=40

watch_start() {
    local dir="$1"
    tmux kill-session -t "$SESSION" 2>/dev/null || true
    # Trailing `echo WATCH_EXITED; sleep 30` lets us detect when watch returns.
    tmux new-session -d -s "$SESSION" -x "$WIDTH" -y "$HEIGHT" \
        "cd '$dir' && earwig watch; echo WATCH_EXITED; sleep 30"
    sleep 3  # wait for initial snapshot + banner
}

watch_stop() {
    tmux kill-session -t "$SESSION" 2>/dev/null || true
}

watch_send() {
    tmux send-keys -t "$SESSION" "$@"
    sleep 1
}

watch_capture() {
    tmux capture-pane -t "$SESSION" -p -S -500
}

expect_watch_contains() {
    local pattern="$1" label="${2:-$1}"
    local screen
    screen=$(watch_capture)
    if echo "$screen" | grep -qF "$pattern"; then
        pass "watch contains '$label'"
    else
        fail "watch contains '$label'" "not found in:
$screen"
    fi
}

expect_watch_not_contains() {
    local pattern="$1" label="${2:-$1}"
    local screen
    screen=$(watch_capture)
    if echo "$screen" | grep -qF "$pattern"; then
        fail "watch should not contain '$label'" "found in:
$screen"
    else
        pass "watch does not contain '$label'"
    fi
}

count_watch_lines() {
    local pattern="$1"
    local screen
    screen=$(watch_capture)
    echo "$screen" | grep -cF "$pattern" || echo 0
}

# =========================================================
# TEST WATCH-1: banner + initial snapshot + key hints
# =========================================================
blue "=== WATCH TEST 1: banner and initial snapshot ==="

TESTDIR=/tmp/earwig-watch-1
init_project "$TESTDIR"
write_file foo.txt "initial content"

watch_start "$TESTDIR"

expect_watch_contains "Watching" "watching banner"
expect_watch_contains "[c]heckpoint" "c key hint"
expect_watch_contains "[s]napshot" "s key hint"
expect_watch_contains "[l]og" "l key hint"
expect_watch_contains "[t]ui" "t key hint"
expect_watch_contains "[/]grep" "/ key hint"
expect_watch_contains "[?]help" "? key hint"
expect_watch_contains "[q]uit" "q key hint"
expect_watch_contains "Snapshot " "initial snapshot taken on startup"

# =========================================================
# TEST WATCH-2: '?' prints key help
# =========================================================
blue "=== WATCH TEST 2: ? help ==="

watch_send "?"
expect_watch_contains "Keys: c=checkpoint" "help line printed"
expect_watch_contains "t=tui" "tui mentioned in help"
expect_watch_contains "q=quit" "q mentioned in help"

# =========================================================
# TEST WATCH-3: 's' takes a manual snapshot
# =========================================================
blue "=== WATCH TEST 3: s manual snapshot ==="

before=$(count_watch_lines "Snapshot ")
echo "second file" > "$TESTDIR/bar.txt"
sleep 0.5
watch_send "s"
sleep 2

after=$(count_watch_lines "Snapshot ")
if [ "$after" -gt "$before" ]; then
    pass "s produced a new Snapshot line ($before -> $after)"
else
    fail "s produced a new Snapshot line" "count unchanged: $before -> $after"
fi

# =========================================================
# TEST WATCH-4: 's' on no-changes reports (no changes)
# =========================================================
blue "=== WATCH TEST 4: s with no changes ==="

watch_send "s"
sleep 2
expect_watch_contains "(no changes)" "s reports no-changes"

# =========================================================
# TEST WATCH-5: 'c' takes snapshot and creates checkpoint
# =========================================================
blue "=== WATCH TEST 5: c checkpoint ==="

echo "third file" > "$TESTDIR/baz.txt"
sleep 0.5
watch_send "c"
sleep 2

expect_watch_contains "Checkpoint " "checkpoint line printed"

# =========================================================
# TEST WATCH-6: 'l' prints log inline
# =========================================================
blue "=== WATCH TEST 6: l prints log ==="

watch_send "l"
sleep 2
# `earwig log` uses graph format with `* <hash>` and message `auto`
expect_watch_contains "auto" "log shows auto message"

# =========================================================
# TEST WATCH-7: 't' launches TUI and 'q' returns to watch
# =========================================================
blue "=== WATCH TEST 7: t launches TUI ==="

watch_send "t"
sleep 3
# TUI status bar contains its own hint strings
expect_watch_contains "j/k:navigate" "TUI status bar visible"
expect_watch_contains "vs filesystem" "TUI diff mode separator visible"

# Quit TUI — should return to watch (no WATCH_EXITED yet)
watch_send "q"
sleep 2
expect_watch_not_contains "WATCH_EXITED" "watch still running after TUI quit"

# =========================================================
# TEST WATCH-8: '/' grep prompt finds content
# =========================================================
blue "=== WATCH TEST 8: / grep ==="

watch_send "/"
sleep 0.5
tmux send-keys -t "$SESSION" "initial"
sleep 0.5
tmux send-keys -t "$SESSION" Enter
sleep 3

expect_watch_contains "foo.txt" "grep matched foo.txt"
expect_watch_contains "initial" "grep result includes pattern"

# =========================================================
# TEST WATCH-9: 'q' quits the watcher
# =========================================================
blue "=== WATCH TEST 9: q quits ==="

watch_send "q"
sleep 2
expect_watch_contains "WATCH_EXITED" "watch process exited after q"

# =========================================================
# TEST WATCH-10: stdin non-TTY (piped) skips interactive layer
# =========================================================
blue "=== WATCH TEST 10: non-TTY stdin ==="

TESTDIR2=/tmp/earwig-watch-10
init_project "$TESTDIR2"
write_file alpha.txt "hello"

# Pipe yes to /dev/null into watch — stdin is a pipe, not a TTY.
# Watch should print non-interactive banner ("Ctrl+C to stop") and
# NOT the bracketed key hints.
tmux kill-session -t "$SESSION" 2>/dev/null || true
tmux new-session -d -s "$SESSION" -x "$WIDTH" -y "$HEIGHT" \
    "cd '$TESTDIR2' && earwig watch < /dev/null; echo WATCH_EXITED; sleep 30"
sleep 3

expect_watch_contains "Ctrl+C to stop" "non-TTY shows Ctrl+C banner"
expect_watch_not_contains "[c]heckpoint" "non-TTY does NOT show interactive hints"

# Send 'c' — should NOT trigger a checkpoint (no raw mode)
echo "more" > "$TESTDIR2/beta.txt"
sleep 0.5
tmux send-keys -t "$SESSION" "c"
sleep 2
expect_watch_not_contains "Checkpoint " "c keypress did NOT create a checkpoint in non-TTY mode"

# Cleanup: signal the watcher with SIGTERM via tmux kill
watch_stop

# =========================================================
# TEST WATCH-11: one watcher per directory (startup gate)
# =========================================================
blue "=== WATCH TEST 11: single-watcher gate ==="

TESTDIR3=/tmp/earwig-watch-11
init_project "$TESTDIR3"          # cd's into TESTDIR3
mkdir -p sub/deeper
write_file g.txt "guard"

# Start the first watcher headless (writes .earwig/PID).
earwig watch -detach
sleep 2
first_pid=$(cat "$TESTDIR3/.earwig/PID")

if earwig processes | grep -q "$TESTDIR3"; then
    pass "processes lists the running watcher"
else
    fail "processes lists the running watcher" "$(earwig processes)"
fi

# A second detached watcher must be refused before spawning: non-zero exit,
# a message that says 'already running', and it names the live PID.
if out=$(earwig watch -detach 2>&1); then
    fail "second -detach watcher refused" "expected non-zero exit, got success: $out"
else
    pass "second -detach watcher refused (non-zero exit)"
fi
if echo "$out" | grep -q "already running"; then
    pass "refusal message says 'already running'"
else
    fail "refusal message says 'already running'" "$out"
fi
if echo "$out" | grep -q "$first_pid"; then
    pass "refusal names the running watcher PID ($first_pid)"
else
    fail "refusal names the running watcher PID" "$out"
fi

# A foreground watcher must also refuse — and exit immediately, not block.
# `timeout` bounds the hang case; asserting on the message distinguishes a
# fast refusal (contains 'already running') from a hung watcher that got
# killed by timeout (would show the 'Watching' banner instead).
out=$(timeout 8 earwig watch 2>&1) || true
if echo "$out" | grep -q "already running"; then
    pass "foreground watcher refused immediately (no hang)"
else
    fail "foreground watcher refused immediately" "output: $out"
fi

# The gate resolves each watcher's root from its cwd, so it fires from a
# subdirectory too (watcher launched at root, second attempt deeper down).
cd "$TESTDIR3/sub/deeper"
out=$(timeout 8 earwig watch 2>&1) || true
if echo "$out" | grep -q "already running"; then
    pass "watcher refused from a subdirectory (root resolved from cwd)"
else
    fail "watcher refused from a subdirectory" "output: $out"
fi
cd "$TESTDIR3"

# Stop the first watcher by its EXACT PID (never pkill / pattern match).
kill "$first_pid" 2>/dev/null || true
sleep 3

# With no watcher running, a fresh watcher starts fine again.
if earwig watch -detach; then
    pass "watcher starts again after the first stopped"
    sleep 1
    kill "$(cat "$TESTDIR3/.earwig/PID")" 2>/dev/null || true
else
    fail "watcher starts again after the first stopped" "unexpected refusal"
fi

summary
