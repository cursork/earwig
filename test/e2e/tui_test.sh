#!/usr/bin/env bash
# ---------------------------------------------------------
# earwig TUI E2E tests (tmux-based)
#
# Launches earwig tui in a tmux session, sends keys, and
# asserts on captured screen output.
# ---------------------------------------------------------
set -euo pipefail
source "$(dirname "$0")/dsl.sh"

# ── tmux helpers ─────────────────────────────────────────

SESSION="earwig-tui-test"
TUI_WIDTH=80
TUI_HEIGHT=24

tui_start() {
    local dir="$1"
    tmux kill-session -t "$SESSION" 2>/dev/null || true
    tmux new-session -d -s "$SESSION" -x "$TUI_WIDTH" -y "$TUI_HEIGHT" \
        "cd '$dir' && earwig tui"
    sleep 2  # wait for initial render + diff computation
}

tui_stop() {
    tmux kill-session -t "$SESSION" 2>/dev/null || true
}

tui_send() {
    # Send keys, then wait for render
    tmux send-keys -t "$SESSION" "$@"
    sleep 1
}

tui_capture() {
    tmux capture-pane -t "$SESSION" -p
}

expect_screen_contains() {
    local pattern="$1" label="${2:-$1}"
    local screen
    screen=$(tui_capture)
    if echo "$screen" | grep -qF "$pattern"; then
        pass "screen contains '$label'"
    else
        fail "screen contains '$label'" "not found in:
$screen"
    fi
}

expect_screen_not_contains() {
    local pattern="$1" label="${2:-$1}"
    local screen
    screen=$(tui_capture)
    if echo "$screen" | grep -qF "$pattern"; then
        fail "screen should not contain '$label'" "found in:
$screen"
    else
        pass "screen does not contain '$label'"
    fi
}

expect_screen_matches() {
    local pattern="$1" label="${2:-$1}"
    local screen
    screen=$(tui_capture)
    if echo "$screen" | grep -qE "$pattern"; then
        pass "screen matches '$label'"
    else
        fail "screen matches '$label'" "not found in:
$screen"
    fi
}

# Count lines matching a pattern
count_screen_lines() {
    local pattern="$1"
    local screen
    screen=$(tui_capture)
    echo "$screen" | grep -cF "$pattern" || echo 0
}

# =========================================================
# Setup: create test project with 3 snapshots
# =========================================================
blue "=== TUI TEST: setup ==="

TESTDIR="/tmp/earwig-tui-test"
init_project "$TESTDIR"

write_file "foo.txt" "hello world"
snapshot  # #1: A foo.txt

write_file "foo.txt" "hello world
line 2"
write_file "bar.txt" "bar content"
snapshot  # #2: A bar.txt, M foo.txt

write_file "foo.txt" "modified"
snapshot  # #3: M foo.txt

HASH1="${SNAPSHOTS[0]}"
HASH2="${SNAPSHOTS[1]}"
HASH3="${SNAPSHOTS[2]}"

# =========================================================
# TEST TUI-1: Initial render
# =========================================================
blue "=== TUI TEST 1: Initial render ==="

tui_start "$TESTDIR"

# Snapshot list visible with all 3 entries
expect_screen_contains "$HASH3" "newest snapshot hash"
expect_screen_contains "$HASH2" "middle snapshot hash"
expect_screen_contains "$HASH1" "oldest snapshot hash"

# Cursor on newest (HEAD), HERE marker visible
expect_screen_contains "<- HERE" "HERE marker"
expect_screen_contains "> $HASH3" "cursor on HEAD"

# Separator shows default diff mode
expect_screen_contains "vs filesystem" "default diff mode"

# Status bar visible
expect_screen_contains "j/k:navigate" "status bar"

# HEAD matches filesystem, so no diff
expect_screen_contains "No differences" "no diff for HEAD"

# =========================================================
# TEST TUI-2: Navigation (j/k)
# =========================================================
blue "=== TUI TEST 2: Navigation ==="

tui_send j

# Cursor moved to second snapshot
expect_screen_contains "> $HASH2" "cursor on snapshot #2"

# Diff shows what restore would change
expect_screen_contains "Restore to $HASH2" "diff header for #2"
expect_screen_contains "M foo.txt" "foo.txt modified"

# Navigate back up
tui_send k

expect_screen_contains "> $HASH3" "cursor back on HEAD"
expect_screen_contains "No differences" "no diff for HEAD again"

# =========================================================
# TEST TUI-3: Toggle diff mode (t)
# =========================================================
blue "=== TUI TEST 3: Toggle diff mode ==="

# Move to snapshot #2 and toggle to vs parent
tui_send j
sleep 1
tui_send t

expect_screen_contains "vs parent" "switched to vs parent"
expect_screen_contains "Changes in $HASH2 vs parent" "parent diff header"
expect_screen_contains "A bar.txt" "bar.txt added vs parent"

# Toggle back to vs filesystem
tui_send t

expect_screen_contains "vs filesystem" "back to vs filesystem"
expect_screen_contains "Restore to $HASH2" "filesystem diff header"

# =========================================================
# TEST TUI-4: Tab focus (top <-> bottom)
# =========================================================
blue "=== TUI TEST 4: Tab focus ==="

# Tab to bottom pane
tui_send Tab

expect_screen_contains "j/k:scroll" "bottom pane status"
expect_screen_contains "tab:focus list" "tab hint shows list"
expect_screen_contains "(j/k to scroll)" "scroll hint in separator"

# Tab back to top pane
tui_send Tab

expect_screen_contains "j/k:navigate" "top pane status"
expect_screen_contains "enter/tab:focus diff" "enter/tab hint"

# =========================================================
# TEST TUI-5: Search / filter
# =========================================================
blue "=== TUI TEST 5: Search / filter ==="

# Search for bar.txt
tui_send /
sleep 0.5
tmux send-keys -t "$SESSION" "bar.txt"
sleep 0.5

# Search bar visible
expect_screen_contains "bar.txt" "search input visible"

# Execute search
tui_send Enter
sleep 1

# Only snapshot #2 should be visible (it added bar.txt)
expect_screen_contains "$HASH2" "matching snapshot visible"
expect_screen_not_contains "$HASH3" "non-matching snapshot hidden"
expect_screen_not_contains "$HASH1" "non-matching snapshot hidden"

# Filter label visible
expect_screen_contains "[filter: bar.txt]" "filter label in separator"

# =========================================================
# TEST TUI-6: Clear filter (Esc)
# =========================================================
blue "=== TUI TEST 6: Clear filter ==="

tui_send Escape
sleep 1

# All snapshots visible again
expect_screen_contains "$HASH3" "all snapshots visible after clear"
expect_screen_contains "$HASH2" "snapshot #2 visible"
expect_screen_contains "$HASH1" "snapshot #1 visible"

# Filter label gone
expect_screen_not_contains "[filter:" "filter label removed"

# =========================================================
# TEST TUI-7: g/G (top/bottom)
# =========================================================
blue "=== TUI TEST 7: g and G navigation ==="

tui_send G
sleep 1
expect_screen_contains "> $HASH1" "G goes to bottom (oldest)"

tui_send g
sleep 1
expect_screen_contains "> $HASH3" "g goes to top (newest)"

# =========================================================
# TEST TUI-8: Root snapshot diff (vs parent)
# =========================================================
blue "=== TUI TEST 8: Root snapshot vs parent ==="

# Navigate to root snapshot and toggle to vs parent
tui_send G
sleep 1
tui_send t

expect_screen_contains "vs parent" "vs parent mode"
expect_screen_contains "(root)" "root snapshot label"
expect_screen_contains "A foo.txt" "root shows added file"

# =========================================================
# Cleanup
# =========================================================
tui_stop

summary
