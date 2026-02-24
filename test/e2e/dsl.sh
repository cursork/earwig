#!/usr/bin/env bash
# ---------------------------------------------------------
# earwig E2E test DSL
#
# Provides readable helper functions for writing E2E tests.
# Source this file, then call the functions.
# ---------------------------------------------------------
set -euo pipefail

PASS=0
FAIL=0
SNAPSHOTS=()   # ordered list of snapshot hashes

red()   { printf '\033[1;31m%s\033[0m\n' "$*"; }
green() { printf '\033[1;32m%s\033[0m\n' "$*"; }
blue()  { printf '\033[1;34m%s\033[0m\n' "$*"; }

# ── assertions ──────────────────────────────────────────

pass() {
    PASS=$((PASS + 1))
    green "  PASS: $1"
}

fail() {
    FAIL=$((FAIL + 1))
    red "  FAIL: $1"
    red "        $2"
}

# ── setup ───────────────────────────────────────────────

init_project() {
    local dir="$1"
    rm -rf "$dir"
    mkdir -p "$dir"
    cd "$dir"
    earwig init > /dev/null
    SNAPSHOTS=()
    blue "---- project: $dir ----"
}

# ── file operations ─────────────────────────────────────

write_file() {
    local path="$1" content="$2"
    mkdir -p "$(dirname "$path")"
    printf '%s' "$content" > "$path"
}

append_file() {
    local path="$1" content="$2"
    printf '%s' "$content" >> "$path"
}

delete_file() {
    rm -f "$1"
}

delete_dir() {
    rm -rf "$1"
}

make_dir() {
    mkdir -p "$1"
}

# ── earwig operations ──────────────────────────────────

snapshot() {
    local output
    output=$(earwig snapshot 2>&1)
    if echo "$output" | grep -q "^Snapshot "; then
        local hash
        hash=$(echo "$output" | grep "^Snapshot " | awk '{print $2}')
        SNAPSHOTS+=("$hash")
        blue "  snapshot #${#SNAPSHOTS[@]}: $hash"
    else
        blue "  snapshot: (no changes)"
    fi
}

restore() {
    local n="$1"  # 1-indexed snapshot number
    local hash="${SNAPSHOTS[$((n - 1))]}"
    earwig restore "$hash" > /dev/null
    blue "  restore -> snapshot #$n ($hash)"
}

show_log() {
    earwig log
}

# ── expect: files ──────────────────────────────────────

expect_file() {
    local path="$1" expected="$2"
    if [ ! -f "$path" ]; then
        fail "$path exists" "file not found"
        return
    fi
    local actual
    actual=$(cat "$path")
    if [ "$actual" = "$expected" ]; then
        pass "$path == '$expected'"
    else
        fail "$path == '$expected'" "got '$actual'"
    fi
}

expect_no_file() {
    local path="$1"
    if [ -f "$path" ]; then
        fail "$path should not exist" "file exists with content: $(cat "$path")"
    else
        pass "$path does not exist"
    fi
}

expect_no_dir() {
    local path="$1"
    if [ -d "$path" ]; then
        fail "$path/ should not exist" "directory exists"
    else
        pass "$path/ does not exist"
    fi
}

expect_dir() {
    local path="$1"
    if [ -d "$path" ]; then
        pass "$path/ exists"
    else
        fail "$path/ exists" "directory not found"
    fi
}

# ── expect: snapshots ──────────────────────────────────

expect_snapshot_count() {
    local expected="$1"
    local actual
    actual=$(earwig log 2>/dev/null | grep -c '^\*' || true)
    if [ "$actual" -eq "$expected" ]; then
        pass "snapshot count == $expected"
    else
        fail "snapshot count == $expected" "got $actual"
    fi
}

expect_show_change() {
    local n="$1" type="$2" path="$3"
    local hash="${SNAPSHOTS[$((n - 1))]}"
    local output
    output=$(earwig show "$hash" 2>&1)
    if echo "$output" | grep -q "  $type $path"; then
        pass "snapshot #$n shows $type $path"
    else
        fail "snapshot #$n shows $type $path" "output: $output"
    fi
}

expect_log_has_branch() {
    local output
    output=$(earwig log 2>&1)
    if echo "$output" | grep -q "(branch)"; then
        pass "log contains branch marker"
    else
        fail "log contains branch marker" "output: $output"
    fi
}

expect_no_changes() {
    local output
    output=$(earwig snapshot 2>&1)
    if echo "$output" | grep -q "No changes"; then
        pass "no changes to snapshot"
    else
        fail "no changes to snapshot" "output: $output"
    fi
}

# ── summary ─────────────────────────────────────────────

summary() {
    echo ""
    echo "================================"
    if [ "$FAIL" -eq 0 ]; then
        green "ALL PASSED: $PASS assertions"
    else
        red "FAILED: $FAIL failures, $PASS passed"
    fi
    echo "================================"
    exit "$FAIL"
}
