#!/usr/bin/env bash
set -euo pipefail

failed=0

echo "========================================="
echo " Scripted E2E tests"
echo "========================================="
if /test/test.sh; then
    echo ""
    echo "Scripted tests passed."
else
    echo ""
    echo "Scripted tests FAILED."
    failed=1
fi

echo ""
echo "========================================="
echo " TUI E2E tests"
echo "========================================="
if /test/tui_test.sh; then
    echo ""
    echo "TUI tests passed."
else
    echo ""
    echo "TUI tests FAILED."
    failed=1
fi

echo ""
echo "========================================="
echo " Generative E2E tests"
echo "========================================="
for i in 1 2 3; do
    echo ""
    echo "--- run $i (random seed) ---"
    if ! earwig-gen 200; then
        failed=1
    fi
done

echo ""
if [ "$failed" -ne 0 ]; then
    echo "SOME TESTS FAILED"
    exit 1
fi
echo "ALL TEST SUITES PASSED"
