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
echo " Generative E2E tests"
echo "========================================="
for seed in 42 1337 9999; do
    echo ""
    echo "--- seed=$seed ---"
    if ! earwig-gen 200 "$seed"; then
        failed=1
    fi
done

echo ""
if [ "$failed" -ne 0 ]; then
    echo "SOME TESTS FAILED"
    exit 1
fi
echo "ALL TEST SUITES PASSED"
