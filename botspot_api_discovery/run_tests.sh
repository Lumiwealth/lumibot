#!/bin/bash
# BotSpot API Discovery Test Runner
# Run pytest tests in virtual environment

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

echo "============================================"
echo "BotSpot API Discovery Test Suite"
echo "============================================"
echo ""

# Check if venv exists
if [ ! -d "venv" ]; then
    echo "❌ Virtual environment not found!"
    echo "Creating virtual environment..."
    python3 -m venv venv
    source venv/bin/activate
    pip install -q -r requirements.txt
    echo "✓ Virtual environment created"
else
    source venv/bin/activate
fi

# Check if ACCESS_TOKEN is set
if [ -z "$ACCESS_TOKEN" ]; then
    echo "⚠️  WARNING: ACCESS_TOKEN environment variable not set"
    echo "Tests requiring authentication will be skipped"
    echo ""
    echo "To run authenticated tests, export ACCESS_TOKEN:"
    echo "  export ACCESS_TOKEN='your_token_here'"
    echo ""
fi

# Run pytest
echo "Running tests..."
echo ""
pytest tests/ "$@"

TEST_EXIT_CODE=$?

echo ""
echo "============================================"
if [ $TEST_EXIT_CODE -eq 0 ]; then
    echo "✓ All tests passed!"
else
    echo "✗ Some tests failed (exit code: $TEST_EXIT_CODE)"
fi
echo "============================================"

exit $TEST_EXIT_CODE
