#!/bin/bash
cd /Users/marvin/repos/lumibot
source venv/bin/activate
echo "Starting profiler with enhanced debug output..."
echo "If it hangs, you'll see exactly where..."
echo ""
python -u -m profiler.runner --phase 0
