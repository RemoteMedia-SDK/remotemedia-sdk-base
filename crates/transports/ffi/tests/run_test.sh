#!/bin/bash
# Fix for Anaconda libstdc++ being too old
# Force use of system libstdc++ which has GLIBCXX_3.4.30+

export LD_PRELOAD=/usr/lib/x86_64-linux-gnu/libstdc++.so.6

# Run the test
python "$@"

