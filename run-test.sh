#!/bin/sh
set -ev
file="testData/spill_out/result.txt"
if [ ! -f "$file" ]; then
   exit 1
fi

