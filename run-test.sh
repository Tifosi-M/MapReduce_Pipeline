#!/bin/sh
set -ev
file="testData/spill_out/result.txt"
if [  -f "$file" ]; then
  echo "error"
  touch testData/spill_out/result.txt
fi

