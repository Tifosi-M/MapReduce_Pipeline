#!/bin/sh
set -ev
file="/root/spill_out/result.txt"
if [ ! -f "$file" ]; then
  exit(1)
fi