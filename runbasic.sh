#!/bin/bash

set -euf -o pipefail

KVBENCH="target/release-nativecpu/kvbench"

KEYS_LIST=(10 100 1000 10000 100000)

for NUM_KEYS in "${KEYS_LIST[@]}"; do
  echo $KVBENCH --num-keys $NUM_KEYS
  $KVBENCH --num-keys $NUM_KEYS
done
