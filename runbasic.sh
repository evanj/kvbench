#!/bin/bash

set -euf -o pipefail

KVBENCH="target/release-nativecpu/kvbench"
STORE_LIST=(STBTreeMap STHashMap Redis)
#STORE_LIST=(STBTreeMap LockedBTreeMap STHashMap LockedHashMap Redis)
KEYS_LIST=(10000 100000 1000000)

for STORE in "${STORE_LIST[@]}"; do
  for NUM_KEYS in "${KEYS_LIST[@]}"; do
    echo $KVBENCH --store-kind "$STORE" --num-keys "$NUM_KEYS"
    $KVBENCH --store-kind "$STORE" --num-keys "$NUM_KEYS"
    echo
  done
done
