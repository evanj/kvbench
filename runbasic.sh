#!/bin/bash

set -euf -o pipefail

KVBENCH="target/release-nativecpu/kvbench"
SINGLE_THREADED_STORES=(STBTreeMap STHashMap)
MULTI_THREADED_STORES=(LockedBTreeMap LockedHashMap SkipList Redis)
KEYS_LIST=(10000 100000 1000000)
THREADS_LIST=(1 2 4)

for STORE in "${SINGLE_THREADED_STORES[@]}"; do
  for NUM_KEYS in "${KEYS_LIST[@]}"; do
    echo $KVBENCH --store-kind "$STORE" --num-keys "$NUM_KEYS"
    $KVBENCH --store-kind "$STORE" --num-keys "$NUM_KEYS"
  done
  echo
done

for STORE in "${MULTI_THREADED_STORES[@]}"; do
  for NUM_KEYS in "${KEYS_LIST[@]}"; do
    for NUM_THREADS in "${THREADS_LIST[@]}"; do
      echo $KVBENCH --store-kind "$STORE" --num-keys "$NUM_KEYS" --num-threads "$NUM_THREADS"
      $KVBENCH --store-kind "$STORE" --num-keys "$NUM_KEYS" --num-threads "$NUM_THREADS"
    done
    echo
  done
done
