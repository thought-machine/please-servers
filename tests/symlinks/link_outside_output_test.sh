#!/usr/bin/env bash

LINK="tests/symlinks/out_dir/out_file"
DEST="$(readlink $LINK)"
EXPECTED="../input.txt"

if [ "$DEST" != "$EXPECTED" ]; then
    echo "Unexpected link destination, was $DEST, expected $EXPECTED"
    exit 1
fi
