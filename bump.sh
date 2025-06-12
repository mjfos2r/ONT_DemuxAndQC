#!/usr/bin/env bash

if [ $# != 1 ]; then
    echo " ERROR: Please only specify a bump flag: -p <patch> -m<minor> -M<major>"
    exit 1
fi

bump_size="$1"

uv run bump_version.py "$bump_size" 
uv lock
exit 0
