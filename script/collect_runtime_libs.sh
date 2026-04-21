#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TARGET_BIN="${1:-$ROOT_DIR/bin/KVServerNode}"
OUTPUT_DIR="${2:-$ROOT_DIR/deploy/runtime/lib}"

if [[ ! -f "$TARGET_BIN" ]]; then
    echo "binary not found: $TARGET_BIN" >&2
    exit 1
fi

mkdir -p "$OUTPUT_DIR"

echo "Collecting runtime libraries for: $TARGET_BIN"
echo "Output directory: $OUTPUT_DIR"

mapfile -t libs < <(
    ldd "$TARGET_BIN" | awk '
        /=> \// { print $3 }
        /^\// { print $1 }
    ' | sort -u
)

if [[ ${#libs[@]} -eq 0 ]]; then
    echo "No dynamic libraries found via ldd." >&2
    exit 1
fi

copied_count=0
for lib in "${libs[@]}"; do
    base_name="$(basename "$lib")"

    case "$base_name" in
        libc.so.*|ld-linux-*.so.*)
            echo "Skipping core system library: $lib"
            continue
            ;;
    esac

    install -m 0755 "$lib" "$OUTPUT_DIR/$base_name"
    echo "Copied: $lib"
    copied_count=$((copied_count + 1))
done

echo "Copied $copied_count libraries."
echo
echo "Collected files:"
find "$OUTPUT_DIR" -maxdepth 1 -type f | sort
