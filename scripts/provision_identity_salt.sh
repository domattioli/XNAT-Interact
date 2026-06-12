#!/usr/bin/env bash
# provision_identity_salt.sh — generate and persist a 32-byte CSPRNG identity salt.
#
# Usage: provision_identity_salt.sh <output-file>
#
# Writes 64 hex chars (32 bytes) to <output-file> with mode 0600.
# Refuses (exit 1) if the file already exists (no-clobber).
# NEVER echoes the salt value; prints only the path and a sha256 fingerprint prefix.
#
# The generated file is suitable for loading via XNAT_IDENTITY_SALT env var
# (point load_identity_salt at the file path or export its contents).
set -euo pipefail

if [[ $# -ne 1 ]]; then
    echo "Usage: $0 <output-file>" >&2
    exit 1
fi

OUTFILE="$1"

if [[ -e "$OUTFILE" ]]; then
    echo "ERROR: '$OUTFILE' already exists. Refusing to overwrite (no-clobber)." >&2
    exit 1
fi

# Generate 32 CSPRNG bytes as 64 hex chars
HEX="$(openssl rand -hex 32 2>/dev/null || od -An -tx1 -N32 /dev/urandom | tr -d ' \n')"

# Write and secure
printf '%s' "$HEX" > "$OUTFILE"
chmod 0600 "$OUTFILE"

# Fingerprint: sha256 of the hex string, first 12 chars only — never the value itself
FINGERPRINT="$(printf '%s' "$HEX" | sha256sum 2>/dev/null | cut -c1-12 \
               || printf '%s' "$HEX" | shasum -a 256 | cut -c1-12)"

echo "Salt written to: $OUTFILE"
echo "SHA256 fingerprint prefix: $FINGERPRINT"
