#!/bin/zsh
# Operator-only: unzip real cases and build the offline review report.
# Everything lands beside the zip, so keep the zip outside this repo.
# Usage: zsh scripts/run_real_report.sh ~/xnat_phi/xnat-Testing.zip ~/xnat_phi/intake.json
set -e
zip_ffn=${1:?zip path}
intake=${2:?intake.json path}
repo=${0:A:h:h}
work=${zip_ffn:A:h}

if [[ "$work" == "$repo"* ]]; then
  echo "The zip is inside the repo. Move it out first, for example to ~/xnat_phi."
  exit 1
fi

dest=$work/xnat_real_cases
out=$work/xnat_real_report/report.html
mkdir -p "$dest" /private/tmp/claude/t
unzip -oq "$zip_ffn" -d "$dest"
case_086=$(find "$dest" -maxdepth 3 -type d -name 086 | head -1)
if [[ -z "$case_086" ]]; then
  echo "No 086 folder inside the zip. Check the zip holds 015, 086, 089."
  exit 1
fi
cases=${case_086:h}
echo "Cases folder: $cases"

cd "$repo"
TMPDIR=/private/tmp/claude/t .venv/bin/python scripts/real_data_report.py \
  --cases "$cases" --intake "$intake" --out "$out"
echo "Done. Opening $out"
open "$out"
