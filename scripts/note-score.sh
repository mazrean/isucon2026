#!/usr/bin/env bash
# usage: scripts/note-score.sh '| job | commit | score | 内容 |'
# inserts the row after the last row of the "スコア追記" table in NOTES.md (never appends at EOF).
set -euo pipefail
row=$1 f=${NOTES:-NOTES.md}
python3 - "$f" "$row" <<'PY'
import re,sys
p,row=sys.argv[1],sys.argv[2]
L=open(p).read().split('\n')
s=next(i for i,x in enumerate(L) if x.startswith('## スコア追記'))
i=s+3
while i<len(L) and re.match(r'^\| \d{3,6} \|',L[i]): i+=1
L.insert(i,row)
open(p,'w').write('\n'.join(L))
PY
