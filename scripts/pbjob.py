#!/usr/bin/env python3
"""Decode isuxportal BenchmarkJob protobuf (stdin) -> prints 'STATE=<n>' + stdout log + final line."""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from pbraw import dec
def get(t, f):
    for k, v in t:
        if k == f: return v
t = dec(sys.stdin.buffer.read())
job = get(t, 1) or []
state = get(job, 4)
res = get(job, 18) or []
mode = sys.argv[1] if len(sys.argv) > 1 else 'full'
if mode == 'state':
    print(state if state is not None else 0); sys.exit()
ex = get(res, 5) or []
out = get(ex, 2) or ''
err = get(ex, 3) or ''
if mode == 'full':
    print(out if isinstance(out, str) else '')
    if err: print('--- stderr ---'); print(err if isinstance(err, str) else '')
    print('reason=', get(ex, 1))
passed = get(res, 2) == 1
score = get(res, 3) or 0
print(f"job={get(job,1)} state={state} pass={'true' if passed else 'false'} スコア={score} 種別エラー数=map[]")
