#!/usr/bin/env python3
"""Generate test inputs for the Go-vs-Rust deflate comparison.

usage: gen_corpus.py OUTDIR [N] [SEED]
"""
import base64
import json
import os
import random
import sys

out = sys.argv[1]
N = int(sys.argv[2]) if len(sys.argv) > 2 else 400
seed = int(sys.argv[3]) if len(sys.argv) > 3 else 1
rng = random.Random(seed)
os.makedirs(out, exist_ok=True)

JA = "あいうえおかきくけこさしすせそたちつてとなにぬねのはひふへほまみむめもやゆよらりるれろわをん" \
     "今日は良い天気ですね写真投稿猫犬ラーメン東京大阪美味しい楽しい最高😀🎉"


def ja_text(n):
    return "".join(rng.choice(JA) for _ in range(n))


def sig():
    return base64.urlsafe_b64encode(rng.randbytes(32)).decode().rstrip("=")


def post(i):
    return {
        "id": i,
        "user_id": rng.randint(1, 5000),
        "account_name": "user%04d" % rng.randint(0, 9999),
        "body": ja_text(rng.randint(0, 120)),
        "image_url": "https://cdn.example.com/image/%d.jpg?exp=%d&sig=%s" % (i, rng.randint(1_700_000_000, 1_800_000_000), sig()),
        "created_at": "2026-10-%02dT%02d:%02d:%02d+09:00" % (rng.randint(1, 31), rng.randint(0, 23), rng.randint(0, 59), rng.randint(0, 59)),
        "tags": [ja_text(rng.randint(1, 5)) for _ in range(rng.randint(0, 4))],
        "likes": rng.randint(0, 100000),
    }


def json_posts(target):
    items, size, i = [], 2, rng.randint(1, 100000)
    while size < target:
        p = post(i)
        i += 1
        items.append(p)
        size += len(json.dumps(p, ensure_ascii=False).encode()) + 1
    # json.Encoder appends a newline
    return (json.dumps({"posts": items}, ensure_ascii=False, separators=(",", ":")) + "\n").encode()


def repeated(target):
    unit = json.dumps(post(1), ensure_ascii=False).encode()
    return (unit * (target // max(1, len(unit)) + 1))[:target]


def highly_rep(target):
    c = rng.choice([b"a", b"ab", b"\x00", b"xyz123", b"{}"])
    return (c * (target // len(c) + 1))[:target]


def rand_bytes(target):
    return rng.randbytes(target)


def mixed(target):
    parts, total = [], 0
    while total < target:
        k = rng.choice([json_posts, repeated, highly_rep, rand_bytes])
        n = min(target - total, rng.randint(1, 70000))
        b = k(n)[:n]
        parts.append(b)
        total += len(b)
    return b"".join(parts)[:target]


def ascii_text(target):
    words = ["the", "quick", "brown", "fox", "jumps", "over", "lazy", "dog", "isucon", "go", "rust", "\n"]
    s = []
    total = 0
    while total < target:
        w = rng.choice(words)
        s.append(w)
        total += len(w) + 1
    return " ".join(s).encode()[:target]


gens = [json_posts, json_posts, json_posts, repeated, highly_rep, rand_bytes, mixed, ascii_text]

# fixed edge sizes
edge_sizes = [0, 1, 2, 12, 13, 31, 32, 33, 64, 127, 128, 129, 255, 256, 1023, 1024, 1025,
              4096, 65534, 65535, 65536, 65537, 131069, 131070, 131071, 196605, 196606,
              327674, 327675, 327676]
idx = 0


def emit(data, tag):
    global idx
    with open(os.path.join(out, "%04d_%s_%d.bin" % (idx, tag, len(data))), "wb") as f:
        f.write(data)
    idx += 1


for sz in edge_sizes:
    for g in gens:
        if sz == 0 and g is not json_posts:
            continue
        d = g(sz)[:sz] if sz else b""
        emit(d, g.__name__)

for _ in range(N):
    g = rng.choice(gens)
    r = rng.random()
    if r < 0.4:
        sz = rng.randint(0, 4096)
    elif r < 0.8:
        sz = rng.randint(1000, 210_000)
    else:
        sz = rng.randint(0, 300_000)
    d = g(sz)
    if g is not json_posts:
        d = d[:sz]
    emit(d, g.__name__)

# a few large inputs to exercise the history move-down (> 5*65535 bytes)
for sz in [400_000, 700_000, 1_100_000]:
    for g in [json_posts, mixed, repeated]:
        emit(g(sz), g.__name__)
print(idx)
