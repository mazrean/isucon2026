#!/usr/bin/env python3
"""GET the same paths from Go (A) and Rust (B); compare status, body, and the
headers that echo sets. Export zips are compared byte-for-byte and, on
mismatch, entry by entry (name + uncompressed bytes).

usage: compare.py A_BASE B_BASE < cases.tsv   (each line: token<TAB>path)
"""
import io
import sys
import urllib.request
import zipfile

HDRS = ["Content-Type", "Cache-Control", "Content-Disposition"]


def get(base, path, token):
    req = urllib.request.Request(base + path)
    if token:
        req.add_header("Authorization", "Bearer " + token)
    try:
        with urllib.request.urlopen(req, timeout=60) as r:
            return r.status, {h: r.headers.get(h) for h in HDRS}, r.read()
    except urllib.error.HTTPError as e:
        return e.code, {h: e.headers.get(h) for h in HDRS}, e.read()


def zip_entries(b):
    z = zipfile.ZipFile(io.BytesIO(b))
    return [(i.filename, i.compress_type, z.read(i.filename)) for i in z.infolist()]


def main():
    a, b = sys.argv[1], sys.argv[2]
    n = bad = zip_semantic = 0
    for line in sys.stdin:
        line = line.rstrip("\n")
        if not line:
            continue
        token, path = line.split("\t", 1)
        ra, rb = get(a, path, token), get(b, path, token)
        n += 1
        if ra == rb:
            continue
        if ra[0] == rb[0] == 200 and ra[1] == rb[1] and ra[1].get("Content-Type") == "application/zip":
            ea, eb = zip_entries(ra[2]), zip_entries(rb[2])
            if [(x[0], x[2]) for x in ea] == [(x[0], x[2]) for x in eb]:
                zip_semantic += 1
                continue
        bad += 1
        print(f"MISMATCH {path}\n  A: {ra[0]} {ra[1]} {ra[2][:400]!r}\n  B: {rb[0]} {rb[1]} {rb[2][:400]!r}")
        if ra[0] == rb[0] and len(ra[2]) == len(rb[2]):
            for i, (x, y) in enumerate(zip(ra[2], rb[2])):
                if x != y:
                    print(f"  first diff at byte {i}: A={ra[2][max(0,i-60):i+60]!r}\n                    B={rb[2][max(0,i-60):i+60]!r}")
                    break
    print(f"compared={n} mismatches={bad} zip_entry_equal_only={zip_semantic}")
    sys.exit(1 if bad else 0)


if __name__ == "__main__":
    main()
