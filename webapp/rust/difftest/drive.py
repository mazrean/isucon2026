#!/usr/bin/env python3
"""Diff test driver: writes go to Go (A) only; GETs of the Rust-served routes
are sent to both A and B and compared (compare.py logic).

usage: drive.py A_BASE B_BASE [steps] [seed]
"""
import io
import json
import random
import struct
import sys
import uuid
import zlib
import urllib.request

sys.path.insert(0, __file__.rsplit("/", 1)[0])
import compare  # noqa: E402

A, B = sys.argv[1], sys.argv[2]
STEPS = int(sys.argv[3]) if len(sys.argv) > 3 else 400
rng = random.Random(int(sys.argv[4]) if len(sys.argv) > 4 else 1)


def req(method, path, token=None, body=None, ctype=None):
    r = urllib.request.Request(A + path, data=body, method=method)
    if ctype:
        r.add_header("Content-Type", ctype)
    if token:
        r.add_header("Authorization", "Bearer " + token)
    try:
        with urllib.request.urlopen(r, timeout=60) as resp:
            return resp.status, resp.read()
    except urllib.error.HTTPError as e:
        return e.code, e.read()


def js(method, path, token=None, obj=None):
    return req(method, path, token, json.dumps(obj).encode() if obj is not None else None, "application/json")


def jpeg(seed):
    from PIL import Image
    im = Image.new("RGB", (16, 16), ((seed * 37) % 256, (seed * 11) % 256, seed % 256))
    for x in range(16):
        im.putpixel((x, x), (x * 16, 255 - x * 16, seed % 256))
    out = io.BytesIO()
    im.save(out, "JPEG")
    return out.getvalue()


def multipart(fields, img):
    bnd = "----difftest" + uuid.uuid4().hex
    out = io.BytesIO()
    for k, v in fields.items():
        out.write(f"--{bnd}\r\nContent-Disposition: form-data; name=\"{k}\"\r\n\r\n{v}\r\n".encode())
    out.write(f"--{bnd}\r\nContent-Disposition: form-data; name=\"image\"; filename=\"a.jpg\"\r\nContent-Type: image/jpeg\r\n\r\n".encode())
    out.write(img)
    out.write(f"\r\n--{bnd}--\r\n".encode())
    return out.getvalue(), "multipart/form-data; boundary=" + bnd


def main():
    code, b = js("POST", "/api/initialize", obj={})
    assert code == 200, (code, b)
    code, b = js("POST", "/api/admin/auth/login", obj={"username": "admin", "password": "password1234"})
    admin = json.loads(b)["token"]
    day = [2026, 11, 1]
    import datetime

    d = datetime.date(2026, 11, 1)

    def open_all():
        nonlocal d
        while True:
            ok = True
            for rg in ["asia", "europe", "america"]:
                c, bb = js("POST", "/api/admin/posting-window", admin, {"region": rg, "localDate": d.isoformat(), "onTimeSeconds": 2})
                if c == 409:
                    ok = False
                    break
                assert c == 201, (c, bb)
            d = d + datetime.timedelta(days=1)
            if ok:
                return

    open_all()
    users = []

    def login(name, secret):
        c, bb = js("POST", "/api/auth/login", obj={"username": name, "secretKey": secret})
        assert c == 200, (c, bb)
        r = json.loads(bb)
        users.append({"name": name, "token": r["token"], "id": r["user"]["id"]})

    login("yuma_kobayashi_186", "0f6-BAnyBELD2MA-t-fekeDtQN4ynqD0mfgOF8JWwWs")
    run = rng.randrange(100000)
    for i in range(14):
        name = f"rs{run}_{i}"
        c, bb = js("POST", "/api/auth/register", obj={"username": name, "displayName": f"Disp <{name}> & \"q\"", "region": "asia"})
        assert c in (200, 201), (c, bb)
        login(name, json.loads(bb)["secretKey"])
    posts, reqs = [], []
    vis = ["public", "friends", "friends_of_friends"]
    cases = []

    def snapshot():
        for u in users:
            t = u["token"]
            for p in [f"/api/memories?month={d.year}-{d.month:02d}", "/api/memories?month=2026-11", "/api/memories?month=2026-12",
                      "/api/memories?month=2025-11", f"/api/memories/{(d - datetime.timedelta(days=1)).isoformat()}",
                      "/api/memories/2026-11-01", "/api/memories/2025-11-09", "/api/me/export"]:
                cases.append((t, p))
        # pagination
        u = users[0]
        c, _, bb = compare.get(A, "/api/memories?month=2025-11", u["token"])
        items = json.loads(bb).get("items", []) if c == 200 else []
        if len(items) > 1:
            it = items[len(items) // 2]
            cases.append((u["token"], f"/api/memories?month=2025-11&beforePostedAt={it['postedAt'].replace('+', '%2B')}&beforeId={it['id']}"))

    for step in range(STEPS):
        u = rng.choice(users)
        o = rng.choice(users)
        if step % 100 == 99:
            open_all()
        op = rng.randrange(12)
        if op in (0, 1, 2):
            f = {"caption": f"c{step} <b>&amp;   \U0001F600", "visibility": rng.choice(vis), "location_kind": "none"}
            k = rng.randrange(3)
            if k == 1:
                f["location_kind"] = "exact"
                f["latitude"] = f"{rng.uniform(-85, 85):.6f}"
                f["longitude"] = f"{rng.uniform(-175, 175):.7f}"
            elif k == 2:
                f["location_kind"] = "approximate"
                f["locality"] = "渋谷区"
            body, ct = multipart(f, jpeg(step))
            c, bb = req("POST", "/api/posts", u["token"], body, ct)
            if c == 201:
                posts.append(json.loads(bb)["id"])
        elif op == 3 and posts:
            js("POST", f"/api/posts/{rng.choice(posts[-10:])}/comments", u["token"], {"body": f"hi {step} <&>\n\t"})
        elif op == 4 and posts:
            body, ct = multipart({"reactionType": rng.choice(["like", "smile", "heart"])}, jpeg(step))
            req("POST", f"/api/posts/{rng.choice(posts[-10:])}/realmojis", u["token"], body, ct)
        elif op in (5, 6):
            c, bb = js("POST", "/api/friend-requests", u["token"], {"recipientId": o["id"]})
            if c in (200, 201):
                reqs.append(json.loads(bb)["relationId"])
        elif op == 7 and reqs:
            r = rng.choice(reqs)
            for x in users:
                js("POST", f"/api/friend-requests/{r}/accept", x["token"])
        elif op == 8 and posts and rng.randrange(4) == 0:
            js("DELETE", f"/api/posts/{rng.choice(posts)}", u["token"])
        elif op == 9:
            js("PATCH", "/api/me", u["token"], {"displayName": f"N{step} <x>"})
        elif op == 10 and rng.randrange(3) == 0:
            body, ct = multipart({}, jpeg(step))
            req("PUT", "/api/me/profile-image", u["token"], body, ct)
        if step % 50 == 49:
            snapshot()
    snapshot()
    n = bad = zsem = 0
    for t, p in cases:
        ra, rb = compare.get(A, p, t), compare.get(B, p, t)
        n += 1
        if ra == rb:
            continue
        if ra[0] == rb[0] == 200 and ra[1] == rb[1] and ra[1].get("Content-Type") == "application/zip":
            ea, eb = compare.zip_entries(ra[2]), compare.zip_entries(rb[2])
            if [(x[0], x[2]) for x in ea] == [(x[0], x[2]) for x in eb]:
                zsem += 1
                continue
        bad += 1
        if bad <= 5:
            print(f"MISMATCH {p}\n  A: {ra[0]} {ra[2][:300]!r}\n  B: {rb[0]} {rb[2][:300]!r}")
            if ra[0] == rb[0]:
                for i, (x, y) in enumerate(zip(ra[2], rb[2])):
                    if x != y:
                        print(f"  diff@{i}: A={ra[2][max(0,i-80):i+80]!r}\n           B={rb[2][max(0,i-80):i+80]!r}")
                        break
    print(f"posts={len(posts)} compared={n} mismatches={bad} zip_entry_equal_only={zsem}")
    sys.exit(1 if bad else 0)


if __name__ == "__main__":
    main()
