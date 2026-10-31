#!/usr/bin/env python3
"""Server CPU per request for the Rust-served routes: Go (:8080) vs Rust (:8081).
usage: loadcmp.py GO_PID RS_PID [secs] [conc]
Logs in yuma on Go, then hammers yuma's Memories (12 months + dates) and export.
"""
import http.client
import json
import os
import sys
import threading
import time

go_pid, rs_pid = int(sys.argv[1]), int(sys.argv[2])
secs = float(sys.argv[3]) if len(sys.argv) > 3 else 10
conc = int(sys.argv[4]) if len(sys.argv) > 4 else 8
TICK = os.sysconf("SC_CLK_TCK")


def cpu(pid):
    f = open(f"/proc/{pid}/stat").read().rsplit(")", 1)[1].split()
    return (int(f[11]) + int(f[12])) / TICK


c = http.client.HTTPConnection("127.0.0.1", 8080)
c.request("POST", "/api/auth/login", json.dumps({"username": "yuma_kobayashi_186", "secretKey": "0f6-BAnyBELD2MA-t-fekeDtQN4ynqD0mfgOF8JWwWs"}), {"Content-Type": "application/json"})
tok = json.loads(c.getresponse().read())["token"]
paths = [f"/api/memories?month=2025-{m:02d}" for m in range(1, 13)] + [f"/api/memories?month=2026-{m:02d}" for m in range(1, 13)]
paths += ["/api/memories/2025-11-09", "/api/memories/2025-11-20", "/api/me/export"]


def run(port, pid):
    stop = time.time() + secs
    count = [0]
    lock = threading.Lock()

    def worker(i):
        conn = http.client.HTTPConnection("127.0.0.1", port)
        k = i
        while time.time() < stop:
            p = paths[k % len(paths)]
            k += 1
            conn.request("GET", p, headers={"Authorization": "Bearer " + tok})
            r = conn.getresponse()
            r.read()
            assert r.status == 200, (p, r.status)
            with lock:
                count[0] += 1

    # warm-up pass (signature caches)
    for p in paths:
        conn = http.client.HTTPConnection("127.0.0.1", port)
        conn.request("GET", p, headers={"Authorization": "Bearer " + tok})
        conn.getresponse().read()
    c0 = cpu(pid)
    ts = [threading.Thread(target=worker, args=(i,)) for i in range(conc)]
    for t in ts:
        t.start()
    for t in ts:
        t.join()
    used = cpu(pid) - c0
    return count[0], used


for name, port, pid in [("go", 8080, go_pid), ("rust", 8081, rs_pid), ("go", 8080, go_pid), ("rust", 8081, rs_pid)]:
    n, used = run(port, pid)
    print(f"{name:5s} req={n:6d} rps={n/secs:8.1f} cpu={used:6.2f}s  cpu/req={used/n*1e6:8.1f}us")
