#!/usr/bin/env python3
"""Minimal protobuf raw decoder (like protoc --decode_raw). stdin: bytes -> stdout: tree."""
import sys

def varint(b, i):
    s = r = 0
    while True:
        c = b[i]; i += 1
        r |= (c & 0x7f) << s; s += 7
        if c < 0x80: return r, i

def dec(b, depth=0):
    i, out = 0, []
    while i < len(b):
        k, i = varint(b, i)
        f, t = k >> 3, k & 7
        if f == 0: raise ValueError
        if t == 0:
            v, i = varint(b, i); out.append((f, v))
        elif t == 1:
            out.append((f, int.from_bytes(b[i:i+8], 'little'))); i += 8
        elif t == 5:
            out.append((f, int.from_bytes(b[i:i+4], 'little'))); i += 4
        elif t == 2:
            n, i = varint(b, i); s = b[i:i+n]; i += n
            if i > len(b): raise ValueError
            try:
                sub = dec(s, depth+1) if s else None
            except Exception:
                sub = None
            if sub is not None and not _printable(s):
                out.append((f, sub))
            else:
                try: out.append((f, s.decode('utf-8')))
                except UnicodeDecodeError:
                    out.append((f, sub if sub is not None else s.hex()))
        else:
            raise ValueError
    return out

def _printable(s):
    try: t = s.decode('utf-8')
    except UnicodeDecodeError: return False
    return all(ch.isprintable() or ch in '\n\r\t' for ch in t)

def show(tree, ind=0):
    for f, v in tree:
        if isinstance(v, list):
            print(' ' * ind + f'{f} {{'); show(v, ind + 2); print(' ' * ind + '}')
        else:
            print(' ' * ind + f'{f}: {v!r}' if isinstance(v, str) else ' ' * ind + f'{f}: {v}')

if __name__ == '__main__':
    show(dec(sys.stdin.buffer.read()))
