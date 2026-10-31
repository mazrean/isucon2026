//! 8-lane AVX2 SHA-256 for the iterated HMAC rounds (message = 32-byte value,
//! so each compression's block words 8..15 are constant padding).

use crate::sig::{first_round, Key, ITERATIONS};
use core::arch::x86_64::*;

const K: [u32; 64] = [
    0x428a2f98, 0x71374491, 0xb5c0fbcf, 0xe9b5dba5, 0x3956c25b, 0x59f111f1, 0x923f82a4, 0xab1c5ed5,
    0xd807aa98, 0x12835b01, 0x243185be, 0x550c7dc3, 0x72be5d74, 0x80deb1fe, 0x9bdc06a7, 0xc19bf174,
    0xe49b69c1, 0xefbe4786, 0x0fc19dc6, 0x240ca1cc, 0x2de92c6f, 0x4a7484aa, 0x5cb0a9dc, 0x76f988da,
    0x983e5152, 0xa831c66d, 0xb00327c8, 0xbf597fc7, 0xc6e00bf3, 0xd5a79147, 0x06ca6351, 0x14292967,
    0x27b70a85, 0x2e1b2138, 0x4d2c6dfc, 0x53380d13, 0x650a7354, 0x766a0abb, 0x81c2c92e, 0x92722c85,
    0xa2bfe8a1, 0xa81a664b, 0xc24b8b70, 0xc76c51a3, 0xd192e819, 0xd6990624, 0xf40e3585, 0x106aa070,
    0x19a4c116, 0x1e376c08, 0x2748774c, 0x34b0bcb5, 0x391c0cb3, 0x4ed8aa4a, 0x5b9cca4f, 0x682e6ff3,
    0x748f82ee, 0x78a5636f, 0x84c87814, 0x8cc70208, 0x90befffa, 0xa4506ceb, 0xbef9a3f7, 0xc67178f2,
];

macro_rules! rotr {
    ($x:expr, $n:literal) => {
        _mm256_or_si256(_mm256_srli_epi32::<$n>($x), _mm256_slli_epi32::<{ 32 - $n }>($x))
    };
}

#[inline(always)]
unsafe fn add(a: __m256i, b: __m256i) -> __m256i {
    _mm256_add_epi32(a, b)
}
#[inline(always)]
unsafe fn xor(a: __m256i, b: __m256i) -> __m256i {
    _mm256_xor_si256(a, b)
}

/// returns st + compress(st, block(w0..7 || 0x80000000 || 0.. || 0x300))
#[target_feature(enable = "avx2")]
unsafe fn compress8(st: &[__m256i; 8], w0: &[__m256i; 8]) -> [__m256i; 8] {
    let mut w = [_mm256_setzero_si256(); 64];
    w[..8].copy_from_slice(w0);
    w[8] = _mm256_set1_epi32(0x80000000u32 as i32);
    w[15] = _mm256_set1_epi32(0x300);
    for t in 16..64 {
        let x = w[t - 15];
        let s0 = xor(xor(rotr!(x, 7), rotr!(x, 18)), _mm256_srli_epi32::<3>(x));
        let y = w[t - 2];
        let s1 = xor(xor(rotr!(y, 17), rotr!(y, 19)), _mm256_srli_epi32::<10>(y));
        w[t] = add(add(s1, w[t - 7]), add(s0, w[t - 16]));
    }
    let [mut a, mut b, mut c, mut d, mut e, mut f, mut g, mut h] = *st;
    for t in 0..64 {
        let s1 = xor(xor(rotr!(e, 6), rotr!(e, 11)), rotr!(e, 25));
        let ch = xor(_mm256_and_si256(e, f), _mm256_andnot_si256(e, g));
        let t1 = add(add(add(h, s1), add(ch, _mm256_set1_epi32(K[t] as i32))), w[t]);
        let s0 = xor(xor(rotr!(a, 2), rotr!(a, 13)), rotr!(a, 22));
        let maj = _mm256_or_si256(_mm256_and_si256(a, b), _mm256_and_si256(c, _mm256_or_si256(a, b)));
        let t2 = add(s0, maj);
        h = g;
        g = f;
        f = e;
        e = add(d, t1);
        d = c;
        c = b;
        b = a;
        a = add(t1, t2);
    }
    [
        add(st[0], a),
        add(st[1], b),
        add(st[2], c),
        add(st[3], d),
        add(st[4], e),
        add(st[5], f),
        add(st[6], g),
        add(st[7], h),
    ]
}

#[target_feature(enable = "avx2")]
pub unsafe fn hmac_iter_x8(jobs: &[(Key, String, i64)]) -> Vec<[u8; 32]> {
    let n = jobs.len();
    assert!(n >= 1 && n <= 8);
    let mut inner = [[0u32; 8]; 8];
    let mut outer = [[0u32; 8]; 8];
    let mut val = [[0u32; 8]; 8];
    for lane in 0..8 {
        let (k, id, exp) = &jobs[lane.min(n - 1)];
        let (p, v) = first_round(k, id, *exp);
        inner[lane] = p.inner;
        outer[lane] = p.outer;
        for j in 0..8 {
            val[lane][j] = u32::from_be_bytes(v[j * 4..j * 4 + 4].try_into().unwrap());
        }
    }
    let gather = |m: &[[u32; 8]; 8], j: usize| -> __m256i {
        _mm256_setr_epi32(
            m[0][j] as i32,
            m[1][j] as i32,
            m[2][j] as i32,
            m[3][j] as i32,
            m[4][j] as i32,
            m[5][j] as i32,
            m[6][j] as i32,
            m[7][j] as i32,
        )
    };
    let mut is = [_mm256_setzero_si256(); 8];
    let mut os = [_mm256_setzero_si256(); 8];
    let mut v = [_mm256_setzero_si256(); 8];
    for j in 0..8 {
        is[j] = gather(&inner, j);
        os[j] = gather(&outer, j);
        v[j] = gather(&val, j);
    }
    for _ in 1..ITERATIONS {
        let x = compress8(&is, &v);
        v = compress8(&os, &x);
    }
    let mut lanes = [[0u32; 8]; 8];
    for j in 0..8 {
        let mut tmp = [0u32; 8];
        _mm256_storeu_si256(tmp.as_mut_ptr() as *mut __m256i, v[j]);
        for lane in 0..8 {
            lanes[lane][j] = tmp[lane];
        }
    }
    (0..n)
        .map(|lane| {
            let mut o = [0u8; 32];
            for j in 0..8 {
                o[j * 4..j * 4 + 4].copy_from_slice(&lanes[lane][j].to_be_bytes());
            }
            o
        })
        .collect()
}
