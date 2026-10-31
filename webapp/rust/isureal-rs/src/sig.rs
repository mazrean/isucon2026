//! Image URL signing, identical to image_signature.go:
//! sig = HMAC-SHA256^1000(key, "<exp>:<imageID>"), exp floored to 10 minutes.
//! The HMAC pad states are precomputed, so every round after the first is
//! exactly two SHA-256 compressions; misses are computed 8 at a time with AVX2.

use base64::Engine;
use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::OnceLock;

pub const ITERATIONS: usize = 1000;
const LIFETIME: i64 = 24 * 3600;
const BUCKET: i64 = 600;

pub type Key = [u8; 32];

const IV: [u32; 8] = [
    0x6a09e667, 0xbb67ae85, 0x3c6ef372, 0xa54ff53a, 0x510e527f, 0x9b05688c, 0x1f83d9ab, 0x5be0cd19,
];

fn compress(state: &mut [u32; 8], block: &[u8; 64]) {
    let b = sha2::digest::generic_array::GenericArray::clone_from_slice(block);
    sha2::compress256(state, &[b]);
}

pub struct Pads {
    pub inner: [u32; 8],
    pub outer: [u32; 8],
}

pub fn pads(key: &Key) -> Pads {
    let mut ib = [0x36u8; 64];
    let mut ob = [0x5cu8; 64];
    for i in 0..32 {
        ib[i] ^= key[i];
        ob[i] ^= key[i];
    }
    let mut inner = IV;
    compress(&mut inner, &ib);
    let mut outer = IV;
    compress(&mut outer, &ob);
    Pads { inner, outer }
}

fn state_bytes(s: &[u32; 8]) -> [u8; 32] {
    let mut o = [0u8; 32];
    for i in 0..8 {
        o[i * 4..i * 4 + 4].copy_from_slice(&s[i].to_be_bytes());
    }
    o
}

/// sha256 continuation of `state` (which already absorbed 64 bytes) over msg.
fn finish(state: &[u32; 8], msg: &[u8]) -> [u8; 32] {
    let mut st = *state;
    let total_bits = ((64 + msg.len()) as u64) * 8;
    let mut chunks = msg.chunks_exact(64);
    for c in &mut chunks {
        compress(&mut st, c.try_into().unwrap());
    }
    let rem = chunks.remainder();
    let mut block = [0u8; 128];
    block[..rem.len()].copy_from_slice(rem);
    block[rem.len()] = 0x80;
    let n = if rem.len() + 9 <= 64 { 64 } else { 128 };
    block[n - 8..n].copy_from_slice(&total_bits.to_be_bytes());
    compress(&mut st, block[..64].try_into().unwrap());
    if n == 128 {
        compress(&mut st, block[64..128].try_into().unwrap());
    }
    state_bytes(&st)
}

fn hmac_once(p: &Pads, msg: &[u8]) -> [u8; 32] {
    let inner = finish(&p.inner, msg);
    finish(&p.outer, &inner)
}

/// the fixed 32-byte-message block: value || 0x80 || 0.. || len(96*8)
#[inline]
fn round_block(v: &[u8; 32]) -> [u8; 64] {
    let mut b = [0u8; 64];
    b[..32].copy_from_slice(v);
    b[32] = 0x80;
    b[62] = 0x03; // 768 bits = 0x300
    b
}

pub fn signature_scalar(key: &Key, image_id: &str, exp: i64) -> [u8; 32] {
    let p = pads(key);
    let msg = format!("{}:{}", exp, image_id);
    let mut v = hmac_once(&p, msg.as_bytes());
    for _ in 1..ITERATIONS {
        let mut st = p.inner;
        compress(&mut st, &round_block(&v));
        let ib = state_bytes(&st);
        let mut st = p.outer;
        compress(&mut st, &round_block(&ib));
        v = state_bytes(&st);
    }
    v
}

/// Computes many signatures; uses the 8-lane AVX2 kernel when available.
pub fn signatures(jobs: &[(Key, String, i64)]) -> Vec<[u8; 32]> {
    #[cfg(target_arch = "x86_64")]
    {
        if jobs.len() >= 2 && std::arch::is_x86_feature_detected!("avx2") {
            let mut out = Vec::with_capacity(jobs.len());
            for chunk in jobs.chunks(8) {
                out.extend(unsafe { crate::sha_x8::hmac_iter_x8(chunk) });
            }
            return out;
        }
    }
    jobs.iter().map(|(k, id, exp)| signature_scalar(k, id, *exp)).collect()
}

pub(crate) fn first_round(key: &Key, image_id: &str, exp: i64) -> (Pads, [u8; 32]) {
    let p = pads(key);
    let msg = format!("{}:{}", exp, image_id);
    let v = hmac_once(&p, msg.as_bytes());
    (p, v)
}

pub fn current_exp() -> i64 {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64;
    let exp = now + LIFETIME;
    exp - exp % BUCKET
}

// ---- signed URL cache (per (key, imageID), valid for one exp bucket) ----

const SHARDS: usize = 64;
const SHARD_LIMIT: usize = 1 << 15;

struct UrlCache {
    shards: Vec<RwLock<HashMap<(Key, Box<str>), (i64, Box<str>)>>>,
}

fn cache() -> &'static UrlCache {
    static C: OnceLock<UrlCache> = OnceLock::new();
    C.get_or_init(|| UrlCache { shards: (0..SHARDS).map(|_| RwLock::new(HashMap::new())).collect() })
}

fn shard_of(key: &Key, id: &str) -> usize {
    let mut h: u64 = 1469598103934665603;
    for &b in key[..8].iter().chain(id.as_bytes()) {
        h ^= b as u64;
        h = h.wrapping_mul(1099511628211);
    }
    (h as usize) % SHARDS
}

/// Go url.PathEscape
pub fn path_escape(s: &str, out: &mut String) {
    for &c in s.as_bytes() {
        let keep = c.is_ascii_alphanumeric()
            || matches!(c, b'-' | b'_' | b'.' | b'~' | b'$' | b'&' | b'+' | b':' | b'=' | b'@');
        if keep {
            out.push(c as char);
        } else {
            out.push('%');
            out.push(b"0123456789ABCDEF"[(c >> 4) as usize] as char);
            out.push(b"0123456789ABCDEF"[(c & 15) as usize] as char);
        }
    }
}

fn build_url(image_id: &str, exp: i64, sig: &[u8; 32]) -> String {
    let mut u = String::with_capacity(110);
    u.push_str("/api/images/");
    path_escape(image_id, &mut u);
    u.push_str("?exp=");
    u.push_str(&exp.to_string());
    u.push_str("&sig=");
    // base64 RawURL alphabet needs no query escaping
    u.push_str(&base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(sig));
    u
}

/// A set of (key, imageID) URLs to resolve together so that misses are
/// computed in parallel lanes.
pub struct UrlBatch {
    exp: i64,
    items: Vec<(Key, String)>,
    resolved: Vec<Option<String>>,
}

impl UrlBatch {
    pub fn new() -> Self {
        UrlBatch { exp: current_exp(), items: Vec::new(), resolved: Vec::new() }
    }
    pub fn add(&mut self, key: &Key, image_id: &str) -> usize {
        self.items.push((*key, image_id.to_string()));
        self.items.len() - 1
    }
    /// Resolve all URLs (cache lookups, then compute misses).
    pub fn resolve(&mut self) {
        let exp = self.exp;
        let c = cache();
        let mut resolved = Vec::with_capacity(self.items.len());
        let mut miss: Vec<usize> = Vec::new();
        let mut miss_index: HashMap<(Key, &str), usize> = HashMap::new();
        let mut jobs: Vec<(Key, String, i64)> = Vec::new();
        let mut job_of: Vec<usize> = Vec::new();
        for (i, (k, id)) in self.items.iter().enumerate() {
            let s = c.shards[shard_of(k, id)].read();
            match s.get(&(*k, id.as_str().into())) {
                Some((e, u)) if *e == exp => resolved.push(Some(u.to_string())),
                _ => {
                    resolved.push(None);
                    miss.push(i);
                    let j = *miss_index.entry((*k, id.as_str())).or_insert_with(|| {
                        jobs.push((*k, id.clone(), exp));
                        jobs.len() - 1
                    });
                    job_of.push(j);
                }
            }
        }
        if !jobs.is_empty() {
            let sigs = signatures(&jobs);
            let urls: Vec<String> = jobs.iter().zip(sigs.iter()).map(|((_, id, _), s)| build_url(id, exp, s)).collect();
            for ((k, id, _), u) in jobs.iter().zip(urls.iter()) {
                let mut s = c.shards[shard_of(k, id)].write();
                if s.len() >= SHARD_LIMIT {
                    s.clear();
                }
                s.insert((*k, id.as_str().into()), (exp, u.as_str().into()));
            }
            for (n, &i) in miss.iter().enumerate() {
                resolved[i] = Some(urls[job_of[n]].clone());
            }
        }
        self.resolved = resolved;
    }
    pub fn needs_compute(&self) -> usize {
        let c = cache();
        let exp = self.exp;
        self.items
            .iter()
            .filter(|(k, id)| !matches!(c.shards[shard_of(k, id)].read().get(&(*k, id.as_str().into())), Some((e, _)) if *e == exp))
            .count()
    }
    pub fn get(&self, i: usize) -> &str {
        self.resolved[i].as_deref().unwrap()
    }
}

pub fn decode_key(hexs: &str) -> Option<Key> {
    let v = hex::decode(hexs).ok()?;
    v.try_into().ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn x8_matches_scalar() {
        let mut jobs = Vec::new();
        for i in 0..11u8 {
            let mut k = [0u8; 32];
            for j in 0..32 {
                k[j] = i.wrapping_mul(31).wrapping_add(j as u8);
            }
            jobs.push((k, format!("{:08x}-aaaa-bbbb-cccc-0123456789ab", i as u32 * 7919), 1793501400 + i as i64 * 600));
        }
        let a = signatures(&jobs);
        for (j, s) in jobs.iter().zip(a.iter()) {
            assert_eq!(*s, signature_scalar(&j.0, &j.1, j.2));
        }
    }
}
