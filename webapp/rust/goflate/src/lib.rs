//! Byte-exact port of the Deflate stream Go 1.27 `archive/zip` produces for an
//! entry created with `zip.Writer.Create` (compress/flate level 5, one or more
//! Writes, then Close).
//!
//! Ported from $GOROOT/src/compress/flate (go1.27.3): deflate.go (deflateFast,
//! close), level5.go, deflatefast.go, token.go, huffman_code.go,
//! huffman_bit_writer.go. Only the code paths reached by level 5 are ported.
//! The output does not depend on how the input is split across Write calls
//! (the compressor buffers 65535-byte windows).

const MAX_STORE_BLOCK_SIZE: usize = 65535;
const MAX_MATCH_OFFSET: i32 = 1 << 15;
const MAX_MATCH_LENGTH: i32 = 258;
const BASE_MATCH_LENGTH: i32 = 3;
const BASE_MATCH_OFFSET: i32 = 1;
const TABLE_BITS: u32 = 15;
const TABLE_SIZE: usize = 1 << TABLE_BITS;
const ALLOC_HISTORY: usize = MAX_STORE_BLOCK_SIZE * 5;

const LITERAL_COUNT: usize = 286;
const OFFSET_CODE_COUNT: usize = 30;
const END_BLOCK_MARKER: usize = 256;
const LENGTH_CODES_START: usize = 257;
const CODEGEN_CODE_COUNT: usize = 19;
const BAD_CODE: u8 = 255;
const MAX_PREDEFINED_TOKENS: u32 = 250;
const LENGTH_EXTRA_BITS_MIN_CODE: u8 = 8;
const OFFSET_EXTRA_BITS_MIN_CODE: u32 = 4;

const LENGTH_SHIFT: u32 = 22;
const OFFSET_MASK: u32 = (1 << LENGTH_SHIFT) - 1;
const MATCH_TYPE: u32 = 1 << 30;
const MATCH_OFFSET_ONLY_MASK: u32 = 0xffff;

const LOG_NEW_TABLE_PENALTY: u32 = 7; // levels 1-6

static LENGTH_CODES: [u8; 256] = [
    0, 1, 2, 3, 4, 5, 6, 7, 8, 8, 9, 9, 10, 10, 11, 11, 12, 12, 12, 12, 13, 13, 13, 13, 14, 14, 14,
    14, 15, 15, 15, 15, 16, 16, 16, 16, 16, 16, 16, 16, 17, 17, 17, 17, 17, 17, 17, 17, 18, 18, 18,
    18, 18, 18, 18, 18, 19, 19, 19, 19, 19, 19, 19, 19, 20, 20, 20, 20, 20, 20, 20, 20, 20, 20, 20,
    20, 20, 20, 20, 20, 21, 21, 21, 21, 21, 21, 21, 21, 21, 21, 21, 21, 21, 21, 21, 21, 22, 22, 22,
    22, 22, 22, 22, 22, 22, 22, 22, 22, 22, 22, 22, 22, 23, 23, 23, 23, 23, 23, 23, 23, 23, 23, 23,
    23, 23, 23, 23, 23, 24, 24, 24, 24, 24, 24, 24, 24, 24, 24, 24, 24, 24, 24, 24, 24, 24, 24, 24,
    24, 24, 24, 24, 24, 24, 24, 24, 24, 24, 24, 24, 24, 25, 25, 25, 25, 25, 25, 25, 25, 25, 25, 25,
    25, 25, 25, 25, 25, 25, 25, 25, 25, 25, 25, 25, 25, 25, 25, 25, 25, 25, 25, 25, 25, 26, 26, 26,
    26, 26, 26, 26, 26, 26, 26, 26, 26, 26, 26, 26, 26, 26, 26, 26, 26, 26, 26, 26, 26, 26, 26, 26,
    26, 26, 26, 26, 26, 27, 27, 27, 27, 27, 27, 27, 27, 27, 27, 27, 27, 27, 27, 27, 27, 27, 27, 27,
    27, 27, 27, 27, 27, 27, 27, 27, 27, 27, 27, 27, 28,
];

static OFFSET_CODES: [u32; 256] = [
    0, 1, 2, 3, 4, 4, 5, 5, 6, 6, 6, 6, 7, 7, 7, 7, 8, 8, 8, 8, 8, 8, 8, 8, 9, 9, 9, 9, 9, 9, 9, 9,
    10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 11, 11, 11, 11, 11, 11, 11, 11,
    11, 11, 11, 11, 11, 11, 11, 11, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12,
    12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 13, 13, 13, 13, 13, 13, 13, 13,
    13, 13, 13, 13, 13, 13, 13, 13, 13, 13, 13, 13, 13, 13, 13, 13, 13, 13, 13, 13, 13, 13, 13, 13,
    14, 14, 14, 14, 14, 14, 14, 14, 14, 14, 14, 14, 14, 14, 14, 14, 14, 14, 14, 14, 14, 14, 14, 14,
    14, 14, 14, 14, 14, 14, 14, 14, 14, 14, 14, 14, 14, 14, 14, 14, 14, 14, 14, 14, 14, 14, 14, 14,
    14, 14, 14, 14, 14, 14, 14, 14, 14, 14, 14, 14, 14, 14, 14, 14, 15, 15, 15, 15, 15, 15, 15, 15,
    15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15,
    15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15,
    15, 15, 15, 15, 15, 15, 15, 15,
];

static LENGTH_EXTRA_BITS: [u8; 32] = [
    0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 1, 1, 2, 2, 2, 2, 3, 3, 3, 3, 4, 4, 4, 4, 5, 5, 5, 5, 0, 0, 0, 0,
];

static LENGTH_BASE: [u8; 32] = [
    0, 1, 2, 3, 4, 5, 6, 7, 8, 10, 12, 14, 16, 20, 24, 28, 32, 40, 48, 56, 64, 80, 96, 112, 128,
    160, 192, 224, 255, 0, 0, 0,
];

static OFFSET_EXTRA_BITS: [i8; 32] = [
    0, 0, 0, 0, 1, 1, 2, 2, 3, 3, 4, 4, 5, 5, 6, 6, 7, 7, 8, 8, 9, 9, 10, 10, 11, 11, 12, 12, 13,
    13, 14, 14,
];

static OFFSET_COMBINED: [u32; 32] = [
    0x0, 0x0, 0x0, 0x0, 0x401, 0x601, 0x802, 0xc02, 0x1003, 0x1803, 0x2004, 0x3004, 0x4005,
    0x6005, 0x8006, 0xc006, 0x10007, 0x18007, 0x20008, 0x30008, 0x40009, 0x60009, 0x8000a,
    0xc000a, 0x10000b, 0x18000b, 0x20000c, 0x30000c, 0x40000d, 0x60000d, 0x0, 0x0,
];

static CODEGEN_ORDER: [usize; 19] = [16, 17, 18, 0, 8, 7, 9, 6, 10, 5, 11, 4, 12, 3, 13, 2, 14, 1, 15];

#[inline(always)]
fn length_codes1(x: u8) -> u8 {
    LENGTH_CODES[x as usize] + 1
}

#[inline(always)]
fn offset_code(off: u32) -> u32 {
    if off < 256 {
        OFFSET_CODES[off as usize]
    } else {
        OFFSET_CODES[((off >> 7) as u8) as usize] + 14
    }
}

#[inline(always)]
fn load32(b: &[u8], i: i32) -> u32 {
    let i = i as usize;
    u32::from_le_bytes(b[i..i + 4].try_into().unwrap())
}

#[inline(always)]
fn load64(b: &[u8], i: i32) -> u64 {
    let i = i as usize;
    u64::from_le_bytes(b[i..i + 8].try_into().unwrap())
}

#[inline(always)]
fn hash4(u: u64) -> usize {
    ((u as u32).wrapping_mul(2654435761) >> (32 - TABLE_BITS)) as usize
}

#[inline(always)]
fn hash7(u: u64) -> usize {
    ((u << 8).wrapping_mul(58295818150454627) >> (64 - TABLE_BITS)) as usize
}

/// matchLen: common prefix length of a and b; a must be the shorter one.
#[inline(always)]
fn match_len(a: &[u8], b: &[u8]) -> usize {
    let b = &b[..a.len()];
    let mut n = 0;
    while a.len() - n >= 8 {
        let x = u64::from_le_bytes(a[n..n + 8].try_into().unwrap());
        let y = u64::from_le_bytes(b[n..n + 8].try_into().unwrap());
        let d = x ^ y;
        if d != 0 {
            return n + (d.trailing_zeros() >> 3) as usize;
        }
        n += 8;
    }
    while n < a.len() && a[n] == b[n] {
        n += 1;
    }
    n
}

// ---------------------------------------------------------------- tokens

struct Tokens {
    extra_hist: [u16; 32],
    off_hist: [u16; 32],
    lit_hist: [u16; 256],
    n: u32,
    tokens: Vec<u32>, // 65536
}

impl Tokens {
    fn new() -> Self {
        Tokens { extra_hist: [0; 32], off_hist: [0; 32], lit_hist: [0; 256], n: 0, tokens: vec![0u32; 65536] }
    }
    fn reset(&mut self) {
        if self.n == 0 {
            return;
        }
        self.n = 0;
        self.lit_hist = [0; 256];
        self.extra_hist = [0; 32];
        self.off_hist = [0; 32];
    }
    #[inline(always)]
    fn add_literal(&mut self, lit: u8) {
        self.tokens[self.n as usize] = lit as u32;
        self.lit_hist[lit as usize] = self.lit_hist[lit as usize].wrapping_add(1);
        self.n += 1;
    }
    fn add_match_long(&mut self, mut xlength: i32, mut xoffset: u32) {
        let oc = offset_code(xoffset);
        xoffset |= oc << 16;
        while xlength > 0 {
            let mut xl = xlength;
            if xl > 258 {
                if xl > 258 + BASE_MATCH_LENGTH {
                    xl = 258;
                } else {
                    xl = 258 - BASE_MATCH_LENGTH;
                }
            }
            xlength -= xl;
            xl -= BASE_MATCH_LENGTH;
            let c = length_codes1(xl as u8) as usize;
            self.extra_hist[c] = self.extra_hist[c].wrapping_add(1);
            self.off_hist[(oc & 31) as usize] = self.off_hist[(oc & 31) as usize].wrapping_add(1);
            self.tokens[self.n as usize] = MATCH_TYPE | (xl as u32) << LENGTH_SHIFT | xoffset;
            self.n += 1;
        }
    }
    fn add_eob(&mut self) {
        self.tokens[self.n as usize] = END_BLOCK_MARKER as u32;
        self.extra_hist[0] = self.extra_hist[0].wrapping_add(1);
        self.n += 1;
    }
    fn estimated_bits(&self) -> i64 {
        let mut shannon: f32 = 0.0;
        let mut bits: i64 = 0;
        let mut n_matches: i64 = 0;
        let total = self.n as i64;
        if total > 0 {
            let inv_total = 1.0f32 / (total as f32);
            for &v in self.lit_hist.iter() {
                if v > 0 {
                    let n = v as f32;
                    shannon += (-m_fast_log2(n * inv_total)).max(1.0).min(15.0) * n;
                }
            }
            shannon += 15.0;
            for (i, &v) in self.extra_hist[1..LITERAL_COUNT - 256].iter().enumerate() {
                if v > 0 {
                    let n = v as f32;
                    shannon += (-m_fast_log2(n * inv_total)).max(1.0).min(15.0) * n;
                    bits += LENGTH_EXTRA_BITS[i & 31] as i64 * v as i64;
                    n_matches += v as i64;
                }
            }
        }
        if n_matches > 0 {
            let inv_total = 1.0f32 / (n_matches as f32);
            for (i, &v) in self.off_hist[..OFFSET_CODE_COUNT].iter().enumerate() {
                if v > 0 {
                    let n = v as f32;
                    shannon += (-m_fast_log2(n * inv_total)).max(1.0).min(15.0) * n;
                    bits += OFFSET_EXTRA_BITS[i & 31] as i64 * v as i64;
                }
            }
        }
        shannon as i32 as i64 + bits
    }
}

#[inline(always)]
fn m_fast_log2(val: f32) -> f32 {
    let mut ux = val.to_bits() as i32;
    let mut log2 = (((ux >> 23) & 255) - 128) as f32;
    ux &= -0x7f800001i32;
    ux = ux.wrapping_add(127 << 23);
    let uval = f32::from_bits(ux as u32);
    log2 += ((-0.34484843f32) * uval + 2.02466578f32) * uval - 0.67487759f32;
    log2
}

#[inline(always)]
fn tok_length(t: u32) -> u8 {
    (t >> LENGTH_SHIFT) as u8
}
#[inline(always)]
fn tok_offset(t: u32) -> u32 {
    t & OFFSET_MASK
}

// ---------------------------------------------------------------- huffman code

type HCode = u32;
#[inline(always)]
fn hc_len(h: HCode) -> u32 {
    (h as u8) as u32
}
#[inline(always)]
fn hc_code(h: HCode) -> u64 {
    (h >> 8) as u64
}
#[inline(always)]
fn new_hcode(code: u16, length: u8) -> HCode {
    length as u32 | (code as u32) << 8
}
#[inline(always)]
fn reverse_bits(x: u16, b: u8) -> u16 {
    (x << ((16 - b as u32) & 15)).reverse_bits()
}

#[derive(Clone, Copy, Default)]
struct LiteralNode {
    literal: u16,
    freq: u16,
}

#[derive(Clone, Copy, Default)]
struct LevelInfo {
    level: i32,
    last_freq: i32,
    next_char_freq: i32,
    next_pair_freq: i32,
    needed: i32,
}

struct HuffmanEncoder {
    codes: Vec<HCode>,
    bit_count: [i32; 17],
}

impl HuffmanEncoder {
    fn new(size: usize) -> Self {
        HuffmanEncoder { codes: vec![0; size], bit_count: [0; 17] }
    }

    fn bit_length(&self, freq: &[u16]) -> i64 {
        let mut total = 0i64;
        for (i, &f) in freq.iter().enumerate() {
            if f != 0 {
                total += f as i64 * hc_len(self.codes[i]) as i64;
            }
        }
        total
    }

    fn can_encode_len(&self, freq: &[u16]) -> i64 {
        let mut total = 0i64;
        for (i, &f) in freq.iter().enumerate() {
            if f != 0 {
                let code = self.codes[i];
                if code == 0 {
                    return i32::MAX as i64;
                }
                total += f as i64 * hc_len(code) as i64;
            }
        }
        total
    }

    fn bit_counts(&mut self, list: &mut [LiteralNode], n: usize, mut max_bits: i32) -> usize {
        let n32 = n as i32;
        list[n] = LiteralNode { literal: u16::MAX, freq: u16::MAX };
        if max_bits > n32 - 1 {
            max_bits = n32 - 1;
        }
        let mut levels = [LevelInfo::default(); 17];
        let mut leaf_counts = [[0i32; 16]; 16];
        for level in 1..=max_bits {
            let lv = level as usize;
            levels[lv] = LevelInfo {
                level,
                last_freq: list[1].freq as i32,
                next_char_freq: list[2].freq as i32,
                next_pair_freq: list[0].freq as i32 + list[1].freq as i32,
                needed: 0,
            };
            leaf_counts[lv][lv] = 2;
            if level == 1 {
                levels[lv].next_pair_freq = i32::MAX;
            }
        }
        levels[max_bits as usize].needed = 2 * n32 - 4;
        let mut level = max_bits as usize;
        while level < 16 {
            if levels[level].next_pair_freq == i32::MAX && levels[level].next_char_freq == i32::MAX {
                levels[level].needed = 0;
                levels[level + 1].next_pair_freq = i32::MAX;
                level += 1;
                continue;
            }
            let prev_freq = levels[level].last_freq;
            if levels[level].next_char_freq < levels[level].next_pair_freq {
                let nn = leaf_counts[level][level] + 1;
                levels[level].last_freq = levels[level].next_char_freq;
                leaf_counts[level][level] = nn;
                let e = list[nn as usize];
                levels[level].next_char_freq = if e.literal < u16::MAX { e.freq as i32 } else { i32::MAX };
            } else {
                levels[level].last_freq = levels[level].next_pair_freq;
                let save = leaf_counts[level][level];
                leaf_counts[level] = leaf_counts[level - 1];
                leaf_counts[level][level] = save;
                let ll = levels[level].level as usize;
                levels[ll - 1].needed = 2;
            }
            levels[level].needed -= 1;
            if levels[level].needed == 0 {
                if levels[level].level == max_bits {
                    break;
                }
                let ll = levels[level].level as usize;
                levels[ll + 1].next_pair_freq = prev_freq + levels[level].last_freq;
                level += 1;
            } else {
                while levels[level - 1].needed > 0 {
                    level -= 1;
                }
            }
        }
        let mb = max_bits as usize;
        assert!(leaf_counts[mb][mb] == n32, "leafCounts[maxBits][maxBits] != n");
        let counts = leaf_counts[mb];
        let mut bits = 1usize;
        let mut lv = mb;
        while lv > 0 {
            self.bit_count[bits] = counts[lv] - counts[lv - 1];
            bits += 1;
            lv -= 1;
        }
        mb + 1
    }

    fn assign_encoding_and_size(&mut self, bc_len: usize, list: &mut [LiteralNode]) {
        let mut code: u16 = 0;
        let mut list = list;
        for n in 0..bc_len {
            let bits = self.bit_count[n];
            code <<= 1;
            if n == 0 || bits == 0 {
                continue;
            }
            let len = list.len();
            let (rest, chunk) = list.split_at_mut(len - bits as usize);
            chunk.sort_unstable_by_key(|a| a.literal);
            for node in chunk.iter() {
                self.codes[node.literal as usize] = new_hcode(reverse_bits(code, n as u8), n as u8);
                code = code.wrapping_add(1);
            }
            list = rest;
        }
    }

    fn generate(&mut self, freq: &[u16], max_bits: i32) {
        let mut list = [LiteralNode::default(); LITERAL_COUNT + 2];
        let mut count = 0usize;
        for (i, &f) in freq.iter().enumerate() {
            if f != 0 {
                list[count] = LiteralNode { literal: i as u16, freq: f };
                count += 1;
            } else {
                self.codes[i] = 0;
            }
        }
        list[count] = LiteralNode::default();
        if count <= 2 {
            for i in 0..count {
                let node = list[i];
                self.codes[node.literal as usize] = new_hcode(i as u16, 1);
            }
            return;
        }
        list[..count].sort_unstable_by_key(|a| ((a.freq as u32) << 10) + a.literal as u32);
        let bc_len = self.bit_counts(&mut list, count, max_bits);
        self.assign_encoding_and_size(bc_len, &mut list[..count]);
    }
}

fn fixed_literal_encoding() -> HuffmanEncoder {
    let mut h = HuffmanEncoder::new(LITERAL_COUNT);
    for ch in 0..LITERAL_COUNT as u16 {
        let (bits, size): (u16, u8) = if ch < 144 {
            (ch + 48, 8)
        } else if ch < 256 {
            (ch + 400 - 144, 9)
        } else if ch < 280 {
            (ch - 256, 7)
        } else {
            (ch + 192 - 280, 8)
        };
        h.codes[ch as usize] = new_hcode(reverse_bits(bits, size), size);
    }
    h
}

fn fixed_offset_encoding() -> HuffmanEncoder {
    let mut h = HuffmanEncoder::new(30);
    for ch in 0..30u16 {
        h.codes[ch as usize] = new_hcode(reverse_bits(ch, 5), 5);
    }
    h
}

fn huff_offset() -> HuffmanEncoder {
    let mut freq = [0u16; OFFSET_CODE_COUNT];
    freq[0] = 1;
    let mut h = HuffmanEncoder::new(OFFSET_CODE_COUNT);
    h.generate(&freq, 15);
    h
}

// ---------------------------------------------------------------- bit writer

struct HuffmanBitWriter {
    out: Vec<u8>,
    bits: u64,
    nbits: u32,

    wrote_huffman: bool,
    literal_encoding: HuffmanEncoder,
    tmp_lit_encoding: HuffmanEncoder,
    offset_encoding: HuffmanEncoder,
    codegen_encoding: HuffmanEncoder,
    fixed_lit: HuffmanEncoder,
    fixed_off: HuffmanEncoder,
    huff_off: HuffmanEncoder,

    prev_header: i64,
    literal_freq: [u16; LENGTH_CODES_START + 32],
    offset_freq: [u16; 32],
    codegen_freq: [u16; CODEGEN_CODE_COUNT],
    codegen: [u8; LITERAL_COUNT + OFFSET_CODE_COUNT + 1],
}

#[derive(Clone, Copy, PartialEq)]
enum Enc {
    Fixed,
    Dynamic,
}

impl HuffmanBitWriter {
    fn new(cap: usize) -> Self {
        HuffmanBitWriter {
            out: Vec::with_capacity(cap),
            bits: 0,
            nbits: 0,
            wrote_huffman: false,
            literal_encoding: HuffmanEncoder::new(LITERAL_COUNT),
            tmp_lit_encoding: HuffmanEncoder::new(LITERAL_COUNT),
            offset_encoding: HuffmanEncoder::new(OFFSET_CODE_COUNT),
            codegen_encoding: HuffmanEncoder::new(CODEGEN_CODE_COUNT),
            fixed_lit: fixed_literal_encoding(),
            fixed_off: fixed_offset_encoding(),
            huff_off: huff_offset(),
            prev_header: 0,
            literal_freq: [0; LENGTH_CODES_START + 32],
            offset_freq: [0; 32],
            codegen_freq: [0; CODEGEN_CODE_COUNT],
            codegen: [0; LITERAL_COUNT + OFFSET_CODE_COUNT + 1],
        }
    }

    #[inline(always)]
    fn flush_bits(&mut self) {
        let b = self.bits.to_le_bytes();
        self.out.extend_from_slice(&b[..6]);
        self.bits >>= 48;
        self.nbits -= 48;
    }

    #[inline(always)]
    fn write_bits(&mut self, b: u64, nb: u32) {
        self.bits |= b << (self.nbits & 63);
        self.nbits += nb;
        if self.nbits >= 48 {
            self.flush_bits();
        }
    }

    #[inline(always)]
    fn write_code(&mut self, c: HCode) {
        self.bits |= hc_code(c) << (self.nbits & 63);
        self.nbits += hc_len(c);
        if self.nbits >= 48 {
            self.flush_bits();
        }
    }

    fn flush(&mut self) {
        if self.prev_header > 0 {
            let c = self.literal_encoding.codes[END_BLOCK_MARKER];
            self.write_code(c);
            self.prev_header = 0;
        }
        while self.nbits != 0 {
            self.out.push(self.bits as u8);
            self.bits >>= 8;
            if self.nbits > 8 {
                self.nbits -= 8;
            } else {
                self.nbits = 0;
            }
        }
        self.bits = 0;
    }

    fn write_bytes(&mut self, bytes: &[u8]) {
        assert!(self.nbits & 7 == 0, "writeBytes with unfinished bits");
        while self.nbits != 0 {
            self.out.push(self.bits as u8);
            self.bits >>= 8;
            self.nbits -= 8;
        }
        self.out.extend_from_slice(bytes);
    }

    fn generate_codegen(&mut self, num_literals: usize, num_offsets: usize, huff_off: bool) {
        self.codegen_freq = [0; CODEGEN_CODE_COUNT];
        let codegen = &mut self.codegen;
        for i in 0..num_literals {
            codegen[i] = hc_len(self.literal_encoding.codes[i]) as u8;
        }
        let off_enc = if huff_off { &self.huff_off } else { &self.offset_encoding };
        for i in 0..num_offsets {
            codegen[num_literals + i] = hc_len(off_enc.codes[i]) as u8;
        }
        codegen[num_literals + num_offsets] = BAD_CODE;

        let freq = &mut self.codegen_freq;
        let mut size = codegen[0];
        let mut count: i32 = 1;
        let mut out_index = 0usize;
        let mut in_index = 1usize;
        while size != BAD_CODE {
            let next_size = codegen[in_index];
            in_index += 1;
            if next_size == size {
                count += 1;
                continue;
            }
            if size != 0 {
                codegen[out_index] = size;
                out_index += 1;
                freq[size as usize] += 1;
                count -= 1;
                while count >= 3 {
                    let n = count.min(6);
                    codegen[out_index] = 16;
                    out_index += 1;
                    codegen[out_index] = (n - 3) as u8;
                    out_index += 1;
                    freq[16] += 1;
                    count -= n;
                }
            } else {
                while count >= 11 {
                    let n = count.min(138);
                    codegen[out_index] = 18;
                    out_index += 1;
                    codegen[out_index] = (n - 11) as u8;
                    out_index += 1;
                    freq[18] += 1;
                    count -= n;
                }
                if count >= 3 {
                    codegen[out_index] = 17;
                    out_index += 1;
                    codegen[out_index] = (count - 3) as u8;
                    out_index += 1;
                    freq[17] += 1;
                    count = 0;
                }
            }
            count -= 1;
            while count >= 0 {
                codegen[out_index] = size;
                out_index += 1;
                freq[size as usize] += 1;
                count -= 1;
            }
            size = next_size;
            count = 1;
        }
        codegen[out_index] = BAD_CODE;
    }

    fn codegens(&self) -> usize {
        let mut n = CODEGEN_CODE_COUNT;
        while n > 4 && self.codegen_freq[CODEGEN_ORDER[n - 1]] == 0 {
            n -= 1;
        }
        n
    }

    fn header_size(&self) -> (i64, usize) {
        let n = self.codegens();
        (
            3 + 5 + 5 + 4 + (3 * n as i64)
                + self.codegen_encoding.bit_length(&self.codegen_freq)
                + self.codegen_freq[16] as i64 * 2
                + self.codegen_freq[17] as i64 * 3
                + self.codegen_freq[18] as i64 * 7,
            n,
        )
    }

    fn dynamic_reuse_size(&self) -> i64 {
        self.literal_encoding.bit_length(&self.literal_freq) + self.offset_encoding.bit_length(&self.offset_freq)
    }

    fn dynamic_size(&self, extra_bits: i64) -> (i64, usize) {
        let (header, n) = self.header_size();
        (
            header
                + self.literal_encoding.bit_length(&self.literal_freq)
                + self.offset_encoding.bit_length(&self.offset_freq)
                + extra_bits,
            n,
        )
    }

    fn extra_bit_size(&self) -> i64 {
        let mut total = 0i64;
        for (i, &n) in self.literal_freq[257..LITERAL_COUNT].iter().enumerate() {
            total += n as i64 * LENGTH_EXTRA_BITS[i & 31] as i64;
        }
        for (i, &n) in self.offset_freq[..OFFSET_CODE_COUNT].iter().enumerate() {
            total += n as i64 * OFFSET_EXTRA_BITS[i & 31] as i64;
        }
        total
    }

    fn fixed_size(&self, extra_bits: i64) -> i64 {
        3 + self.fixed_lit.bit_length(&self.literal_freq) + self.fixed_off.bit_length(&self.offset_freq) + extra_bits
    }

    fn stored_size(input: Option<&[u8]>) -> (i64, bool) {
        match input {
            None => (0, false),
            Some(b) if b.len() <= MAX_STORE_BLOCK_SIZE => (((b.len() + 5) * 8) as i64, true),
            _ => (0, false),
        }
    }

    fn write_dynamic_header(&mut self, num_literals: usize, num_offsets: usize, num_codegens: usize, is_eof: bool) {
        let first_bits = if is_eof { 5 } else { 4 };
        self.write_bits(first_bits, 3);
        self.write_bits((num_literals - 257) as u64, 5);
        self.write_bits((num_offsets - 1) as u64, 5);
        self.write_bits((num_codegens - 4) as u64, 4);
        for i in 0..num_codegens {
            let value = hc_len(self.codegen_encoding.codes[CODEGEN_ORDER[i]]);
            self.write_bits(value as u64, 3);
        }
        let mut i = 0usize;
        loop {
            let code_word = self.codegen[i];
            i += 1;
            if code_word == BAD_CODE {
                break;
            }
            let c = self.codegen_encoding.codes[code_word as usize];
            self.write_code(c);
            match code_word {
                16 => {
                    let v = self.codegen[i];
                    self.write_bits(v as u64, 2);
                    i += 1;
                }
                17 => {
                    let v = self.codegen[i];
                    self.write_bits(v as u64, 3);
                    i += 1;
                }
                18 => {
                    let v = self.codegen[i];
                    self.write_bits(v as u64, 7);
                    i += 1;
                }
                _ => {}
            }
        }
    }

    fn write_stored_header(&mut self, length: usize, is_eof: bool) {
        if self.prev_header > 0 {
            let c = self.literal_encoding.codes[END_BLOCK_MARKER];
            self.write_code(c);
            self.prev_header = 0;
        }
        if length == 0 && is_eof {
            self.write_fixed_header(is_eof);
            self.write_bits(0, 7);
            self.flush();
            return;
        }
        let flag = if is_eof { 1 } else { 0 };
        self.write_bits(flag, 3);
        self.flush();
        self.write_bits(length as u64, 16);
        self.write_bits((!(length as u16)) as u64, 16);
    }

    fn write_fixed_header(&mut self, is_eof: bool) {
        if self.prev_header > 0 {
            let c = self.literal_encoding.codes[END_BLOCK_MARKER];
            self.write_code(c);
            self.prev_header = 0;
        }
        let value = if is_eof { 3 } else { 2 };
        self.write_bits(value, 3);
    }

    fn index_tokens(&mut self, t: &Tokens) -> (usize, usize) {
        self.literal_freq[..256].copy_from_slice(&t.lit_hist);
        self.literal_freq[256..288].copy_from_slice(&t.extra_hist);
        self.offset_freq = t.off_hist;
        if t.n == 0 {
            return (0, 0);
        }
        let mut num_literals = self.literal_freq.len();
        while self.literal_freq[num_literals - 1] == 0 {
            num_literals -= 1;
        }
        let mut num_offsets = self.offset_freq.len();
        while num_offsets > 0 && self.offset_freq[num_offsets - 1] == 0 {
            num_offsets -= 1;
        }
        if num_offsets == 0 {
            self.offset_freq[0] = 1;
            num_offsets = 1;
        }
        (num_literals, num_offsets)
    }

    fn generate(&mut self) {
        let lf = self.literal_freq;
        self.literal_encoding.generate(&lf[..LITERAL_COUNT], 15);
        let of = self.offset_freq;
        self.offset_encoding.generate(&of[..OFFSET_CODE_COUNT], 15);
    }

    fn write_block_dynamic(&mut self, tokens: &mut Tokens, eof: bool, input: Option<&[u8]>, sync: bool) {
        let sync = sync || eof;
        if sync {
            tokens.add_eob();
        } else {
            tokens.extra_hist[0] = 1;
        }

        if (self.wrote_huffman || eof) && self.prev_header > 0 {
            let c = self.literal_encoding.codes[END_BLOCK_MARKER];
            self.write_code(c);
            self.prev_header = 0;
            self.wrote_huffman = false;
        }

        if self.prev_header > 0 && !self.can_reuse(tokens) {
            let c = self.literal_encoding.codes[END_BLOCK_MARKER];
            self.write_code(c);
            self.prev_header = 0;
        }

        let (num_literals, num_offsets) = self.index_tokens(tokens);
        let mut extra_bits = 0i64;
        let (ssize, storable) = Self::stored_size(input);
        if storable || self.prev_header > 0 {
            extra_bits = self.extra_bit_size();
        }

        let mut size: i64;
        if self.prev_header > 0 {
            let mut new_size = self.prev_header + tokens.estimated_bits();
            new_size += hc_len(self.literal_encoding.codes[END_BLOCK_MARKER]) as i64 + (new_size >> LOG_NEW_TABLE_PENALTY);
            let reuse_size = self.dynamic_reuse_size() + extra_bits;
            if new_size < reuse_size {
                let c = self.literal_encoding.codes[END_BLOCK_MARKER];
                self.write_code(c);
                size = new_size;
                self.prev_header = 0;
            } else {
                size = reuse_size;
            }

            if tokens.n < MAX_PREDEFINED_TOKENS {
                let pre_size = self.fixed_size(extra_bits) + 7;
                if pre_size < size {
                    if storable && ssize <= size {
                        let inp = input.unwrap();
                        self.write_stored_header(inp.len(), eof);
                        self.write_bytes(inp);
                        return;
                    }
                    self.write_fixed_header(eof);
                    if !sync {
                        tokens.add_eob();
                    }
                    self.write_tokens(tokens, Enc::Fixed);
                    return;
                }
            }
            if storable && ssize <= size {
                let inp = input.unwrap();
                self.write_stored_header(inp.len(), eof);
                self.write_bytes(inp);
                return;
            }
        }

        if self.prev_header == 0 {
            self.literal_freq[END_BLOCK_MARKER] = 1;
            self.generate();
            self.generate_codegen(num_literals, num_offsets, false);
            let cf = self.codegen_freq;
            self.codegen_encoding.generate(&cf, 7);
            let (s, num_codegens) = self.dynamic_size(extra_bits);
            size = s;

            if tokens.n < MAX_PREDEFINED_TOKENS {
                let pre_size = self.fixed_size(extra_bits);
                if pre_size <= size {
                    if storable && ssize <= pre_size {
                        let inp = input.unwrap();
                        self.write_stored_header(inp.len(), eof);
                        self.write_bytes(inp);
                        return;
                    }
                    self.write_fixed_header(eof);
                    if !sync {
                        tokens.add_eob();
                    }
                    self.write_tokens(tokens, Enc::Fixed);
                    return;
                }
            }
            if storable && ssize <= size {
                let inp = input.unwrap();
                self.write_stored_header(inp.len(), eof);
                self.write_bytes(inp);
                return;
            }
            self.write_dynamic_header(num_literals, num_offsets, num_codegens, eof);
            if !sync {
                self.prev_header = self.header_size().0;
            }
            self.wrote_huffman = false;
        }
        let _ = size;
        if sync {
            self.prev_header = 0;
        }
        self.write_tokens(tokens, Enc::Dynamic);
    }

    fn can_reuse(&self, t: &Tokens) -> bool {
        let b = &self.offset_encoding.codes;
        for (i, &v) in t.off_hist[..OFFSET_CODE_COUNT].iter().enumerate() {
            if v != 0 && b[i] == 0 {
                return false;
            }
        }
        let b = &self.literal_encoding.codes[256..LITERAL_COUNT];
        for (i, &v) in t.extra_hist[..LITERAL_COUNT - 256].iter().enumerate() {
            if v != 0 && b[i] == 0 {
                return false;
            }
        }
        let b = &self.literal_encoding.codes[..256];
        for (i, &v) in t.lit_hist.iter().enumerate() {
            if v != 0 && b[i] == 0 {
                return false;
            }
        }
        true
    }

    fn write_tokens(&mut self, tokens: &Tokens, enc: Enc) {
        let mut toks = &tokens.tokens[..tokens.n as usize];
        if toks.is_empty() {
            return;
        }
        let mut defer_eob = false;
        if toks[toks.len() - 1] == END_BLOCK_MARKER as u32 {
            toks = &toks[..toks.len() - 1];
            defer_eob = true;
        }
        // Temporarily take the code tables out to satisfy the borrow checker.
        let (lit, off) = match enc {
            Enc::Fixed => (std::mem::take(&mut self.fixed_lit.codes), std::mem::take(&mut self.fixed_off.codes)),
            Enc::Dynamic => {
                (std::mem::take(&mut self.literal_encoding.codes), std::mem::take(&mut self.offset_encoding.codes))
            }
        };
        let mut bits = self.bits;
        let mut nbits = self.nbits;
        let out = &mut self.out;
        macro_rules! put {
            ($v:expr, $n:expr) => {{
                bits |= ($v) << (nbits & 63);
                nbits += $n;
                if nbits >= 48 {
                    out.extend_from_slice(&bits.to_le_bytes()[..6]);
                    bits >>= 48;
                    nbits -= 48;
                }
            }};
        }
        for &t in toks {
            if t < 256 {
                let c = lit[t as usize];
                put!(hc_code(c), hc_len(c));
                continue;
            }
            let length = tok_length(t);
            let len_code = LENGTH_CODES[length as usize] & 31;
            let c = lit[LENGTH_CODES_START + len_code as usize];
            put!(hc_code(c), hc_len(c));
            if len_code >= LENGTH_EXTRA_BITS_MIN_CODE {
                let eb = LENGTH_EXTRA_BITS[len_code as usize] as u32;
                let extra = (length.wrapping_sub(LENGTH_BASE[len_code as usize])) as u64;
                put!(extra, eb);
            }
            let offset = tok_offset(t);
            let off_code = (offset >> 16) & 31;
            let c = off[off_code as usize];
            put!(hc_code(c), hc_len(c));
            if off_code >= OFFSET_EXTRA_BITS_MIN_CODE {
                let comb = OFFSET_COMBINED[off_code as usize];
                put!((offset.wrapping_sub(comb >> 8) & MATCH_OFFSET_ONLY_MASK) as u64, comb as u8 as u32);
            }
        }
        self.bits = bits;
        self.nbits = nbits;
        if defer_eob {
            let c = lit[END_BLOCK_MARKER];
            self.write_code(c);
        }
        match enc {
            Enc::Fixed => {
                self.fixed_lit.codes = lit;
                self.fixed_off.codes = off;
            }
            Enc::Dynamic => {
                self.literal_encoding.codes = lit;
                self.offset_encoding.codes = off;
            }
        }
    }

    fn write_block_huff(&mut self, eof: bool, input: &[u8], sync: bool) {
        self.literal_freq = [0; LENGTH_CODES_START + 32];
        if !self.wrote_huffman {
            self.offset_freq = [0; 32];
        }
        const NUM_LITERALS: usize = END_BLOCK_MARKER + 1;
        const NUM_OFFSETS: usize = 1;
        const GUESS_HEADER_SIZE_BITS: i64 = 70 * 8;
        for &b in input {
            self.literal_freq[b as usize] = self.literal_freq[b as usize].wrapping_add(1);
        }
        let (ssize, storable) = Self::stored_size(Some(input));
        if storable && input.len() > 1024 {
            let mut abs = 0f64;
            let avg = input.len() as f64 / 256.0;
            let max = (input.len() * 2) as f64;
            for &v in self.literal_freq[..256].iter() {
                let diff = v as f64 - avg;
                abs += diff * diff;
                if abs >= max {
                    break;
                }
            }
            if abs < max {
                self.write_stored_header(input.len(), eof);
                self.write_bytes(input);
                return;
            }
        }
        self.literal_freq[END_BLOCK_MARKER] = 1;
        let lf = self.literal_freq;
        self.tmp_lit_encoding.generate(&lf[..NUM_LITERALS], 15);
        let mut est_bits = self.tmp_lit_encoding.can_encode_len(&lf[..NUM_LITERALS]);
        if est_bits < i32::MAX as i64 {
            est_bits += self.prev_header;
            if self.prev_header == 0 {
                est_bits += GUESS_HEADER_SIZE_BITS;
            }
            est_bits += est_bits >> LOG_NEW_TABLE_PENALTY;
        }
        if storable && ssize <= est_bits {
            self.write_stored_header(input.len(), eof);
            self.write_bytes(input);
            return;
        }
        if self.prev_header > 0 {
            let reuse_size = self.literal_encoding.can_encode_len(&lf[..256]);
            if est_bits < reuse_size {
                let c = self.literal_encoding.codes[END_BLOCK_MARKER];
                self.write_code(c);
                self.prev_header = 0;
            }
        }
        if self.prev_header == 0 {
            std::mem::swap(&mut self.literal_encoding, &mut self.tmp_lit_encoding);
            self.generate_codegen(NUM_LITERALS, NUM_OFFSETS, true);
            let cf = self.codegen_freq;
            self.codegen_encoding.generate(&cf, 7);
            let num_codegens = self.codegens();
            self.write_dynamic_header(NUM_LITERALS, NUM_OFFSETS, num_codegens, eof);
            self.wrote_huffman = true;
            self.prev_header = self.header_size().0;
        }
        let enc = std::mem::take(&mut self.literal_encoding.codes);
        let mut bits = self.bits;
        let mut nbits = self.nbits;
        let out = &mut self.out;
        for &t in input {
            let c = enc[t as usize];
            bits |= hc_code(c) << (nbits & 63);
            nbits += hc_len(c);
            if nbits >= 48 {
                out.extend_from_slice(&bits.to_le_bytes()[..6]);
                bits >>= 48;
                nbits -= 48;
            }
        }
        self.bits = bits;
        self.nbits = nbits;
        self.literal_encoding.codes = enc;
        if eof || sync {
            let c = self.literal_encoding.codes[END_BLOCK_MARKER];
            self.write_code(c);
            self.prev_header = 0;
            self.wrote_huffman = false;
        }
    }

    fn write_stored_block(&mut self, buf: &[u8]) {
        self.write_stored_header(buf.len(), false);
        self.write_bytes(buf);
    }
}

// ---------------------------------------------------------------- level 5

struct FastEncL5 {
    hist: Vec<u8>,
    cur: i32,
    table: Vec<i32>,           // tableSize
    btable: Vec<(i32, i32)>,   // (cur, prev)
}

impl FastEncL5 {
    fn new() -> Self {
        FastEncL5 {
            hist: Vec::new(),
            cur: MAX_STORE_BLOCK_SIZE as i32,
            table: vec![0; TABLE_SIZE],
            btable: vec![(0, 0); TABLE_SIZE],
        }
    }

    fn add_block(&mut self, src: &[u8]) -> i32 {
        let cap = if self.hist.capacity() == 0 { 0 } else { ALLOC_HISTORY };
        if self.hist.len() + src.len() > cap {
            if cap == 0 {
                self.hist = Vec::with_capacity(ALLOC_HISTORY);
            } else {
                let offset = self.hist.len() - MAX_MATCH_OFFSET as usize;
                self.hist.copy_within(offset..offset + MAX_MATCH_OFFSET as usize, 0);
                self.cur += offset as i32;
                self.hist.truncate(MAX_MATCH_OFFSET as usize);
            }
        }
        let s = self.hist.len() as i32;
        self.hist.extend_from_slice(src);
        s
    }

    #[inline(always)]
    fn push_long(&mut self, h: usize, v: i32) {
        let e = &mut self.btable[h];
        e.1 = e.0;
        e.0 = v;
    }

    fn encode(&mut self, dst: &mut Tokens, src_in: &[u8]) {
        const INPUT_MARGIN: i32 = 12 - 1;
        const MIN_NON_LITERAL_BLOCK_SIZE: usize = 1 + 1 + INPUT_MARGIN as usize;
        // e.cur wraparound protection omitted: only reachable after ~2 GiB of input.
        let mut s = self.add_block(src_in);
        if src_in.len() < MIN_NON_LITERAL_BLOCK_SIZE {
            dst.n = src_in.len() as u32;
            return;
        }
        let hist = std::mem::take(&mut self.hist);
        let src: &[u8] = &hist;
        let cur = self.cur;

        let mut next_emit = s;
        let s_limit = src.len() as i32 - INPUT_MARGIN;
        let mut cv = load64(src, s);

        let mm = |a: i32, b: i32, limited: bool| -> i32 {
            let (a, b) = (a as usize, b as usize);
            if limited {
                let end = (a + MAX_MATCH_LENGTH as usize - 4).min(src.len());
                match_len(&src[a..end], &src[b..]) as i32
            } else {
                match_len(&src[a..], &src[b..]) as i32
            }
        };

        'outer: loop {
            let mut next_s = s;
            let mut l: i32 = 0;
            let mut t: i32;
            loop {
                let mut next_hash_s = hash4(cv);
                let mut next_hash_l = hash7(cv);
                s = next_s;
                next_s = s + 1 + ((s - next_emit) >> 6);
                if next_s > s_limit {
                    break 'outer;
                }
                let s_candidate = self.table[next_hash_s];
                let mut l_candidate = self.btable[next_hash_l];
                let next = load64(src, next_s);
                let entry = s + cur;
                self.table[next_hash_s] = entry;
                self.push_long(next_hash_l, entry);

                next_hash_s = hash4(next);
                next_hash_l = hash7(next);

                t = l_candidate.0 - cur;
                if s - t < MAX_MATCH_OFFSET {
                    if cv as u32 == load32(src, t) {
                        self.table[next_hash_s] = next_s + cur;
                        self.push_long(next_hash_l, next_s + cur);
                        let t2 = l_candidate.1 - cur;
                        if s - t2 < MAX_MATCH_OFFSET && cv as u32 == load32(src, t2) {
                            l = mm(s + 4, t + 4, true) + 4;
                            let ml1 = mm(s + 4, t2 + 4, true) + 4;
                            if ml1 > l {
                                t = t2;
                                l = ml1;
                                break;
                            }
                        }
                        break;
                    }
                    t = l_candidate.1 - cur;
                    if s - t < MAX_MATCH_OFFSET && cv as u32 == load32(src, t) {
                        self.table[next_hash_s] = next_s + cur;
                        self.push_long(next_hash_l, next_s + cur);
                        break;
                    }
                }

                t = s_candidate - cur;
                if s - t < MAX_MATCH_OFFSET && cv as u32 == load32(src, t) {
                    l = mm(s + 4, t + 4, true) + 4;
                    l_candidate = self.btable[next_hash_l];
                    self.table[next_hash_s] = next_s + cur;
                    self.push_long(next_hash_l, next_s + cur);

                    let mut t2 = l_candidate.0 - cur;
                    if next_s - t2 < MAX_MATCH_OFFSET {
                        if load32(src, t2) == next as u32 {
                            let ml = mm(next_s + 4, t2 + 4, true) + 4;
                            if ml > l {
                                t = t2;
                                s = next_s;
                                l = ml;
                                break;
                            }
                        }
                        t2 = l_candidate.1 - cur;
                        if next_s - t2 < MAX_MATCH_OFFSET && load32(src, t2) == next as u32 {
                            let ml = mm(next_s + 4, t2 + 4, true) + 4;
                            if ml > l {
                                t = t2;
                                s = next_s;
                                l = ml;
                                break;
                            }
                        }
                    }
                    break;
                }
                cv = next;
            }

            if l == 0 {
                l = mm(s + 4, t + 4, false) + 4;
            } else if l == MAX_MATCH_LENGTH {
                l += mm(s + l, t + l, false);
            }

            let s_at = s + l;
            if l < 30 && s_at < s_limit {
                const SKIP_BEGINNING: i32 = 2;
                let e_long = self.btable[hash7(load64(src, s_at))].0;
                let t2 = e_long - cur - l + SKIP_BEGINNING;
                let s2 = s + SKIP_BEGINNING;
                let off = s2 - t2;
                if t2 >= 0 && off < MAX_MATCH_OFFSET && off > 0 {
                    let l2 = mm(s2, t2, false);
                    if l2 > l {
                        t = t2;
                        l = l2;
                        s = s2;
                    }
                }
            }

            while t > 0 && s > next_emit && src[(t - 1) as usize] == src[(s - 1) as usize] {
                s -= 1;
                t -= 1;
                l += 1;
            }
            if next_emit < s {
                for &v in &src[next_emit as usize..s as usize] {
                    dst.add_literal(v);
                }
            }

            dst.add_match_long(l, (s - t - BASE_MATCH_OFFSET) as u32);
            s += l;
            next_emit = s;
            if next_s >= s {
                s = next_s + 1;
            }
            if s >= s_limit {
                break 'outer;
            }

            const HASH_EVERY: i32 = 3;
            let mut i = s - l + 1;
            if i < s - 1 {
                let mut cv = load64(src, i);
                let mut tt = i + cur;
                self.table[hash4(cv)] = tt;
                self.push_long(hash7(cv), tt);

                cv >>= 8;
                tt += 1;
                self.push_long(hash7(cv), tt);

                cv >>= 8;
                tt += 1;
                self.table[hash4(cv)] = tt;

                i += 4;
                while i < s - 1 {
                    let cv = load64(src, i);
                    let tt = i + cur;
                    self.push_long(hash7(cv), tt);
                    self.table[hash4(cv >> 8)] = tt + 1;
                    i += HASH_EVERY;
                }
            }

            let x = load64(src, s - 1);
            let o = cur + s - 1;
            self.table[hash4(x)] = o;
            self.push_long(hash7(x), o);
            cv = x >> 8;
        }

        // emitRemainder
        if (next_emit as usize) < src.len() && dst.n != 0 {
            for &v in &src[next_emit as usize..] {
                dst.add_literal(v);
            }
        }
        self.hist = hist;
    }
}

// ---------------------------------------------------------------- compressor

/// Returns the raw Deflate stream Go's `archive/zip` (`zip.Writer.Create`,
/// method Deflate, compress/flate level 5) writes for `input`.
pub fn deflate_like_go_zip(input: &[u8]) -> Vec<u8> {
    let mut w = HuffmanBitWriter::new(input.len() / 3 + 64);
    let mut tokens = Tokens::new();
    let mut fast = FastEncL5::new();

    if !input.is_empty() {
        let nchunks = input.len().div_ceil(MAX_STORE_BLOCK_SIZE);
        for (idx, win) in input.chunks(MAX_STORE_BLOCK_SIZE).enumerate() {
            let sync = idx + 1 == nchunks;
            deflate_fast(&mut w, &mut tokens, &mut fast, win, sync);
        }
    }
    // close(): sync step done above; write EOF marker.
    w.write_stored_header(0, true);
    w.flush();
    w.out
}

fn deflate_fast(w: &mut HuffmanBitWriter, tokens: &mut Tokens, fast: &mut FastEncL5, win: &[u8], sync: bool) {
    if win.len() < MAX_STORE_BLOCK_SIZE {
        if !sync {
            return; // unreachable: only the last chunk can be short
        }
        if win.len() < 128 {
            if win.is_empty() {
                return;
            }
            if win.len() <= 32 {
                w.write_stored_block(win);
            } else {
                w.write_block_huff(false, win, true);
            }
            tokens.reset();
            return;
        }
    }
    fast.encode(tokens, win);
    let n = tokens.n as i32;
    let we = win.len() as i32;
    if n == 0 {
        w.write_stored_block(win);
    } else if n > we - (we >> 4) {
        w.write_block_huff(false, win, sync);
    } else {
        w.write_block_dynamic(tokens, false, Some(win), sync);
    }
    tokens.reset();
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_input() {
        assert_eq!(deflate_like_go_zip(b""), vec![0x03, 0x00]);
    }

    #[test]
    fn small_stored() {
        // <= 32 bytes: stored block + empty fixed EOF block (matches Go output).
        let out = deflate_like_go_zip(b"hello");
        assert_eq!(out, vec![0x00, 0x05, 0x00, 0xfa, 0xff, b'h', b'e', b'l', b'l', b'o', 0x03, 0x00]);
    }
}
