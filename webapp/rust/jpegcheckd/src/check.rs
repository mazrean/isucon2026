//! Line-by-line port of webapp/go/internal/jpegcheck (itself a fork of Go
//! 1.27 image/jpeg with pixel reconstruction removed). `check` returns the
//! same accept/reject verdict and bounds as Go's jpeg.Decode.
//!
//! The byte layer (cursor, nUnreadable, bit accumulator, lazy one-byte-at-a-time
//! refill) mirrors Go exactly, because where marker parsing resumes after a
//! scan depends on it. Coefficient values are never needed: only whether an AC
//! coefficient of a progressive block is non-zero (refinement control flow),
//! so each block is a u64 mask indexed by zig-zag position.

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Verdict {
    Ok(u32, u32),
    Bad,
    /// The input needs more memory than we are willing to spend; the caller
    /// should fall back to the Go implementation.
    TooBig,
}

#[derive(Debug, Clone, Copy)]
enum E {
    Bad,
    TooBig,
}

type R<T> = Result<T, E>;

const MAX_CODE_LENGTH: usize = 16;
const MAX_NCODES: i32 = 256;
const MAX_TC: u8 = 1;
const MAX_TH: u8 = 3;
const MAX_TQ: u8 = 3;
const BLOCK_SIZE: usize = 64;
const LUT_BITS: u32 = 10;

const SOF0: u8 = 0xc0;
const SOF1: u8 = 0xc1;
const SOF2: u8 = 0xc2;
const DHT: u8 = 0xc4;
const RST0: u8 = 0xd0;
const RST7: u8 = 0xd7;
const SOI: u8 = 0xd8;
const EOI: u8 = 0xd9;
const SOS: u8 = 0xda;
const DQT: u8 = 0xdb;
const DRI: u8 = 0xdd;
const COM: u8 = 0xfe;
const APP0: u8 = 0xe0;
const APP14: u8 = 0xee;
const APP15: u8 = 0xef;

/// Upper bound on progressive coefficient masks (8 bytes per block).
const MAX_PROG_BLOCKS: usize = 64 << 20;

#[derive(Clone)]
struct Huffman {
    n_codes: i32,
    lut: [u16; 256],
    /// lut10[next 10 bits] = value<<8 | codeLength for what Go's decodeHuffman
    /// (8-bit LUT, then canonical slow path) yields within 10 bits; 0 = not
    /// resolved within 10 bits.
    lut10: [u16; 1 << LUT_BITS],
    vals: [u8; 256],
    min_codes: [i32; MAX_CODE_LENGTH],
    max_codes: [i32; MAX_CODE_LENGTH],
    vals_indices: [i32; MAX_CODE_LENGTH],
}

impl Huffman {
    const fn new() -> Self {
        Huffman {
            n_codes: 0,
            lut: [0; 256],
            lut10: [0; 1 << LUT_BITS],
            vals: [0; 256],
            min_codes: [0; MAX_CODE_LENGTH],
            max_codes: [0; MAX_CODE_LENGTH],
            vals_indices: [0; MAX_CODE_LENGTH],
        }
    }
}

#[derive(Clone, Copy, Default)]
struct Component {
    h: usize,
    v: usize,
    c: u8,
    tq: u8,
}

/// Byte + bit layer. `a`/`n` are Go's bits.a / bits.n (bits.m is always
/// 1<<(n-1), or 0 when n == 0, so it is derived).
struct Br<'a> {
    data: &'a [u8],
    pos: usize,
    n_unreadable: usize,
    a: u64,
    n: i32,
}

#[allow(dead_code)] // the literal lazy-refill bit layer of Go, kept as reference for Sr
impl<'a> Br<'a> {
    #[inline(always)]
    fn byte_at(&self, i: usize) -> u8 {
        debug_assert!(i < self.data.len());
        unsafe { *self.data.get_unchecked(i) }
    }

    #[inline(always)]
    fn unread_byte_stuffed_byte(&mut self) {
        self.pos -= self.n_unreadable;
        self.n_unreadable = 0;
        if self.n >= 8 {
            self.a >>= 8;
            self.n -= 8;
        }
    }

    #[inline(always)]
    fn read_byte(&mut self) -> R<u8> {
        if self.pos >= self.data.len() {
            return Err(E::Bad);
        }
        let x = self.byte_at(self.pos);
        self.pos += 1;
        self.n_unreadable = 0;
        Ok(x)
    }

    #[inline(always)]
    fn read_byte_stuffed_byte(&mut self) -> R<u8> {
        let len = self.data.len();
        if self.pos + 2 <= len {
            let x = self.byte_at(self.pos);
            self.pos += 1;
            self.n_unreadable = 1;
            if x != 0xff {
                return Ok(x);
            }
            if self.byte_at(self.pos) != 0x00 {
                return Err(E::Bad); // errMissingFF00
            }
            self.pos += 1;
            self.n_unreadable = 2;
            return Ok(0xff);
        }
        self.read_byte_stuffed_byte_slow()
    }

    #[cold]
    #[inline(never)]
    fn read_byte_stuffed_byte_slow(&mut self) -> R<u8> {
        self.n_unreadable = 0;
        let x = self.read_byte()?;
        self.n_unreadable = 1;
        if x != 0xff {
            return Ok(x);
        }
        let x = self.read_byte()?;
        self.n_unreadable = 2;
        if x != 0x00 {
            return Err(E::Bad);
        }
        Ok(0xff)
    }

    /// Precondition (as in Go): self.n < n.
    #[inline(always)]
    fn ensure_n_bits(&mut self, n: i32) -> R<()> {
        loop {
            let c = self.read_byte_stuffed_byte()?;
            self.a = (self.a << 8) | c as u64;
            self.n += 8;
            if self.n >= n {
                return Ok(());
            }
        }
    }

    /// RECEIVE+EXTEND; the value is never needed, only the bit consumption.
    #[inline(always)]
    fn receive_extend(&mut self, t: u8) -> R<()> {
        let t = t as i32;
        if self.n < t {
            self.ensure_n_bits(t)?;
        }
        self.n -= t;
        Ok(())
    }

    #[inline(always)]
    fn decode_huffman(&mut self, h: &Huffman) -> R<u8> {
        if h.n_codes == 0 {
            return Err(E::Bad);
        }
        if self.n < 8 {
            if self.ensure_n_bits(8).is_err() {
                if self.n_unreadable != 0 {
                    self.unread_byte_stuffed_byte();
                }
                return self.decode_huffman_slow(h);
            }
        }
        let v = h.lut[((self.a >> (self.n - 8) as u32) & 0xff) as usize];
        if v != 0 {
            let n = (v & 0xff) as i32 - 1;
            self.n -= n;
            return Ok((v >> 8) as u8);
        }
        self.decode_huffman_slow(h)
    }

    #[inline(never)]
    fn decode_huffman_slow(&mut self, h: &Huffman) -> R<u8> {
        let mut code: i32 = 0;
        for i in 0..MAX_CODE_LENGTH {
            if self.n == 0 {
                self.ensure_n_bits(1)?;
            }
            if (self.a >> (self.n - 1) as u32) & 1 != 0 {
                code |= 1;
            }
            self.n -= 1;
            if code <= h.max_codes[i] {
                let idx = h.vals_indices[i] + code - h.min_codes[i];
                if !(0..256).contains(&idx) {
                    // Go would panic (index out of range).
                    return Err(E::Bad);
                }
                return Ok(h.vals[idx as usize]);
            }
            code <<= 1;
        }
        Err(E::Bad)
    }

    #[inline(always)]
    fn decode_bit(&mut self) -> R<bool> {
        if self.n == 0 {
            self.ensure_n_bits(1)?;
        }
        let ret = (self.a >> (self.n - 1) as u32) & 1 != 0;
        self.n -= 1;
        Ok(ret)
    }

    #[inline(always)]
    fn decode_bits(&mut self, n: i32) -> R<u32> {
        if self.n < n {
            self.ensure_n_bits(n)?;
        }
        let ret = (self.a >> (self.n - n) as u32) as u32 & ((1u32 << n) - 1);
        self.n -= n;
        Ok(ret)
    }

    #[inline(always)]
    fn undo_overshoot(&mut self) {
        if self.n_unreadable != 0 {
            if self.n >= 8 {
                self.unread_byte_stuffed_byte();
            }
            self.n_unreadable = 0;
        }
    }

    /// readFull: returns the start offset of the k bytes read.
    #[inline(always)]
    fn read_full(&mut self, k: usize) -> R<usize> {
        self.undo_overshoot();
        if self.pos + k > self.data.len() {
            self.pos = self.data.len();
            return Err(E::Bad);
        }
        let s = self.pos;
        self.pos += k;
        Ok(s)
    }

    #[inline(always)]
    fn ignore(&mut self, k: usize) -> R<()> {
        self.read_full(k).map(|_| ())
    }

    #[inline(always)]
    fn reset_bits(&mut self) {
        self.a = 0;
        self.n = 0;
    }
}

struct Decoder<'a> {
    br: Br<'a>,
    width: usize,
    height: usize,
    sos_seen: bool, // d.img1 != nil || d.img3 != nil
    max_h: usize,
    max_v: usize,
    ri: usize,
    n_comp: usize,
    baseline: bool,
    progressive: bool,
    jfif: bool,
    adobe_transform_valid: bool,
    adobe_transform: u8,
    eob_run: u16,
    comp: [Component; 4],
    prog: [Vec<u64>; 4],
    huff: [[Huffman; 4]; 2],
}

#[inline(always)]
fn be16(d: &[u8], s: usize) -> usize {
    ((d[s] as usize) << 8) + d[s + 1] as usize
}

impl<'a> Decoder<'a> {
    fn process_sof(&mut self, n: usize) -> R<()> {
        if self.n_comp != 0 {
            return Err(E::Bad);
        }
        self.n_comp = match n {
            9 => 1,
            15 => 3,
            18 => 4,
            _ => return Err(E::Bad),
        };
        let s = self.br.read_full(n)?;
        let t = &self.br.data[s..s + n];
        if t[0] != 8 {
            return Err(E::Bad);
        }
        self.height = be16(t, 1);
        self.width = be16(t, 3);
        if t[5] as usize != self.n_comp {
            return Err(E::Bad);
        }
        for i in 0..self.n_comp {
            self.comp[i].c = t[6 + 3 * i];
            for j in 0..i {
                if self.comp[i].c == self.comp[j].c {
                    return Err(E::Bad);
                }
            }
            self.comp[i].tq = t[8 + 3 * i];
            if self.comp[i].tq > MAX_TQ {
                return Err(E::Bad);
            }
            let hv = t[7 + 3 * i];
            let (mut h, mut v) = ((hv >> 4) as usize, (hv & 0x0f) as usize);
            if h < 1 || 4 < h || v < 1 || 4 < v {
                return Err(E::Bad);
            }
            if h == 3 || v == 3 {
                return Err(E::Bad);
            }
            match self.n_comp {
                1 => {
                    h = 1;
                    v = 1;
                }
                3 => {}
                4 => match i {
                    0 => {
                        if hv != 0x11 && hv != 0x22 {
                            return Err(E::Bad);
                        }
                    }
                    1 | 2 => {
                        if hv != 0x11 {
                            return Err(E::Bad);
                        }
                    }
                    _ => {
                        if self.comp[0].h != h || self.comp[0].v != v {
                            return Err(E::Bad);
                        }
                    }
                },
                _ => unreachable!(),
            }
            self.max_h = self.max_h.max(h);
            self.max_v = self.max_v.max(v);
            self.comp[i].h = h;
            self.comp[i].v = v;
        }
        if self.n_comp == 3 {
            for i in 0..3 {
                if self.max_h % self.comp[i].h != 0 || self.max_v % self.comp[i].v != 0 {
                    return Err(E::Bad);
                }
            }
        }
        Ok(())
    }

    fn process_dht(&mut self, mut n: isize) -> R<()> {
        while n > 0 {
            if n < 17 {
                return Err(E::Bad);
            }
            let s = self.br.read_full(17)?;
            let data = self.br.data;
            let t = &data[s..s + 17];
            let tc = t[0] >> 4;
            if tc > MAX_TC {
                return Err(E::Bad);
            }
            let th = t[0] & 0x0f;
            if th > MAX_TH || (self.baseline && th > 1) {
                return Err(E::Bad);
            }
            let mut ncodes = [0i32; MAX_CODE_LENGTH];
            let mut total = 0i32;
            for i in 0..MAX_CODE_LENGTH {
                ncodes[i] = t[i + 1] as i32;
                total += ncodes[i];
            }
            let h = &mut self.huff[tc as usize][th as usize];
            h.n_codes = total;
            if total == 0 {
                return Err(E::Bad);
            }
            if total > MAX_NCODES {
                return Err(E::Bad);
            }
            n -= total as isize + 17;
            if n < 0 {
                return Err(E::Bad);
            }
            let s = self.br.read_full(total as usize)?;
            h.vals[..total as usize].copy_from_slice(&data[s..s + total as usize]);

            h.lut = [0; 256];
            let (mut x, mut code) = (0usize, 0u32);
            for i in 0..8u32 {
                code <<= 1;
                for _ in 0..ncodes[i as usize] {
                    let base = (code << (7 - i)) as u8;
                    let lut_value = ((h.vals[x] as u16) << 8) | (2 + i) as u16;
                    for k in 0..(1u32 << (7 - i)) {
                        h.lut[(base | k as u8) as usize] = lut_value;
                    }
                    code = code.wrapping_add(1);
                    x += 1;
                }
            }

            let (mut c, mut index) = (0i32, 0i32);
            for i in 0..MAX_CODE_LENGTH {
                let nn = ncodes[i];
                if nn == 0 {
                    h.min_codes[i] = -1;
                    h.max_codes[i] = -1;
                    h.vals_indices[i] = -1;
                } else {
                    h.min_codes[i] = c;
                    h.max_codes[i] = c + nn - 1;
                    h.vals_indices[i] = index;
                    c += nn;
                    index += nn;
                }
                c <<= 1;
            }
            build_lut10(h);
        }
        Ok(())
    }

    fn process_dqt(&mut self, mut n: isize) -> R<()> {
        while n > 0 {
            n -= 1;
            let x = self.br.read_byte()?;
            let tq = x & 0x0f;
            if tq > MAX_TQ {
                return Err(E::Bad);
            }
            match x >> 4 {
                0 => {
                    if n < BLOCK_SIZE as isize {
                        break;
                    }
                    n -= BLOCK_SIZE as isize;
                    self.br.read_full(BLOCK_SIZE)?;
                }
                1 => {
                    if n < 2 * BLOCK_SIZE as isize {
                        break;
                    }
                    n -= 2 * BLOCK_SIZE as isize;
                    self.br.read_full(2 * BLOCK_SIZE)?;
                }
                _ => return Err(E::Bad),
            }
        }
        if n != 0 {
            return Err(E::Bad);
        }
        Ok(())
    }

    fn process_dri(&mut self, n: usize) -> R<()> {
        if n != 2 {
            return Err(E::Bad);
        }
        let s = self.br.read_full(2)?;
        self.ri = be16(self.br.data, s);
        Ok(())
    }

    fn process_app0(&mut self, mut n: usize) -> R<()> {
        if n < 5 {
            return self.br.ignore(n);
        }
        let s = self.br.read_full(5)?;
        n -= 5;
        self.jfif = &self.br.data[s..s + 5] == b"JFIF\x00";
        if n > 0 {
            return self.br.ignore(n);
        }
        Ok(())
    }

    fn process_app14(&mut self, mut n: usize) -> R<()> {
        if n < 12 {
            return self.br.ignore(n);
        }
        let s = self.br.read_full(12)?;
        n -= 12;
        let t = &self.br.data[s..s + 12];
        if &t[..5] == b"Adobe" {
            self.adobe_transform_valid = true;
            self.adobe_transform = t[11];
        }
        if n > 0 {
            return self.br.ignore(n);
        }
        Ok(())
    }

    fn process_sos(&mut self, n: usize) -> R<()> {
        if self.n_comp == 0 {
            return Err(E::Bad);
        }
        if n < 6 || 4 + 2 * self.n_comp < n || n % 2 != 0 {
            return Err(E::Bad);
        }
        let s = self.br.read_full(n)?;
        let mut t = [0u8; 16];
        t[..n].copy_from_slice(&self.br.data[s..s + n]);
        let n_comp = t[0] as usize;
        if n != 4 + 2 * n_comp {
            return Err(E::Bad);
        }
        // (compIndex, td, ta)
        let mut scan = [(0usize, 0u8, 0u8); 4];
        let mut total_hv = 0usize;
        for i in 0..n_comp {
            let cs = t[1 + 2 * i];
            let mut comp_index: isize = -1;
            for j in 0..self.n_comp {
                if cs == self.comp[j].c {
                    comp_index = j as isize;
                }
            }
            if comp_index < 0 {
                return Err(E::Bad);
            }
            scan[i].0 = comp_index as usize;
            for j in 0..i {
                if scan[i].0 == scan[j].0 {
                    return Err(E::Bad);
                }
            }
            let ci = comp_index as usize;
            total_hv += self.comp[ci].h * self.comp[ci].v;
            scan[i].1 = t[2 + 2 * i] >> 4;
            if scan[i].1 > MAX_TH || (self.baseline && scan[i].1 > 1) {
                return Err(E::Bad);
            }
            scan[i].2 = t[2 + 2 * i] & 0x0f;
            if scan[i].2 > MAX_TH || (self.baseline && scan[i].2 > 1) {
                return Err(E::Bad);
            }
        }
        if self.n_comp > 1 && total_hv > 10 {
            return Err(E::Bad);
        }

        let (mut zig_start, mut zig_end, mut ah, mut al) = (0i32, 63i32, 0u32, 0u32);
        if self.progressive {
            zig_start = t[1 + 2 * n_comp] as i32;
            zig_end = t[2 + 2 * n_comp] as i32;
            ah = (t[3 + 2 * n_comp] >> 4) as u32;
            al = (t[3 + 2 * n_comp] & 0x0f) as u32;
            if (zig_start == 0 && zig_end != 0) || zig_start > zig_end || 64 <= zig_end {
                return Err(E::Bad);
            }
            if zig_start != 0 && n_comp != 1 {
                return Err(E::Bad);
            }
            if ah != 0 && ah != al + 1 {
                return Err(E::Bad);
            }
        }
        let _ = al;

        let mxx = (self.width + 8 * self.max_h - 1) / (8 * self.max_h);
        let myy = (self.height + 8 * self.max_v - 1) / (8 * self.max_v);
        self.sos_seen = true;
        if self.progressive {
            for i in 0..n_comp {
                let ci = scan[i].0;
                if self.prog[ci].is_empty() {
                    let sz = mxx * myy * self.comp[ci].h * self.comp[ci].v;
                    if sz > MAX_PROG_BLOCKS {
                        return Err(E::TooBig);
                    }
                    self.prog[ci] = vec![0u64; sz.max(1)];
                }
            }
        }

        self.br.reset_bits();
        let data = self.br.data;
        let mut sr = Sr::new(data, self.br.pos);
        let mut cfg = ScanCfg {
            n_comp,
            comps: [(0, 0, 0, 0, 0); 4],
            mxx,
            width: self.width,
            height: self.height,
            zig_start,
            zig_end,
            ah,
            progressive: self.progressive,
        };
        for i in 0..n_comp {
            let ci = scan[i].0;
            cfg.comps[i] = (ci, self.comp[ci].h, self.comp[ci].v, scan[i].1 as usize, scan[i].2 as usize);
        }
        let total = mxx * myy;
        let mut st = (0usize, 0usize);
        let mut expected_rst = RST0;
        let mut mcu = 0usize;
        loop {
            // MCUs up to the next restart point (Go: mcu%ri == 0 && mcu < mxx*myy).
            let end = if self.ri > 0 { (mcu + self.ri).min(total) } else { total };
            run_mcus(&cfg, &self.huff, &mut self.prog, &mut sr, &mut self.eob_run, &mut st, mcu, end)?;
            mcu = end;
            if mcu >= total {
                break;
            }
            sr.materialize(&mut self.br);
            let s = self.br.read_full(2)?;
            let (t0, t1) = (self.br.data[s], self.br.data[s + 1]);
            if t0 != 0xff || t1 != expected_rst {
                self.find_rst(expected_rst, t0, t1)?;
            }
            expected_rst += 1;
            if expected_rst == RST7 + 1 {
                expected_rst = RST0;
            }
            self.br.reset_bits();
            sr = Sr::new(data, self.br.pos);
            self.eob_run = 0;
        }
        sr.materialize(&mut self.br);
        Ok(())
    }

    fn find_rst(&mut self, expected: u8, mut t0: u8, mut t1: u8) -> R<()> {
        loop {
            let mut i = 0;
            if t0 == 0xff {
                if t1 == expected {
                    return Ok(());
                } else if t1 == 0xff {
                    i = 1;
                } else if t1 != 0x00 {
                    return Err(E::Bad);
                }
            } else if t1 == 0xff {
                t0 = 0xff;
                i = 1;
            }
            let s = self.br.read_full(2 - i)?;
            if i == 0 {
                t0 = self.br.data[s];
                t1 = self.br.data[s + 1];
            } else {
                t1 = self.br.data[s];
            }
        }
    }

    fn is_rgb(&self) -> bool {
        if self.jfif {
            return false;
        }
        if self.adobe_transform_valid && self.adobe_transform == 0 {
            return true;
        }
        self.comp[0].c == b'R' && self.comp[1].c == b'G' && self.comp[2].c == b'B'
    }

    fn decode(&mut self) -> R<(u32, u32)> {
        let s = self.br.read_full(2)?;
        if self.br.data[s] != 0xff || self.br.data[s + 1] != SOI {
            return Err(E::Bad);
        }
        loop {
            let s = self.br.read_full(2)?;
            let (mut t0, mut t1) = (self.br.data[s], self.br.data[s + 1]);
            while t0 != 0xff {
                t0 = t1;
                t1 = self.br.read_byte()?;
            }
            let mut marker = t1;
            if marker == 0 {
                continue;
            }
            while marker == 0xff {
                marker = self.br.read_byte()?;
            }
            if marker == EOI {
                break;
            }
            if (RST0..=RST7).contains(&marker) {
                continue;
            }
            let s = self.br.read_full(2)?;
            let n = be16(self.br.data, s) as isize - 2;
            if n < 0 {
                return Err(E::Bad);
            }
            match marker {
                SOF0 | SOF1 | SOF2 => {
                    self.baseline = marker == SOF0;
                    self.progressive = marker == SOF2;
                    self.process_sof(n as usize)?;
                }
                DHT => self.process_dht(n)?,
                DQT => self.process_dqt(n)?,
                SOS => self.process_sos(n as usize)?,
                DRI => self.process_dri(n as usize)?,
                APP0 => self.process_app0(n as usize)?,
                APP14 => self.process_app14(n as usize)?,
                _ => {
                    if (APP0..=APP15).contains(&marker) || marker == COM {
                        self.br.ignore(n as usize)?;
                    } else {
                        return Err(E::Bad);
                    }
                }
            }
        }
        if !self.sos_seen {
            return Err(E::Bad);
        }
        match self.n_comp {
            1 => {}
            3 => {
                if self.is_rgb() {
                    let c = &self.comp;
                    let (h0, h1, h2) = (c[0].h, c[1].h, c[2].h);
                    let (v0, v1, v2) = (c[0].v, c[1].v, c[2].v);
                    if h1 != h2 || h0 % h1 != 0 || v1 != v2 || v0 % v1 != 0 {
                        return Err(E::Bad);
                    }
                }
            }
            4 => {
                if !self.adobe_transform_valid {
                    return Err(E::Bad);
                }
            }
            _ => return Err(E::Bad),
        }
        Ok((self.width as u32, self.height as u32))
    }
}

struct ScanCfg {
    n_comp: usize,
    /// (compIndex, h, v, td, ta) per scan component
    comps: [(usize, usize, usize, usize, usize); 4],
    mxx: usize,
    width: usize,
    height: usize,
    zig_start: i32,
    zig_end: i32,
    ah: u32,
    progressive: bool,
}

/// Decodes MCUs [from, to) of a scan (Go's processSOS inner loops). The
/// reader is copied into a local so its fields stay in registers; on error
/// the state is irrelevant (the decode fails).
#[inline(never)]
fn run_mcus(
    cfg: &ScanCfg,
    huff: &[[Huffman; 4]; 2],
    prog: &mut [Vec<u64>; 4],
    srp: &mut Sr,
    eob_run: &mut u16,
    st: &mut (usize, usize),
    from: usize,
    to: usize,
) -> R<()> {
    let mut sr = *srp;
    let mut eob = *eob_run;
    let (mut nb_x, mut nb_y) = *st;
    let mxx = cfg.mxx;
    let mut my = from / mxx.max(1);
    let mut mx = from - my * mxx;
    for _ in from..to {
        for i in 0..cfg.n_comp {
            let (ci, hi, vi, td, ta) = cfg.comps[i];
            let dch = &huff[0][td];
            let ach = &huff[1][ta];
            let (mut jx, mut jy) = (0usize, 0usize);
            for _j in 0..hi * vi {
                let (bx, by);
                if cfg.n_comp != 1 {
                    // bx = hi*mx + j%hi, by = vi*my + j/hi
                    bx = hi * mx + jx;
                    by = vi * my + jy;
                    jx += 1;
                    if jx == hi {
                        jx = 0;
                        jy += 1;
                    }
                } else {
                    // bx = blockCount % q, by = blockCount / q
                    bx = nb_x;
                    by = nb_y;
                    nb_x += 1;
                    if nb_x == mxx * hi {
                        nb_x = 0;
                        nb_y += 1;
                    }
                    if bx * 8 >= cfg.width || by * 8 >= cfg.height {
                        continue;
                    }
                }
                if !cfg.progressive {
                    decode_block(&mut sr, &mut eob, 0, dch, ach, 0, 63)?;
                    continue;
                }
                let bidx = by * mxx * hi + bx;
                let mut mask = prog[ci][bidx];
                if cfg.ah != 0 {
                    refine(&mut sr, &mut eob, &mut mask, ach, cfg.zig_start, cfg.zig_end)?;
                } else {
                    mask = decode_block(&mut sr, &mut eob, mask, dch, ach, cfg.zig_start, cfg.zig_end)?;
                }
                prog[ci][bidx] = mask;
            }
        }
        mx += 1;
        if mx == mxx {
            mx = 0;
            my += 1;
        }
    }
    *srp = sr;
    *eob_run = eob;
    *st = (nb_x, nb_y);
    Ok(())
}

/// One block of a sequential or first-pass progressive scan (ah == 0).
#[inline(always)]
fn decode_block(
    sr: &mut Sr,
    eob_run: &mut u16,
    mut mask: u64,
    dch: &Huffman,
    ach: &Huffman,
    zig_start: i32,
    zig_end: i32,
) -> R<u64> {
    let mut eob = *eob_run;
    let mut zig = zig_start;
    if zig == 0 {
        zig += 1;
        let value = sr.decode_huffman(dch)?;
        if value > 16 {
            return Err(E::Bad);
        }
        sr.receive_extend(value)?;
    }
    if zig <= zig_end && eob > 0 {
        eob -= 1;
    } else {
        while zig <= zig_end {
            let value = sr.decode_huffman(ach)?;
            let val0 = value >> 4;
            let val1 = value & 0x0f;
            if val1 != 0 {
                zig += val0 as i32;
                if zig > zig_end {
                    break;
                }
                sr.receive_extend(val1)?;
                mask |= 1u64 << zig;
            } else {
                if val0 != 0x0f {
                    eob = 1u16 << val0;
                    if val0 != 0 {
                        let bits = sr.decode_bits(val0 as u32)?;
                        eob |= bits as u16;
                    }
                    eob = eob.wrapping_sub(1);
                    break;
                }
                zig += 0x0f;
            }
            zig += 1;
        }
    }
    *eob_run = eob;
    Ok(mask)
}

#[inline(always)]
fn refine(br: &mut Sr, eob_run: &mut u16, mask: &mut u64, h: &Huffman, zig_start: i32, zig_end: i32) -> R<()> {
    if zig_start == 0 {
        br.decode_bit()?;
        return Ok(());
    }
    let mut zig = zig_start;
    if *eob_run == 0 {
        while zig <= zig_end {
            let mut z = false;
            let value = br.decode_huffman(h)?;
            let val0 = value >> 4;
            let val1 = value & 0x0f;
            match val1 {
                0 => {
                    if val0 != 0x0f {
                        *eob_run = 1u16 << val0;
                        if val0 != 0 {
                            let bits = br.decode_bits(val0 as u32)?;
                            *eob_run |= bits as u16;
                        }
                        break;
                    }
                }
                1 => {
                    z = true;
                    br.decode_bit()?;
                }
                _ => return Err(E::Bad),
            }
            zig = refine_non_zeroes(br, *mask, zig, zig_end, val0 as i32)?;
            if zig > zig_end {
                return Err(E::Bad);
            }
            if z {
                *mask |= 1u64 << zig;
            }
            zig += 1;
        }
    }
    if *eob_run > 0 {
        *eob_run -= 1;
        refine_non_zeroes(br, *mask, zig, zig_end, -1)?;
    }
    Ok(())
}

#[inline(always)]
fn refine_non_zeroes(br: &mut Sr, mask: u64, mut zig: i32, zig_end: i32, mut nz: i32) -> R<i32> {
    while zig <= zig_end {
        if mask & (1u64 << zig) == 0 {
            if nz == 0 {
                break;
            }
            nz -= 1;
            zig += 1;
            continue;
        }
        br.decode_bit()?;
        zig += 1;
    }
    Ok(zig)
}

/// Only reached with nb >= 24: Go's 8-bit LUT missed (or the code needs
/// more than LUT_BITS bits).
#[inline(never)]
fn canonical16(h: &Huffman, acc: u64) -> Option<(u8, u32)> {
    let v = h.lut[(acc >> 56) as usize];
    if v != 0 {
        return Some(((v >> 8) as u8, (v & 0xff) as u32 - 1));
    }
    canonical(h, (acc >> 48) as u16, 16)
}

/// Go's slow path (canonical decode from bit 1) over the top `nbits` bits of
/// `bits` (left-aligned in a u16). Returns (value, length), or None if not
/// resolved within nbits or if Go would index out of range.
#[inline(always)]
fn canonical(h: &Huffman, bits: u16, nbits: usize) -> Option<(u8, u32)> {
    let mut code: i32 = 0;
    for i in 0..nbits {
        code |= ((bits >> (15 - i)) & 1) as i32;
        if code <= h.max_codes[i] {
            let idx = h.vals_indices[i] + code - h.min_codes[i];
            if !(0..256).contains(&idx) {
                return None;
            }
            return Some((h.vals[idx as usize], i as u32 + 1));
        }
        code <<= 1;
    }
    None
}

fn build_lut10(h: &mut Huffman) {
    const SUF: usize = 1 << (LUT_BITS - 8);
    for top in 0..256usize {
        let v = h.lut[top];
        if v != 0 {
            let e = (v & 0xff00) | ((v & 0xff) - 1);
            for k in 0..SUF {
                h.lut10[top * SUF + k] = e;
            }
        } else {
            for k in 0..SUF {
                let idx = top * SUF + k;
                let bits = (idx as u16) << (16 - LUT_BITS);
                h.lut10[idx] = match canonical(h, bits, LUT_BITS as usize) {
                    Some((val, l)) => ((val as u16) << 8) | l as u16,
                    None => 0,
                };
            }
        }
    }
}

/// Eager scan bit reader that reproduces Go's lazily-read byte state.
///
/// Go reads one stuffed unit (a non-0xff byte, or 0xff 0x00) whenever its
/// bit count n is below the demand of the current operation. Here units are
/// loaded ahead into a 64-bit accumulator; `n_go` tracks what Go's n would
/// be (pure arithmetic), so Go's failures (a demanded unit lies beyond the
/// wall: a marker or EOF) and Go's byte position at the end of the scan are
/// reconstructed exactly.
#[derive(Clone, Copy)]
struct Sr<'a> {
    data: &'a [u8],
    start: usize, // raw position where this entropy-coded segment begins
    rp: usize,    // raw position after the last loaded unit
    acc: u64,     // left-aligned unread bits
    nb: u32,      // number of valid bits in acc (bits beyond are zero)
    wall: bool,   // no further unit can be loaded
    n_go: u32,    // Go's bits.n
    soft_failed: bool,
}

impl<'a> Sr<'a> {
    fn new(data: &'a [u8], pos: usize) -> Self {
        Sr { data, start: pos, rp: pos, acc: 0, nb: 0, wall: false, n_go: 0, soft_failed: false }
    }

    #[inline(always)]
    fn refill(&mut self) {
        let len = self.data.len();
        while self.nb <= 56 && !self.wall {
            if self.rp + 8 <= len {
                let w = u64::from_be_bytes(unsafe { *(self.data.as_ptr().add(self.rp) as *const [u8; 8]) });
                let x = !w;
                if (x.wrapping_sub(0x0101010101010101) & !x & 0x8080808080808080) == 0 {
                    // no 0xff in the next 8 bytes
                    let k = (64 - self.nb) / 8;
                    self.acc |= w >> self.nb;
                    self.nb += 8 * k;
                    self.rp += k as usize;
                    if self.nb < 64 {
                        self.acc &= !(u64::MAX >> self.nb);
                    }
                    continue;
                }
            }
            self.refill_byte();
        }
    }

    #[inline(always)]
    fn refill_byte(&mut self) {
        let len = self.data.len();
        if self.rp >= len {
            self.wall = true;
            return;
        }
        let b = self.data[self.rp];
        if b != 0xff {
            self.rp += 1;
        } else if self.rp + 1 < len && self.data[self.rp + 1] == 0 {
            self.rp += 2;
        } else {
            self.wall = true;
            return;
        }
        self.acc |= (b as u64) << (56 - self.nb);
        self.nb += 8;
    }

    #[inline(always)]
    fn consume(&mut self, l: u32) {
        self.acc <<= l;
        self.nb -= l;
        self.n_go -= l;
    }

    /// Go: if n < d { ensureNBits(d) } (fatal on failure).
    #[inline(always)]
    fn demand(&mut self, d: u32) -> R<()> {
        if self.n_go < d {
            self.n_go += (d - self.n_go + 7) & !7;
            if self.n_go > self.nb {
                self.refill();
                if self.n_go > self.nb {
                    return Err(E::Bad);
                }
            }
        }
        Ok(())
    }

    #[inline(always)]
    fn receive_extend(&mut self, t: u8) -> R<()> {
        self.demand(t as u32)?;
        self.consume(t as u32);
        Ok(())
    }

    #[inline(always)]
    fn decode_bit(&mut self) -> R<bool> {
        self.demand(1)?;
        let b = (self.acc >> 63) != 0;
        self.consume(1);
        Ok(b)
    }

    #[inline(always)]
    fn decode_bits(&mut self, n: u32) -> R<u32> {
        self.demand(n)?;
        let v = (self.acc >> (64 - n)) as u32;
        self.consume(n);
        Ok(v)
    }

    #[inline(always)]
    fn decode_huffman(&mut self, h: &Huffman) -> R<u8> {
        if h.n_codes == 0 {
            return Err(E::Bad);
        }
        if self.nb < 32 {
            self.refill();
            if self.nb < 24 {
                let (s, r) = Self::decode_huffman_exact_by_value(*self, h);
                *self = s;
                return r;
            }
        }
        let e = h.lut10[(self.acc >> (64 - LUT_BITS)) as usize];
        let (v, l) = if e != 0 {
            ((e >> 8) as u8, (e & 0xff) as u32)
        } else {
            match canonical16(h, self.acc) {
                Some(r) => r,
                None => return Err(E::Bad),
            }
        };
        // Go demands 8 bits for the LUT, then the slow path reads on.
        let d = if l > 8 { l } else { 8 };
        if self.n_go < d {
            self.n_go += (d - self.n_go + 7) & !7;
        }
        self.consume(l);
        Ok(v)
    }

    /// Cold path; works on a copy so the caller's reader never has its
    /// address taken (keeps the hot fields in registers).
    #[cold]
    #[inline(never)]
    fn decode_huffman_exact_by_value(mut s: Sr<'a>, h: &Huffman) -> (Sr<'a>, R<u8>) {
        let r = s.decode_huffman_exact(h);
        (s, r)
    }

    #[inline(always)]
    fn decode_huffman_exact(&mut self, h: &Huffman) -> R<u8> {
        let mut lut_ok = true;
        if self.n_go < 8 {
            if self.n_go + 8 <= self.nb {
                self.n_go += 8;
            } else {
                self.soft_failed = true;
                lut_ok = false;
            }
        }
        if lut_ok {
            let v = h.lut[(self.acc >> 56) as usize];
            if v != 0 {
                self.consume((v & 0xff) as u32 - 1);
                return Ok((v >> 8) as u8);
            }
        }
        let mut code: i32 = 0;
        for i in 0..MAX_CODE_LENGTH {
            if self.n_go == 0 {
                if 8 > self.nb {
                    return Err(E::Bad);
                }
                self.n_go = 8;
            }
            if (self.acc >> 63) != 0 {
                code |= 1;
            }
            self.consume(1);
            if code <= h.max_codes[i] {
                let idx = h.vals_indices[i] + code - h.min_codes[i];
                if !(0..256).contains(&idx) {
                    return Err(E::Bad);
                }
                return Ok(h.vals[idx as usize]);
            }
            code <<= 1;
        }
        Err(E::Bad)
    }

    #[inline(always)]
    fn unit_ending_at_is_2(&self, p: usize) -> bool {
        p >= self.start + 2 && self.data[p - 1] == 0x00 && self.data[p - 2] == 0xff
    }

    /// Write Go's byte-layer state (pos, nUnreadable, bits.n) into br.
    fn materialize(&self, br: &mut Br) {
        let ahead = (self.nb - self.n_go) / 8; // units loaded but not yet read by Go
        let mut p = self.rp;
        for _ in 0..ahead {
            p -= if self.unit_ending_at_is_2(p) { 2 } else { 1 };
        }
        br.pos = p;
        br.n = self.n_go as i32;
        br.a = 0;
        br.n_unreadable = if self.soft_failed || p == self.start {
            0
        } else if self.unit_ending_at_is_2(p) {
            2
        } else {
            1
        };
    }
}

pub fn check(data: &[u8]) -> Verdict {
    let mut d = Decoder {
        br: Br { data, pos: 0, n_unreadable: 0, a: 0, n: 0 },
        width: 0,
        height: 0,
        sos_seen: false,
        max_h: 0,
        max_v: 0,
        ri: 0,
        n_comp: 0,
        baseline: false,
        progressive: false,
        jfif: false,
        adobe_transform_valid: false,
        adobe_transform: 0,
        eob_run: 0,
        comp: [Component::default(); 4],
        prog: [Vec::new(), Vec::new(), Vec::new(), Vec::new()],
        huff: [
            [Huffman::new(), Huffman::new(), Huffman::new(), Huffman::new()],
            [Huffman::new(), Huffman::new(), Huffman::new(), Huffman::new()],
        ],
    };
    match d.decode() {
        Ok((w, h)) => Verdict::Ok(w, h),
        Err(E::Bad) => Verdict::Bad,
        Err(E::TooBig) => Verdict::TooBig,
    }
}
