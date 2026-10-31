//! archive/zip.Writer output (Go 1.27) for: N entries via CreateHeader(Store)
//! followed by one Create (Deflate) entry. Always data descriptors (flag 0x8),
//! version 20, zero DOS time, no extra, external attrs 0.

struct Dir {
    name: String,
    method: u16,
    flags: u16,
    crc: u32,
    csize: u32,
    usize: u32,
    offset: u32,
}

pub struct ZipWriter {
    pub buf: Vec<u8>,
    dir: Vec<Dir>,
}

fn u16le(b: &mut Vec<u8>, v: u16) {
    b.extend_from_slice(&v.to_le_bytes());
}
fn u32le(b: &mut Vec<u8>, v: u32) {
    b.extend_from_slice(&v.to_le_bytes());
}

fn needs_utf8_flag(name: &str) -> bool {
    // detectUTF8: require if any rune >= 0x80 or one of 0x00-0x1f? Go: r < 0x20 || r > 0x7d || r == 0x5c => require
    name.chars().any(|r| (r as u32) < 0x20 || (r as u32) > 0x7d || r == '\\')
}

impl ZipWriter {
    pub fn with_capacity(n: usize) -> Self {
        ZipWriter { buf: Vec::with_capacity(n), dir: Vec::new() }
    }

    fn entry(&mut self, name: &str, method: u16, raw: &[u8], data: &[u8]) {
        let mut flags = 0x8u16;
        if needs_utf8_flag(name) {
            flags |= 0x800;
        }
        let offset = self.buf.len() as u32;
        let b = &mut self.buf;
        u32le(b, 0x04034b50);
        u16le(b, 20);
        u16le(b, flags);
        u16le(b, method);
        u16le(b, 0);
        u16le(b, 0);
        u32le(b, 0);
        u32le(b, 0);
        u32le(b, 0);
        u16le(b, name.len() as u16);
        u16le(b, 0);
        b.extend_from_slice(name.as_bytes());
        b.extend_from_slice(data);
        let crc = crc32fast::hash(raw);
        u32le(b, 0x08074b50);
        u32le(b, crc);
        u32le(b, data.len() as u32);
        u32le(b, raw.len() as u32);
        self.dir.push(Dir { name: name.to_string(), method, flags, crc, csize: data.len() as u32, usize: raw.len() as u32, offset });
    }

    pub fn store(&mut self, name: &str, data: &[u8]) {
        self.entry(name, 0, data, data);
    }

    pub fn deflated(&mut self, name: &str, raw: &[u8], compressed: &[u8]) {
        self.entry(name, 8, raw, compressed);
    }

    pub fn finish(mut self) -> Vec<u8> {
        let start = self.buf.len() as u32;
        let b = &mut self.buf;
        for d in &self.dir {
            u32le(b, 0x02014b50);
            u16le(b, 20);
            u16le(b, 20);
            u16le(b, d.flags);
            u16le(b, d.method);
            u16le(b, 0);
            u16le(b, 0);
            u32le(b, d.crc);
            u32le(b, d.csize);
            u32le(b, d.usize);
            u16le(b, d.name.len() as u16);
            u16le(b, 0);
            u16le(b, 0);
            u16le(b, 0);
            u16le(b, 0);
            u32le(b, 0);
            u32le(b, d.offset);
            b.extend_from_slice(d.name.as_bytes());
        }
        let end = b.len() as u32;
        let records = self.dir.len().min(0xffff) as u16;
        u32le(b, 0x06054b50);
        u16le(b, 0);
        u16le(b, 0);
        u16le(b, records);
        u16le(b, records);
        u32le(b, end - start);
        u32le(b, start);
        u16le(b, 0);
        self.buf
    }
}
