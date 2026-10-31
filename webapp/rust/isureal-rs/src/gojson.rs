//! Byte-compatible subset of Go's encoding/json output (HTML escaping on, as
//! echo's c.JSON / json.NewEncoder use) plus time.Time RFC3339Nano formatting.

use chrono::{DateTime, Datelike, FixedOffset, Local, Offset, TimeZone, Timelike, Utc};

const HEX: &[u8; 16] = b"0123456789abcdef";

pub fn str(out: &mut Vec<u8>, s: &str) {
    out.push(b'"');
    let b = s.as_bytes();
    let mut start = 0;
    let mut i = 0;
    while i < b.len() {
        let c = b[i];
        if c < 0x80 {
            let esc: Option<&[u8]> = match c {
                b'"' => Some(b"\\\""),
                b'\\' => Some(b"\\\\"),
                b'\n' => Some(b"\\n"),
                b'\r' => Some(b"\\r"),
                b'\t' => Some(b"\\t"),
                0x08 => Some(b"\\b"),
                0x0c => Some(b"\\f"),
                _ => None,
            };
            if let Some(e) = esc {
                out.extend_from_slice(&b[start..i]);
                out.extend_from_slice(e);
                i += 1;
                start = i;
                continue;
            }
            if c < 0x20 || c == b'<' || c == b'>' || c == b'&' {
                out.extend_from_slice(&b[start..i]);
                out.extend_from_slice(b"\\u00");
                out.push(HEX[(c >> 4) as usize]);
                out.push(HEX[(c & 0xf) as usize]);
                i += 1;
                start = i;
                continue;
            }
            i += 1;
            continue;
        }
        // U+2028 / U+2029 (E2 80 A8 / E2 80 A9)
        if c == 0xE2 && i + 2 < b.len() && b[i + 1] == 0x80 && (b[i + 2] == 0xA8 || b[i + 2] == 0xA9) {
            out.extend_from_slice(&b[start..i]);
            out.extend_from_slice(if b[i + 2] == 0xA8 { b"\\u2028" } else { b"\\u2029" });
            i += 3;
            start = i;
            continue;
        }
        i += 1;
    }
    out.extend_from_slice(&b[start..]);
    out.push(b'"');
}

pub fn opt_str(out: &mut Vec<u8>, s: Option<&str>) {
    match s {
        Some(s) => str(out, s),
        None => out.extend_from_slice(b"null"),
    }
}

pub fn bool(out: &mut Vec<u8>, v: bool) {
    out.extend_from_slice(if v { b"true" } else { b"false" });
}

pub fn int(out: &mut Vec<u8>, v: i64) {
    let mut buf = itoa_buf(v);
    out.append(&mut buf);
}

fn itoa_buf(v: i64) -> Vec<u8> {
    v.to_string().into_bytes()
}

/// Go encoding/json float64 encoding.
pub fn float(out: &mut Vec<u8>, f: f64) {
    let abs = f.abs();
    if abs != 0.0 && (abs < 1e-6 || abs >= 1e21) {
        let s = format!("{:e}", f);
        // Rust: "1.5e-7" / "1e21"; Go (after its e-0X cleanup): "1.5e-7" / "1e+21"
        if let Some(p) = s.find('e') {
            let (m, e) = s.split_at(p);
            let e = &e[1..];
            out.extend_from_slice(m.as_bytes());
            out.push(b'e');
            if !e.starts_with('-') {
                out.push(b'+');
            }
            out.extend_from_slice(e.as_bytes());
        } else {
            out.extend_from_slice(s.as_bytes());
        }
    } else {
        out.extend_from_slice(format!("{}", f).as_bytes());
    }
}

fn two(out: &mut Vec<u8>, v: u32) {
    out.push(b'0' + (v / 10) as u8);
    out.push(b'0' + (v % 10) as u8);
}

/// time.Time.MarshalJSON (RFC3339Nano) in the process' local zone.
pub fn time(out: &mut Vec<u8>, t: &DateTime<Utc>) {
    out.push(b'"');
    time_raw(out, t);
    out.push(b'"');
}

pub fn time_raw(out: &mut Vec<u8>, t: &DateTime<Utc>) {
    let off: FixedOffset = Local.offset_from_utc_datetime(&t.naive_utc()).fix();
    let lt = t.with_timezone(&off);
    let y = lt.year();
    let ys = format!("{:04}", y);
    out.extend_from_slice(ys.as_bytes());
    out.push(b'-');
    two(out, lt.month());
    out.push(b'-');
    two(out, lt.day());
    out.push(b'T');
    two(out, lt.hour());
    out.push(b':');
    two(out, lt.minute());
    out.push(b':');
    two(out, lt.second());
    let ns = lt.nanosecond() % 1_000_000_000;
    if ns != 0 {
        let mut digits = format!("{:09}", ns).into_bytes();
        while digits.last() == Some(&b'0') {
            digits.pop();
        }
        out.push(b'.');
        out.extend_from_slice(&digits);
    }
    let secs = off.local_minus_utc();
    if secs == 0 {
        out.push(b'Z');
    } else {
        let (sign, s) = if secs < 0 { (b'-', -secs) } else { (b'+', secs) };
        out.push(sign);
        two(out, (s / 3600) as u32);
        out.push(b':');
        two(out, ((s / 60) % 60) as u32);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn esc() {
        let mut o = Vec::new();
        str(&mut o, "a<b>&\"\\\n\u{1}\u{2028}x");
        assert_eq!(String::from_utf8(o).unwrap(), "\"a\\u003cb\\u003e\\u0026\\\"\\\\\\n\\u0001\\u2028x\"");
        let mut o = Vec::new();
        float(&mut o, -18.099983853560772);
        assert_eq!(String::from_utf8(o).unwrap(), "-18.099983853560772");
        let mut o = Vec::new();
        float(&mut o, 1e-7);
        assert_eq!(String::from_utf8(o).unwrap(), "1e-7");
        let mut o = Vec::new();
        float(&mut o, 35.0);
        assert_eq!(String::from_utf8(o).unwrap(), "35");
    }
}
