//! jpegcheckd: HTTP/1.1 over a unix socket, compatible with the Go app's
//! POST /internal/jpegcheck (body = image, response "1 w h" / "0 0 0").
//! A 5xx response means "fall back to the in-process Go check".
//!
//! usage:
//!   jpegcheckd --sock /run/isureal/jpegcheck.sock   (prefix '@' = abstract socket)
//!   jpegcheckd --bench FILE [ITERS]
//!   jpegcheckd --check FILE...

mod check;

use std::io::{Read, Write};
use std::os::unix::net::{UnixListener, UnixStream};
use std::time::Instant;

const MAX_BODY: usize = 64 << 20;
const MAX_HEADER: usize = 16 << 10;

fn main() {
    let args: Vec<String> = std::env::args().collect();
    match args.get(1).map(|s| s.as_str()) {
        Some("--bench") => bench(&args[2], args.get(3).and_then(|s| s.parse().ok()).unwrap_or(2000)),
        Some("--check") => {
            for f in &args[2..] {
                let data = std::fs::read(f).expect("read");
                println!("{f}: {:?}", check::check(&data));
            }
        }
        Some("--sock") => serve(&args[2]),
        _ => {
            eprintln!("usage: jpegcheckd --sock PATH | --bench FILE [N] | --check FILE...");
            std::process::exit(2);
        }
    }
}

fn bench(path: &str, iters: usize) {
    let data = std::fs::read(path).expect("read");
    let mut r = check::Verdict::Bad;
    for _ in 0..iters / 10 + 1 {
        r = std::hint::black_box(check::check(std::hint::black_box(&data)));
    }
    let t = Instant::now();
    for _ in 0..iters {
        r = std::hint::black_box(check::check(std::hint::black_box(&data)));
    }
    let ns = t.elapsed().as_nanos() as f64 / iters as f64;
    println!("{path}: {r:?} {ns:.0} ns/op ({} bytes)", data.len());
}

fn bind(path: &str) -> UnixListener {
    if let Some(name) = path.strip_prefix('@') {
        use std::os::linux::net::SocketAddrExt;
        let addr = std::os::unix::net::SocketAddr::from_abstract_name(name.as_bytes()).expect("abstract addr");
        return UnixListener::bind_addr(&addr).expect("bind");
    }
    let _ = std::fs::remove_file(path);
    let l = UnixListener::bind(path).expect("bind");
    use std::os::unix::fs::PermissionsExt;
    let _ = std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o660));
    l
}

fn serve(path: &str) {
    let l = bind(path);
    eprintln!("jpegcheckd listening on {path}");
    for s in l.incoming() {
        match s {
            Ok(s) => {
                let _ = std::thread::Builder::new().stack_size(256 << 10).spawn(move || conn(s));
            }
            Err(e) => {
                eprintln!("accept: {e}");
                std::thread::sleep(std::time::Duration::from_millis(10));
            }
        }
    }
}

fn find_crlf2(b: &[u8]) -> Option<usize> {
    b.windows(4).position(|w| w == b"\r\n\r\n")
}

fn respond(s: &mut UnixStream, status: &str, body: &str, close: bool) -> std::io::Result<()> {
    let hdr = format!(
        "HTTP/1.1 {status}\r\nContent-Type: text/plain; charset=UTF-8\r\nContent-Length: {}\r\n{}\r\n{body}",
        body.len(),
        if close { "Connection: close\r\n" } else { "" }
    );
    s.write_all(hdr.as_bytes())
}

fn conn(mut s: UnixStream) {
    let mut buf: Vec<u8> = Vec::with_capacity(64 << 10);
    let mut chunk = vec![0u8; 64 << 10];
    loop {
        // Read the header.
        let hend = loop {
            if let Some(p) = find_crlf2(&buf) {
                break p;
            }
            if buf.len() > MAX_HEADER {
                let _ = respond(&mut s, "431 Request Header Fields Too Large", "", true);
                return;
            }
            match s.read(&mut chunk) {
                Ok(0) | Err(_) => return,
                Ok(n) => buf.extend_from_slice(&chunk[..n]),
            }
        };
        let head = match std::str::from_utf8(&buf[..hend]) {
            Ok(h) => h.to_owned(),
            Err(_) => {
                let _ = respond(&mut s, "400 Bad Request", "", true);
                return;
            }
        };
        let mut lines = head.split("\r\n");
        let reqline = lines.next().unwrap_or("");
        let mut parts = reqline.split(' ');
        let method = parts.next().unwrap_or("");
        let target = parts.next().unwrap_or("");
        let mut clen: Option<usize> = None;
        let mut close = reqline.ends_with("HTTP/1.0");
        let mut chunked = false;
        for l in lines {
            let Some((k, v)) = l.split_once(':') else { continue };
            let v = v.trim();
            if k.eq_ignore_ascii_case("content-length") {
                clen = v.parse().ok();
            } else if k.eq_ignore_ascii_case("connection") {
                if v.eq_ignore_ascii_case("close") {
                    close = true;
                }
            } else if k.eq_ignore_ascii_case("transfer-encoding") {
                chunked = true;
            }
        }
        if chunked {
            let _ = respond(&mut s, "501 Not Implemented", "", true);
            return;
        }
        let clen = clen.unwrap_or(0);
        if clen > MAX_BODY {
            let _ = respond(&mut s, "413 Payload Too Large", "", true);
            return;
        }
        let bstart = hend + 4;
        while buf.len() < bstart + clen {
            let need = (bstart + clen - buf.len()).min(chunk.len());
            match s.read(&mut chunk[..need.max(1)]) {
                Ok(0) | Err(_) => return,
                Ok(n) => buf.extend_from_slice(&chunk[..n]),
            }
        }
        let path = target.split('?').next().unwrap_or("");
        let r = if method == "POST" && path == "/internal/jpegcheck" {
            let body = &buf[bstart..bstart + clen];
            match std::panic::catch_unwind(|| check::check(body)) {
                Ok(check::Verdict::Ok(w, h)) => respond(&mut s, "200 OK", &format!("1 {w} {h}"), close),
                Ok(check::Verdict::Bad) => respond(&mut s, "200 OK", "0 0 0", close),
                Ok(check::Verdict::TooBig) => respond(&mut s, "503 Service Unavailable", "", close),
                Err(_) => respond(&mut s, "500 Internal Server Error", "", close),
            }
        } else {
            respond(&mut s, "404 Not Found", "", close)
        };
        if r.is_err() || close {
            return;
        }
        buf.drain(..bstart + clen);
    }
}
