//! Test/bench helper.
//!   goflate-cli                : stdin -> stdout raw deflate
//!   goflate-cli --batch IN OUT : each file in IN -> OUT/<name>.rs
//!   goflate-cli --bench IN     : compress all files in IN repeatedly, print throughput
use std::io::{Read, Write};
use std::time::Instant;

fn main() {
    let args: Vec<String> = std::env::args().collect();
    if args.len() >= 4 && args[1] == "--batch" {
        let mut ents: Vec<_> = std::fs::read_dir(&args[2]).unwrap().map(|e| e.unwrap().path()).collect();
        ents.sort();
        for p in ents {
            let data = std::fs::read(&p).unwrap();
            let out = goflate::deflate_like_go_zip(&data);
            let name = p.file_name().unwrap().to_string_lossy().to_string();
            std::fs::write(std::path::Path::new(&args[3]).join(format!("{name}.rs")), out).unwrap();
        }
        return;
    }
    if args.len() >= 3 && args[1] == "--bench" {
        let mut inputs = Vec::new();
        for e in std::fs::read_dir(&args[2]).unwrap() {
            inputs.push(std::fs::read(e.unwrap().path()).unwrap());
        }
        let total: usize = inputs.iter().map(|v| v.len()).sum();
        let iters = 5;
        let t0 = Instant::now();
        let mut outb = 0usize;
        for _ in 0..iters {
            for d in &inputs {
                outb += goflate::deflate_like_go_zip(d).len();
            }
        }
        let el = t0.elapsed().as_secs_f64();
        println!(
            "files={} bytes/iter={} iters={} time={:.3}s throughput={:.1} MB/s (out {} bytes)",
            inputs.len(),
            total,
            iters,
            el,
            (total * iters) as f64 / el / 1e6,
            outb / iters
        );
        return;
    }
    let mut data = Vec::new();
    std::io::stdin().read_to_end(&mut data).unwrap();
    std::io::stdout().write_all(&goflate::deflate_like_go_zip(&data)).unwrap();
}
