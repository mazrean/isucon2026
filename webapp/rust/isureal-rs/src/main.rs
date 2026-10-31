//! isureal-rs: Rust implementation of the DB-backed read endpoints
//! (GET /api/memories, /api/memories/:date, /api/me/export). Everything it
//! does not handle byte-identically (other paths, other methods, unusual
//! query encodings, request bodies) is proxied to the Go server unchanged.

mod api;
mod gojson;
mod gozip;
mod images;
mod sha_x8;
mod sig;

use bytes::Bytes;
use http_body_util::{combinators::BoxBody, BodyExt, Full};
use hyper::body::Incoming;
use hyper::{Request, Response};
use hyper_util::client::legacy::Client;
use hyper_util::rt::{TokioExecutor, TokioIo};
use parking_lot::RwLock;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::TcpListener;

pub type Body = BoxBody<Bytes, hyper::Error>;

pub struct Config {
    pub region: String,
    pub upstream: String,
    pub fwd_secret: String,
    pub log_min_rank: u8,
}

pub struct App {
    pub cfg: Config,
    pub pool: deadpool_postgres::Pool,
    pub images: images::ImageStore,
    pub bans: RwLock<Option<HashMap<String, chrono::DateTime<chrono::Utc>>>>,
    pub client: Client<hyper_util::client::legacy::connect::HttpConnector, Incoming>,
    pub export_cache: api::ExportCache,
}

fn env_or(k: &str, d: &str) -> String {
    match std::env::var(k) {
        Ok(v) if !v.is_empty() => v,
        _ => d.to_string(),
    }
}

pub fn full(b: impl Into<Bytes>) -> Body {
    Full::new(b.into()).map_err(|never| match never {}).boxed()
}

async fn ban_loop(app: Arc<App>) {
    loop {
        if let Ok(client) = app.pool.get().await {
            if let Ok(rows) = client
                .query("SELECT ip, banned_until FROM ip_bans WHERE banned_until > clock_timestamp()", &[])
                .await
            {
                let mut m = HashMap::with_capacity(rows.len());
                for r in rows {
                    m.insert(r.get::<_, String>(0), r.get::<_, chrono::DateTime<chrono::Utc>>(1));
                }
                *app.bans.write() = Some(m);
            }
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

fn main() {
    let threads: usize = env_or("ISUREAL_RS_THREADS", "2").parse().unwrap_or(2);
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(threads)
        .max_blocking_threads(4)
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(run());
}

async fn run() {
    let cfg = Config {
        region: env_or("ISUREAL_REGION", "asia"),
        upstream: env_or("ISUREAL_RS_UPSTREAM", "127.0.0.1:8080"),
        fwd_secret: std::env::var("ISUREAL_FWD_SECRET").unwrap_or_default(),
        log_min_rank: match env_or("ISUREAL_LOG_LEVEL", "debug").as_str() {
            "info" => 1,
            "error" => 2,
            _ => 0,
        },
    };
    let mut pg = tokio_postgres::Config::new();
    pg.host(&env_or("ISUREAL_DB_HOST", "127.0.0.1"))
        .port(env_or("ISUREAL_DB_PORT", "5432").parse().unwrap_or(5432))
        .user(&env_or("ISUREAL_DB_USER", "isureal"))
        .password(env_or("ISUREAL_DB_PASSWORD", "isureal"))
        .dbname(&env_or("ISUREAL_DB_NAME", "isureal"));
    let mgr = deadpool_postgres::Manager::from_config(
        pg,
        tokio_postgres::NoTls,
        deadpool_postgres::ManagerConfig { recycling_method: deadpool_postgres::RecyclingMethod::Fast },
    );
    let max: usize = env_or("ISUREAL_RS_DB_MAX", "12").parse().unwrap_or(12);
    let pool = deadpool_postgres::Pool::builder(mgr).max_size(max).build().unwrap();
    let mut http = hyper_util::client::legacy::connect::HttpConnector::new();
    http.set_nodelay(true);
    let client = Client::builder(TokioExecutor::new())
        .pool_idle_timeout(Duration::from_secs(90))
        .pool_max_idle_per_host(256)
        .build(http);
    let app = Arc::new(App {
        cfg,
        pool,
        images: images::ImageStore::new(
            env_or("ISUREAL_IMAGE_CACHE_DIR", "/home/isucon/imgcache"),
            env_or("ISUREAL_RS_IMAGE_MEM_MB", "64").parse::<usize>().unwrap_or(64) << 20,
        ),
        bans: RwLock::new(None),
        client,
        export_cache: api::ExportCache::new(env_or("ISUREAL_RS_EXPORT_CACHE_MB", "256").parse::<usize>().unwrap_or(256) << 20),
    });
    tokio::spawn(ban_loop(app.clone()));

    let listen: SocketAddr = env_or("ISUREAL_RS_LISTEN", "127.0.0.1:8081").parse().expect("listen addr");
    let sock = socket2_listen(listen);
    let listener = TcpListener::from_std(sock).unwrap();
    eprintln!("isureal-rs listening on {}", listen);
    loop {
        let (stream, peer) = match listener.accept().await {
            Ok(x) => x,
            Err(_) => continue,
        };
        let _ = stream.set_nodelay(true);
        let app = app.clone();
        tokio::spawn(async move {
            let svc = hyper::service::service_fn(move |req| {
                let app = app.clone();
                async move { Ok::<_, hyper::Error>(api::handle(app, req, peer).await) }
            });
            let _ = hyper::server::conn::http1::Builder::new()
                .keep_alive(true)
                .serve_connection(TokioIo::new(stream), svc)
                .await;
        });
    }
}

fn socket2_listen(addr: SocketAddr) -> std::net::TcpListener {
    let l = std::net::TcpListener::bind(addr).expect("bind");
    l.set_nonblocking(true).unwrap();
    l
}

pub async fn proxy(app: &App, req: Request<Incoming>) -> Response<Body> {
    let (mut parts, body) = req.into_parts();
    let pq = parts.uri.path_and_query().map(|p| p.as_str().to_string()).unwrap_or_else(|| "/".into());
    let uri: hyper::Uri = match format!("http://{}{}", app.cfg.upstream, pq).parse() {
        Ok(u) => u,
        Err(_) => return Response::builder().status(502).body(full("")).unwrap(),
    };
    parts.uri = uri;
    let req = Request::from_parts(parts, body);
    match app.client.request(req).await {
        Ok(resp) => resp.map(|b| b.boxed()),
        Err(_) => Response::builder().status(502).body(full("")).unwrap(),
    }
}

/// raw deflate of the export manifest, byte-identical to Go 1.27 archive/zip (flate level 5).
pub fn deflate(input: &[u8]) -> Vec<u8> {
    goflate::deflate_like_go_zip(input)
}
