use crate::gojson as j;
use crate::sig::{decode_key, Key, UrlBatch};
use crate::{full, proxy, App, Body};
use bytes::Bytes;
use chrono::{DateTime, Datelike, NaiveDate, Utc};
use hyper::body::Incoming;
use hyper::{Request, Response};
use parking_lot::Mutex;
use sha2::{Digest, Sha256};
use std::collections::{HashMap, VecDeque};
use std::net::SocketAddr;
use std::sync::Arc;

pub enum Fail {
    Api(u16, &'static str),
    Internal(String),
    Proxy,
}

impl From<tokio_postgres::Error> for Fail {
    fn from(e: tokio_postgres::Error) -> Self {
        Fail::Internal(e.to_string())
    }
}
impl From<deadpool_postgres::PoolError> for Fail {
    fn from(e: deadpool_postgres::PoolError) -> Self {
        Fail::Internal(e.to_string())
    }
}

#[derive(Clone, Copy, PartialEq)]
enum Route {
    Memories,
    MemoriesByDate,
    Export,
}

impl Route {
    fn action(self) -> &'static str {
        match self {
            Route::Memories => "get_memories",
            Route::MemoriesByDate => "get_memories_by_date",
            Route::Export => "export_posts",
        }
    }
}

fn json_response(status: u16, body: Vec<u8>) -> Response<Body> {
    Response::builder()
        .status(status)
        .header("Content-Type", "application/json")
        .body(full(body))
        .unwrap()
}

fn error_response(status: u16, msg: &str) -> Response<Body> {
    let mut b = Vec::with_capacity(32);
    b.extend_from_slice(b"{\"error\":");
    j::str(&mut b, msg);
    b.extend_from_slice(b"}\n");
    json_response(status, b)
}

fn client_ip(req: &Request<Incoming>, peer: &SocketAddr) -> String {
    if let Some(v) = req.headers().get("x-client-ip") {
        if let Ok(s) = v.to_str() {
            if !s.is_empty() {
                return s.to_string();
            }
        }
    }
    peer.ip().to_string()
}

fn action_log(app: &App, route: Route, status: u16, ip: &str, user: Option<&str>) {
    // only 5xx lines are emitted for these routes (see guardRelevant in action_log.go)
    if status < 500 || app.cfg.log_min_rank > 2 {
        return;
    }
    let mut b = Vec::with_capacity(128);
    b.extend_from_slice(b"{\"action\":");
    j::str(&mut b, route.action());
    b.extend_from_slice(b",\"ip\":");
    j::str(&mut b, ip);
    b.extend_from_slice(b",\"level\":\"error\",\"status\":");
    j::int(&mut b, status as i64);
    b.extend_from_slice(b",\"user_id\":");
    j::opt_str(&mut b, user);
    b.extend_from_slice(b"}\n");
    use std::io::Write;
    let _ = std::io::stdout().write_all(&b);
}

fn route_of(path: &str) -> Option<(Route, &str)> {
    match path {
        "/api/memories" => Some((Route::Memories, "")),
        "/api/me/export" => Some((Route::Export, "")),
        _ => {
            let rest = path.strip_prefix("/api/memories/")?;
            if rest.is_empty() || !rest.bytes().all(|c| c.is_ascii_alphanumeric() || c == b'-') {
                return None;
            }
            Some((Route::MemoriesByDate, rest))
        }
    }
}

pub async fn handle(app: Arc<App>, req: Request<Incoming>, peer: SocketAddr) -> Response<Body> {
    let path = req.uri().path().to_string();
    let Some((route, param)) = route_of(&path) else {
        return proxy(&app, req).await;
    };
    let param = param.to_string();
    if req.method() != hyper::Method::GET
        || req.headers().contains_key("transfer-encoding")
        || req.headers().get("content-length").map_or(false, |v| v.as_bytes() != b"0")
    {
        return proxy(&app, req).await;
    }
    let query = match parse_query(req.uri().query().unwrap_or("")) {
        Some(q) => q,
        None => return proxy(&app, req).await,
    };
    let ip = client_ip(&req, &peer);
    let mut user: Option<String> = None;
    let res = serve(&app, &req, &peer, route, &param, &query, &ip, &mut user).await;
    match res {
        Ok(r) => r,
        Err(Fail::Api(s, code)) => error_response(s, code),
        Err(Fail::Proxy) => proxy(&app, req).await,
        Err(Fail::Internal(msg)) => {
            action_log(&app, route, 500, &ip, user.as_deref());
            eprintln!("request failed: {}", msg);
            error_response(500, &msg)
        }
    }
}

pub struct Session {
    pub user_id: String,
    pub region: String,
}

fn request_region(app: &App, req: &Request<Incoming>, peer: &SocketAddr) -> String {
    let h = req.headers();
    let region = h.get("x-isureal-fwd-region").and_then(|v| v.to_str().ok()).unwrap_or("");
    let secret = h.get("x-isureal-fwd-secret").map(|v| v.as_bytes()).unwrap_or(b"");
    if region.is_empty()
        || !matches!(region, "america" | "europe" | "asia")
        || app.cfg.fwd_secret.is_empty()
        || secret != app.cfg.fwd_secret.as_bytes()
    {
        return app.cfg.region.clone();
    }
    let ok = match peer.ip() {
        std::net::IpAddr::V4(v4) => matches!(v4.octets(), [192, 168, 1, 10] | [192, 168, 2, 10] | [192, 168, 3, 10]),
        std::net::IpAddr::V6(v6) => v6
            .to_ipv4_mapped()
            .map_or(false, |v4| matches!(v4.octets(), [192, 168, 1, 10] | [192, 168, 2, 10] | [192, 168, 3, 10])),
    };
    if ok {
        region.to_string()
    } else {
        app.cfg.region.clone()
    }
}

async fn session(app: &App, req: &Request<Incoming>) -> Result<Session, Fail> {
    let auth = match req.headers().get("authorization") {
        None => return Err(Fail::Api(401, "invalid_session")),
        Some(v) => v.as_bytes(),
    };
    if auth.is_empty() {
        return Err(Fail::Api(401, "invalid_session"));
    }
    let Some(token) = auth.strip_prefix(b"Bearer ") else {
        return Err(Fail::Api(401, "invalid_session"));
    };
    let hash = hex::encode(Sha256::digest(token));
    let client = app.pool.get().await?;
    let st = client
        .prepare_cached(
            "SELECT s.user_id, u.region FROM login_sessions s JOIN users u ON u.id = s.user_id \
             WHERE s.token_hash = $1 AND s.active = true AND u.status = 'active'",
        )
        .await?;
    let row = client.query_opt(&st, &[&hash]).await?;
    match row {
        Some(r) => Ok(Session { user_id: r.get(0), region: r.get(1) }),
        None => Err(Fail::Api(401, "invalid_session")),
    }
}

// ---- query parsing (url.ParseQuery subset; anything unusual → proxy) ----

pub struct Query {
    pairs: Vec<(String, String)>,
}

impl Query {
    fn first(&self, k: &str) -> Option<&str> {
        self.pairs.iter().find(|(a, _)| a == k).map(|(_, v)| v.as_str())
    }
}

fn unescape(s: &str) -> Option<String> {
    let b = s.as_bytes();
    let mut out = Vec::with_capacity(b.len());
    let mut i = 0;
    while i < b.len() {
        match b[i] {
            b'%' => {
                let h = std::str::from_utf8(b.get(i + 1..i + 3)?).ok()?;
                out.push(u8::from_str_radix(h, 16).ok()?);
                i += 3;
            }
            b'+' => {
                out.push(b' ');
                i += 1;
            }
            c => {
                out.push(c);
                i += 1;
            }
        }
    }
    String::from_utf8(out).ok()
}

fn parse_query(raw: &str) -> Option<Query> {
    let mut pairs = Vec::new();
    if !raw.bytes().all(|c| c.is_ascii_alphanumeric() || b"-._~:=&%+".contains(&c)) {
        return None;
    }
    for part in raw.split('&') {
        if part.is_empty() {
            continue;
        }
        let (k, v) = part.split_once('=').unwrap_or((part, ""));
        pairs.push((unescape(k)?, unescape(v)?));
    }
    Some(Query { pairs })
}

fn digits(s: &[u8]) -> Option<u32> {
    if s.is_empty() || !s.iter().all(|c| c.is_ascii_digit()) {
        return None;
    }
    std::str::from_utf8(s).ok()?.parse().ok()
}

/// strict RFC3339 as time.Time.UnmarshalText accepts it; None = not sure → proxy.
fn parse_rfc3339(s: &str) -> Option<DateTime<Utc>> {
    let b = s.as_bytes();
    if b.len() < 20 || b[4] != b'-' || b[7] != b'-' || b[10] != b'T' || b[13] != b':' || b[16] != b':' {
        return None;
    }
    let y = digits(&b[0..4])? as i32;
    let mo = digits(&b[5..7])?;
    let d = digits(&b[8..10])?;
    let h = digits(&b[11..13])?;
    let mi = digits(&b[14..16])?;
    let se = digits(&b[17..19])?;
    let mut i = 19;
    let mut nanos: u32 = 0;
    if b[i] == b'.' {
        let st = i + 1;
        let mut e = st;
        while e < b.len() && b[e].is_ascii_digit() {
            e += 1;
        }
        let n = e - st;
        if n == 0 || n > 9 {
            return None;
        }
        nanos = digits(&b[st..e])? * 10u32.pow((9 - n) as u32);
        i = e;
    }
    let off: i32 = match b.get(i)? {
        b'Z' if i + 1 == b.len() => 0,
        c @ (b'+' | b'-') if i + 6 == b.len() && b[i + 3] == b':' => {
            let oh = digits(&b[i + 1..i + 3])?;
            let om = digits(&b[i + 4..i + 6])?;
            if oh > 23 || om > 59 {
                return None;
            }
            let v = (oh * 3600 + om * 60) as i32;
            if *c == b'-' {
                -v
            } else {
                v
            }
        }
        _ => return None,
    };
    if h > 23 || mi > 59 || se > 59 {
        return None;
    }
    let date = NaiveDate::from_ymd_opt(y, mo, d)?;
    let ndt = date.and_hms_nano_opt(h, mi, se, nanos)?;
    let utc = ndt - chrono::Duration::seconds(off as i64);
    Some(DateTime::<Utc>::from_naive_utc_and_offset(utc, Utc))
}

fn parse_period(s: &str, with_day: bool) -> Result<NaiveDate, Fail> {
    let b = s.as_bytes();
    if b.first().map_or(false, |&c| c == b'+' || c == b'-') {
        return Err(Fail::Proxy);
    }
    let bad = Fail::Api(400, "invalid_memory_period");
    let want = if with_day { 10 } else { 7 };
    if b.len() != want || b[4] != b'-' || (with_day && b[7] != b'-') {
        // Go's year/month parsers accept a few odd forms (signs, short fields); be exact only on the common shape.
        if b.iter().all(|c| c.is_ascii_digit() || *c == b'-') && b.len() >= 4 && b.len() < want {
            return Err(Fail::Proxy);
        }
        return Err(bad);
    }
    let (Some(y), Some(m)) = (digits(&b[0..4]), digits(&b[5..7])) else { return Err(bad) };
    let d = if with_day {
        match digits(&b[8..10]) {
            Some(d) => d,
            None => return Err(bad),
        }
    } else {
        1
    };
    NaiveDate::from_ymd_opt(y as i32, m, d).ok_or(bad)
}

// ---- rows ----

struct UserRow {
    id: String,
    username: String,
    display_name: String,
    bio: String,
    profile_image_id: Option<String>,
    account_type: String,
    key: Key,
}

const USER_COLS: &str = "u.id, u.username, u.secret_key_hash, u.display_name, u.bio, u.profile_image_id, u.account_type";

fn user_from(r: &tokio_postgres::Row, o: usize) -> Result<UserRow, Fail> {
    let hash: String = r.get(o + 2);
    let key = decode_key(&hash).ok_or_else(|| Fail::Internal("invalid image signing key".into()))?;
    Ok(UserRow {
        id: r.get(o),
        username: r.get(o + 1),
        display_name: r.get(o + 3),
        bio: r.get(o + 4),
        profile_image_id: r.get(o + 5),
        account_type: r.get(o + 6),
        key,
    })
}

struct PostRow {
    id: String,
    posting_window_id: String,
    image_id: String,
    caption: String,
    visibility: String,
    kind: String,
    lat: Option<String>,
    lng: Option<String>,
    locality: Option<String>,
    posted_at: DateTime<Utc>,
    deadline: DateTime<Utc>,
}

const POST_COLS: &str = "p.id, p.posting_window_id, p.image_id, p.caption, p.visibility, p.location_kind, \
  p.latitude::text, p.longitude::text, p.locality, p.posted_at, w.on_time_deadline";

fn post_from(r: &tokio_postgres::Row) -> PostRow {
    PostRow {
        id: r.get(0),
        posting_window_id: r.get(1),
        image_id: r.get(2),
        caption: r.get(3),
        visibility: r.get(4),
        kind: r.get(5),
        lat: r.get(6),
        lng: r.get(7),
        locality: r.get(8),
        posted_at: r.get(9),
        deadline: r.get(10),
    }
}

/// URL slots of one user: profile image (if any)
struct UserUrls {
    profile: Option<usize>,
}

fn add_user(batch: &mut UrlBatch, u: &UserRow) -> UserUrls {
    UserUrls { profile: u.profile_image_id.as_ref().map(|id| batch.add(&u.key, id)) }
}

fn write_user(out: &mut Vec<u8>, u: &UserRow, urls: &UserUrls, batch: &UrlBatch) {
    out.extend_from_slice(b"{\"id\":");
    j::str(out, &u.id);
    out.extend_from_slice(b",\"username\":");
    j::str(out, &u.username);
    out.extend_from_slice(b",\"displayName\":");
    j::str(out, &u.display_name);
    out.extend_from_slice(b",\"profileImageUrl\":");
    j::opt_str(out, urls.profile.map(|i| batch.get(i)));
    out.extend_from_slice(b",\"bio\":");
    j::str(out, &u.bio);
    out.extend_from_slice(b",\"accountType\":");
    j::str(out, &u.account_type);
    out.push(b'}');
}

fn parse_f64(s: &str) -> f64 {
    s.parse::<f64>().unwrap_or(0.0)
}

/// createPostResponse fields without the closing brace.
fn write_post_open(out: &mut Vec<u8>, p: &PostRow, img: usize, au: &UserRow, auu: &UserUrls, batch: &UrlBatch) {
    out.extend_from_slice(b"{\"id\":");
    j::str(out, &p.id);
    out.extend_from_slice(b",\"author\":");
    write_user(out, au, auu, batch);
    out.extend_from_slice(b",\"postingWindowId\":");
    j::str(out, &p.posting_window_id);
    out.extend_from_slice(b",\"imageUrl\":");
    j::str(out, batch.get(img));
    out.extend_from_slice(b",\"caption\":");
    j::str(out, &p.caption);
    out.extend_from_slice(b",\"visibility\":");
    j::str(out, &p.visibility);
    out.extend_from_slice(b",\"location\":{\"kind\":");
    j::str(out, &p.kind);
    if let Some(v) = &p.lat {
        out.extend_from_slice(b",\"latitude\":");
        j::float(out, parse_f64(v));
    }
    if let Some(v) = &p.lng {
        out.extend_from_slice(b",\"longitude\":");
        j::float(out, parse_f64(v));
    }
    if let Some(v) = &p.locality {
        out.extend_from_slice(b",\"locality\":");
        j::str(out, v);
    }
    out.extend_from_slice(b"},\"postedAt\":");
    j::time(out, &p.posted_at);
    let on_time = p.posted_at <= p.deadline;
    out.extend_from_slice(b",\"onTime\":");
    j::bool(out, on_time);
    out.extend_from_slice(b",\"lateSeconds\":");
    let late = if on_time {
        0
    } else {
        let us = p.posted_at.timestamp_micros() - p.deadline.timestamp_micros();
        us / 1_000_000
    };
    j::int(out, late);
}

async fn resolve(batch: &mut UrlBatch) {
    if batch.needs_compute() > 2 {
        let mut b = std::mem::replace(batch, UrlBatch::new());
        let b = tokio::task::spawn_blocking(move || {
            b.resolve();
            b
        })
        .await
        .unwrap();
        *batch = b;
    } else {
        batch.resolve();
    }
}

// ---- handlers ----

#[allow(clippy::too_many_arguments)]
async fn serve(
    app: &App,
    req: &Request<Incoming>,
    peer: &SocketAddr,
    route: Route,
    param: &str,
    q: &Query,
    ip: &str,
    user: &mut Option<String>,
) -> Result<Response<Body>, Fail> {
    // IPBanMiddleware
    {
        let g = app.bans.read();
        if let Some(m) = g.as_ref() {
            if let Some(until) = m.get(ip) {
                if Utc::now() < *until {
                    return Err(Fail::Api(403, "ip_banned"));
                }
            }
        }
    }
    let sess = session(app, req).await?;
    *user = Some(sess.user_id.clone());
    if sess.region != request_region(app, req, peer) {
        return Err(Fail::Api(403, "region_mismatch"));
    }
    match route {
        Route::Memories | Route::MemoriesByDate => {
            // Bind: query params (beforePostedAt must parse as RFC3339 when present)
            let before_at = match q.first("beforePostedAt") {
                None => None,
                Some(v) => Some(parse_rfc3339(v).ok_or(Fail::Proxy)?),
            };
            let before_id = q.first("beforeId").unwrap_or("").to_string();
            let (start, end) = if route == Route::Memories {
                let s = parse_period(q.first("month").unwrap_or(""), false)?;
                let e = if s.month() == 12 {
                    NaiveDate::from_ymd_opt(s.year() + 1, 1, 1)
                } else {
                    NaiveDate::from_ymd_opt(s.year(), s.month() + 1, 1)
                }
                .ok_or(Fail::Proxy)?;
                (s, e)
            } else {
                let s = parse_period(param, true)?;
                (s, s.succ_opt().ok_or(Fail::Proxy)?)
            };
            if before_at.is_none() != before_id.is_empty() {
                return Err(Fail::Api(400, "invalid_pagination"));
            }
            let body = memories(app, &sess.user_id, start, end, before_at, &before_id).await?;
            Ok(json_response(200, body))
        }
        Route::Export => export(app, &sess.user_id).await,
    }
}

async fn memories(
    app: &App,
    user_id: &str,
    start: NaiveDate,
    end: NaiveDate,
    before_at: Option<DateTime<Utc>>,
    before_id: &str,
) -> Result<Vec<u8>, Fail> {
    let client = app.pool.get().await?;
    let sql = format!(
        "SELECT {POST_COLS}, {USER_COLS} FROM posts p \
         JOIN posting_windows w ON w.id = p.posting_window_id \
         JOIN users u ON u.id = p.author_id AND u.status = 'active' \
         WHERE p.deleted_at IS NULL AND p.author_id = $1 AND w.local_date >= $2 AND w.local_date < $3 \
         AND ($4::timestamptz IS NULL OR (p.posted_at, p.id) < ($4, $5)) \
         ORDER BY p.posted_at DESC, p.id DESC LIMIT 51"
    );
    let st = client.prepare_cached(&sql).await?;
    let before_at = before_at.map(|t| DateTime::<Utc>::from_timestamp_micros(t.timestamp_micros()).unwrap());
    let rows = client.query(&st, &[&user_id, &start, &end, &before_at, &before_id]).await?;
    let has_more = rows.len() > 50;
    let mut posts = Vec::with_capacity(rows.len().min(50));
    for r in rows.iter().take(50) {
        posts.push((post_from(r), user_from(r, 11)?));
    }
    let mut out = Vec::with_capacity(4096);
    let mut batch = UrlBatch::new();
    let mut rm_rows = Vec::new();
    let mut cm_rows = Vec::new();
    if !posts.is_empty() {
        let ids: Vec<&str> = posts.iter().map(|(p, _)| p.id.as_str()).collect();
        let rsql = format!(
            "SELECT r.id, r.post_id, r.image_id, r.reaction_type, r.created_at, {USER_COLS} \
             FROM realmojis r JOIN users u ON u.id = r.reactor_id AND u.status = 'active' \
             WHERE r.replaced_at IS NULL AND r.deleted_at IS NULL AND r.post_id = ANY($1) \
             ORDER BY r.created_at DESC, r.id DESC"
        );
        let csql = format!(
            "SELECT c.id, c.post_id, c.body, c.created_at, {USER_COLS} \
             FROM comments c JOIN users u ON u.id = c.author_id AND u.status = 'active' \
             WHERE c.deleted_at IS NULL AND c.post_id = ANY($1) ORDER BY c.created_at, c.id"
        );
        let (rst, cst) = tokio::try_join!(client.prepare_cached(&rsql), client.prepare_cached(&csql))?;
        let params: [&(dyn tokio_postgres::types::ToSql + Sync); 1] = [&ids];
        let (rr, cr) = tokio::try_join!(client.query(&rst, &params), client.query(&cst, &params))?;
        rm_rows = rr;
        cm_rows = cr;
    }
    drop(client);
    // group interactions per post (max 50 each, in query order)
    let mut idx: HashMap<&str, usize> = HashMap::new();
    for (i, (p, _)) in posts.iter().enumerate() {
        idx.insert(p.id.as_str(), i);
    }
    struct Rm {
        id: String,
        image_id: String,
        kind: String,
        at: DateTime<Utc>,
        user: UserRow,
    }
    struct Cm {
        id: String,
        body: String,
        at: DateTime<Utc>,
        user: UserRow,
    }
    let mut rms: Vec<Vec<Rm>> = (0..posts.len()).map(|_| Vec::new()).collect();
    let mut cms: Vec<Vec<Cm>> = (0..posts.len()).map(|_| Vec::new()).collect();
    for r in &rm_rows {
        let pid: String = r.get(1);
        let i = idx[pid.as_str()];
        if rms[i].len() < 50 {
            rms[i].push(Rm { id: r.get(0), image_id: r.get(2), kind: r.get(3), at: r.get(4), user: user_from(r, 5)? });
        }
    }
    for r in &cm_rows {
        let pid: String = r.get(1);
        let i = idx[pid.as_str()];
        if cms[i].len() < 50 {
            cms[i].push(Cm { id: r.get(0), body: r.get(2), at: r.get(3), user: user_from(r, 4)? });
        }
    }
    // URL slots
    struct Slots {
        img: usize,
        au: UserUrls,
        rm: Vec<(usize, UserUrls)>,
        cm: Vec<UserUrls>,
    }
    let mut slots = Vec::with_capacity(posts.len());
    for (i, (p, au)) in posts.iter().enumerate() {
        let au_u = add_user(&mut batch, au);
        let img = batch.add(&au.key, &p.image_id);
        let rm = rms[i].iter().map(|r| (batch.add(&r.user.key, &r.image_id), add_user(&mut batch, &r.user))).collect();
        let cm = cms[i].iter().map(|c| add_user(&mut batch, &c.user)).collect();
        slots.push(Slots { img, au: au_u, rm, cm });
    }
    resolve(&mut batch).await;
    out.extend_from_slice(b"{\"items\":[");
    for (i, (p, au)) in posts.iter().enumerate() {
        if i > 0 {
            out.push(b',');
        }
        let s = &slots[i];
        write_post_open(&mut out, p, s.img, au, &s.au, &batch);
        out.extend_from_slice(b",\"realmojis\":[");
        for (k, r) in rms[i].iter().enumerate() {
            if k > 0 {
                out.push(b',');
            }
            out.extend_from_slice(b"{\"id\":");
            j::str(&mut out, &r.id);
            out.extend_from_slice(b",\"reactor\":");
            write_user(&mut out, &r.user, &s.rm[k].1, &batch);
            out.extend_from_slice(b",\"reactionType\":");
            j::str(&mut out, &r.kind);
            out.extend_from_slice(b",\"imageUrl\":");
            j::str(&mut out, batch.get(s.rm[k].0));
            out.extend_from_slice(b",\"createdAt\":");
            j::time(&mut out, &r.at);
            out.push(b'}');
        }
        out.extend_from_slice(b"],\"comments\":[");
        for (k, c) in cms[i].iter().enumerate() {
            if k > 0 {
                out.push(b',');
            }
            out.extend_from_slice(b"{\"id\":");
            j::str(&mut out, &c.id);
            out.extend_from_slice(b",\"author\":");
            write_user(&mut out, &c.user, &s.cm[k], &batch);
            out.extend_from_slice(b",\"body\":");
            j::str(&mut out, &c.body);
            out.extend_from_slice(b",\"createdAt\":");
            j::time(&mut out, &c.at);
            out.push(b'}');
        }
        out.extend_from_slice(b"]}");
    }
    out.extend_from_slice(b"],\"hasMore\":");
    j::bool(&mut out, has_more);
    out.extend_from_slice(b"}\n");
    Ok(out)
}

// ---- export ----

pub struct ExportCache {
    inner: Mutex<(HashMap<String, ([u8; 32], Bytes)>, VecDeque<String>, usize)>,
    budget: usize,
}

impl ExportCache {
    pub fn new(budget: usize) -> Self {
        ExportCache { inner: Mutex::new((HashMap::new(), VecDeque::new(), 0)), budget }
    }
    fn get(&self, user: &str, fp: &[u8; 32]) -> Option<Bytes> {
        let g = self.inner.lock();
        match g.0.get(user) {
            Some((f, b)) if f == fp => Some(b.clone()),
            _ => None,
        }
    }
    fn put(&self, user: &str, fp: [u8; 32], b: Bytes) {
        if b.len() > self.budget / 4 {
            return;
        }
        let mut g = self.inner.lock();
        let len = b.len();
        if let Some((_, old)) = g.0.insert(user.to_string(), (fp, b)) {
            g.2 -= old.len();
        } else {
            g.1.push_back(user.to_string());
        }
        g.2 += len;
        while g.2 > self.budget {
            let Some(k) = g.1.pop_front() else { break };
            if let Some((_, old)) = g.0.remove(&k) {
                g.2 -= old.len();
            }
        }
    }
}

fn zip_response(b: Bytes) -> Response<Body> {
    Response::builder()
        .status(200)
        .header("Cache-Control", "no-store")
        .header("Content-Disposition", "attachment; filename=\"isureal-posts.zip\"")
        .header("Content-Type", "application/zip")
        .body(full(b))
        .unwrap()
}

async fn export(app: &App, user_id: &str) -> Result<Response<Body>, Fail> {
    let client = app.pool.get().await?;
    let ust = client
        .prepare_cached(&format!("SELECT {USER_COLS} FROM users u WHERE u.id = $1 AND u.status = 'active'"))
        .await?;
    let pst = client
        .prepare_cached(&format!(
            "SELECT {POST_COLS} FROM posts p JOIN posting_windows w ON w.id = p.posting_window_id \
             WHERE p.author_id = $1 AND p.deleted_at IS NULL ORDER BY p.posted_at, p.id"
        ))
        .await?;
    let params: [&(dyn tokio_postgres::types::ToSql + Sync); 1] = [&user_id];
    let (urow, prows) = tokio::try_join!(client.query_opt(&ust, &params), client.query(&pst, &params))?;
    drop(client);
    let Some(urow) = urow else {
        return Err(Fail::Internal("sql: no rows in result set".into()));
    };
    let author = user_from(&urow, 0)?;
    let posts: Vec<PostRow> = prows.iter().map(post_from).collect();
    let mut batch = UrlBatch::new();
    let au = add_user(&mut batch, &author);
    let imgs: Vec<usize> = posts.iter().map(|p| batch.add(&author.key, &p.image_id)).collect();
    resolve(&mut batch).await;
    // manifest without imagePath numbering depends only on these bytes + image presence (immutable)
    let mut fp = Sha256::new();
    let mut pre = Vec::new();
    write_user(&mut pre, &author, &au, &batch);
    fp.update(&pre);
    for (i, p) in posts.iter().enumerate() {
        pre.clear();
        write_post_open(&mut pre, p, imgs[i], &author, &au, &batch);
        fp.update(&(pre.len() as u64).to_le_bytes());
        fp.update(&pre);
    }
    let fp: [u8; 32] = fp.finalize().into();
    if let Some(b) = app.export_cache.get(user_id, &fp) {
        return Ok(zip_response(b));
    }
    // fetch images (8 in flight like the Go errgroup)
    let mut objs: Vec<Option<crate::images::ImageObject>> = Vec::with_capacity(posts.len());
    for chunk in posts.chunks(8) {
        let futs = chunk.iter().map(|p| app.images.get(&app.pool, &p.image_id));
        for r in futures_join_all(futs).await {
            objs.push(r.map_err(Fail::Internal)?);
        }
    }
    let total: usize = objs.iter().flatten().map(|o| o.data.len() + 128).sum();
    let mut zw = crate::gozip::ZipWriter::with_capacity(total + 4096 + posts.len() * 1024);
    let mut manifest = Vec::with_capacity(posts.len() * 900 + 2);
    manifest.push(b'[');
    let mut n = 0;
    for (i, p) in posts.iter().enumerate() {
        let Some(o) = &objs[i] else { continue };
        n += 1;
        let path = format!("images/{:06}.jpg", n);
        zw.store(&path, &o.data);
        if n > 1 {
            manifest.push(b',');
        }
        write_post_open(&mut manifest, p, imgs[i], &author, &au, &batch);
        manifest.extend_from_slice(b",\"imagePath\":");
        j::str(&mut manifest, &path);
        manifest.push(b'}');
    }
    manifest.extend_from_slice(b"]\n");
    let compressed = crate::deflate(&manifest);
    zw.deflated("posts.json", &manifest, &compressed);
    let body = Bytes::from(zw.finish());
    app.export_cache.put(user_id, fp, body.clone());
    Ok(zip_response(body))
}

async fn futures_join_all<F: std::future::Future>(futs: impl Iterator<Item = F>) -> Vec<F::Output> {
    let handles: Vec<_> = futs.collect();
    let mut out = Vec::with_capacity(handles.len());
    // sequential awaits of already-created futures would serialize; poll them together
    let mut pinned: Vec<std::pin::Pin<Box<F>>> = handles.into_iter().map(Box::pin).collect();
    let mut results: Vec<Option<F::Output>> = (0..pinned.len()).map(|_| None).collect();
    std::future::poll_fn(|cx| {
        let mut pending = false;
        for (i, f) in pinned.iter_mut().enumerate() {
            if results[i].is_none() {
                match f.as_mut().poll(cx) {
                    std::task::Poll::Ready(v) => results[i] = Some(v),
                    std::task::Poll::Pending => pending = true,
                }
            }
        }
        if pending {
            std::task::Poll::Pending
        } else {
            std::task::Poll::Ready(())
        }
    })
    .await;
    for r in results {
        out.push(r.unwrap());
    }
    out
}
