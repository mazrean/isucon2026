//! Immutable image objects: memory FIFO-bounded cache → Go's disk cache
//! (read only; Go is the only writer) → DB.

use bytes::Bytes;
use parking_lot::Mutex;
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;

#[derive(Clone)]
pub struct ImageObject {
    #[allow(dead_code)]
    pub owner: Arc<str>,
    #[allow(dead_code)]
    pub media_type: Arc<str>,
    pub data: Bytes,
}

pub struct ImageStore {
    dir: Option<String>,
    inner: Mutex<(HashMap<String, ImageObject>, VecDeque<String>, usize)>,
    budget: usize,
}

fn safe_id(id: &str) -> bool {
    id.len() >= 3 && id.len() <= 64 && id.bytes().all(|c| c.is_ascii_alphanumeric() || c == b'-' || c == b'_')
}

impl ImageStore {
    pub fn new(dir: String, budget: usize) -> Self {
        let dir = if dir.is_empty() || dir == "-" { None } else { Some(dir) };
        ImageStore { dir, inner: Mutex::new((HashMap::new(), VecDeque::new(), 0)), budget }
    }

    fn mem_get(&self, id: &str) -> Option<ImageObject> {
        self.inner.lock().0.get(id).cloned()
    }

    fn mem_add(&self, id: &str, obj: &ImageObject) {
        let mut g = self.inner.lock();
        if g.0.contains_key(id) {
            return;
        }
        let sz = obj.data.len() + 128;
        g.0.insert(id.to_string(), obj.clone());
        g.1.push_back(id.to_string());
        g.2 += sz;
        while g.2 > self.budget && g.1.len() > 1 {
            if let Some(old) = g.1.pop_front() {
                if let Some(o) = g.0.remove(&old) {
                    g.2 -= o.data.len() + 128;
                }
            }
        }
    }

    async fn read_disk(&self, id: &str) -> Option<ImageObject> {
        let dir = self.dir.as_ref()?;
        if !safe_id(id) {
            return None;
        }
        let path = format!("{}/{}/{}", dir, &id[..2], id);
        let raw = tokio::fs::read(&path).await.ok()?;
        let i = raw.iter().position(|&b| b == b'\n')?;
        let j = raw[i + 1..].iter().position(|&b| b == b'\n')?;
        let owner = std::str::from_utf8(&raw[..i]).ok()?.into();
        let media = std::str::from_utf8(&raw[i + 1..i + 1 + j]).ok()?.into();
        let raw = Bytes::from(raw);
        Some(ImageObject { owner, media_type: media, data: raw.slice(i + 1 + j + 1..) })
    }

    pub async fn get(&self, pool: &deadpool_postgres::Pool, id: &str) -> Result<Option<ImageObject>, String> {
        if let Some(o) = self.mem_get(id) {
            return Ok(Some(o));
        }
        if let Some(o) = self.read_disk(id).await {
            self.mem_add(id, &o);
            return Ok(Some(o));
        }
        let client = pool.get().await.map_err(|e| e.to_string())?;
        let st = client
            .prepare_cached("SELECT owner_user_id, media_type, data FROM image_objects WHERE id = $1 LIMIT 1")
            .await
            .map_err(|e| e.to_string())?;
        let row = client.query_opt(&st, &[&id]).await.map_err(|e| e.to_string())?;
        let Some(row) = row else { return Ok(None) };
        let owner: String = row.get(0);
        let media: String = row.get(1);
        let data: Vec<u8> = row.get(2);
        let o = ImageObject { owner: owner.into(), media_type: media.into(), data: Bytes::from(data) };
        self.mem_add(id, &o);
        Ok(Some(o))
    }
}
