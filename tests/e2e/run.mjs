// ISUReal browser E2E structural check.
//   node run.mjs record [BASE_URL]   -> writes baseline/ (summary.json, raw/, screens/)
//   node run.mjs check  [BASE_URL]   -> runs the same flow, diffs against baseline/, exit 0 if no FAIL
//   node run.mjs diff   SUMMARY.json -> re-diff a saved run (out/<ts>/summary.json) without a browser
// Structure only: DOM tag/class path sets, API key sets + value formats + status codes,
// image load success, console errors, final URL patterns. Data values (counts, times, text) are ignored.
import { chromium } from 'playwright';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const HERE = path.dirname(fileURLToPath(import.meta.url));
const MODE = process.argv[2] || 'check';
const BASE = ((process.argv[2] !== 'diff' && process.argv[3]) || process.env.E2E_BASE_URL || 'https://57.183.58.128').replace(/\/$/, '');
const STRICT = process.env.E2E_STRICT === '1';
const PAUSE = Number(process.env.E2E_PAUSE_MS || 250);
const YUMA = { username: 'yuma_kobayashi_186', secretKey: '0f6-BAnyBELD2MA-t-fekeDtQN4ynqD0mfgOF8JWwWs' };

const stamp = new Date().toISOString().replace(/[-:]/g, '').replace(/\..*/, '');
const OUT = MODE === 'record' ? path.join(HERE, 'baseline') : path.join(HERE, 'out', stamp);
fs.mkdirSync(path.join(OUT, 'raw'), { recursive: true });
fs.mkdirSync(path.join(OUT, 'screens'), { recursive: true });

// ---------- normalization helpers ----------
const UUID = /[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}/gi;
function normPath(u) {
  let s = u.replace(BASE, '');
  s = s.replace(UUID, ':id').replace(/\d{4}-\d{2}-\d{2}/g, ':date').replace(/\d{4}-\d{2}/g, ':month');
  s = s.replace(/\/api\/images\/[^?]+\?.*/, '/api/images/:id?exp&sig');
  s = s.replace(/([?&][a-zA-Z]+)=[^&]*/g, '$1=');
  return s;
}
function strFormat(v) {
  if (/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(v)) return 'uuid';
  if (/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?(Z|[+-]\d{2}:\d{2})$/.test(v)) return 'datetime';
  if (/^\d{4}-\d{2}-\d{2}$/.test(v)) return 'date';
  if (/^\d{4}-\d{2}$/.test(v)) return 'month';
  if (/^\/api\/images\/[0-9a-f-]{36}\?exp=\d+&sig=[A-Za-z0-9_-]+$/.test(v)) return 'image-url';
  if (v.startsWith('/api/images/')) return 'image-url-OTHER';
  return 'string';
}
function typeOf(v) {
  if (v === null) return 'null';
  if (Array.isArray(v)) return 'array';
  if (typeof v === 'string') return 'string:' + strFormat(v);
  return typeof v;
}
// flatten JSON to {path: Set(types)}; arrays use [] and merge elements
function shapeOf(v, p = '$', out = {}) {
  (out[p] ||= new Set()).add(typeOf(v));
  if (Array.isArray(v)) { (out[p + '[]'] ||= new Set()); if (v.length === 0) out[p + '[]'].add('EMPTY'); for (const e of v) shapeOf(e, p + '[]', out); }
  else if (v && typeof v === 'object') for (const k of Object.keys(v)) shapeOf(v[k], p + '.' + k, out);
  return out;
}
function mergeShape(a, b) { for (const [k, s] of Object.entries(b)) { a[k] ||= new Set(); for (const t of s) a[k].add(t); } return a; }
function shapeToJSON(s) { return Object.fromEntries(Object.entries(s).sort().map(([k, v]) => [k, [...v].sort()])); }
function redact(v) {
  if (Array.isArray(v)) return v.map(redact);
  if (v && typeof v === 'object') return Object.fromEntries(Object.entries(v).map(([k, x]) => [k, /^(token|secretKey)$/.test(k) ? '<redacted>' : redact(x)]));
  return v;
}
const normText = (t) => t.replace(UUID, ':id').replace(/\d+/g, '#').replace(/\s+/g, ' ').trim();
const normConsole = (t) => normText(t.replace(/https?:\/\/\S+/g, '<url>'));

const isSSE = (u) => /\/api\/notifications(\?|$)/.test(u);
// ---------- per-step capture ----------
const steps = [];
async function settle(page, tracker) {
  const t0 = Date.now();
  let quietSince = Date.now();
  while (Date.now() - t0 < 6000) {
    if (tracker.inflight > 0) quietSince = Date.now();
    if (Date.now() - quietSince > 400) break;
    await page.waitForTimeout(100);
  }
  await page.waitForFunction(() => [...document.images].every((i) => i.complete), null, { timeout: 5000 }).catch(() => {});
  await page.waitForTimeout(PAUSE);
}

function attachTracker(page, label) {
  const tr = { label, saw401: false, inflight: 0, api: [], images: [], console: [], pending: [] };
  // /api/notifications is an SSE stream: never counted as in-flight, body never read.
  const counted = (u) => u.includes('/api/') && !isSSE(u);
  page.on('request', (r) => { if (counted(r.url())) tr.inflight++; });
  page.on('requestfinished', (r) => { if (counted(r.url())) tr.inflight--; });
  page.on('requestfailed', (r) => { if (counted(r.url())) tr.inflight--; if (!isSSE(r.url())) tr.console.push('requestfailed ' + normPath(r.url())); });
  page.on('response', (r) => {
    const url = r.url();
    if (!url.includes('/api/')) return;
    const rec = { key: r.request().method() + ' ' + normPath(url), status: r.status(), url: url.replace(BASE, '') };
    if (r.status() === 401 && !url.includes('/api/auth/')) tr.saw401 = true;
    const done = (async () => {
      try {
        if (isSSE(url)) { rec.contentType = (r.headers()['content-type'] || '').split(';')[0]; if (r.status() !== 200) { rec.json = await r.json().catch(() => undefined); tr.api.push(rec); } return; }
        if (url.includes('/api/images/')) { rec.contentType = r.headers()['content-type']; tr.images.push(rec); return; }
        const body = await r.text().catch(() => '');
        try { rec.json = JSON.parse(body); } catch { rec.body = body.slice(0, 200); }
        tr.api.push(rec);
      } catch { /* ignore */ }
    })();
    tr.pending.push(done);
  });
  page.on('console', (m) => { if (m.type() === 'error') tr.console.push('console.error ' + m.text()); });
  page.on('pageerror', (e) => tr.console.push('pageerror ' + e.message));
  return tr;
}

async function snap(page, tr, name) {
  await settle(page, tr);
  await Promise.allSettled(tr.pending);
  const dom = await page.evaluate(() => {
    const UU = /[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}/gi;
    const paths = new Set();
    const texts = new Set();
    const sigOf = (el) => {
      let s = el.tagName.toLowerCase();
      const cls = [...el.classList].filter(Boolean).sort();
      if (cls.length) s += '.' + cls.join('.');
      for (const a of ['role', 'type', 'name', 'hidden', 'aria-current']) if (el.hasAttribute(a)) s += `[${a}${el.getAttribute(a) ? '=' + el.getAttribute(a) : ''}]`;
      if (el.tagName === 'A' && el.getAttribute('href')) s += `[href=${el.getAttribute('href').replace(UU, ':id').replace(/=[^&]*/g, '=').replace(/\d+/g, 'N')}]`;
      return s;
    };
    const walk = (el, prefix) => {
      const p = prefix + '>' + sigOf(el);
      paths.add(p);
      if (el.tagName.toLowerCase() === 'svg') return;
      for (const c of el.children) walk(c, p);
    };
    const root = document.querySelector('#root');
    if (root) walk(root, '');
    for (const el of document.querySelectorAll('#root h1, #root h2, #root h3, #root legend, #root button, #root [role=alert], #root [role=status], #root nav a, #root th')) {
      const t = (el.innerText || el.textContent || '').trim();
      if (t && t.length < 60) texts.add(t);
    }
    for (const el of document.querySelectorAll('#root label')) {
      const own = [...el.childNodes].filter((n) => n.nodeType === 3).map((n) => n.textContent.trim()).join('');
      if (own) texts.add('label:' + own);
    }
    const imgs = [...document.querySelectorAll('#root img')].map((i) => ({ src: i.getAttribute('src') || '', loaded: i.complete && i.naturalWidth > 0, cls: i.className }));
    return { paths: [...paths], texts: [...texts], imgs, html: root ? root.innerHTML : '' };
  });
  const urlPath = normPath(page.url());
  const shapes = {};
  const statuses = {};
  const dataSig = new Set();
  for (const r of tr.api) {
    (statuses[r.key] ||= new Set()).add(r.status);
    shapes[r.key + ' ' + r.status] = mergeShape(shapes[r.key + ' ' + r.status] || {}, r.json !== undefined ? shapeOf(r.json) : { $: new Set(['non-json']) });
  }
  for (const [k, s] of Object.entries(shapes)) for (const [p, t] of Object.entries(s)) if (p.endsWith('[]')) dataSig.add(`${k} ${p} ${t.has('EMPTY') && t.size === 1 ? 'empty' : 'nonempty'}`);
  for (const [k, s] of Object.entries(statuses)) dataSig.add(`${k} status ${[...s].sort().join(',')}`);
  const apiImgs = dom.imgs.filter((i) => i.src.startsWith('/api/images/'));
  const summary = {
    name,
    url: urlPath,
    paths: dom.paths.map((p) => p.replace(UUID, ':id')).sort(),
    texts: [...new Set(dom.texts.map(normText))].sort(),
    api: Object.fromEntries(Object.entries(shapes).sort().map(([k, v]) => [k, shapeToJSON(v)])),
    dataSig: [...dataSig].sort(),
    images: {
      apiImgCount: apiImgs.length,
      apiImgLoaded: apiImgs.filter((i) => i.loaded).length,
      broken: dom.imgs.filter((i) => !i.loaded).map((i) => normPath(i.src)),
      srcFormats: [...new Set(apiImgs.map((i) => strFormat(i.src)))].sort(),
      fetches: [...new Set(tr.images.map((r) => `${r.status} ${r.contentType}`))].sort(),
    },
    console: [...new Set(tr.console.map(normConsole))].sort(),
  };
  const safe = name.replace(/[^a-z0-9_-]/gi, '_');
  fs.writeFileSync(path.join(OUT, 'raw', safe + '.html'), dom.html);
  fs.writeFileSync(path.join(OUT, 'raw', safe + '.api.json'), JSON.stringify(tr.api.map((r) => ({ ...r, json: redact(r.json) })), null, 1));
  await page.screenshot({ path: path.join(OUT, 'screens', safe + '.png'), fullPage: true }).catch(() => {});
  steps.push(summary);
  tr.api = []; tr.images = []; tr.console = []; tr.pending = [];
  console.log(`[step] ${name} url=${urlPath} paths=${summary.paths.length} api=${Object.keys(summary.api).length} img=${summary.images.apiImgLoaded}/${summary.images.apiImgCount} console=${summary.console.length}`);
  return summary;
}

class Skip extends Error {}
const skip = (name, why) => { console.log(`[skip] ${name}: ${why}`); steps.push({ name, skipped: why }); };
const T0 = Date.now();
const BUDGET_MS = Number(process.env.E2E_BUDGET_S || 110) * 1000;
async function step(name, fn) {
  if (Date.now() - T0 > BUDGET_MS) { console.log(`[budget] ${name}: time budget exceeded, not run`); steps.push({ name, error: 'time budget exceeded (E2E_BUDGET_S)' }); return; }
  try { await fn(); } catch (e) {
    console.log(`[step-error] ${name}: ${e.message.split('\n')[0]}`);
    steps.push({ name, error: e.message.split('\n')[0] });
  }
}

async function makeJpeg(page) {
  const b64 = await page.evaluate(() => {
    const c = document.createElement('canvas'); c.width = 320; c.height = 240;
    const g = c.getContext('2d');
    g.fillStyle = '#c84'; g.fillRect(0, 0, 320, 240); g.fillStyle = '#333'; g.fillRect(100, 60, 120, 20); g.fillRect(110, 80, 15, 100); g.fillRect(195, 80, 15, 100);
    return c.toDataURL('image/jpeg', 0.8).split(',')[1];
  });
  return Buffer.from(b64, 'base64');
}

async function newPage(browser, label, country) {
  const ctx = await browser.newContext({ ignoreHTTPSErrors: true, viewport: { width: 420, height: 900 }, extraHTTPHeaders: country ? { 'X-Country-Code': country } : {} });
  const page = await ctx.newPage();
  page.setDefaultTimeout(15000);
  const tr = attachTracker(page, label);
  openTrackers.push(tr);
  return { ctx, page, tr };
}

async function uiLogin(page, username, secretKey) {
  await page.goto(BASE + '/login');
  await page.getByLabel('ユーザー名').fill(username);
  await page.getByLabel('SecretKey').fill(secretKey);
  await page.getByRole('button', { name: 'ログイン' }).click();
  await page.waitForURL((u) => !u.pathname.startsWith('/login'), { timeout: 15000 });
}

async function uiRegister(page, tr, prefix, username, region) {
  await page.goto(BASE + '/register');
  await snap(page, tr, prefix + 'register-form');
  await page.getByLabel('ユーザー名').fill(username);
  await page.getByLabel('表示名').fill('E2E ' + region);
  await page.getByLabel('地域').selectOption(region);
  await page.getByRole('button', { name: '登録' }).click();
  const key = await page.locator('output.secret-key').textContent({ timeout: 15000 });
  await snap(page, tr, prefix + 'register-secret');
  return key.trim();
}

// ---------- flow ----------
// Each block runs with fresh users. If a session gets invalidated mid-block (401: someone ran
// /api/initialize, e.g. a benchmark), the block's steps are discarded and the block is retried.
const openTrackers = [];
const browser = MODE === 'diff' ? null : await chromium.launch();
const run = Date.now().toString(36);
let inconclusive = [];
async function block(name, fn) {
  for (let attempt = 1; attempt <= 2; attempt++) {
    const mark = steps.length;
    openTrackers.length = 0;
    try { await fn(attempt); } catch (e) { console.log(`[block-error] ${name}: ${e.message.split('\n')[0]}`); steps.push({ name: name + '-block', error: e.message.split('\n')[0] }); }
    const invalidated = openTrackers.some((t) => t.saw401);
    const errored = steps.slice(mark).some((s) => s.error);
    if (!invalidated && (!errored || attempt === 2)) return;
    if (!invalidated) { console.log(`[retry] ${name}: step error, retrying block once with fresh users`); steps.length = mark; continue; }
    console.log(`[retry] ${name}: session invalidated (401; concurrent /api/initialize?) attempt ${attempt}`);
    steps.length = mark;
    if (attempt < 2) await new Promise((r) => setTimeout(r, 3000));
  }
  inconclusive.push(name);
  steps.push({ name: name + '-block', error: 'session invalidated twice (benchmark running?) -> INCONCLUSIVE' });
}
if (MODE === 'diff') steps.push(...JSON.parse(fs.readFileSync(process.argv[3], 'utf8')).steps);
else try {
  // A: throwaway asia user -> post creation
  await block('poster', async (attempt) => {
    const { ctx, page, tr } = await newPage(browser, 'poster');
    const uname = `e2e_${run}${attempt}_a`;
    let key = null;
    await step('a-register', async () => { key = await uiRegister(page, tr, 'a-', uname, 'asia'); });
    await step('a-login', async () => { await uiLogin(page, uname, key); await snap(page, tr, 'a-home-after-login'); });
    let posted = false;
    await step('a-post-new', async () => {
      await page.goto(BASE + '/posts/new');
      await snap(page, tr, 'a-post-new');
      const file = page.locator('input[type=file][accept="image/jpeg"]');
      if (await file.count() === 0) { skip('a-post-settings', 'posts/new has no file input (posting window closed?)'); skip('a-post-complete', 'not posted'); return; }
      await file.setInputFiles({ name: 'chair.jpg', mimeType: 'image/jpeg', buffer: await makeJpeg(page) });
      await page.getByLabel('キャプション').fill('E2E check chair');
      await page.getByLabel('公開', { exact: true }).check();
      await snap(page, tr, 'a-post-settings');
      await page.getByRole('button', { name: '投稿する' }).click();
      await page.waitForURL((u) => !u.pathname.startsWith('/posts/new'), { timeout: 20000 });
      await snap(page, tr, 'a-post-complete');
      posted = true;
    });
    await step('a-my-current-post', async () => {
      await page.goto(BASE + '/posts/me/current');
      await snap(page, tr, 'a-my-current-post');
    });
    await step('a-own-post-detail', async () => {
      const a = page.locator('main a.text-link[href^="/posts/"]').first();
      if (!posted || await a.count() === 0) { skip('a-own-post-detail', 'no own post'); skip('a-own-post-comments', 'no own post'); skip('a-own-post-realmoji', 'no own post'); return; }
      const href = await a.getAttribute('href');
      await a.click(); await page.waitForURL('**' + href);
      await snap(page, tr, 'a-own-post-detail');
      await page.goto(BASE + href + '/comments'); await snap(page, tr, 'a-own-post-comments');
      await page.goto(BASE + href + '/realmoji'); await snap(page, tr, 'a-own-post-realmoji');
    });
    await step('a-feeds', async () => {
      await page.goto(BASE + '/'); await snap(page, tr, 'a-feed-friends');
      for (const [tab, label] of [['friends-of-friends', 'Friends of friends'], ['public', '公開'], ['following', 'フォロー中']]) {
        await page.locator('nav.feed-tabs a', { hasText: label }).click();
        await page.waitForURL('**/feeds/' + tab);
        await snap(page, tr, 'a-feed-' + tab);
      }
    });
    await step('a-public-post-detail', async () => {
      await page.goto(BASE + '/feeds/public');
      await settle(page, tr);
      const a = page.locator('main article a[href^="/posts/"]').first();
      if (await a.count() === 0) { tr.api = []; tr.images = []; tr.console = []; skip('a-public-post-detail', 'public feed empty'); return; }
      await a.click(); await page.waitForURL('**/posts/**');
      await snap(page, tr, 'a-public-post-detail');
    });
    await ctx.close();
  });

  // Y: initial user (asia)
  await block('yuma', async () => {
    const { ctx, page, tr } = await newPage(browser, 'yuma');
    await step('y-top', async () => { await page.goto(BASE + '/'); await snap(page, tr, 'y-top-redirect-login'); });
    await step('y-login', async () => {
      await page.getByLabel('ユーザー名').fill(YUMA.username);
      await page.getByLabel('SecretKey').fill(YUMA.secretKey);
      await page.getByRole('button', { name: 'ログイン' }).click();
      await page.waitForURL((u) => !u.pathname.startsWith('/login'));
      await snap(page, tr, 'y-feed-friends');
    });
    for (const [tab, label] of [['friends-of-friends', 'Friends of friends'], ['public', '公開'], ['following', 'フォロー中']]) {
      await step('y-feed-' + tab, async () => {
        await page.locator('nav.feed-tabs a', { hasText: label }).click();
        await page.waitForURL('**/feeds/' + tab);
        await snap(page, tr, 'y-feed-' + tab);
      });
    }
    await step('y-memories', async () => {
      await page.locator('nav.bottom-nav a', { hasText: 'Memories' }).click();
      await page.waitForURL('**/memories');
      await snap(page, tr, 'y-memories');
      const a = page.locator('a[href^="/memories/posts/"]').first();
      if (await a.count()) { await a.click(); await page.waitForURL('**/memories/posts/**'); await snap(page, tr, 'y-memory-detail'); }
    });
    await step('y-settings', async () => {
      await page.locator('header a.profile-link').click();
      await page.waitForURL('**/settings');
      await snap(page, tr, 'y-settings');
      await page.goto(BASE + '/settings/profile'); await snap(page, tr, 'y-settings-profile');
      await page.goto(BASE + '/settings/account'); await snap(page, tr, 'y-settings-account');
      await page.goto(BASE + '/posts/me/current'); await snap(page, tr, 'y-my-current-post');
    });
    await step('y-profile', async () => {
      await page.goto(BASE + '/users');
      await page.locator('main input').first().fill('yuma_kobayashi');
      await page.keyboard.press('Enter');
      await snap(page, tr, 'y-search');
      const a = page.locator('main a[href^="/users/"]').first();
      if (await a.count()) { await a.click(); await page.waitForURL('**/users/**'); await snap(page, tr, 'y-user-profile'); }
    });
    await step('y-relationships', async () => {
      await page.locator('nav.bottom-nav a', { hasText: 'つながり' }).click();
      await page.waitForURL('**/relationships/friends');
      await snap(page, tr, 'y-relationships-friends');
      const fr = page.locator('main a[href^="/users/"]').first();
      if (await fr.count()) { await fr.click(); await page.waitForURL('**/users/**'); await snap(page, tr, 'y-friend-profile'); }
    });
    await step('y-notifications', async () => {
      await page.locator('nav.bottom-nav a', { hasText: '通知' }).click();
      await page.waitForURL('**/notifications');
      await snap(page, tr, 'y-notifications');
    });
    await step('y-logout', async () => {
      await page.goto(BASE + '/settings');
      await settle(page, tr);
      await page.getByRole('button', { name: 'ログアウト' }).click();
      await page.waitForURL('**/login');
      await snap(page, tr, 'y-after-logout');
    });
    await ctx.close();
  });

  // E / U: europe / america users via X-Country-Code
  for (const [region, cc] of [['europe', 'DE'], ['america', 'US']]) await block(region, async (attempt) => {
    const { ctx, page, tr } = await newPage(browser, region, cc);
    const uname = `e2e_${run}${attempt}_${region}`;
    let key = null;
    await step(region + '-register', async () => { key = await uiRegister(page, tr, region + '-', uname, region); });
    await step(region + '-login', async () => {
      await uiLogin(page, uname, key);
      await snap(page, tr, region + '-home');
      await page.goto(BASE + '/feeds/public'); await snap(page, tr, region + '-feed-public');
      await page.goto(BASE + '/settings'); await snap(page, tr, region + '-settings');
    });
    await ctx.close();
    if (region === 'europe') {
      // same europe user from a JP client -> region mismatch behavior
      const m = await newPage(browser, 'mismatch', 'JP');
      await step('mismatch-login', async () => {
        await m.page.goto(BASE + '/login');
        await m.page.getByLabel('ユーザー名').fill(uname);
        await m.page.getByLabel('SecretKey').fill(key);
        await m.page.getByRole('button', { name: 'ログイン' }).click();
        await m.page.waitForTimeout(2500);
        await snap(m.page, m.tr, 'mismatch-europe-user-from-jp');
      });
      await m.ctx.close();
    }
  });
} finally {
  await browser.close();
}

fs.writeFileSync(path.join(OUT, 'summary.json'), JSON.stringify({ base: BASE, recordedAt: new Date().toISOString(), steps }, null, 1));

if (MODE === 'record') {
  const errs = steps.filter((s) => s.error || s.skipped);
  console.log(`recorded ${steps.length} steps to ${OUT}` + (errs.length ? ` (${errs.length} step errors)` : ''));
  process.exit(errs.length ? 1 : 0);
}

// ---------- diff ----------
// omitempty fields in the initial Go implementation (present only for some data): never a structural diff
const OPTIONAL_KEY = /\.(score|location\.(latitude|longitude|locality))$/;
const base = JSON.parse(fs.readFileSync(path.join(HERE, 'baseline', 'summary.json'), 'utf8')).steps;
const fails = [], warns = [];
const F = (s, m) => fails.push(`FAIL [${s}] ${m}`);
const W = (s, m) => warns.push(`WARN [${s}] ${m}`);
const byName = Object.fromEntries(steps.map((s) => [s.name, s]));
for (const b of base) {
  const c = byName[b.name];
  if (!c) { F(b.name, 'step missing in this run (flow diverged earlier?)'); continue; }
  if (c.error) { F(b.name, 'step error: ' + c.error); continue; }
  if (b.error || b.skipped) continue;
  if (c.skipped) { W(b.name, 'SKIPPED (data/state dependent, not verified): ' + c.skipped); continue; }
  const dataSame = JSON.stringify(b.dataSig) === JSON.stringify(c.dataSig);
  const domIssue = dataSame ? F : W;
  if (b.url !== c.url) F(b.name, `url ${b.url} -> ${c.url}`);
  const cp = new Set(c.paths), bp = new Set(b.paths);
  const missing = b.paths.filter((p) => !cp.has(p)), extra = c.paths.filter((p) => !bp.has(p));
  for (const p of missing.slice(0, 15)) domIssue(b.name, 'DOM missing: ' + p);
  if (missing.length > 15) domIssue(b.name, `DOM missing: ...and ${missing.length - 15} more`);
  for (const p of extra.slice(0, 15)) (STRICT && dataSame ? F : W)(b.name, 'DOM extra: ' + p);
  if (extra.length > 15) W(b.name, `DOM extra: ...and ${extra.length - 15} more`);
  if (!dataSame) W(b.name, 'data state differs from baseline (DOM diffs downgraded to WARN): ' + JSON.stringify(b.dataSig.filter((x) => !c.dataSig.includes(x))) + ' -> ' + JSON.stringify(c.dataSig.filter((x) => !b.dataSig.includes(x))));
  const ct = new Set(c.texts);
  for (const t of b.texts.filter((t) => !ct.has(t)).slice(0, 10)) W(b.name, 'text missing: ' + t);
  // API
  const stripStatus = (k) => k.replace(/ \d{3}$/, '');
  const cKeys = Object.keys(c.api), bKeys = Object.keys(b.api);
  for (const k of bKeys) {
    if (!c.api[k]) {
      const alt = cKeys.find((x) => stripStatus(x) === stripStatus(k));
      if (alt) F(b.name, `API status ${k} -> ${alt}`);
      else (dataSame ? F : W)(b.name, `API call missing: ${k}`);
      continue;
    }
    const bs = b.api[k], cs = c.api[k];
    const emptyIn = (s, p) => Object.entries(s).some(([q, t]) => q.endsWith('[]') && p.startsWith(q) && p !== q && t.length === 1 && t[0] === 'EMPTY');
    for (const p of Object.keys(bs)) {
      if (!cs[p]) { if (!emptyIn(cs, p) && !OPTIONAL_KEY.test(p)) F(b.name, `API ${k} key missing: ${p}`); continue; }
      const bt = bs[p].filter((t) => t !== 'null' && t !== 'EMPTY'), ctt = cs[p].filter((t) => t !== 'null' && t !== 'EMPTY');
      if (bt.length && ctt.length && !ctt.every((t) => bt.includes(t))) F(b.name, `API ${k} type ${p}: ${bt} -> ${ctt}`);
      else if (bt.length && ctt.length === 0 && cs[p].includes('null') && !bs[p].includes('null')) W(b.name, `API ${k} ${p} is null now (was ${bt})`);
    }
    for (const p of Object.keys(cs)) if (!bs[p] && !OPTIONAL_KEY.test(p)) (emptyIn(bs, p) ? W : F)(b.name, `API ${k} extra key: ${p}`);
  }
  for (const k of cKeys) if (!b.api[k] && !bKeys.some((x) => stripStatus(x) === stripStatus(k))) W(b.name, `API extra call: ${k}`);
  // images
  if (c.images.broken.length) F(b.name, 'broken images: ' + c.images.broken.join(', '));
  if (b.images.apiImgLoaded > 0 && c.images.apiImgLoaded === 0) domIssue(b.name, 'no /api/images img loaded (baseline had some)');
  for (const f of c.images.srcFormats) if (!b.images.srcFormats.includes(f) && f !== 'image-url') F(b.name, 'image src format: ' + f);
  for (const f of c.images.fetches) if (!/^200 /.test(f)) F(b.name, 'image fetch: ' + f);
  // console
  for (const e of c.console) if (!b.console.includes(e)) F(b.name, 'new console error: ' + e);
}
for (const c of steps) if (!base.find((b) => b.name === c.name)) W(c.name, 'step not in baseline');

const report = [...fails, ...warns].join('\n');
fs.writeFileSync(path.join(OUT, 'diff.txt'), report + '\n');
console.log('\n' + (report || '(no differences)'));
if (inconclusive.length) { console.log(`\nresult: INCONCLUSIVE (sessions kept being invalidated in: ${inconclusive.join(', ')}; is a benchmark running? retry later)`); process.exit(2); }
console.log(`\nresult: ${fails.length ? 'FAIL' : 'PASS'}  fails=${fails.length} warns=${warns.length}  out=${path.relative(process.cwd(), OUT)}`);
process.exit(fails.length ? 1 : 0);
