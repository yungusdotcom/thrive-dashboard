// server/flowhub.js
// ============================================================
// Flowhub API Client — Thrive Cannabis Marketplace
// OPTIMIZED: streaming bucket fetch, disk-cached completed weeks
// ============================================================

const fetch = require('node-fetch');
const fs = require('fs');

const BASE = 'https://api.flowhub.co';
const CLIENT_ID = process.env.FLOWHUB_CLIENT_ID;
const CLIENT_KEY = process.env.FLOWHUB_API_KEY;

const STORE_CONFIG = [
  { id: 'cactus',   match: 'cactus',      display: 'Cactus',      color: '#00e5a0' },
  { id: 'cheyenne', match: 'cheyenne',    display: 'Cheyenne',    color: '#4db8ff' },
  { id: 'jackpot',  match: 'jackpot',     display: 'Jackpot',     color: '#c084fc' },
  { id: 'main',     match: 'main street', display: 'Main Street', color: '#ffd166' },
  { id: 'reno',     match: 'reno',        display: 'Reno',        color: '#ff8c42' },
  { id: 'sahara',   match: 'sahara',      display: 'Sahara',      color: '#ff4d6d' },
  { id: 'sammy',    match: 'sammy',       display: 'Sammy',       color: '#a8e6cf' },
];
const EXCLUDED_KEYWORDS = ['smoke', 'mirrors', 'mbnv', 'cultivation'];
function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }
function round2(n) { return Math.round(n * 100) / 100; }

// ── Core GET with 429/500 retry ───────────────────────────────
async function flowhubGet(path, params = {}) {
  const url = new URL(`${BASE}${path}`);
  Object.entries(params).forEach(([k, v]) => {
    if (v !== undefined && v !== null) url.searchParams.set(k, String(v));
  });
  for (let attempt = 0; attempt < 5; attempt++) {
    if (attempt > 0) {
      const delay = Math.min(1500 * Math.pow(2, attempt), 20000);
      console.log(`  ↻ retry #${attempt} in ${delay}ms...`);
      await sleep(delay);
    }
    const res = await fetch(url.toString(), {
      headers: { 'clientId': CLIENT_ID, 'key': CLIENT_KEY, 'Accept': 'application/json', 'Content-Type': 'application/json' },
    });
    if (res.status === 429) { console.log(`⚠ 429: ${path}`); continue; }
    if (res.status === 500 && attempt < 4) { console.log(`⚠ 500: ${path}`); continue; }
    if (!res.ok) { const b = await res.text().catch(() => ''); throw new Error(`Flowhub ${res.status} ${path}: ${b.slice(0, 300)}`); }
    return res.json();
  }
  throw new Error(`Flowhub: max retries for ${path}`);
}

// ── Locations ─────────────────────────────────────────────────
let _locations = null;
async function getLocations() {
  if (_locations) return _locations;
  const data = await flowhubGet('/v0/clientsLocations');
  const raw = Array.isArray(data) ? data : (data.locations || data.data || []);
  _locations = raw
    .filter(loc => { const n = (loc.locationName || loc.name || '').toLowerCase(); return !EXCLUDED_KEYWORDS.some(ex => n.includes(ex)); })
    .map(loc => {
      const rawName = loc.locationName || loc.name || '';
      const importId = loc.importId || loc.locationId || loc._id || loc.id;
      const cfg = STORE_CONFIG.find(s => rawName.toLowerCase().includes(s.match));
      return { importId, rawName, name: cfg?.display || rawName, id: cfg?.id || rawName.toLowerCase().replace(/[^a-z]+/g, '_'), color: cfg?.color || '#888' };
    });
  console.log('✓', _locations.length, 'locations:', _locations.map(l => l.name).join(', '));
  return _locations;
}

// ── Date helpers (Pacific Time) ───────────────────────────────
const TZ = 'America/Los_Angeles';
function todayPacific() { return new Date().toLocaleDateString('en-CA', { timeZone: TZ }); }
function dowPacific() { const d = new Date().toLocaleDateString('en-US', { timeZone: TZ, weekday: 'short' }); return { Sun:0,Mon:1,Tue:2,Wed:3,Thu:4,Fri:5,Sat:6 }[d] ?? 0; }
function addDays(s, days) { const d = new Date(s + 'T12:00:00Z'); d.setDate(d.getDate() + days); return d.toISOString().split('T')[0]; }
function toDateStr(d) { return `${d.getFullYear()}-${String(d.getMonth()+1).padStart(2,'0')}-${String(d.getDate()).padStart(2,'0')}`; }
function weekRange(weeksBack = 0) { const t = todayPacific(), dw = dowPacific(), ds = (dw+6)%7, m = addDays(t, -ds - weeksBack*7); return { start: m, end: addDays(m, 6) }; }
function todayRange() { const d = todayPacific(); return { start: d, end: d }; }
function ytdRange() { const t = todayPacific(); return { start: `${t.split('-')[0]}-01-01`, end: t }; }

// ── Fetch orders (for short ranges: 1 week or less) ──────────
let _schemaLogged = false;
async function getOrdersForLocation(importId, startDate, endDate) {
  const start = startDate.split('T')[0], end = endDate.split('T')[0];
  let page = 1, allOrders = [], total = 0;
  while (true) {
    try {
      const data = await flowhubGet(`/v1/orders/findByLocationId/${importId}`, { created_after: start, created_before: end, page_size: 500, page, order_by: 'asc' });
      const batch = data.orders || []; total = data.total || 0; allOrders = allOrders.concat(batch);
      if (!_schemaLogged && batch.length > 0) { _schemaLogged = true; console.log('ORDER KEYS:', Object.keys(batch[0]).join(', ')); }
      if (allOrders.length >= total || batch.length < 500) break;
      page++;
    } catch (err) { console.error(`✗ ${importId.slice(0,8)} ${start}→${end} p${page}: ${err.message}`); break; }
  }
  return { total: allOrders.length, orders: allOrders };
}

// ══════════════════════════════════════════════════════════════
// STREAMING BUCKET FETCH — processes page-by-page, never holds
// all orders in memory. Used for bulk 12-week fetches.
// ══════════════════════════════════════════════════════════════
async function streamBucketFetch(importId, startDate, endDate, weeks) {
  const start = startDate.split('T')[0], end = endDate.split('T')[0];
  let page = 1, totalFetched = 0, apiTotal = 0;

  // Lightweight accumulators (no raw orders stored)
  const accum = weeks.map(w => ({
    week: w, net_sales: 0, gross_sales: 0, total_items: 0, transaction_count: 0,
    catMap: {}, btMap: {}, ctypes: { rec: 0, med: 0 },
  }));

  while (true) {
    let data;
    try {
      data = await flowhubGet(`/v1/orders/findByLocationId/${importId}`, { created_after: start, created_before: end, page_size: 500, page, order_by: 'asc' });
    } catch (err) { console.error(`✗ Stream ${importId.slice(0,8)} p${page}: ${err.message}`); break; }

    const batch = data.orders || [];
    apiTotal = data.total || 0;

    // Process & bucket each order immediately
    for (const order of batch) {
      if (order.voided === true || order.orderStatus === 'voided') continue;
      const d = new Date(order.createdAt || order.completedOn || '').toLocaleDateString('en-CA', { timeZone: TZ });
      let acc = null;
      for (const a of accum) { if (d >= a.week.start && d <= a.week.end) { acc = a; break; } }
      if (!acc) continue;

      acc.transaction_count++;
      const ct = (order.customerType || '').toLowerCase();
      if (ct.includes('med')) acc.ctypes.med++; else acc.ctypes.rec++;
      const bt = order.budtender || 'Unknown';
      if (!acc.btMap[bt]) acc.btMap[bt] = { name: bt, transactions: 0, net_sales: 0, items: 0 };
      acc.btMap[bt].transactions++;

      let oNet = 0, oItems = 0;
      for (const item of (order.itemsInCart || [])) {
        if (item.voided === true) continue;
        const qty = item.quantity || 1; oItems += qty;
        const lg = Number(item.totalPrice) || (Number(item.unitPrice || 0) * qty);
        const disc = Number(item.totalDiscounts) || 0;
        const ln = lg - disc;
        acc.gross_sales += lg; oNet += ln;
        const cat = item.category || item.type || 'Other';
        if (!acc.catMap[cat]) acc.catMap[cat] = { name: cat, net_sales: 0, units: 0, transactions: 0 };
        acc.catMap[cat].net_sales += ln; acc.catMap[cat].units += qty; acc.catMap[cat].transactions++;
      }
      acc.net_sales += oNet; acc.total_items += oItems;
      acc.btMap[bt].net_sales += oNet; acc.btMap[bt].items += oItems;
    }

    totalFetched += batch.length;
    if (totalFetched >= apiTotal || batch.length < 500) break;
    page++;
  }

  console.log(`    → ${totalFetched} orders streamed into ${weeks.length} weeks`);

  return accum.map(a => ({
    week: a.week,
    summary: {
      transaction_count: a.transaction_count, net_sales: round2(a.net_sales), gross_sales: round2(a.gross_sales),
      avg_basket: round2(a.transaction_count > 0 ? a.net_sales / a.transaction_count : 0), total_items: a.total_items,
      customer_types: a.ctypes,
      categories: Object.values(a.catMap).sort((x,y) => y.net_sales - x.net_sales).map(c => ({ ...c, net_sales: round2(c.net_sales) })),
      budtenders: Object.values(a.btMap).map(b => ({ ...b, net_sales: round2(b.net_sales), avg_basket: round2(b.transactions ? b.net_sales / b.transactions : 0) })).sort((x,y) => y.net_sales - x.net_sales),
    },
    error: null,
  }));
}

// ── Summarize orders → KPIs ───────────────────────────────────
function summarizeOrders(orders) {
  if (!orders || !orders.length) return { transaction_count: 0, net_sales: 0, gross_sales: 0, avg_basket: 0, total_items: 0, categories: [], budtenders: [], customer_types: { rec: 0, med: 0 } };
  let ns = 0, gs = 0, ti = 0; const cm = {}, bm = {}, ct = { rec: 0, med: 0 };
  orders.forEach(o => {
    if (o.voided === true || o.orderStatus === 'voided') return;
    const c = (o.customerType || '').toLowerCase(); if (c.includes('med')) ct.med++; else ct.rec++;
    const bt = o.budtender || 'Unknown'; if (!bm[bt]) bm[bt] = { name: bt, transactions: 0, net_sales: 0, items: 0 }; bm[bt].transactions++;
    let on = 0, og = 0, oi = 0;
    (o.itemsInCart || []).forEach(item => {
      if (item.voided === true) return; const q = item.quantity || 1; oi += q;
      const lg = Number(item.totalPrice) || (Number(item.unitPrice || 0) * q); const d = Number(item.totalDiscounts) || 0; const ln = lg - d;
      og += lg; on += ln; const cat = item.category || item.type || 'Other';
      if (!cm[cat]) cm[cat] = { name: cat, net_sales: 0, units: 0, transactions: 0 }; cm[cat].net_sales += ln; cm[cat].units += q; cm[cat].transactions++;
    });
    ns += on; gs += og; ti += oi; bm[bt].net_sales += on; bm[bt].items += oi;
  });
  const tc = orders.length;
  return { transaction_count: tc, net_sales: round2(ns), gross_sales: round2(gs), avg_basket: round2(tc > 0 ? ns/tc : 0), total_items: ti, customer_types: ct,
    categories: Object.values(cm).sort((a,b) => b.net_sales - a.net_sales).map(c => ({ ...c, net_sales: round2(c.net_sales) })),
    budtenders: Object.values(bm).map(b => ({ ...b, net_sales: round2(b.net_sales), avg_basket: round2(b.transactions ? b.net_sales/b.transactions : 0) })).sort((a,b) => b.net_sales - a.net_sales),
  };
}

// ── Top products ──────────────────────────────────────────────
function extractTopProducts(orders, limit = 15) {
  const m = {};
  orders.forEach(o => { if (o.voided) return; (o.itemsInCart || []).forEach(i => {
    if (i.voided) return; const n = i.productName || i.title1 || 'Unknown', b = i.brand || '', c = i.category || i.type || 'Other', q = i.quantity || 1;
    const g = Number(i.totalPrice) || 0, d = Number(i.totalDiscounts) || 0, net = g - d, k = `${n}__${b}`;
    if (!m[k]) m[k] = { name: n, brand: b, category: c, units_sold: 0, net_sales: 0, prices: [] };
    m[k].units_sold += q; m[k].net_sales += net; if (i.unitPrice) m[k].prices.push(Number(i.unitPrice));
  }); });
  return Object.values(m).map(p => ({ ...p, net_sales: round2(p.net_sales), avg_price: p.prices.length ? round2(p.prices.reduce((a,b) => a+b, 0) / p.prices.length) : 0 })).sort((a,b) => b.net_sales - a.net_sales).slice(0, limit);
}

// ── Disk cache ────────────────────────────────────────────────
const CACHE_DIR = process.env.CACHE_DIR || '/tmp';
const CACHE_FILE = `${CACHE_DIR}/thrive-week-cache.json`;
let _weekCache = {};
function loadWeekCache() { try { if (fs.existsSync(CACHE_FILE)) { _weekCache = JSON.parse(fs.readFileSync(CACHE_FILE, 'utf8')); console.log(`✓ Loaded ${Object.keys(_weekCache).length} cached weeks`); } else { console.log('✓ No cache, starting fresh'); } } catch (e) { console.log('⚠ Cache load fail:', e.message); _weekCache = {}; } }
let _savePending = false;
function saveWeekCache() { if (_savePending) return; _savePending = true; setTimeout(() => { _savePending = false; try { fs.writeFileSync(CACHE_FILE, JSON.stringify(_weekCache), 'utf8'); console.log(`✓ Saved ${Object.keys(_weekCache).length} weeks`); } catch (e) { console.log('⚠ Save fail:', e.message); } }, 3000); }
loadWeekCache();
function weekCacheKey(id, ws) { return `${id}:${ws}`; }
function isWeekCompleted(w) { return w.end < todayPacific(); }

// ── Trend (all stores) ────────────────────────────────────────
async function getAllStoresWeeklyTrend(weeksBack = 12) {
  const locs = await getLocations(), weeks = Array.from({ length: weeksBack }, (_, i) => weekRange(weeksBack - 1 - i)), results = [];
  for (const loc of locs) {
    const unc = weeks.filter(w => !(isWeekCompleted(w) && _weekCache[weekCacheKey(loc.importId, w.start)]));
    if (unc.length === 0) { results.push({ store: loc, trend: weeks.map(w => _weekCache[weekCacheKey(loc.importId, w.start)]) }); console.log(`  ${loc.name}: cached`); continue; }
    if (unc.length === 1 && !isWeekCompleted(unc[0])) {
      console.log(`  ${loc.name}: 1 fresh week...`);
      const trend = weeks.map(w => { const ck = weekCacheKey(loc.importId, w.start); return (isWeekCompleted(w) && _weekCache[ck]) ? _weekCache[ck] : null; });
      try { const cw = unc[0], { orders } = await getOrdersForLocation(loc.importId, cw.start, cw.end); trend[weeks.findIndex(w => w.start === cw.start)] = { week: cw, summary: summarizeOrders(orders), error: null }; }
      catch (e) { trend[weeks.findIndex(w => w.start === unc[0].start)] = { week: unc[0], summary: null, error: e.message }; }
      results.push({ store: loc, trend }); continue;
    }
    console.log(`  ${loc.name}: streaming ${unc.length} weeks...`);
    try {
      const trend = await streamBucketFetch(loc.importId, weeks[0].start, weeks[weeks.length-1].end, weeks);
      for (const e of trend) { if (isWeekCompleted(e.week) && e.summary?.net_sales > 0) _weekCache[weekCacheKey(loc.importId, e.week.start)] = e; }
      saveWeekCache(); results.push({ store: loc, trend });
    } catch (e) { console.error(`  ${loc.name} fail: ${e.message}`); results.push({ store: loc, trend: weeks.map(w => ({ week: w, summary: null, error: e.message })) }); }
  }
  return results;
}

// ── Trend (single store) ──────────────────────────────────────
async function getWeeklyTrend(importId, weeksBack = 12) {
  const weeks = Array.from({ length: weeksBack }, (_, i) => weekRange(weeksBack - 1 - i));
  const unc = weeks.filter(w => !(isWeekCompleted(w) && _weekCache[weekCacheKey(importId, w.start)]));
  if (unc.length <= 1) {
    const trend = weeks.map(w => { const ck = weekCacheKey(importId, w.start); return (isWeekCompleted(w) && _weekCache[ck]) ? _weekCache[ck] : null; });
    if (unc.length === 1) { try { const { orders } = await getOrdersForLocation(importId, unc[0].start, unc[0].end); trend[weeks.findIndex(w => w.start === unc[0].start)] = { week: unc[0], summary: summarizeOrders(orders), error: null }; } catch (e) { trend[weeks.findIndex(w => w.start === unc[0].start)] = { week: unc[0], summary: null, error: e.message }; } }
    return trend;
  }
  const trend = await streamBucketFetch(importId, weeks[0].start, weeks[weeks.length-1].end, weeks);
  for (const e of trend) { if (isWeekCompleted(e.week) && e.summary?.net_sales > 0) _weekCache[weekCacheKey(importId, e.week.start)] = e; }
  saveWeekCache(); return trend;
}

// ── Dashboard ─────────────────────────────────────────────────
async function getDashboardData() {
  const tw = weekRange(0), lw = weekRange(1), td = todayRange(), locs = await getLocations();
  console.log('Dashboard: fetching 7 stores...');
  const sd = [];
  for (const loc of locs) {
    try {
      const lwCK = weekCacheKey(loc.importId, lw.start);
      let lws = (isWeekCompleted(lw) && _weekCache[lwCK]) ? _weekCache[lwCK].summary : null;
      const { orders } = await getOrdersForLocation(loc.importId, tw.start, tw.end);
      const tws = summarizeOrders(orders);
      const tds = summarizeOrders(orders.filter(o => {
        const utc = o.createdAt || o.completedOn || '';
        if (!utc) return false;
        const pacificDate = new Date(utc).toLocaleDateString('en-CA', { timeZone: TZ });
        return pacificDate === td.start;
      }));
      if (!lws) { console.log(`  ${loc.name}: +last week`); const r = await getOrdersForLocation(loc.importId, lw.start, lw.end); lws = summarizeOrders(r.orders); if (isWeekCompleted(lw) && lws.net_sales > 0) { _weekCache[lwCK] = { week: lw, summary: lws, error: null }; saveWeekCache(); } }
      sd.push({ ...loc, thisWeek: tws, lastWeek: lws, today: tds });
      console.log(`  ✓ ${loc.name}: $${tds.net_sales} today`);
    } catch (e) { console.error(`  ✗ ${loc.name}: ${e.message}`); sd.push({ ...loc, thisWeek: null, lastWeek: null, today: null }); }
  }
  return { meta: { fetchedAt: new Date().toISOString(), dateRanges: { thisWeek: tw, lastWeek: lw, today: td, ytd: ytdRange() } }, stores: sd };
}

// ── Other endpoints ───────────────────────────────────────────
async function getAllStoresSales(startDate, endDate) {
  const locs = await getLocations(), r = [];
  for (const loc of locs) { try { const { orders } = await getOrdersForLocation(loc.importId, startDate, endDate); r.push({ store: loc, summary: summarizeOrders(orders), orders }); } catch (e) { r.push({ store: loc, summary: null, orders: [], error: e.message }); } }
  return r;
}

async function getRawOrderSample(importId) {
  const td = todayRange(), data = await flowhubGet(`/v1/orders/findByLocationId/${importId}`, { created_after: td.start, created_before: td.end, page_size: 3, page: 1 });
  return { total: data.total, sample: (data.orders || []).slice(0, 2) };
}

async function getSingleDayVsDay(dow, weeksBack = 4) {
  const locs = await getLocations(), today = todayPacific(), tdow = dowPacific();
  const dn = ['Sunday','Monday','Tuesday','Wednesday','Thursday','Friday','Saturday'];
  const dates = [];
  for (let w = 0; w < weeksBack; w++) { let db = (tdow - dow + 7) % 7; if (db === 0 && w > 0) db = 7*w; else if (w > 0) db += 7*w; const d = addDays(today, -db); if (d <= today) dates.push(d); }
  const dd = [];
  for (const date of dates) { console.log(`  DvD: ${dn[dow]} ${date}`); const sr = []; for (const loc of locs) { try { const { orders } = await getOrdersForLocation(loc.importId, date, date); sr.push({ store: loc, summary: summarizeOrders(orders) }); } catch (e) { sr.push({ store: loc, summary: null, error: e.message }); } } dd.push({ date, stores: sr }); }
  return { dow, dayName: dn[dow], dates: dd };
}

module.exports = { getLocations, getOrdersForLocation, summarizeOrders, extractTopProducts, getAllStoresSales, getWeeklyTrend, getAllStoresWeeklyTrend, getDashboardData, getRawOrderSample, getSingleDayVsDay, weekRange, todayRange, ytdRange, toDateStr, STORE_CONFIG };
