// server/flowhub.js
// ============================================================
// Flowhub API Client — Thrive Cannabis Marketplace
// OPTIMIZED: 1 bulk fetch per store, disk-cached completed weeks
// ============================================================

const fetch = require('node-fetch');
const fs = require('fs');

const BASE = 'https://api.flowhub.co';
const CLIENT_ID = process.env.FLOWHUB_CLIENT_ID;
const CLIENT_KEY = process.env.FLOWHUB_API_KEY;

// ── Store display config ──────────────────────────────────────
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
      headers: {
        'clientId': CLIENT_ID,
        'key': CLIENT_KEY,
        'Accept': 'application/json',
        'Content-Type': 'application/json',
      },
    });

    if (res.status === 429) {
      console.log(`⚠ 429 rate limited: ${path}`);
      continue;
    }
    if (res.status === 500 && attempt < 4) {
      console.log(`⚠ 500 error: ${path}, retrying...`);
      continue;
    }
    if (!res.ok) {
      const body = await res.text().catch(() => '');
      throw new Error(`Flowhub ${res.status} ${path}: ${body.slice(0, 300)}`);
    }
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
    .filter(loc => {
      const rawName = (loc.locationName || loc.name || '').toLowerCase();
      return !EXCLUDED_KEYWORDS.some(ex => rawName.includes(ex));
    })
    .map(loc => {
      const rawName = loc.locationName || loc.name || '';
      const importId = loc.importId || loc.locationId || loc._id || loc.id;
      const cfg = STORE_CONFIG.find(s => rawName.toLowerCase().includes(s.match));
      return {
        importId, rawName,
        name: cfg?.display || rawName,
        id: cfg?.id || rawName.toLowerCase().replace(/[^a-z]+/g, '_'),
        color: cfg?.color || '#888',
      };
    });

  console.log('✓', _locations.length, 'locations:', _locations.map(l => l.name).join(', '));
  return _locations;
}

// ── Date helpers (Pacific Time — Nevada) ──────────────────────
const TZ = 'America/Los_Angeles';

function todayPacific() {
  return new Date().toLocaleDateString('en-CA', { timeZone: TZ });
}

function dowPacific() {
  const dayName = new Date().toLocaleDateString('en-US', { timeZone: TZ, weekday: 'short' });
  return { Sun: 0, Mon: 1, Tue: 2, Wed: 3, Thu: 4, Fri: 5, Sat: 6 }[dayName] ?? 0;
}

function addDays(dateStr, days) {
  const d = new Date(dateStr + 'T12:00:00Z');
  d.setDate(d.getDate() + days);
  return d.toISOString().split('T')[0];
}

function toDateStr(d) {
  const y = d.getFullYear();
  const m = String(d.getMonth() + 1).padStart(2, '0');
  const day = String(d.getDate()).padStart(2, '0');
  return `${y}-${m}-${day}`;
}

function weekRange(weeksBack = 0) {
  const today = todayPacific();
  const dow = dowPacific();
  const daysSinceMonday = (dow + 6) % 7;
  const mondayStr = addDays(today, -daysSinceMonday - (weeksBack * 7));
  const sundayStr = addDays(mondayStr, 6);
  return { start: mondayStr, end: sundayStr };
}

function todayRange() {
  const d = todayPacific();
  return { start: d, end: d };
}

function ytdRange() {
  const today = todayPacific();
  return { start: `${today.split('-')[0]}-01-01`, end: today };
}

// ── Fetch orders (paginated, no artificial rate limiter) ──────
let _schemaLogged = false;

async function getOrdersForLocation(importId, startDate, endDate) {
  const start = startDate.split('T')[0];
  const end = endDate.split('T')[0];
  const PAGE_SIZE = 500;
  let page = 1;
  let allOrders = [];
  let total = 0;

  while (true) {
    try {
      const data = await flowhubGet(`/v1/orders/findByLocationId/${importId}`, {
        created_after: start,
        created_before: end,
        page_size: PAGE_SIZE,
        page,
        order_by: 'asc',
      });

      const batch = data.orders || [];
      total = data.total || 0;
      allOrders = allOrders.concat(batch);

      if (!_schemaLogged && batch.length > 0) {
        _schemaLogged = true;
        const s = batch[0];
        console.log('ORDER KEYS:', Object.keys(s).join(', '));
        if (s.itemsInCart?.[0]) console.log('ITEM KEYS:', Object.keys(s.itemsInCart[0]).join(', '));
      }

      if (allOrders.length >= total || batch.length < PAGE_SIZE) break;
      page++;
    } catch (err) {
      console.error(`✗ ${importId.slice(0, 8)} ${start}→${end} p${page}: ${err.message}`);
      break;
    }
  }

  return { total: allOrders.length, orders: allOrders };
}

// ── Summarize orders → KPIs ───────────────────────────────────
function summarizeOrders(orders) {
  if (!orders || !orders.length) {
    return {
      transaction_count: 0, net_sales: 0, gross_sales: 0,
      avg_basket: 0, total_items: 0,
      categories: [], budtenders: [],
      customer_types: { rec: 0, med: 0 },
    };
  }

  let net_sales = 0, gross_sales = 0, total_items = 0;
  const catMap = {}, btMap = {}, ctypes = { rec: 0, med: 0 };

  orders.forEach(order => {
    if (order.voided === true || order.orderStatus === 'voided') return;
    const ctype = (order.customerType || '').toLowerCase();
    if (ctype.includes('med')) ctypes.med++; else ctypes.rec++;

    const bt = order.budtender || 'Unknown';
    if (!btMap[bt]) btMap[bt] = { name: bt, transactions: 0, net_sales: 0, items: 0 };
    btMap[bt].transactions++;

    let orderNet = 0, orderGross = 0, orderItems = 0;
    (order.itemsInCart || []).forEach(item => {
      if (item.voided === true) return;
      const qty = item.quantity || 1;
      orderItems += qty;
      const lineGross = Number(item.totalPrice) || (Number(item.unitPrice || 0) * qty);
      const discount = Number(item.totalDiscounts) || 0;
      const lineNet = lineGross - discount;
      orderGross += lineGross;
      orderNet += lineNet;

      const cat = item.category || item.type || 'Other';
      if (!catMap[cat]) catMap[cat] = { name: cat, net_sales: 0, units: 0, transactions: 0 };
      catMap[cat].net_sales += lineNet;
      catMap[cat].units += qty;
      catMap[cat].transactions++;
    });

    net_sales += orderNet;
    gross_sales += orderGross;
    total_items += orderItems;
    btMap[bt].net_sales += orderNet;
    btMap[bt].items += orderItems;
  });

  const txnCount = orders.length;
  return {
    transaction_count: txnCount,
    net_sales: round2(net_sales),
    gross_sales: round2(gross_sales),
    avg_basket: round2(txnCount > 0 ? net_sales / txnCount : 0),
    total_items,
    customer_types: ctypes,
    categories: Object.values(catMap).sort((a, b) => b.net_sales - a.net_sales)
      .map(c => ({ ...c, net_sales: round2(c.net_sales) })),
    budtenders: Object.values(btMap)
      .map(b => ({
        ...b,
        net_sales: round2(b.net_sales),
        avg_basket: round2(b.transactions ? b.net_sales / b.transactions : 0),
      }))
      .sort((a, b) => b.net_sales - a.net_sales),
  };
}

function round2(n) { return Math.round(n * 100) / 100; }

// ── Top products ──────────────────────────────────────────────
function extractTopProducts(orders, limit = 15) {
  const map = {};
  orders.forEach(order => {
    if (order.voided === true) return;
    (order.itemsInCart || []).forEach(item => {
      if (item.voided === true) return;
      const name = item.productName || item.title1 || 'Unknown';
      const brand = item.brand || '';
      const cat = item.category || item.type || 'Other';
      const qty = item.quantity || 1;
      const gross = Number(item.totalPrice) || 0;
      const disc = Number(item.totalDiscounts) || 0;
      const net = gross - disc;
      const key = `${name}__${brand}`;
      if (!map[key]) map[key] = { name, brand, category: cat, units_sold: 0, net_sales: 0, prices: [] };
      map[key].units_sold += qty;
      map[key].net_sales += net;
      if (item.unitPrice) map[key].prices.push(Number(item.unitPrice));
    });
  });
  return Object.values(map)
    .map(p => ({
      ...p,
      net_sales: round2(p.net_sales),
      avg_price: p.prices.length ? round2(p.prices.reduce((a, b) => a + b, 0) / p.prices.length) : 0,
    }))
    .sort((a, b) => b.net_sales - a.net_sales)
    .slice(0, limit);
}

// ══════════════════════════════════════════════════════════════
// PERSISTENT DISK CACHE FOR COMPLETED WEEKS
// ══════════════════════════════════════════════════════════════
const CACHE_DIR = process.env.CACHE_DIR || '/tmp';
const CACHE_FILE = `${CACHE_DIR}/thrive-week-cache.json`;
let _weekCache = {};

function loadWeekCache() {
  try {
    if (fs.existsSync(CACHE_FILE)) {
      _weekCache = JSON.parse(fs.readFileSync(CACHE_FILE, 'utf8'));
      console.log(`✓ Loaded ${Object.keys(_weekCache).length} cached weeks from disk`);
    } else {
      console.log('✓ No cache file found, starting fresh');
    }
  } catch (err) {
    console.log('⚠ Cache load failed:', err.message);
    _weekCache = {};
  }
}

let _savePending = false;
function saveWeekCache() {
  if (_savePending) return;
  _savePending = true;
  setTimeout(() => {
    _savePending = false;
    try {
      fs.writeFileSync(CACHE_FILE, JSON.stringify(_weekCache), 'utf8');
      console.log(`✓ Saved ${Object.keys(_weekCache).length} weeks to disk`);
    } catch (err) {
      console.log('⚠ Cache save failed:', err.message);
    }
  }, 3000);
}

loadWeekCache();

function weekCacheKey(importId, weekStart) { return `${importId}:${weekStart}`; }
function isWeekCompleted(week) { return week.end < todayPacific(); }

// ── Bucket orders into week ranges ────────────────────────────
function bucketOrdersByWeek(orders, weeks) {
  const buckets = weeks.map(w => ({ week: w, orders: [] }));
  for (const order of orders) {
    const d = (order.createdAt || order.completedOn || '').split('T')[0];
    for (const b of buckets) {
      if (d >= b.week.start && d <= b.week.end) {
        b.orders.push(order);
        break;
      }
    }
  }
  return buckets.map(b => ({
    week: b.week,
    summary: summarizeOrders(b.orders),
    error: null,
  }));
}

// ══════════════════════════════════════════════════════════════
// BULK FETCH: 1 API call per store for all 12 weeks
// On warm cache: only fetches current week (1 call per store)
// ══════════════════════════════════════════════════════════════
async function getAllStoresWeeklyTrend(weeksBack = 12) {
  const locations = await getLocations();
  const weeks = Array.from({ length: weeksBack }, (_, i) => weekRange(weeksBack - 1 - i));
  const results = [];

  for (const loc of locations) {
    const uncachedWeeks = weeks.filter(w =>
      !(isWeekCompleted(w) && _weekCache[weekCacheKey(loc.importId, w.start)])
    );

    if (uncachedWeeks.length === 0) {
      // Fully cached (rare — current week is never completed)
      const trend = weeks.map(w => _weekCache[weekCacheKey(loc.importId, w.start)]);
      results.push({ store: loc, trend });
      console.log(`  ${loc.name}: fully cached`);
      continue;
    }

    if (uncachedWeeks.length === 1 && !isWeekCompleted(uncachedWeeks[0])) {
      // ★ FAST PATH: only current week needs fetching (1 API call)
      console.log(`  ${loc.name}: 1 fresh week...`);
      const trend = weeks.map(w => {
        const ck = weekCacheKey(loc.importId, w.start);
        if (isWeekCompleted(w) && _weekCache[ck]) return _weekCache[ck];
        return null;
      });

      try {
        const cw = uncachedWeeks[0];
        const { orders } = await getOrdersForLocation(loc.importId, cw.start, cw.end);
        const idx = weeks.findIndex(w => w.start === cw.start);
        trend[idx] = { week: cw, summary: summarizeOrders(orders), error: null };
      } catch (err) {
        console.error(`  ${loc.name} current week error: ${err.message}`);
        const idx = weeks.findIndex(w => w.start === uncachedWeeks[0].start);
        trend[idx] = { week: uncachedWeeks[0], summary: null, error: err.message };
      }

      results.push({ store: loc, trend });
      continue;
    }

    // ★ COLD PATH: bulk fetch entire 12-week range in 1 API call
    console.log(`  ${loc.name}: bulk fetch (${uncachedWeeks.length} weeks uncached)...`);
    try {
      const { orders } = await getOrdersForLocation(
        loc.importId, weeks[0].start, weeks[weeks.length - 1].end
      );
      console.log(`    → ${orders.length} orders fetched`);
      const trend = bucketOrdersByWeek(orders, weeks);

      // Cache all completed weeks
      for (const entry of trend) {
        if (isWeekCompleted(entry.week) && entry.summary?.net_sales > 0) {
          _weekCache[weekCacheKey(loc.importId, entry.week.start)] = entry;
        }
      }
      saveWeekCache();
      results.push({ store: loc, trend });
    } catch (err) {
      console.error(`  ${loc.name} bulk fetch failed: ${err.message}`);
      results.push({
        store: loc,
        trend: weeks.map(w => ({ week: w, summary: null, error: err.message })),
      });
    }
  }

  return results;
}

// Single store trend (for store detail tab)
async function getWeeklyTrend(importId, weeksBack = 12) {
  const weeks = Array.from({ length: weeksBack }, (_, i) => weekRange(weeksBack - 1 - i));
  const uncached = weeks.filter(w =>
    !(isWeekCompleted(w) && _weekCache[weekCacheKey(importId, w.start)])
  );

  if (uncached.length <= 1) {
    // Fast path: only current week
    const trend = weeks.map(w => {
      const ck = weekCacheKey(importId, w.start);
      if (isWeekCompleted(w) && _weekCache[ck]) return _weekCache[ck];
      return null;
    });
    if (uncached.length === 1) {
      try {
        const { orders } = await getOrdersForLocation(importId, uncached[0].start, uncached[0].end);
        const idx = weeks.findIndex(w => w.start === uncached[0].start);
        trend[idx] = { week: uncached[0], summary: summarizeOrders(orders), error: null };
      } catch (err) {
        const idx = weeks.findIndex(w => w.start === uncached[0].start);
        trend[idx] = { week: uncached[0], summary: null, error: err.message };
      }
    }
    return trend;
  }

  // Bulk fetch
  const { orders } = await getOrdersForLocation(importId, weeks[0].start, weeks[weeks.length - 1].end);
  const trend = bucketOrdersByWeek(orders, weeks);
  for (const entry of trend) {
    if (isWeekCompleted(entry.week) && entry.summary?.net_sales > 0) {
      _weekCache[weekCacheKey(importId, entry.week.start)] = entry;
    }
  }
  saveWeekCache();
  return trend;
}

// ══════════════════════════════════════════════════════════════
// DASHBOARD: 1 API call per store (this week), last week from cache
// ══════════════════════════════════════════════════════════════
async function getDashboardData() {
  const tw = weekRange(0);
  const lw = weekRange(1);
  const td = todayRange();
  const locations = await getLocations();

  console.log('Dashboard: fetching 7 stores...');
  const storeData = [];

  for (const loc of locations) {
    try {
      // Last week: use disk cache if available
      const lwCK = weekCacheKey(loc.importId, lw.start);
      let lwSummary = null;
      if (isWeekCompleted(lw) && _weekCache[lwCK]) {
        lwSummary = _weekCache[lwCK].summary;
      }

      // This week: always fetch fresh (1 API call — includes today)
      const { orders } = await getOrdersForLocation(loc.importId, tw.start, tw.end);
      const twSummary = summarizeOrders(orders);

      // Today: filter from this week's orders (no extra API call)
      const todayOrders = orders.filter(o =>
        (o.createdAt || '').split('T')[0] === td.start
      );
      const todaySummary = summarizeOrders(todayOrders);

      // If last week not cached, fetch it (1 extra call)
      if (!lwSummary) {
        console.log(`  ${loc.name}: fetching last week (not cached)...`);
        const lwr = await getOrdersForLocation(loc.importId, lw.start, lw.end);
        lwSummary = summarizeOrders(lwr.orders);
        if (isWeekCompleted(lw) && lwSummary.net_sales > 0) {
          _weekCache[lwCK] = { week: lw, summary: lwSummary, error: null };
          saveWeekCache();
        }
      }

      storeData.push({
        ...loc,
        thisWeek: twSummary,
        lastWeek: lwSummary,
        today: todaySummary,
      });
      console.log(`  ✓ ${loc.name}: today=$${todaySummary.net_sales} tw=$${twSummary.net_sales}`);
    } catch (err) {
      console.error(`  ✗ ${loc.name}: ${err.message}`);
      storeData.push({ ...loc, thisWeek: null, lastWeek: null, today: null });
    }
  }

  return {
    meta: {
      fetchedAt: new Date().toISOString(),
      dateRanges: { thisWeek: tw, lastWeek: lw, today: td, ytd: ytdRange() },
    },
    stores: storeData,
  };
}

// ── getAllStoresSales (custom range tab) ───────────────────────
async function getAllStoresSales(startDate, endDate) {
  const locations = await getLocations();
  const results = [];
  for (const loc of locations) {
    try {
      const { orders } = await getOrdersForLocation(loc.importId, startDate, endDate);
      results.push({ store: loc, summary: summarizeOrders(orders), orders });
    } catch (err) {
      results.push({ store: loc, summary: null, orders: [], error: err.message });
    }
  }
  return results;
}

// ── Diagnostic ────────────────────────────────────────────────
async function getRawOrderSample(importId) {
  const td = todayRange();
  const data = await flowhubGet(`/v1/orders/findByLocationId/${importId}`, {
    created_after: td.start,
    created_before: td.end,
    page_size: 3,
    page: 1,
  });
  return { total: data.total, sample: (data.orders || []).slice(0, 2) };
}

// ── Day vs Day (Pacific timezone) ─────────────────────────────
async function getSingleDayVsDay(dow, weeksBack = 4) {
  const locations = await getLocations();
  const today = todayPacific();
  const todayDow = dowPacific();
  const dayNames = ['Sunday', 'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday'];

  const dates = [];
  for (let w = 0; w < weeksBack; w++) {
    let daysBack = (todayDow - dow + 7) % 7;
    if (daysBack === 0 && w > 0) daysBack = 7 * w;
    else if (w > 0) daysBack += 7 * w;
    const ds = addDays(today, -daysBack);
    if (ds <= today) dates.push(ds);
  }

  const dayData = [];
  for (const date of dates) {
    console.log(`  DvD: ${dayNames[dow]} ${date}...`);
    const storeResults = [];
    for (const loc of locations) {
      try {
        const { orders } = await getOrdersForLocation(loc.importId, date, date);
        storeResults.push({ store: loc, summary: summarizeOrders(orders) });
      } catch (err) {
        storeResults.push({ store: loc, summary: null, error: err.message });
      }
    }
    dayData.push({ date, stores: storeResults });
  }

  return { dow, dayName: dayNames[dow], dates: dayData };
}

module.exports = {
  getLocations, getOrdersForLocation, summarizeOrders, extractTopProducts,
  getAllStoresSales, getWeeklyTrend, getAllStoresWeeklyTrend, getDashboardData,
  getRawOrderSample, getSingleDayVsDay,
  weekRange, todayRange, ytdRange, toDateStr, STORE_CONFIG,
};
