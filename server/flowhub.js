// server/flowhub.js
// ============================================================
// Flowhub API Client — Thrive Cannabis Marketplace
// Auth: key + clientId headers (no OAuth)
// Base: https://api.flowhub.co
// ============================================================

const fetch = require('node-fetch');

const BASE   = 'https://api.flowhub.co';
const CLIENT_ID  = process.env.FLOWHUB_CLIENT_ID;
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

// ── Core GET ──────────────────────────────────────────────────
async function flowhubGet(path, params = {}) {
  const url = new URL(`${BASE}${path}`);
  Object.entries(params).forEach(([k, v]) => {
    if (v !== undefined && v !== null) url.searchParams.set(k, String(v));
  });

  // Retry loop with exponential backoff for 429/500
  for (let attempt = 0; attempt < 4; attempt++) {
    if (attempt > 0) {
      const delay = Math.min(2000 * Math.pow(2, attempt), 15000);
      console.log(`  ↻ retry #${attempt} in ${delay}ms...`);
      await new Promise(r => setTimeout(r, delay));
    }

    const res = await fetch(url.toString(), {
      headers: {
        'clientId':     CLIENT_ID,
        'key':          CLIENT_KEY,
        'Accept':       'application/json',
        'Content-Type': 'application/json',
      },
    });

    if (res.status === 429) {
      console.log(`⚠ 429 rate limited: ${path}`);
      continue; // retry with backoff
    }

    if (res.status === 500 && attempt < 3) {
      console.log(`⚠ 500 server error: ${path}, retrying...`);
      continue; // retry with backoff
    }

    if (!res.ok) {
      const body = await res.text().catch(() => '');
      throw new Error(`Flowhub ${res.status} ${path}: ${body.slice(0, 500)}`);
    }
    return res.json();
  }

  throw new Error(`Flowhub: max retries exceeded for ${path}`);
}

// ── Locations ─────────────────────────────────────────────────
let _locations = null;

async function getLocations() {
  if (_locations) return _locations;

  const data = await flowhubGet('/v0/clientsLocations');
  const raw = Array.isArray(data) ? data : (data.locations || data.data || []);

  if (raw.length > 0) {
    console.log('RAW LOCATION KEYS:', Object.keys(raw[0]).join(', '));
  }

  _locations = raw
    .filter(loc => {
      const rawName = (loc.locationName || loc.name || '').toLowerCase();
      return !EXCLUDED_KEYWORDS.some(ex => rawName.includes(ex));
    })
    .map(loc => {
      const rawName = loc.locationName || loc.name || '';
      const importId = loc.importId || loc.locationId || loc._id || loc.id;

      // Match to our config
      const cfg = STORE_CONFIG.find(s =>
        rawName.toLowerCase().includes(s.match)
      );

      return {
        importId,
        rawName,
        name:  cfg?.display || rawName,
        id:    cfg?.id || rawName.toLowerCase().replace(/[^a-z]+/g, '_'),
        color: cfg?.color || '#888',
      };
    });

  console.log('✓', _locations.length, 'locations:',
    _locations.map(l => `${l.name}(${l.importId})`).join(', '));

  return _locations;
}

// ── Date helpers (Pacific Time — Nevada stores) ──────────────
const TZ = 'America/Los_Angeles';

function toDateStr(d) {
  // Format as yyyy-mm-dd in Pacific time
  return d.toLocaleDateString('en-CA', { timeZone: TZ }); // en-CA gives yyyy-mm-dd
}

function nowPacific() {
  // Get current date/time components in Pacific
  const parts = new Intl.DateTimeFormat('en-US', {
    timeZone: TZ,
    year: 'numeric', month: '2-digit', day: '2-digit',
    hour: '2-digit', minute: '2-digit', second: '2-digit', hour12: false,
  }).formatToParts(new Date());
  const get = (type) => parts.find(p => p.type === type)?.value;
  return new Date(`${get('year')}-${get('month')}-${get('day')}T${get('hour')}:${get('minute')}:${get('second')}`);
}

function weekRange(weeksBack = 0) {
  const now = nowPacific();
  const dow = now.getDay();
  const mon = new Date(now);
  mon.setDate(now.getDate() - ((dow + 6) % 7) - weeksBack * 7);
  mon.setHours(0, 0, 0, 0);
  const sun = new Date(mon);
  sun.setDate(mon.getDate() + 6);
  sun.setHours(23, 59, 59, 999);
  return { start: toDateStr(mon), end: toDateStr(sun) };
}

function todayRange() {
  const d = toDateStr(nowPacific());
  return { start: d, end: d };
}

function ytdRange() {
  const now = nowPacific();
  return { start: `${now.getFullYear()}-01-01`, end: toDateStr(now) };
}

// ── Fetch orders ──────────────────────────────────────────────
let _schemaLogged = false;

// Global rate limiter: ensure minimum gap between ANY Flowhub API call
let _lastApiCall = 0;
const MIN_API_GAP_MS = 400; // ~2.5 req/sec max

async function rateLimitedGet(path, params) {
  const now = Date.now();
  const wait = Math.max(0, MIN_API_GAP_MS - (now - _lastApiCall));
  if (wait > 0) await sleep(wait);
  _lastApiCall = Date.now();
  return flowhubGet(path, params);
}

async function getOrdersForLocation(importId, startDate, endDate) {
  const start = startDate.split('T')[0];
  const end   = endDate.split('T')[0];
  const PAGE_SIZE = 500;
  let page = 1;
  let allOrders = [];
  let total = 0;

  while (true) {
    try {
      const data = await rateLimitedGet(`/v1/orders/findByLocationId/${importId}`, {
        created_after:  start,
        created_before: end,
        page_size:      PAGE_SIZE,
        page,
        order_by:       'asc',
      });

      const batch = data.orders || [];
      total = data.total || 0;
      allOrders = allOrders.concat(batch);

      // Log schema once
      if (!_schemaLogged && batch.length > 0) {
        _schemaLogged = true;
        const sample = batch[0];
        console.log('\n═══ ORDER SCHEMA DISCOVERY ═══');
        console.log('ORDER KEYS:', Object.keys(sample).join(', '));
        for (const [k, v] of Object.entries(sample)) {
          if (k === 'itemsInCart') continue;
          const val = typeof v === 'object' ? JSON.stringify(v) : String(v);
          console.log(`  ${k}: ${val.slice(0, 120)}`);
        }
        if (sample.itemsInCart && sample.itemsInCart.length > 0) {
          const item = sample.itemsInCart[0];
          console.log('\nITEM KEYS:', Object.keys(item).join(', '));
          for (const [k, v] of Object.entries(item)) {
            const val = typeof v === 'object' ? JSON.stringify(v) : String(v);
            console.log(`  ${k}: ${val.slice(0, 200)}`);
          }
        }
        console.log('═══ END SCHEMA ═══\n');
      }

      if (allOrders.length >= total || batch.length < PAGE_SIZE) break;
      page++;
    } catch (err) {
      console.error(`✗ Fetch error ${importId.slice(0,8)} ${start}→${end} p${page}: ${err.message}`);
      break; // return what we have
    }
  }

  if (allOrders.length === 0 && total === 0) {
    console.log(`⚠ 0 orders: ${importId.slice(0,8)} ${start}→${end}`);
  }

  return { total: allOrders.length, orders: allOrders };
}

// ── Summarize orders → KPIs ───────────────────────────────────
// Flowhub schema (confirmed from live API):
//   Order: { budtender, customerType, orderStatus, totals: { finalTotal, subTotal, totalDiscounts, totalFees, totalTaxes } }
//   Item:  { totalPrice (gross), totalDiscounts (flat number), totalCost, unitPrice, quantity, category, brand, productName, type }
//   Pre-tax net sales per item = totalPrice - totalDiscounts
function summarizeOrders(orders) {
  if (!orders || !orders.length) {
    return {
      transaction_count: 0, net_sales: 0, gross_sales: 0,
      avg_basket: 0, total_items: 0,
      categories: [], budtenders: [],
      customer_types: { rec: 0, med: 0 },
    };
  }

  let net_sales = 0;
  let gross_sales = 0;
  let total_items = 0;
  const catMap = {};
  const btMap = {};
  const ctypes = { rec: 0, med: 0 };

  orders.forEach(order => {
    // Skip voided orders
    if (order.voided === true || order.orderStatus === 'voided') return;

    // Customer type
    const ctype = (order.customerType || '').toLowerCase();
    if (ctype.includes('med')) ctypes.med++;
    else ctypes.rec++;

    // Budtender
    const bt = order.budtender || 'Unknown';
    if (!btMap[bt]) btMap[bt] = { name: bt, transactions: 0, net_sales: 0, items: 0 };
    btMap[bt].transactions++;

    let orderNet = 0;
    let orderGross = 0;
    let orderItems = 0;

    // Iterate items — use item-level totalPrice and totalDiscounts
    (order.itemsInCart || []).forEach(item => {
      if (item.voided === true) return;

      const qty = item.quantity || 1;
      orderItems += qty;

      // Gross = totalPrice (this is unitPrice × quantity, before discounts)
      const lineGross = Number(item.totalPrice) || (Number(item.unitPrice || 0) * qty);

      // Discount = totalDiscounts (flat number on item, NOT an array)
      const discount = Number(item.totalDiscounts) || 0;

      // Net = gross - discounts (pre-tax)
      const lineNet = lineGross - discount;

      orderGross += lineGross;
      orderNet += lineNet;

      // Category breakdown
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
  const avgBasket = txnCount > 0 ? net_sales / txnCount : 0;

  return {
    transaction_count: txnCount,
    net_sales:   round2(net_sales),
    gross_sales: round2(gross_sales),
    avg_basket:  round2(avgBasket),
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
      const name  = item.productName || item.title1 || 'Unknown';
      const brand = item.brand || '';
      const cat   = item.category || item.type || 'Other';
      const qty   = item.quantity || 1;
      const gross = Number(item.totalPrice) || 0;
      const disc  = Number(item.totalDiscounts) || 0;
      const net   = gross - disc;

      const key = `${name}__${brand}`;
      if (!map[key]) map[key] = { name, brand, category: cat, units_sold: 0, net_sales: 0, gross_sales: 0, prices: [] };
      map[key].units_sold += qty;
      map[key].net_sales += net;
      map[key].gross_sales += gross;
      if (item.unitPrice) map[key].prices.push(Number(item.unitPrice));
    });
  });

  return Object.values(map)
    .map(p => ({
      name: p.name, brand: p.brand, category: p.category,
      units_sold: p.units_sold,
      net_sales: round2(p.net_sales),
      avg_price: p.prices.length ? round2(p.prices.reduce((a, b) => a + b, 0) / p.prices.length) : 0,
    }))
    .sort((a, b) => b.net_sales - a.net_sales)
    .slice(0, limit);
}

// ── All stores — sequential to avoid rate limiting ────────────
async function getAllStoresSales(startDate, endDate) {
  const locations = await getLocations();
  const results = [];
  for (const loc of locations) {
    try {
      const { orders } = await getOrdersForLocation(loc.importId, startDate, endDate);
      results.push({ store: loc, summary: summarizeOrders(orders), orders });
    } catch (err) {
      console.error(`getAllStoresSales error for ${loc.name}:`, err.message);
      results.push({ store: loc, summary: null, orders: [], error: err.message });
    }
  }
  return results;
}

// ── Permanent cache for completed weeks (never changes) ──────
// Key format: "locationId:weekStart" → { week, summary }
const _weekCache = {};

function weekCacheKey(importId, weekStart) {
  return `${importId}:${weekStart}`;
}

// Check if a week is completed (its end date is before today)
function isWeekCompleted(week) {
  const today = toDateStr(new Date());
  return week.end < today;
}

// ── Weekly trend — uses permanent cache for old weeks ────────
async function getWeeklyTrend(importId, weeksBack = 12) {
  const weeks = Array.from({ length: weeksBack }, (_, i) => weekRange(weeksBack - 1 - i));
  const results = [];

  for (const w of weeks) {
    const cacheKey = weekCacheKey(importId, w.start);

    // If completed week is in cache, use it instantly
    if (isWeekCompleted(w) && _weekCache[cacheKey]) {
      results.push(_weekCache[cacheKey]);
      continue;
    }

    // Otherwise fetch it
    try {
      const { orders } = await getOrdersForLocation(importId, w.start, w.end);
      const entry = { week: w, summary: summarizeOrders(orders), error: null };
      results.push(entry);

      // Permanently cache completed weeks
      if (isWeekCompleted(w) && entry.summary && entry.summary.net_sales > 0) {
        _weekCache[cacheKey] = entry;
      }
    } catch (err) {
      console.error(`Trend error week ${w.start}: ${err.message}`);
      results.push({ week: w, summary: null, error: err.message });
    }
  }

  return results;
}

function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

async function getAllStoresWeeklyTrend(weeksBack = 12) {
  const locations = await getLocations();
  const results = [];

  for (const loc of locations) {
    try {
      // Count how many weeks are already cached for this store
      const weeks = Array.from({ length: weeksBack }, (_, i) => weekRange(weeksBack - 1 - i));
      const cached = weeks.filter(w => _weekCache[weekCacheKey(loc.importId, w.start)] && isWeekCompleted(w)).length;
      const toFetch = weeksBack - cached;
      console.log(`  Fetching trend: ${loc.name} (${cached} cached, ${toFetch} to fetch)...`);

      const trend = await getWeeklyTrend(loc.importId, weeksBack);
      results.push({ store: loc, trend });
    } catch (err) {
      console.error(`Trend failed for ${loc.name}:`, err.message);
      results.push({ store: loc, trend: [], error: err.message });
    }
  }

  return results;
}

// ── Full dashboard payload ────────────────────────────────────
async function getDashboardData() {
  const tw = weekRange(0);
  const lw = weekRange(1);
  const td = todayRange();

  // Sequential to avoid rate limiting
  console.log('Fetching today...');
  const todayData = await getAllStoresSales(td.start, td.end);
  console.log('Fetching this week...');
  const thisWeek = await getAllStoresSales(tw.start, tw.end);
  console.log('Fetching last week...');
  const lastWeek = await getAllStoresSales(lw.start, lw.end);

  const locations = await getLocations();

  return {
    meta: {
      fetchedAt: new Date().toISOString(),
      dateRanges: { thisWeek: tw, lastWeek: lw, today: td, ytd: ytdRange() },
    },
    stores: locations.map((loc, i) => ({
      ...loc,
      thisWeek: thisWeek[i]?.summary || null,
      lastWeek: lastWeek[i]?.summary || null,
      today:    todayData[i]?.summary || null,
    })),
  };
}

// ── Diagnostic: raw order sample ──────────────────────────────
async function getRawOrderSample(importId) {
  const td = todayRange();
  const data = await flowhubGet(`/v1/orders/findByLocationId/${importId}`, {
    created_after:  td.start,
    created_before: td.end,
    page_size:      3,
    page:           1,
  });
  return {
    total: data.total,
    sample: (data.orders || []).slice(0, 2),
  };
}

// ── Day vs Day: last N occurrences of each weekday ───────────
// Returns data for a specific weekday (0=Sun..6=Sat) going back N weeks
async function getDayVsDayData(weeksBack = 4) {
  const locations = await getLocations();
  const now = new Date();
  const todayDow = now.getDay(); // 0=Sun, 1=Mon, ...6=Sat

  // Build array of days to query: for each weekday, get the last `weeksBack` occurrences
  const dayNames = ['Sunday','Monday','Tuesday','Wednesday','Thursday','Friday','Saturday'];
  const results = {};

  for (let dow = 0; dow < 7; dow++) {
    // Find the last `weeksBack` occurrences of this weekday
    const dates = [];
    for (let w = 0; w < weeksBack; w++) {
      const d = new Date(now);
      // How many days ago was the most recent occurrence of this dow?
      let daysBack = (todayDow - dow + 7) % 7;
      if (daysBack === 0 && dow !== todayDow) daysBack = 7; // if same dow but we want previous
      if (dow === todayDow && w === 0) daysBack = 0; // today
      else if (dow === todayDow) daysBack = w * 7; // previous same weekday
      else daysBack = daysBack + (w * 7);

      d.setDate(now.getDate() - daysBack);
      dates.push(toDateStr(d));
    }

    // Only include days that are in the past or today
    const validDates = dates.filter(dt => dt <= toDateStr(now));

    // For each date, fetch all stores sequentially
    const dayData = [];
    for (const date of validDates) {
      console.log(`  Day vs Day: ${dayNames[dow]} ${date}...`);
      const storeResults = [];
      for (const loc of locations) {
        try {
          const { orders } = await getOrdersForLocation(loc.importId, date, date);
          storeResults.push({ store: loc, summary: summarizeOrders(orders) });
        } catch (err) {
          storeResults.push({ store: loc, summary: null, error: err.message });
        }
        await sleep(50); // small delay
      }
      dayData.push({ date, stores: storeResults });
    }

    results[dow] = { dayName: dayNames[dow], dates: dayData };
  }

  return results;
}

// Lighter version: just one specific weekday
async function getSingleDayVsDay(dow, weeksBack = 4) {
  const locations = await getLocations();
  const now = new Date();
  const todayDow = now.getDay();
  const dayNames = ['Sunday','Monday','Tuesday','Wednesday','Thursday','Friday','Saturday'];

  const dates = [];
  for (let w = 0; w < weeksBack; w++) {
    const d = new Date(now);
    let daysBack = (todayDow - dow + 7) % 7;
    if (daysBack === 0 && w > 0) daysBack = 7 * w;
    else if (w > 0) daysBack += 7 * w;
    d.setDate(now.getDate() - daysBack);
    const ds = toDateStr(d);
    if (ds <= toDateStr(now)) dates.push(ds);
  }

  const dayData = [];
  for (const date of dates) {
    console.log(`  Day vs Day: ${dayNames[dow]} ${date}...`);
    const storeResults = [];
    for (const loc of locations) {
      try {
        const { orders } = await getOrdersForLocation(loc.importId, date, date);
        storeResults.push({ store: loc, summary: summarizeOrders(orders) });
      } catch (err) {
        storeResults.push({ store: loc, summary: null, error: err.message });
      }
      await sleep(50);
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
