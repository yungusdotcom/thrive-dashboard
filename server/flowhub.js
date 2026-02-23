// server/flowhub.js
// ============================================================
// Flowhub API Client — built from official Stoplight docs
// Auth:    POST https://flowhub.auth0.com/oauth/token
// Base:    https://api.flowhub.co
// Headers: clientId + key on every request
// Sales:   GET /v1/orders/findByLocationId/{importId}
// Locs:    GET /v0/clientsLocations
// ============================================================

const fetch = require('node-fetch');

const BASE       = 'https://api.flowhub.co';
const AUTH_URL   = 'https://flowhub.auth0.com/oauth/token';
const CLIENT_ID  = process.env.FLOWHUB_CLIENT_ID;
const CLIENT_KEY = process.env.FLOWHUB_API_KEY;
const AUDIENCE   = 'https://api.flowhub.co/';

// Store color/id config — matched by name to Flowhub location names
const STORE_CONFIG = [
  { id: 'cactus',   name: 'Cactus',      color: '#00e5a0' },
  { id: 'cheyenne', name: 'Cheyenne',    color: '#4db8ff' },
  { id: 'jackpot',  name: 'Jackpot',     color: '#c084fc' },
  { id: 'main',     name: 'Main Street', color: '#ffd166' },
  { id: 'reno',     name: 'Reno',        color: '#ff8c42' },
  { id: 'sahara',   name: 'Sahara',      color: '#ff4d6d' },
  { id: 'sammy',    name: 'Sammy',       color: '#a8e6cf' },
];

const EXCLUDED_STORES = ['Smoke & Mirrors', 'MBNV'];

// ── Auth token cache ─────────────────────────────────────────
let _token    = null;
let _tokenExp = 0;

async function getToken() {
  if (_token && Date.now() < _tokenExp - 60000) return _token;

  const res = await fetch(AUTH_URL, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      grant_type:    'client_credentials',
      client_id:     CLIENT_ID,
      client_secret: CLIENT_KEY,
      audience:      AUDIENCE,
    }),
  });

  if (!res.ok) {
    const body = await res.text();
    throw new Error(`Auth0 failed ${res.status}: ${body}`);
  }

  const data = await res.json();
  _token    = data.access_token;
  _tokenExp = Date.now() + (data.expires_in * 1000);
  console.log(`✓ Flowhub token refreshed — expires in ${data.expires_in}s`);
  return _token;
}

// ── Core GET helper ──────────────────────────────────────────
async function flowhubGet(path, params = {}) {
  const token = await getToken();
  const url   = new URL(`${BASE}${path}`);

  Object.entries(params).forEach(([k, v]) => {
    if (v !== undefined && v !== null) url.searchParams.set(k, String(v));
  });

  const res = await fetch(url.toString(), {
    headers: {
      'Authorization': `Bearer ${token}`,
      'clientId':      CLIENT_ID,
      'key':           CLIENT_KEY,
      'Accept':        'application/json',
      'Content-Type':  'application/json',
    },
  });

  if (!res.ok) {
    const body = await res.text().catch(() => '');
    throw new Error(`Flowhub ${res.status} ${path}: ${body.slice(0, 300)}`);
  }

  return res.json();
}

// ── Locations (cached) ───────────────────────────────────────
let _locations = null;

async function getLocations() {
  if (_locations) return _locations;

  const data = await flowhubGet('/v0/clientsLocations');
  const raw  = Array.isArray(data) ? data : (data.locations || data.data || []);

  _locations = raw
    .filter(loc => !EXCLUDED_STORES.some(ex =>
      (loc.name || '').toLowerCase().includes(ex.toLowerCase())
    ))
    .map(loc => {
      const cfg = STORE_CONFIG.find(s =>
        (loc.name || '').toLowerCase().includes(s.name.toLowerCase()) ||
        s.name.toLowerCase().includes((loc.name || '').toLowerCase())
      );
      return {
        importId: loc.importId || loc._id || loc.id,
        name:     loc.name,
        id:       cfg?.id    || (loc.name || '').toLowerCase().replace(/\s+/g, '_'),
        color:    cfg?.color || '#888888',
      };
    });

  console.log(`✓ ${_locations.length} locations:`, _locations.map(l => `${l.name}(${l.importId})`).join(', '));
  return _locations;
}

// ── Date helpers ─────────────────────────────────────────────
function toDateStr(d) { return d.toISOString().split('T')[0]; }

function weekRange(weeksBack = 0) {
  const now = new Date();
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
  const d = toDateStr(new Date());
  return { start: d, end: d };
}

function ytdRange() {
  return { start: `${new Date().getFullYear()}-01-01`, end: toDateStr(new Date()) };
}

// ── Fetch ALL orders for a location in a date range ──────────
// Auto-paginates using page_size=10000 (Flowhub max)
async function getOrdersForLocation(importId, startDate, endDate) {
  const PAGE_SIZE = 10000;
  let page      = 1;
  let allOrders = [];
  let total     = 0;

  while (true) {
    const data = await flowhubGet(`/v1/orders/findByLocationId/${importId}`, {
      created_after:  startDate,
      created_before: endDate,
      page_size:      PAGE_SIZE,
      page,
      order_by:       'asc',
    });

    const batch = data.orders || [];
    total       = data.total  || 0;
    allOrders   = allOrders.concat(batch);

    if (allOrders.length >= total || batch.length < PAGE_SIZE) break;
    page++;
  }

  return { total, orders: allOrders };
}

// ── Compute sales summary from raw order array ───────────────
// Orders contain itemsInCart — each item has price, qty, category, discounts
function summarizeOrders(orders) {
  if (!orders || !orders.length) {
    return {
      transaction_count: 0, net_sales: 0, gross_sales: 0,
      avg_basket: 0, total_items: 0,
      categories: [], budtenders: [], customer_types: { rec: 0, med: 0 },
    };
  }

  let net_sales   = 0;
  let gross_sales = 0;
  let total_items = 0;
  const catMap = {};
  const btMap  = {};
  const ctypes = { rec: 0, med: 0 };

  orders.forEach(order => {
    // Customer type
    const ctype = (order.customerType || '').toLowerCase();
    if (ctype.includes('med')) ctypes.med++;
    else ctypes.rec++;

    // Budtender
    const bt = order.budtender || order.fulfilledBy || order.fullName || 'Unknown';
    if (!btMap[bt]) btMap[bt] = { name: bt, transactions: 0, net_sales: 0, items: 0 };
    btMap[bt].transactions++;

    // Line items
    (order.itemsInCart || []).forEach(item => {
      total_items++;

      const qty       = item.quantity  || 1;
      const unitPrice = item.price     || item.unitPrice   || 0;
      const lineGross = item.lineTotal || item.totalPrice  || (unitPrice * qty);
      const discount  = (item.itemDiscounts || [])
        .reduce((sum, d) => sum + (d.discountAmount || 0), 0);
      const lineNet   = lineGross - discount;

      net_sales   += lineNet;
      gross_sales += lineGross;

      btMap[bt].net_sales += lineNet;
      btMap[bt].items     += qty;

      const cat = item.category || item.productType || 'Other';
      if (!catMap[cat]) catMap[cat] = { name: cat, net_sales: 0, units: 0, transactions: 0 };
      catMap[cat].net_sales    += lineNet;
      catMap[cat].units        += qty;
      catMap[cat].transactions++;
    });
  });

  const txnCount  = orders.length;
  const avgBasket = txnCount > 0 ? net_sales / txnCount : 0;

  return {
    transaction_count: txnCount,
    net_sales:         round2(net_sales),
    gross_sales:       round2(gross_sales),
    avg_basket:        round2(avgBasket),
    total_items,
    customer_types:    ctypes,
    categories: Object.values(catMap).sort((a, b) => b.net_sales - a.net_sales),
    budtenders:  Object.values(btMap)
      .map(b => ({ ...b, avg_basket: round2(b.transactions ? b.net_sales / b.transactions : 0) }))
      .sort((a, b) => b.net_sales - a.net_sales),
  };
}

function round2(n) { return Math.round(n * 100) / 100; }

// ── Top products from raw orders ─────────────────────────────
function extractTopProducts(orders, limit = 15) {
  const map = {};
  orders.forEach(order => {
    (order.itemsInCart || []).forEach(item => {
      const name   = item.name       || item.productName || 'Unknown';
      const brand  = item.brand      || item.brandName   || '';
      const cat    = item.category   || 'Other';
      const qty    = item.quantity   || 1;
      const price  = item.price      || item.unitPrice   || 0;
      const gross  = item.lineTotal  || (price * qty);
      const disc   = (item.itemDiscounts || []).reduce((s,d) => s + (d.discountAmount||0), 0);
      const net    = gross - disc;
      const key    = `${name}__${brand}`;

      if (!map[key]) map[key] = { name, brand, category: cat, units_sold: 0, net_sales: 0, prices: [] };
      map[key].units_sold += qty;
      map[key].net_sales  += net;
      if (price) map[key].prices.push(price);
    });
  });

  return Object.values(map)
    .map(p => ({
      name:       p.name,
      brand:      p.brand,
      category:   p.category,
      units_sold: p.units_sold,
      net_sales:  round2(p.net_sales),
      avg_price:  p.prices.length ? round2(p.prices.reduce((a,b)=>a+b,0)/p.prices.length) : 0,
    }))
    .sort((a, b) => b.net_sales - a.net_sales)
    .slice(0, limit);
}

// ── All stores in parallel ────────────────────────────────────
async function getAllStoresSales(startDate, endDate) {
  const locations = await getLocations();

  const results = await Promise.allSettled(
    locations.map(async loc => {
      const { orders } = await getOrdersForLocation(loc.importId, startDate, endDate);
      return { store: loc, summary: summarizeOrders(orders), orders };
    })
  );

  return results.map((r, i) => ({
    store:   locations[i],
    summary: r.status === 'fulfilled' ? r.value.summary : null,
    orders:  r.status === 'fulfilled' ? r.value.orders  : [],
    error:   r.status === 'rejected'  ? r.reason?.message : null,
  }));
}

// ── Weekly trend for one store ────────────────────────────────
async function getWeeklyTrend(importId, weeksBack = 12) {
  const weeks = Array.from({ length: weeksBack }, (_, i) => weekRange(weeksBack - 1 - i));

  const results = await Promise.allSettled(
    weeks.map(async w => {
      const { orders } = await getOrdersForLocation(importId, w.start, w.end);
      return { week: w, summary: summarizeOrders(orders) };
    })
  );

  return results.map((r, i) => ({
    week:    weeks[i],
    summary: r.status === 'fulfilled' ? r.value.summary : null,
    error:   r.status === 'rejected'  ? r.reason?.message : null,
  }));
}

// ── Weekly trend for ALL stores in parallel ───────────────────
async function getAllStoresWeeklyTrend(weeksBack = 12) {
  const locations = await getLocations();

  const results = await Promise.allSettled(
    locations.map(loc => getWeeklyTrend(loc.importId, weeksBack))
  );

  return results.map((r, i) => ({
    store: locations[i],
    trend: r.status === 'fulfilled' ? r.value : [],
    error: r.status === 'rejected'  ? r.reason?.message : null,
  }));
}

// ── Full dashboard data ───────────────────────────────────────
async function getDashboardData() {
  const tw = weekRange(0);
  const lw = weekRange(1);
  const td = todayRange();

  const [thisWeek, lastWeek, todayData] = await Promise.all([
    getAllStoresSales(tw.start, tw.end),
    getAllStoresSales(lw.start, lw.end),
    getAllStoresSales(td.start, td.end),
  ]);

  const locations = await getLocations();

  return {
    meta: {
      fetchedAt:  new Date().toISOString(),
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

module.exports = {
  getToken,
  getLocations,
  getOrdersForLocation,
  summarizeOrders,
  extractTopProducts,
  getAllStoresSales,
  getWeeklyTrend,
  getAllStoresWeeklyTrend,
  getDashboardData,
  weekRange,
  todayRange,
  ytdRange,
  toDateStr,
  STORE_CONFIG,
};
