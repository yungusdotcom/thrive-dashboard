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

  const res = await fetch(url.toString(), {
    headers: {
      'clientId':     CLIENT_ID,
      'key':          CLIENT_KEY,
      'Accept':       'application/json',
      'Content-Type': 'application/json',
    },
  });

  if (!res.ok) {
    const body = await res.text().catch(() => '');
    throw new Error(`Flowhub ${res.status} ${path}: ${body.slice(0, 500)}`);
  }
  return res.json();
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

// ── Date helpers ──────────────────────────────────────────────
function toDateStr(d) {
  return d.toISOString().split('T')[0];
}

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

// ── Fetch orders ──────────────────────────────────────────────
let _schemaLogged = false;
let _dateParamLogged = false;

async function getOrdersForLocation(importId, startDate, endDate) {
  const PAGE_SIZE = 10000;
  let page = 1;
  let allOrders = [];
  let total = 0;

  // Flowhub expects ISO datetime strings, not just dates
  const startISO = startDate.includes('T') ? startDate : startDate + 'T00:00:00.000Z';
  const endISO   = endDate.includes('T')   ? endDate   : endDate   + 'T23:59:59.999Z';

  while (true) {
    const data = await flowhubGet(`/v1/orders/findByLocationId/${importId}`, {
      created_after:  startISO,
      created_before: endISO,
      page_size:      PAGE_SIZE,
      page,
      order_by:       'asc',
    });

    const batch = data.orders || [];
    total = data.total || 0;
    allOrders = allOrders.concat(batch);

    // Log date params + result count ONCE for debugging
    if (!_dateParamLogged) {
      _dateParamLogged = true;
      console.log(`DATE PARAM TEST: created_after=${startISO} created_before=${endISO} → ${total} orders`);
    }

    // Log the schema of the first order + first item ONCE so we can see real field names
    if (!_schemaLogged && batch.length > 0) {
      _schemaLogged = true;
      const sample = batch[0];
      console.log('\n═══ ORDER SCHEMA DISCOVERY ═══');
      console.log('ORDER KEYS:', Object.keys(sample).join(', '));

      // Log all top-level fields with their values (truncated)
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
  }

  return { total, orders: allOrders };
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
    store: locations[i],
    summary: r.status === 'fulfilled' ? r.value.summary : null,
    orders:  r.status === 'fulfilled' ? r.value.orders  : [],
    error:   r.status === 'rejected'  ? r.reason?.message : null,
  }));
}

// ── Weekly trend ──────────────────────────────────────────────
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

// ── Full dashboard payload ────────────────────────────────────
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

module.exports = {
  getLocations, getOrdersForLocation, summarizeOrders, extractTopProducts,
  getAllStoresSales, getWeeklyTrend, getAllStoresWeeklyTrend, getDashboardData,
  getRawOrderSample,
  weekRange, todayRange, ytdRange, toDateStr, STORE_CONFIG,
};
