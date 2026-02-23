// server/index.js
// ============================================================
// Thrive Dashboard — Express Server
// ============================================================

require('dotenv').config();
const express    = require('express');
const cors       = require('cors');
const path       = require('path');
const NodeCache  = require('node-cache');
const fh         = require('./flowhub');

const app   = express();
const cache = new NodeCache({ stdTTL: parseInt(process.env.CACHE_TTL) || 300 });
const PORT  = process.env.PORT || 3000;

app.use(cors());
app.use(express.json());
app.use(express.static(path.join(__dirname, '../public')));

// ── Auth middleware ────────────────────────────────────────────
function auth(req, res, next) {
  const pw = process.env.DASHBOARD_PASSWORD;
  if (!pw) return next();
  const provided = req.query.key
    || req.headers.authorization?.replace('Bearer ', '')
    || req.headers.cookie?.split(';').find(c => c.trim().startsWith('thrive_key='))?.split('=')[1];
  if (provided === pw) return next();
  res.status(401).json({ error: 'Unauthorized' });
}

// ── Cache helper ──────────────────────────────────────────────
async function cached(key, ttl, fn) {
  const hit = cache.get(key);
  if (hit !== undefined) return hit;
  const result = await fn();
  cache.set(key, result, ttl);
  return result;
}

// ── Routes ────────────────────────────────────────────────────

// Health
app.get('/health', (req, res) => {
  res.json({ status: 'ok', uptime: process.uptime(), cacheKeys: cache.keys().length });
});

// Store list
app.get('/api/stores', async (req, res) => {
  try {
    const locations = await fh.getLocations();
    res.json(locations.map(l => ({ id: l.id, name: l.name, color: l.color })));
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ★ DIAGNOSTIC — raw order sample so we can see actual field names
app.get('/api/diag/order-sample', auth, async (req, res) => {
  try {
    const locations = await fh.getLocations();
    const loc = locations[0]; // first store
    const sample = await fh.getRawOrderSample(loc.importId);
    res.json({ store: loc.name, ...sample });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ★ DIAGNOSTIC — test date range query for a specific store + week
app.get('/api/diag/date-test', auth, async (req, res) => {
  try {
    const locations = await fh.getLocations();
    const storeId = req.query.store || locations[0].id;
    const loc = locations.find(l => l.id === storeId) || locations[0];
    const start = req.query.start || fh.weekRange(1).start;
    const end = req.query.end || fh.weekRange(1).end;
    const { total, orders } = await fh.getOrdersForLocation(loc.importId, start, end);
    res.json({
      store: loc.name,
      importId: loc.importId,
      dateRange: { start, end },
      total,
      ordersReturned: orders.length,
      firstOrderDate: orders[0]?.createdAt || null,
      lastOrderDate: orders[orders.length - 1]?.createdAt || null,
    });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// Executive dashboard
app.get('/api/dashboard', auth, async (req, res) => {
  try {
    const data = await cached('dashboard', 300, () => fh.getDashboardData());
    res.json(data);
  } catch (err) {
    console.error('Dashboard error:', err.message);
    res.status(500).json({ error: err.message });
  }
});

// Weekly trend — all stores
app.get('/api/trend', auth, async (req, res) => {
  const weeks = Math.min(parseInt(req.query.weeks) || 12, 52);
  try {
    const data = await cached(`trend_all_${weeks}`, 600, () => fh.getAllStoresWeeklyTrend(weeks));
    res.json(data);
  } catch (err) {
    console.error('Trend error:', err.message);
    res.status(500).json({ error: err.message });
  }
});

// Weekly trend — single store
app.get('/api/trend/:storeId', auth, async (req, res) => {
  const weeks = Math.min(parseInt(req.query.weeks) || 12, 52);
  try {
    const locations = await fh.getLocations();
    const loc = locations.find(l => l.id === req.params.storeId);
    if (!loc) return res.status(404).json({ error: 'Store not found' });
    const data = await cached(`trend_${loc.id}_${weeks}`, 600, () => fh.getWeeklyTrend(loc.importId, weeks));
    res.json({ store: loc, trend: data });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// Sales for a date range
app.get('/api/sales', auth, async (req, res) => {
  const { start, end, store } = req.query;
  if (!start || !end) return res.status(400).json({ error: 'start and end required (YYYY-MM-DD)' });
  try {
    if (store) {
      const locations = await fh.getLocations();
      const loc = locations.find(l => l.id === store);
      if (!loc) return res.status(404).json({ error: 'Store not found' });
      const data = await cached(`sales_${store}_${start}_${end}`, 300, async () => {
        const { orders } = await fh.getOrdersForLocation(loc.importId, start, end);
        return { store: loc, summary: fh.summarizeOrders(orders) };
      });
      res.json(data);
    } else {
      const data = await cached(`sales_all_${start}_${end}`, 300, () =>
        fh.getAllStoresSales(start, end).then(results =>
          results.map(r => ({ store: r.store, summary: r.summary, error: r.error }))
        )
      );
      res.json(data);
    }
  } catch (err) {
    console.error('Sales error:', err.message);
    res.status(500).json({ error: err.message });
  }
});

// Top products
app.get('/api/products', auth, async (req, res) => {
  const { store, start, end, limit = 15 } = req.query;
  if (!store || !start || !end) return res.status(400).json({ error: 'store, start, end required' });
  try {
    const locations = await fh.getLocations();
    const loc = locations.find(l => l.id === store);
    if (!loc) return res.status(404).json({ error: 'Store not found' });
    const data = await cached(`products_${store}_${start}_${end}_${limit}`, 600, async () => {
      const { orders } = await fh.getOrdersForLocation(loc.importId, start, end);
      return { store: loc, products: fh.extractTopProducts(orders, parseInt(limit)) };
    });
    res.json(data);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// Categories
app.get('/api/categories', auth, async (req, res) => {
  const { store, start, end } = req.query;
  if (!store || !start || !end) return res.status(400).json({ error: 'store, start, end required' });
  try {
    const locations = await fh.getLocations();
    const loc = locations.find(l => l.id === store);
    if (!loc) return res.status(404).json({ error: 'Store not found' });
    const data = await cached(`cats_${store}_${start}_${end}`, 600, async () => {
      const { orders } = await fh.getOrdersForLocation(loc.importId, start, end);
      return { store: loc, categories: fh.summarizeOrders(orders).categories };
    });
    res.json(data);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// Budtender performance
app.get('/api/employees', auth, async (req, res) => {
  const { store, start, end } = req.query;
  if (!store || !start || !end) return res.status(400).json({ error: 'store, start, end required' });
  try {
    const locations = await fh.getLocations();
    const loc = locations.find(l => l.id === store);
    if (!loc) return res.status(404).json({ error: 'Store not found' });
    const data = await cached(`emp_${store}_${start}_${end}`, 600, async () => {
      const { orders } = await fh.getOrdersForLocation(loc.importId, start, end);
      return { store: loc, employees: fh.summarizeOrders(orders).budtenders };
    });
    res.json(data);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// Day vs Day — compare same weekday across last N weeks
// ?dow=1 (Monday) &weeks=4
app.get('/api/day-vs-day', auth, async (req, res) => {
  const dow = parseInt(req.query.dow ?? new Date().getDay());
  const weeks = Math.min(parseInt(req.query.weeks) || 4, 8);
  try {
    const data = await cached(`dvd_${dow}_${weeks}`, 600, () => fh.getSingleDayVsDay(dow, weeks));
    res.json(data);
  } catch (err) {
    console.error('Day vs Day error:', err.message);
    res.status(500).json({ error: err.message });
  }
});

// Cache management
app.post('/api/cache/clear', auth, (req, res) => {
  const count = cache.keys().length;
  cache.flushAll();
  res.json({ cleared: count });
});

// Catch-all → frontend
app.get('*', (req, res) => {
  res.sendFile(path.join(__dirname, '../public/index.html'));
});

// ── Start ─────────────────────────────────────────────────────
app.listen(PORT, async () => {
  console.log(`\n🌿 THRIVE DASHBOARD — port ${PORT}\n`);
  try {
    await fh.getLocations();
    console.log('✓ Ready\n');
  } catch (err) {
    console.warn('⚠ Location warm-up failed:', err.message);
  }
});

module.exports = app;
