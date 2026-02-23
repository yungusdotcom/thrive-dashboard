// server/index.js
// ============================================================
// Thrive Dashboard — Express Server
// ============================================================

require('dotenv').config();
const express   = require('express');
const cors      = require('cors');
const path      = require('path');
const NodeCache = require('node-cache');
const fh        = require('./flowhub');

const app   = express();
const cache = new NodeCache({ stdTTL: parseInt(process.env.CACHE_TTL) || 300 });
const PORT  = process.env.PORT || 3000;

app.use(cors());
app.use(express.json());
app.use(express.static(path.join(__dirname, '../public')));

// ── Auth middleware ──────────────────────────────────────────
function auth(req, res, next) {
  const pw = process.env.DASHBOARD_PASSWORD;
  if (!pw) return next();
  const provided = req.query.key
    || req.headers.authorization?.replace('Bearer ', '')
    || req.headers.cookie?.split(';').find(c => c.trim().startsWith('thrive_key='))?.split('=')[1];
  if (provided === pw) return next();
  res.status(401).json({ error: 'Unauthorized' });
}

// ── Cache wrapper ────────────────────────────────────────────
async function cached(key, ttl, fn) {
  const hit = cache.get(key);
  if (hit !== undefined) return hit;
  const result = await fn();
  cache.set(key, result, ttl);
  return result;
}

// ── Routes ───────────────────────────────────────────────────

// Health — no auth
app.get('/health', (req, res) => {
  res.json({ status: 'ok', uptime: process.uptime(), cacheKeys: cache.keys().length });
});

// Store list — no auth (no secrets exposed)
app.get('/api/stores', async (req, res) => {
  try {
    const locations = await fh.getLocations();
    res.json(locations.map(l => ({ id: l.id, name: l.name, color: l.color })));
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// Executive dashboard — this week / last week / today for all stores
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
// ?weeks=12
app.get('/api/trend', auth, async (req, res) => {
  const weeks = Math.min(parseInt(req.query.weeks) || 12, 52);
  try {
    const data = await cached(`trend_all_${weeks}`, 600, () =>
      fh.getAllStoresWeeklyTrend(weeks)
    );
    res.json(data);
  } catch (err) {
    console.error('Trend error:', err.message);
    res.status(500).json({ error: err.message });
  }
});

// Weekly trend — single store
// ?store=cactus&weeks=12
app.get('/api/trend/:storeId', auth, async (req, res) => {
  const weeks = Math.min(parseInt(req.query.weeks) || 12, 52);
  try {
    const locations = await fh.getLocations();
    const loc = locations.find(l => l.id === req.params.storeId);
    if (!loc) return res.status(404).json({ error: 'Store not found' });

    const data = await cached(`trend_${loc.id}_${weeks}`, 600, () =>
      fh.getWeeklyTrend(loc.importId, weeks)
    );
    res.json({ store: loc, trend: data });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// Sales for a date range — all stores or single store
// ?start=2025-01-01&end=2025-01-07
// ?start=...&end=...&store=cactus
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

// Top products for a store + date range
// ?store=cactus&start=...&end=...&limit=15
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

// Category breakdown for a store + date range
// ?store=cactus&start=...&end=...
app.get('/api/categories', auth, async (req, res) => {
  const { store, start, end } = req.query;
  if (!store || !start || !end) return res.status(400).json({ error: 'store, start, end required' });

  try {
    const locations = await fh.getLocations();
    const loc = locations.find(l => l.id === store);
    if (!loc) return res.status(404).json({ error: 'Store not found' });

    const data = await cached(`cats_${store}_${start}_${end}`, 600, async () => {
      const { orders } = await fh.getOrdersForLocation(loc.importId, start, end);
      const summary = fh.summarizeOrders(orders);
      return { store: loc, categories: summary.categories };
    });
    res.json(data);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// Budtender performance for a store + date range
// ?store=cactus&start=...&end=...
app.get('/api/employees', auth, async (req, res) => {
  const { store, start, end } = req.query;
  if (!store || !start || !end) return res.status(400).json({ error: 'store, start, end required' });

  try {
    const locations = await fh.getLocations();
    const loc = locations.find(l => l.id === store);
    if (!loc) return res.status(404).json({ error: 'Store not found' });

    const data = await cached(`emp_${store}_${start}_${end}`, 600, async () => {
      const { orders } = await fh.getOrdersForLocation(loc.importId, start, end);
      const summary = fh.summarizeOrders(orders);
      return { store: loc, employees: summary.budtenders };
    });
    res.json(data);
  } catch (err) {
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

// ── Start ────────────────────────────────────────────────────
app.listen(PORT, async () => {
  console.log(`\n🌿 THRIVE DASHBOARD — port ${PORT}\n`);
  // Warm up: get token + locations on start
  try {
    await fh.getLocations();
    console.log('✓ Ready\n');
  } catch (err) {
    console.warn('⚠ Location warm-up failed:', err.message);
    console.warn('  Check FLOWHUB_CLIENT_ID and FLOWHUB_API_KEY in .env\n');
  }
});

module.exports = app;
