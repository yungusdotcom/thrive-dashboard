// server/index.js
// ============================================================
// Thrive Dashboard — Express Server
// Redis-backed stale-while-revalidate for ALL historical data
// ============================================================

require('dotenv').config();
const express    = require('express');
const cors       = require('cors');
const path       = require('path');
const NodeCache  = require('node-cache');
const fh         = require('./flowhub');
const redis      = require('./redis');
const rebuild    = require('./rebuild');

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

// ── Internal auth (for rebuild endpoint) ──────────────────────
function internalAuth(req, res, next) {
  const secret = process.env.INTERNAL_SECRET;
  if (!secret) return next();
  const provided = req.headers['x-internal-secret'] || req.query.secret;
  if (provided === secret) return next();
  res.status(403).json({ error: 'Forbidden' });
}

// ── In-memory cache helper (for non-Redis endpoints) ──────────
async function cached(key, ttl, fn) {
  const hit = cache.get(key);
  if (hit !== undefined) return hit;
  const result = await fn();
  cache.set(key, result, ttl);
  return result;
}

// ── Rebuild trigger helper ────────────────────────────────────
// Triggers async rebuild for a section if not already running
const _rebuildingFlags = {};
function triggerRebuild(section) {
  if (_rebuildingFlags[section]) return; // already in flight
  _rebuildingFlags[section] = true;
  console.log(`→ Triggering async rebuild: ${section}`);
  rebuild.rebuildSection(section)
    .catch(err => console.error(`Async rebuild ${section} failed:`, err.message))
    .finally(() => { _rebuildingFlags[section] = false; });
}

// ── Routes ────────────────────────────────────────────────────

// Health
app.get('/health', async (req, res) => {
  const redisOk = await redis.ping();
  res.json({
    status: 'ok',
    uptime: process.uptime(),
    cacheKeys: cache.keys().length,
    redis: redisOk ? 'connected' : 'disconnected',
  });
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

// ★ DIAGNOSTIC — raw order sample
app.get('/api/diag/order-sample', auth, async (req, res) => {
  try {
    const locations = await fh.getLocations();
    const sample = await fh.getRawOrderSample(locations[0].importId);
    res.json({ store: locations[0].name, ...sample });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ★ DIAGNOSTIC — date test
app.get('/api/diag/date-test', auth, async (req, res) => {
  try {
    const locations = await fh.getLocations();
    const loc = locations.find(l => l.id === (req.query.store || locations[0].id)) || locations[0];
    const start = req.query.start || fh.weekRange(1).start;
    const end = req.query.end || fh.weekRange(1).end;
    const { total, orders } = await fh.getOrdersForLocation(loc.importId, start, end);
    res.json({ store: loc.name, importId: loc.importId, dateRange: { start, end }, total, ordersReturned: orders.length,
      firstOrderDate: orders[0]?.createdAt || null, lastOrderDate: orders[orders.length - 1]?.createdAt || null });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ═══════════════════════════════════════════════════════════════
// DASHBOARD — Redis first, fallback to direct Flowhub
// ═══════════════════════════════════════════════════════════════
app.get('/api/dashboard', auth, async (req, res) => {
  try {
    // Try Redis
    const redisCached = await rebuild.getCachedDashboard();
    if (redisCached) {
      return res.json({ ...redisCached, source: 'redis' });
    }

    // Fallback: direct fetch (and trigger background cache)
    triggerRebuild('dashboard');
    const data = await cached('dashboard', 300, () => fh.getDashboardData());
    res.json({ ...data, source: 'direct' });
  } catch (err) {
    console.error('Dashboard error:', err.message);
    res.status(500).json({ error: err.message });
  }
});

// ═══════════════════════════════════════════════════════════════
// TREND — Redis only, never blocks on Flowhub
// ═══════════════════════════════════════════════════════════════
app.get('/api/trend', auth, async (req, res) => {
  try {
    const redisCached = await rebuild.getCachedTrend();
    if (redisCached) {
      return res.json({ source: 'redis', ...redisCached });
    }

    // No cache — trigger rebuild, return building status
    triggerRebuild('trend');
    return res.json({ status: 'building', message: 'Trend data is being built. Refresh in ~60 seconds.' });
  } catch (err) {
    console.error('Trend error:', err.message);
    res.status(500).json({ error: err.message });
  }
});

// ═══════════════════════════════════════════════════════════════
// TREND (single store) — extract from Redis all-stores cache
// ═══════════════════════════════════════════════════════════════
app.get('/api/trend/:storeId', auth, async (req, res) => {
  const weeks = Math.min(parseInt(req.query.weeks) || 12, 52);
  try {
    // Try Redis
    const allCached = await rebuild.getCachedTrend();
    if (allCached && allCached.stores[req.params.storeId]) {
      const storeData = allCached.stores[req.params.storeId];
      const locations = await fh.getLocations();
      const loc = locations.find(l => l.id === req.params.storeId);
      return res.json({
        source: 'redis',
        store: loc || { id: req.params.storeId, name: storeData.name, color: storeData.color },
        trend: storeData.weeks.map(w => ({ week: { start: w.week, end: w.weekEnd }, summary: w.summary, error: w.error })),
      });
    }

    // Fallback: direct
    const locations = await fh.getLocations();
    const loc = locations.find(l => l.id === req.params.storeId);
    if (!loc) return res.status(404).json({ error: 'Store not found' });
    const data = await cached(`trend_${loc.id}_${weeks}`, 1800, () => fh.getWeeklyTrend(loc.importId, weeks));
    res.json({ source: 'direct', store: loc, trend: data });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ═══════════════════════════════════════════════════════════════
// DAY VS DAY — Redis first, fallback to direct
// ═══════════════════════════════════════════════════════════════
app.get('/api/day-vs-day', auth, async (req, res) => {
  const dow = parseInt(req.query.dow ?? new Date().getDay());
  const weeks = Math.min(parseInt(req.query.weeks) || 4, 8);
  try {
    // Try Redis
    const redisCached = await rebuild.getCachedDvd(dow);
    if (redisCached) {
      return res.json({ ...redisCached, source: 'redis' });
    }

    // Fallback: direct fetch (and trigger full dvd rebuild)
    triggerRebuild('dvd');
    const data = await cached(`dvd_${dow}_${weeks}`, 1800, () => fh.getSingleDayVsDay(dow, weeks));
    res.json({ ...data, source: 'direct' });
  } catch (err) {
    console.error('Day vs Day error:', err.message);
    res.status(500).json({ error: err.message });
  }
});

// ═══════════════════════════════════════════════════════════════
// BUDTENDERS — Redis first, fallback to direct
// ═══════════════════════════════════════════════════════════════
app.get('/api/employees', auth, async (req, res) => {
  const { store, start, end } = req.query;
  if (!store || !start || !end) return res.status(400).json({ error: 'store, start, end required' });
  try {
    // Try Redis (only for last-week queries, which is what the tab uses)
    const redisCached = await rebuild.getCachedBudtenders(store);
    if (redisCached) {
      return res.json({ ...redisCached, source: 'redis' });
    }

    // Fallback: direct fetch (and trigger budtender rebuild)
    triggerRebuild('budtenders');
    const locations = await fh.getLocations();
    const loc = locations.find(l => l.id === store);
    if (!loc) return res.status(404).json({ error: 'Store not found' });
    const data = await cached(`emp_${store}_${start}_${end}`, 600, async () => {
      const { orders } = await fh.getOrdersForLocation(loc.importId, start, end);
      return { store: loc, employees: fh.summarizeOrders(orders).budtenders };
    });
    res.json({ ...data, source: 'direct' });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ═══════════════════════════════════════════════════════════════
// REMAINING ROUTES (no Redis caching — user-driven queries)
// ═══════════════════════════════════════════════════════════════

// Sales for a date range (Custom Range tab)
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

// ═══════════════════════════════════════════════════════════════
// REBUILD ENDPOINTS
// ═══════════════════════════════════════════════════════════════

// Full rebuild
app.post('/internal/rebuild', internalAuth, async (req, res) => {
  console.log('→ Full rebuild triggered');
  try {
    const result = await rebuild.rebuildAll();
    res.json(result);
  } catch (err) {
    res.status(500).json({ status: 'error', error: err.message });
  }
});

app.get('/internal/rebuild', internalAuth, async (req, res) => {
  console.log('→ Full rebuild triggered (GET)');
  try {
    const result = await rebuild.rebuildAll();
    res.json(result);
  } catch (err) {
    res.status(500).json({ status: 'error', error: err.message });
  }
});

// Section rebuild
app.post('/internal/rebuild/:section', internalAuth, async (req, res) => {
  console.log(`→ Section rebuild: ${req.params.section}`);
  try {
    const result = await rebuild.rebuildSection(req.params.section);
    res.json({ status: 'ok', section: req.params.section });
  } catch (err) {
    res.status(500).json({ status: 'error', error: err.message });
  }
});

// Cache status
app.get('/internal/cache-status', internalAuth, async (req, res) => {
  const redisOk = await redis.ping();
  const trend = await rebuild.getCachedTrend();
  const dashboard = await rebuild.getCachedDashboard();
  const dvdResults = {};
  for (let d = 0; d < 7; d++) {
    const v = await rebuild.getCachedDvd(d);
    dvdResults[d] = v ? v.generatedAt || true : false;
  }
  res.json({
    redis: redisOk ? 'connected' : 'disconnected',
    inMemoryKeys: cache.keys().length,
    trend: trend ? { generatedAt: trend.generatedAt, stores: Object.keys(trend.stores).length } : null,
    dashboard: dashboard ? { fetchedAt: dashboard.meta?.fetchedAt } : null,
    dayVsDay: dvdResults,
  });
});

// Cache clear
app.post('/api/cache/clear', auth, async (req, res) => {
  const memCount = cache.keys().length;
  cache.flushAll();
  // Also clear Redis caches
  try {
    const client = redis.getClient();
    const keys = await client.keys('cache:*');
    if (keys.length > 0) await client.del(...keys);
    res.json({ cleared: memCount, redisCleared: keys.length });
  } catch (err) {
    res.json({ cleared: memCount, redisError: err.message });
  }
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
    console.log('✓ Ready');

    const redisOk = await redis.ping();
    console.log(redisOk ? '✓ Redis connected' : '⚠ Redis not available — using direct Flowhub fallback');

    if (redisOk) {
      const existing = await rebuild.getCachedTrend();
      if (!existing) {
        console.log('→ No cache — triggering full rebuild...');
        rebuild.rebuildAll().catch(err =>
          console.error('Initial rebuild failed:', err.message)
        );
      } else {
        console.log(`✓ Cache exists (generated ${existing.generatedAt})`);
      }
    }

    console.log('');
  } catch (err) {
    console.warn('⚠ Startup warning:', err.message);
  }
});

module.exports = app;
