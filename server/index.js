// server/index.js
// ============================================================
// Thrive Dashboard — Express Server
// Redis-backed stale-while-revalidate caching
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

// ── Internal auth (for cron) ──────────────────────────────────
function internalAuth(req, res, next) {
  const secret = process.env.INTERNAL_SECRET;
  if (!secret) return next();
  const provided = req.headers['x-internal-secret'] || req.query.secret;
  if (provided === secret) return next();
  res.status(403).json({ error: 'Forbidden' });
}

// ── Cache helper (in-memory, for dashboard/non-trend) ─────────
async function cached(key, ttl, fn) {
  const hit = cache.get(key);
  if (hit !== undefined) return hit;
  const result = await fn();
  cache.set(key, result, ttl);
  return result;
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
    const loc = locations[0];
    const sample = await fh.getRawOrderSample(loc.importId);
    res.json({ store: loc.name, ...sample });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ★ DIAGNOSTIC — test date range query
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

// ═══════════════════════════════════════════════════════════════
// EXECUTIVE DASHBOARD — fetches from Flowhub (today + this week)
// ═══════════════════════════════════════════════════════════════
app.get('/api/dashboard', auth, async (req, res) => {
  try {
    const data = await cached('dashboard', 300, () => fh.getDashboardData());
    res.json(data);
  } catch (err) {
    console.error('Dashboard error:', err.message);
    res.status(500).json({ error: err.message });
  }
});

// ═══════════════════════════════════════════════════════════════
// TREND — stale-while-revalidate from Redis
// NEVER blocks on Flowhub. Reads Redis only.
// ═══════════════════════════════════════════════════════════════
app.get('/api/trend', auth, async (req, res) => {
  try {
    // Try Redis first
    const redisCached = await rebuild.getCachedTrend();

    if (redisCached) {
      return res.json({
        source: 'redis',
        generatedAt: redisCached.generatedAt,
        rebuildDurationMs: redisCached.rebuildDurationMs,
        weekStarts: redisCached.weekStarts,
        stores: redisCached.stores,
      });
    }

    // No Redis cache — try in-memory fallback (old behavior)
    const weeks = Math.min(parseInt(req.query.weeks) || 12, 52);
    const memCached = cache.get(`trend_all_${weeks}`);
    if (memCached) {
      return res.json(memCached);
    }

    // Nothing cached anywhere — trigger async rebuild, return building status
    console.log('Trend: no cache — triggering async rebuild');
    rebuild.rebuildTrendCache().catch(err =>
      console.error('Async rebuild failed:', err.message)
    );

    return res.json({
      status: 'building',
      message: 'Trend data is being built. Refresh in ~60 seconds.',
    });

  } catch (err) {
    console.error('Trend error:', err.message);
    res.status(500).json({ error: err.message });
  }
});

// ═══════════════════════════════════════════════════════════════
// TREND (single store) — tries Redis, falls back to direct
// ═══════════════════════════════════════════════════════════════
app.get('/api/trend/:storeId', auth, async (req, res) => {
  const weeks = Math.min(parseInt(req.query.weeks) || 12, 52);
  try {
    // Try Redis — extract single store from all-stores cache
    const allCached = await rebuild.getCachedTrend();
    if (allCached && allCached.stores[req.params.storeId]) {
      const storeData = allCached.stores[req.params.storeId];
      const locations = await fh.getLocations();
      const loc = locations.find(l => l.id === req.params.storeId);
      return res.json({
        source: 'redis',
        store: loc || { id: req.params.storeId, name: storeData.name, color: storeData.color },
        trend: storeData.weeks.map(w => ({
          week: { start: w.week, end: w.weekEnd },
          summary: w.summary,
          error: w.error,
        })),
      });
    }

    // Fallback: direct Flowhub fetch
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
// REBUILD — triggered by Railway cron every 5 min
// ═══════════════════════════════════════════════════════════════
app.post('/internal/rebuild-trend-cache', internalAuth, async (req, res) => {
  console.log('→ Rebuild triggered via POST');
  try {
    const result = await rebuild.rebuildTrendCache();
    res.json(result);
  } catch (err) {
    console.error('Rebuild endpoint error:', err.message);
    res.status(500).json({ status: 'error', error: err.message });
  }
});

// Also support GET for easy testing
app.get('/internal/rebuild-trend-cache', internalAuth, async (req, res) => {
  console.log('→ Rebuild triggered via GET');
  try {
    const result = await rebuild.rebuildTrendCache();
    res.json(result);
  } catch (err) {
    console.error('Rebuild endpoint error:', err.message);
    res.status(500).json({ status: 'error', error: err.message });
  }
});

// ═══════════════════════════════════════════════════════════════
// REMAINING EXISTING ROUTES
// ═══════════════════════════════════════════════════════════════

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

// Day vs Day
app.get('/api/day-vs-day', auth, async (req, res) => {
  const dow = parseInt(req.query.dow ?? new Date().getDay());
  const weeks = Math.min(parseInt(req.query.weeks) || 4, 8);
  try {
    const data = await cached(`dvd_${dow}_${weeks}`, 1800, () => fh.getSingleDayVsDay(dow, weeks));
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
    console.log('✓ Ready');

    // Check Redis
    const redisOk = await redis.ping();
    console.log(redisOk ? '✓ Redis connected' : '⚠ Redis not available — trend will use direct fetch fallback');

    // Trigger initial trend cache build if Redis is up and cache is empty
    if (redisOk) {
      const existing = await rebuild.getCachedTrend();
      if (!existing) {
        console.log('→ No trend cache — triggering initial rebuild...');
        rebuild.rebuildTrendCache().catch(err =>
          console.error('Initial rebuild failed:', err.message)
        );
      } else {
        console.log(`✓ Trend cache exists (generated ${existing.generatedAt})`);
      }
    }

    console.log('');
  } catch (err) {
    console.warn('⚠ Startup warning:', err.message);
  }
});

module.exports = app;
