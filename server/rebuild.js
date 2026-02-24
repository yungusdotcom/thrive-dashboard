// server/rebuild.js
// ============================================================
// Background Trend Cache Rebuilder
// Fetches Flowhub data, aggregates into weekly buckets,
// writes single Redis key. Called by cron every 5 minutes.
// ============================================================

const fh = require('./flowhub');
const redis = require('./redis');

const LOCK_KEY = 'trend:12w:lock';
const CACHE_KEY = 'trend:12w:all';
const LOCK_TTL = 120;       // 2 min lock (enough for full rebuild)
const CACHE_TTL = 600;      // 10 min TTL on cached data
const CONCURRENCY = 2;      // max stores fetched in parallel

// ── Simple concurrency limiter ───────────────────────────────
function pLimit(concurrency) {
  let active = 0;
  const queue = [];
  function next() {
    if (active >= concurrency || queue.length === 0) return;
    active++;
    const { fn, resolve, reject } = queue.shift();
    fn().then(resolve, reject).finally(() => { active--; next(); });
  }
  return function limit(fn) {
    return new Promise((resolve, reject) => {
      queue.push({ fn, resolve, reject });
      next();
    });
  };
}

// ── Rebuild trend cache ──────────────────────────────────────
async function rebuildTrendCache() {
  const t0 = Date.now();
  console.log('\n═══ TREND REBUILD: starting ═══');

  // 1. Acquire lock
  const locked = await redis.acquireLock(LOCK_KEY, LOCK_TTL);
  if (!locked) {
    console.log('  ⊘ Lock held by another worker, skipping');
    return { status: 'skipped', reason: 'lock_held' };
  }
  console.log('  ✓ Lock acquired');

  try {
    // 2. Get locations
    const locations = await fh.getLocations();
    console.log(`  → ${locations.length} locations`);

    // 3. Build week ranges
    const weeksBack = 12;
    const weeks = Array.from({ length: weeksBack }, (_, i) => fh.weekRange(weeksBack - 1 - i));
    const weekStarts = weeks.map(w => w.start);

    // 4. Fetch all stores with concurrency limit
    const limit = pLimit(CONCURRENCY);
    const tFetch0 = Date.now();

    const storeResults = await Promise.all(
      locations.map(loc => limit(async () => {
        const ts = Date.now();
        try {
          const trend = await fh.getTrendForStore(loc, weeks);
          const elapsed = Date.now() - ts;
          console.log(`  ✓ ${loc.name}: ${elapsed}ms`);
          return { store: loc, trend, error: null };
        } catch (err) {
          const elapsed = Date.now() - ts;
          console.error(`  ✗ ${loc.name}: ${err.message} (${elapsed}ms)`);
          return { store: loc, trend: null, error: err.message };
        }
      }))
    );

    const tFetch1 = Date.now();
    console.log(`  → Fetch phase: ${tFetch1 - tFetch0}ms`);

    // 5. Aggregate into payload
    const tAgg0 = Date.now();
    const stores = {};

    for (const result of storeResults) {
      const storeId = result.store.id;
      if (!result.trend) {
        stores[storeId] = {
          name: result.store.name,
          color: result.store.color,
          weeks: weekStarts.map(ws => ({ week: ws, error: result.error })),
        };
        continue;
      }

      stores[storeId] = {
        name: result.store.name,
        color: result.store.color,
        weeks: result.trend.map(entry => ({
          week: entry.week.start,
          weekEnd: entry.week.end,
          summary: entry.summary,
          error: entry.error || null,
        })),
      };
    }

    const payload = {
      generatedAt: new Date().toISOString(),
      rebuildDurationMs: Date.now() - t0,
      weekStarts,
      stores,
    };

    const tAgg1 = Date.now();
    console.log(`  → Aggregation: ${tAgg1 - tAgg0}ms`);

    // 6. Write to Redis
    const tWrite0 = Date.now();
    const written = await redis.setJSON(CACHE_KEY, payload, CACHE_TTL);
    const tWrite1 = Date.now();
    console.log(`  → Cache write: ${tWrite1 - tWrite0}ms (${written ? 'ok' : 'FAILED'})`);

    const total = Date.now() - t0;
    console.log(`═══ TREND REBUILD: complete in ${total}ms ═══\n`);

    return { status: 'ok', durationMs: total, stores: Object.keys(stores).length };

  } catch (err) {
    console.error(`═══ TREND REBUILD: FAILED — ${err.message} ═══`);
    return { status: 'error', error: err.message };

  } finally {
    await redis.releaseLock(LOCK_KEY);
  }
}

// ── Read cached trend ────────────────────────────────────────
async function getCachedTrend() {
  return redis.getJSON(CACHE_KEY);
}

module.exports = { rebuildTrendCache, getCachedTrend, CACHE_KEY };
