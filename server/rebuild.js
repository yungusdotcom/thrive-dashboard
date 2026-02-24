// server/rebuild.js
// ============================================================
// Background Cache Rebuilder — Thrive Dashboard
// Builds Redis caches for: trend, day-vs-day, budtenders, dashboard
// Called on startup (if empty) and on cache miss.
// ============================================================

const fh = require('./flowhub');
const redis = require('./redis');

// ── Keys ─────────────────────────────────────────────────────
const KEYS = {
  trend:      'cache:trend:12w',
  dvd:        (dow) => `cache:dvd:${dow}`,
  budtenders: (storeId) => `cache:bt:${storeId}`,
  dashboard:  'cache:dashboard',
  lock:       'rebuild:lock',
};

const LOCK_TTL = 180;       // 3 min lock (covers full rebuild)
const CACHE_TTL = 600;      // 10 min TTL
const CONCURRENCY = 2;

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

// ═══════════════════════════════════════════════════════════════
// TREND REBUILD
// ═══════════════════════════════════════════════════════════════
async function rebuildTrend(locations, limit) {
  const t0 = Date.now();
  console.log('  [trend] starting...');

  const weeksBack = 12;
  const weeks = Array.from({ length: weeksBack }, (_, i) => fh.weekRange(weeksBack - 1 - i));

  const storeResults = await Promise.all(
    locations.map(loc => limit(async () => {
      const ts = Date.now();
      try {
        const trend = await fh.getTrendForStore(loc, weeks);
        console.log(`    ${loc.name}: ${Date.now() - ts}ms`);
        return { store: loc, trend, error: null };
      } catch (err) {
        console.error(`    ${loc.name}: FAIL ${err.message} (${Date.now() - ts}ms)`);
        return { store: loc, trend: null, error: err.message };
      }
    }))
  );

  const stores = {};
  for (const result of storeResults) {
    const id = result.store.id;
    if (!result.trend) {
      stores[id] = { name: result.store.name, color: result.store.color, weeks: weeks.map(w => ({ week: w.start, weekEnd: w.end, error: result.error })) };
      continue;
    }
    stores[id] = {
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
    weekStarts: weeks.map(w => w.start),
    stores,
  };

  await redis.setJSON(KEYS.trend, payload, CACHE_TTL);
  console.log(`  [trend] done in ${Date.now() - t0}ms`);
  return payload;
}

// ═══════════════════════════════════════════════════════════════
// DAY VS DAY REBUILD — all 7 DOWs
// ═══════════════════════════════════════════════════════════════
async function rebuildDayVsDay(locations, limit) {
  const t0 = Date.now();
  console.log('  [dvd] starting all 7 DOWs...');

  for (let dow = 0; dow < 7; dow++) {
    const ts = Date.now();
    try {
      const data = await fh.getSingleDayVsDay(dow, 4);
      await redis.setJSON(KEYS.dvd(dow), data, CACHE_TTL);
      const dayName = ['Sun','Mon','Tue','Wed','Thu','Fri','Sat'][dow];
      console.log(`    ${dayName}: ${Date.now() - ts}ms`);
    } catch (err) {
      console.error(`    DOW ${dow}: FAIL ${err.message}`);
    }
  }

  console.log(`  [dvd] done in ${Date.now() - t0}ms`);
}

// ═══════════════════════════════════════════════════════════════
// BUDTENDERS REBUILD — all stores, last week
// ═══════════════════════════════════════════════════════════════
async function rebuildBudtenders(locations, limit) {
  const t0 = Date.now();
  console.log('  [budtenders] starting...');
  const lw = fh.weekRange(1);

  await Promise.all(
    locations.map(loc => limit(async () => {
      const ts = Date.now();
      try {
        const { orders } = await fh.getOrdersForLocation(loc.importId, lw.start, lw.end);
        const summary = fh.summarizeOrders(orders);
        const payload = {
          store: { id: loc.id, name: loc.name, color: loc.color },
          employees: summary.budtenders,
          categories: summary.categories,
          week: lw,
          generatedAt: new Date().toISOString(),
        };
        await redis.setJSON(KEYS.budtenders(loc.id), payload, CACHE_TTL);
        console.log(`    ${loc.name}: ${summary.budtenders.length} budtenders (${Date.now() - ts}ms)`);
      } catch (err) {
        console.error(`    ${loc.name}: FAIL ${err.message}`);
      }
    }))
  );

  console.log(`  [budtenders] done in ${Date.now() - t0}ms`);
}

// ═══════════════════════════════════════════════════════════════
// DASHBOARD REBUILD — today + this week + last week
// ═══════════════════════════════════════════════════════════════
async function rebuildDashboard() {
  const t0 = Date.now();
  console.log('  [dashboard] starting...');

  try {
    const data = await fh.getDashboardData();
    data.rebuildDurationMs = Date.now() - t0;
    await redis.setJSON(KEYS.dashboard, data, CACHE_TTL);
    console.log(`  [dashboard] done in ${Date.now() - t0}ms`);
    return data;
  } catch (err) {
    console.error(`  [dashboard] FAIL: ${err.message}`);
    return null;
  }
}

// ═══════════════════════════════════════════════════════════════
// FULL REBUILD — everything
// ═══════════════════════════════════════════════════════════════
async function rebuildAll() {
  const t0 = Date.now();
  console.log('\n═══ FULL REBUILD: starting ═══');

  // Acquire lock
  const locked = await redis.acquireLock(KEYS.lock, LOCK_TTL);
  if (!locked) {
    console.log('  ⊘ Lock held, skipping');
    return { status: 'skipped', reason: 'lock_held' };
  }

  try {
    const locations = await fh.getLocations();
    const limit = pLimit(CONCURRENCY);

    // Run dashboard first (fastest, most important for instant load)
    await rebuildDashboard();

    // Then trend (needed for heatmap, velocity, store detail)
    await rebuildTrend(locations, limit);

    // Then budtenders (last week data, doesn't change)
    await rebuildBudtenders(locations, limit);

    // Finally day-vs-day (7 DOWs, heaviest)
    await rebuildDayVsDay(locations, limit);

    const total = Date.now() - t0;
    console.log(`═══ FULL REBUILD: complete in ${total}ms (${(total/1000).toFixed(1)}s) ═══\n`);
    return { status: 'ok', durationMs: total };

  } catch (err) {
    console.error(`═══ FULL REBUILD: FAILED — ${err.message} ═══`);
    return { status: 'error', error: err.message };

  } finally {
    await redis.releaseLock(KEYS.lock);
  }
}

// ── Selective rebuild (just one section) ─────────────────────
async function rebuildSection(section) {
  const locations = await fh.getLocations();
  const limit = pLimit(CONCURRENCY);

  switch (section) {
    case 'trend':      return rebuildTrend(locations, limit);
    case 'dvd':        return rebuildDayVsDay(locations, limit);
    case 'budtenders': return rebuildBudtenders(locations, limit);
    case 'dashboard':  return rebuildDashboard();
    default:           return { error: 'unknown section' };
  }
}

// ── Read cached data ─────────────────────────────────────────
async function getCachedTrend()        { return redis.getJSON(KEYS.trend); }
async function getCachedDvd(dow)       { return redis.getJSON(KEYS.dvd(dow)); }
async function getCachedBudtenders(id) { return redis.getJSON(KEYS.budtenders(id)); }
async function getCachedDashboard()    { return redis.getJSON(KEYS.dashboard); }

module.exports = {
  rebuildAll,
  rebuildSection,
  getCachedTrend,
  getCachedDvd,
  getCachedBudtenders,
  getCachedDashboard,
  KEYS,
};
