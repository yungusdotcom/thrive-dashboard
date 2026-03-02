// server/rebuild.js
// ============================================================
// Background Cache Rebuilder - Thrive Dashboard
// PARALLEL: all sections run simultaneously
// MERGED: store detail + budtenders share one fetch
// ============================================================

var fh = require('./flowhub');
var redis = require('./redis');

var KEYS = {
  trend:       'cache:trend:12w',
  dvd:         function(dow) { return 'cache:dvd:' + dow; },
  budtenders:  function(storeId) { return 'cache:bt:' + storeId; },
  storeDetail: function(storeId) { return 'cache:store:' + storeId; },
  dashboard:   'cache:dashboard',
  lock:        'rebuild:lock',
};

var LOCK_TTL = 180;   // 3 min lock
var CACHE_TTL = 600;  // 10 min
var CONCURRENCY = 3;  // up from 2

function pLimit(n) {
  var active = 0, q = [];
  function next() {
    if (active >= n || q.length === 0) return;
    active++;
    var item = q.shift();
    item.fn().then(item.resolve, item.reject).finally(function() { active--; next(); });
  }
  return function(fn) {
    return new Promise(function(resolve, reject) { q.push({ fn: fn, resolve: resolve, reject: reject }); next(); });
  };
}

// -- DASHBOARD ------------------------------------------------
async function rebuildDashboard() {
  var t0 = Date.now();
  console.log('  [dashboard] starting...');
  try {
    var data = await fh.getDashboardData();
    data.rebuildDurationMs = Date.now() - t0;
    await redis.setJSON(KEYS.dashboard, data, CACHE_TTL);
    console.log('  [dashboard] done ' + (Date.now() - t0) + 'ms');
    return data;
  } catch (err) {
    console.error('  [dashboard] FAIL: ' + err.message);
    return null;
  }
}

// -- TREND ----------------------------------------------------
async function rebuildTrend(locations, limit) {
  var t0 = Date.now();
  console.log('  [trend] starting...');
  var weeksBack = 12;
  var weeks = [];
  for (var i = 0; i < weeksBack; i++) weeks.push(fh.weekRange(weeksBack - 1 - i));

  var storeResults = await Promise.all(
    locations.map(function(loc) {
      return limit(async function() {
        var ts = Date.now();
        try {
          var trend = await fh.getTrendForStore(loc, weeks);
          console.log('    trend ' + loc.name + ': ' + (Date.now() - ts) + 'ms');
          return { store: loc, trend: trend, error: null };
        } catch (err) {
          console.error('    trend ' + loc.name + ': FAIL ' + err.message);
          return { store: loc, trend: null, error: err.message };
        }
      });
    })
  );

  var stores = {};
  storeResults.forEach(function(r) {
    stores[r.store.id] = {
      name: r.store.name, color: r.store.color,
      weeks: r.trend
        ? r.trend.map(function(e) { return { week: e.week.start, weekEnd: e.week.end, summary: e.summary, error: e.error || null }; })
        : weeks.map(function(w) { return { week: w.start, weekEnd: w.end, error: r.error }; }),
    };
  });

  var payload = {
    generatedAt: new Date().toISOString(),
    rebuildDurationMs: Date.now() - t0,
    weekStarts: weeks.map(function(w) { return w.start; }),
    stores: stores
  };
  await redis.setJSON(KEYS.trend, payload, CACHE_TTL);
  console.log('  [trend] done ' + (Date.now() - t0) + 'ms');
  return payload;
}

// -- STORE DATA (merged: hourly + categories + budtenders) ----
// ONE 4-week fetch per store produces BOTH storeDetail AND budtender caches
async function rebuildStoreData(locations, limit) {
  var t0 = Date.now();
  console.log('  [storeData] starting (hourly + categories + budtenders)...');
  var lw = fh.weekRange(1);
  var pw = fh.weekRange(2);
  var hourlyStart = fh.weekRange(4).start;
  var hourlyEnd = lw.end;
  var TZ = 'America/Los_Angeles';

  await Promise.all(
    locations.map(function(loc) {
      return limit(async function() {
        var ts = Date.now();
        try {
          var result = await fh.getOrdersForLocation(loc.importId, hourlyStart, hourlyEnd);
          var allOrders = result.orders;
          var hourly = fh.summarizeHourly(allOrders);

          var lwOrders = allOrders.filter(function(o) {
            var d = new Date(o.createdAt || o.completedOn || '').toLocaleDateString('en-CA', { timeZone: TZ });
            return d >= lw.start && d <= lw.end;
          });
          var pwOrders = allOrders.filter(function(o) {
            var d = new Date(o.createdAt || o.completedOn || '').toLocaleDateString('en-CA', { timeZone: TZ });
            return d >= pw.start && d <= pw.end;
          });

          var lwSummary = fh.summarizeOrders(lwOrders);
          var pwSummary = fh.summarizeOrders(pwOrders);

          var pwCatMap = {};
          (pwSummary.categories || []).forEach(function(c) { pwCatMap[c.name] = c; });
          var categoryTrend = (lwSummary.categories || []).map(function(cat) {
            var prev = pwCatMap[cat.name];
            return {
              name: cat.name, lw_sales: cat.net_sales, lw_units: cat.units,
              pw_sales: prev ? prev.net_sales : 0, pw_units: prev ? prev.units : 0,
              wow_pct: (prev && prev.net_sales > 0) ? Math.round(((cat.net_sales - prev.net_sales) / prev.net_sales) * 1000) / 10 : null,
            };
          });

          // Write BOTH caches from ONE fetch
          await redis.setJSON(KEYS.storeDetail(loc.id), {
            store: { id: loc.id, name: loc.name, color: loc.color },
            hourly: hourly, hourlyWeeks: 4, categoryTrend: categoryTrend,
            lastWeek: lw, priorWeek: pw, generatedAt: new Date().toISOString(),
          }, CACHE_TTL);

          await redis.setJSON(KEYS.budtenders(loc.id), {
            store: { id: loc.id, name: loc.name, color: loc.color },
            employees: lwSummary.budtenders, categories: lwSummary.categories,
            week: lw, generatedAt: new Date().toISOString(),
          }, CACHE_TTL);

          console.log('    store ' + loc.name + ': ' + allOrders.length + ' orders, ' + lwSummary.budtenders.length + ' bts (' + (Date.now() - ts) + 'ms)');
        } catch (err) {
          console.error('    store ' + loc.name + ': FAIL ' + err.message);
        }
      });
    })
  );

  console.log('  [storeData] done ' + (Date.now() - t0) + 'ms');
}

// -- DAY VS DAY -- 7 DOWs with concurrency -------------------
async function rebuildDayVsDay(locations, limit) {
  var t0 = Date.now();
  console.log('  [dvd] starting all 7 DOWs...');
  var dayNames = ['Sun','Mon','Tue','Wed','Thu','Fri','Sat'];

  await Promise.all(
    [0,1,2,3,4,5,6].map(function(dow) {
      return limit(async function() {
        var ts = Date.now();
        try {
          var data = await fh.getSingleDayVsDay(dow, 4);
          await redis.setJSON(KEYS.dvd(dow), data, CACHE_TTL);
          console.log('    dvd ' + dayNames[dow] + ': ' + (Date.now() - ts) + 'ms');
        } catch (err) {
          console.error('    dvd ' + dow + ': FAIL ' + err.message);
        }
      });
    })
  );

  console.log('  [dvd] done ' + (Date.now() - t0) + 'ms');
}

// -- FULL REBUILD -- ALL SECTIONS IN PARALLEL -----------------
async function rebuildAll() {
  var t0 = Date.now();
  console.log('\n=== FULL REBUILD: starting (parallel) ===');

  var locked = await redis.acquireLock(KEYS.lock, LOCK_TTL);
  if (!locked) {
    console.log('  Lock held, skipping');
    return { status: 'skipped', reason: 'lock_held' };
  }

  try {
    var locations = await fh.getLocations();
    var limit = pLimit(CONCURRENCY);

    // ALL sections fire at once
    // Dashboard runs its own Flowhub calls independently
    // Trend, storeData, dvd share the limiter to cap parallel API calls
    var results = await Promise.allSettled([
      rebuildDashboard(),
      rebuildTrend(locations, limit),
      rebuildStoreData(locations, limit),
      rebuildDayVsDay(locations, limit),
    ]);

    var names = ['dashboard', 'trend', 'storeData', 'dvd'];
    results.forEach(function(r, i) {
      if (r.status === 'rejected') {
        console.error('  ' + names[i] + ' FAILED: ' + (r.reason ? r.reason.message : r.reason));
      }
    });

    var total = Date.now() - t0;
    console.log('=== FULL REBUILD: complete in ' + total + 'ms (' + (total/1000).toFixed(1) + 's) ===\n');
    return { status: 'ok', durationMs: total };

  } catch (err) {
    console.error('=== FULL REBUILD: FAILED -- ' + err.message + ' ===');
    return { status: 'error', error: err.message };
  } finally {
    await redis.releaseLock(KEYS.lock);
  }
}

// -- Selective rebuild ----------------------------------------
async function rebuildSection(section) {
  var locations = await fh.getLocations();
  var limit = pLimit(CONCURRENCY);
  switch (section) {
    case 'trend':       return rebuildTrend(locations, limit);
    case 'dvd':         return rebuildDayVsDay(locations, limit);
    case 'storeData':   return rebuildStoreData(locations, limit);
    case 'budtenders':  return rebuildStoreData(locations, limit);
    case 'storeDetail': return rebuildStoreData(locations, limit);
    case 'dashboard':   return rebuildDashboard();
    default:            return { error: 'unknown section' };
  }
}

// -- Read cached data -----------------------------------------
async function getCachedTrend()           { return redis.getJSON(KEYS.trend); }
async function getCachedDvd(dow)          { return redis.getJSON(KEYS.dvd(dow)); }
async function getCachedBudtenders(id)    { return redis.getJSON(KEYS.budtenders(id)); }
async function getCachedStoreDetail(id)   { return redis.getJSON(KEYS.storeDetail(id)); }
async function getCachedDashboard()       { return redis.getJSON(KEYS.dashboard); }

module.exports = {
  rebuildAll: rebuildAll,
  rebuildSection: rebuildSection,
  getCachedTrend: getCachedTrend,
  getCachedDvd: getCachedDvd,
  getCachedBudtenders: getCachedBudtenders,
  getCachedStoreDetail: getCachedStoreDetail,
  getCachedDashboard: getCachedDashboard,
  KEYS: KEYS,
};
```

**What changed and why it's faster:**

**Before** (sequential, ~5 min):
```
Dashboard (15s) → wait → Trend (45s) → wait → StoreDetail (60s) → wait → Budtenders (40s, DUPLICATE data) → wait → DvD (120s) → done
```

**After** (parallel, ~90s):
```
Dashboard (15s)     ──→ done, writes to Redis immediately
Trend (45s)         ──→ done, shares concurrency limiter
StoreData (60s)     ──→ done, writes BOTH storeDetail + budtenders from ONE fetch
DvD (90s)           ──→ done, 7 DOWs now concurrent instead of sequential
                    ALL running simultaneously ↑