// server/rebuild.js
// Dashboard piggybacks hourly + budtenders (zero extra API calls)
// Trend + DvD run in parallel alongside dashboard
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

var LOCK_TTL = 180;
var CACHE_TTL = 600;
var CONCURRENCY = 3;

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

// -- DASHBOARD + STORE DETAIL + BUDTENDERS (all from one fetch) --
async function rebuildDashboard() {
  var t0 = Date.now();
  console.log('  [dashboard] starting (with hourly + budtenders)...');
  try {
    var data = await fh.getDashboardData();
    data.rebuildDurationMs = Date.now() - t0;
    await redis.setJSON(KEYS.dashboard, data, CACHE_TTL);

    // Write storeDetail + budtender caches from dashboard data (zero extra fetches)
    var lw = data.meta.dateRanges.lastWeek;
    for (var i = 0; i < data.stores.length; i++) {
      var store = data.stores[i];
      if (!store.id) continue;

      // Store detail cache (hourly heatmap + category trends)
      if (store.hourly) {
        var categoryTrend = (store.lwCategories || []).map(function(cat) {
          return { name: cat.name, lw_sales: cat.net_sales, lw_units: cat.units, pw_sales: 0, pw_units: 0, wow_pct: null };
        });
        await redis.setJSON(KEYS.storeDetail(store.id), {
          store: { id: store.id, name: store.name, color: store.color },
          hourly: store.hourly, hourlyWeeks: 2, categoryTrend: categoryTrend,
          lastWeek: lw, generatedAt: new Date().toISOString(),
        }, CACHE_TTL);
      }

      // Budtender cache
      if (store.budtenders) {
        await redis.setJSON(KEYS.budtenders(store.id), {
          store: { id: store.id, name: store.name, color: store.color },
          employees: store.budtenders, categories: store.lwCategories || [],
          week: lw, generatedAt: new Date().toISOString(),
        }, CACHE_TTL);
      }
    }

    console.log('  [dashboard] done ' + (Date.now() - t0) + 'ms (+ storeDetail + budtenders)');
    return data;
  } catch (err) {
    console.error('  [dashboard] FAIL: ' + err.message);
    return null;
  }
}

// -- TREND --------------------------------------------------------
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

// -- DAY VS DAY ---------------------------------------------------
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

// -- FULL REBUILD (parallel) --------------------------------------
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

    var results = await Promise.allSettled([
      rebuildDashboard(),
      rebuildTrend(locations, limit),
      rebuildDayVsDay(locations, limit),
    ]);

    var names = ['dashboard+stores', 'trend', 'dvd'];
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

async function rebuildSection(section) {
  var locations = await fh.getLocations();
  var limit = pLimit(CONCURRENCY);
  switch (section) {
    case 'trend':       return rebuildTrend(locations, limit);
    case 'dvd':         return rebuildDayVsDay(locations, limit);
    case 'dashboard':   return rebuildDashboard();
    case 'budtenders':  return rebuildDashboard();
    case 'storeDetail': return rebuildDashboard();
    case 'storeData':   return rebuildDashboard();
    default:            return { error: 'unknown section' };
  }
}

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
