# Thrive Dashboard

Executive intelligence dashboard for Thrive Cannabis Marketplace. Aggregates POS data from Flowhub's API across 7 retail dispensaries in Nevada and presents real-time sales, trend analysis, budtender performance, and operational metrics.

**Live:** `thrive-dashboard-production.up.railway.app`

---

## Architecture

```
Browser (index.html)
  ↓
Express Server (index.js)
  ↓
Redis Cache (stale-while-revalidate)
  ↓  (on cache miss, async background rebuild)
Flowhub POS API → rebuild.js → Redis
  ↑
Disk Cache (completed weeks persisted to /data volume)
```

### Data Flow

1. **Startup**: Server boots → checks Redis → if empty, triggers `rebuildAll()` async
2. **Request path**: Every API endpoint reads Redis first → returns cached data in <100ms
3. **Cache miss**: Returns fallback (direct Flowhub fetch or "building" status) → triggers async background rebuild for that section
4. **Rebuild**: Fetches from Flowhub with concurrency limit (2 stores at a time), aggregates, writes single Redis key per section
5. **Cache TTL**: 10 minutes. On expiry, next request triggers rebuild. No cron needed.
6. **Disk cache**: Completed weeks (immutable historical data) persisted to Railway volume at `/data/thrive-week-cache.json`. Survives redeploys.

---

## Tabs & Endpoints

| Tab | Endpoint | Redis Key | Description |
|-----|----------|-----------|-------------|
| Executive | `GET /api/dashboard` | `cache:dashboard` | Today + this week + last week, all 7 stores |
| Heatmap | `GET /api/trend` | `cache:trend:12w` | 12 weeks × 7 stores, weekly buckets |
| Velocity | `GET /api/trend` | `cache:trend:12w` | Same trend data, different visualization |
| Stores | `GET /api/trend/:storeId` | Extracted from `cache:trend:12w` | Single store 12-week trend + KPIs from dashboard |
| Stores (detail) | `GET /api/store-detail/:storeId` | `cache:store:{storeId}` | Hourly traffic heatmap (txns by hour × DOW) + category trends (LW vs PW with WoW%) |
| Day vs Day | `GET /api/day-vs-day?dow=N` | `cache:dvd:0` through `cache:dvd:6` | All 7 DOWs, 4 weeks back, all stores |
| Budtenders | `GET /api/employees?store=X` | `cache:bt:{storeId}` | Last week budtender stats per store. Sortable table (multi-column, 3-click cycle: desc → asc → reset) |
| Custom Range | `GET /api/sales?start=X&end=Y` | In-memory only (5 min) | User-driven date range, not pre-cacheable |

### Internal Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/internal/rebuild` | GET/POST | Trigger full rebuild (all sections) |
| `/internal/rebuild/:section` | POST | Rebuild one section: `trend`, `dvd`, `budtenders`, `dashboard` |
| `/internal/cache-status` | GET | Show cache state for all sections |
| `/health` | GET | Uptime, cache keys, Redis status |
| `/api/cache/clear` | POST | Flush in-memory + Redis caches |

---

## Stores (7 Locations)

| ID | Display Name | Color | Match Pattern |
|----|-------------|-------|---------------|
| cactus | Cactus | `#00e5a0` | "cactus" |
| cheyenne | Cheyenne | `#4db8ff` | "cheyenne" |
| jackpot | Jackpot | `#c084fc` | "jackpot" |
| main | Main Street | `#ffd166` | "main street" |
| reno | Reno | `#ff8c42` | "reno" |
| sahara | Sahara | `#ff4d6d` | "sahara" |
| sammy | Sammy | `#a8e6cf` | "sammy" |

Excluded from API results: locations matching "smoke", "mirrors", "mbnv", "cultivation".

---

## File Structure

```
thrive-dashboard/
├── server/
│   ├── index.js        # Express routes, auth, Redis-first endpoints
│   ├── flowhub.js      # Flowhub API client, order fetching, summarization
│   ├── redis.js        # ioredis client, JSON get/set, distributed lock
│   └── rebuild.js      # Background cache builder (trend, dvd, budtenders, dashboard)
├── public/
│   └── index.html      # Single-page dashboard (HTML/CSS/JS, no build step)
├── package.json
├── railway.toml
└── README.md
```

### server/flowhub.js
- **flowhubGet()**: Core API caller with 429/500 retry + exponential backoff
- **getLocations()**: Fetches and caches Flowhub locations, maps to store config
- **getOrdersForLocation()**: Paginated order fetch for short ranges (≤1 week)
- **streamBucketFetch()**: Memory-efficient streaming fetch for bulk ranges (12 weeks). Processes page-by-page, never holds >500 orders in RAM. Buckets into weekly accumulators on the fly.
- **summarizeOrders()**: Aggregates orders into KPIs (net sales, gross, avg basket, categories, budtenders, customer types)
- **summarizeHourly()**: Builds hour × day-of-week grid from order timestamps (converted to Pacific time). Returns transaction count and net sales per cell.
- **getTrendForStore()**: Per-store trend fetch used by rebuild worker. Uses disk cache for completed weeks.
- **Date helpers**: All dates computed in Pacific Time (`America/Los_Angeles`). Order timestamps converted from UTC to Pacific before bucketing.

### server/redis.js
- ioredis client with retry strategy
- `getJSON(key)` / `setJSON(key, value, ttl)` — JSON serialization wrapper
- `acquireLock(key, ttl)` / `releaseLock(key)` — distributed lock via `SET NX EX`
- `ping()` — health check

### server/rebuild.js
- **rebuildAll()**: Acquires lock → rebuilds dashboard → trend → store detail → budtenders → day-vs-day. Sequential by section, concurrent within section (2 stores at a time).
- **rebuildSection(name)**: Rebuild a single section on demand: `trend`, `dvd`, `budtenders`, `storeDetail`, `dashboard`
- **rebuildTrend()**: 12 weeks × 7 stores. Uses disk cache for completed weeks, only fetches current week fresh.
- **rebuildDayVsDay()**: All 7 DOWs × 4 weeks × 7 stores
- **rebuildStoreDetail()**: All stores — fetches last week + prior week orders. Builds hourly traffic heatmap (transactions by hour × day-of-week in Pacific time) and category WoW trends.
- **rebuildBudtenders()**: All stores, last week orders → budtender summaries
- **rebuildDashboard()**: Today + this week + last week for all stores
- Cache readers: `getCachedTrend()`, `getCachedDvd(dow)`, `getCachedBudtenders(id)`, `getCachedStoreDetail(id)`, `getCachedDashboard()`

### server/index.js
- Express server with auth middleware (optional `DASHBOARD_PASSWORD`)
- Internal auth for rebuild endpoints (optional `INTERNAL_SECRET`)
- Every historical endpoint: Redis first → fallback to direct Flowhub fetch → trigger async rebuild
- In-memory cache (node-cache) for custom range and other non-Redis endpoints
- Startup: warm locations, check Redis, trigger full rebuild if cache empty

### public/index.html
- Single-page app, no build step, no framework
- Dark theme with store-colored accents
- Mobile responsive (≤768px): scrollable tabs, 2-col KPIs, rotated chart bar labels, sticky heatmap columns
- Frontend normalizes Redis response format (object with `stores` key) into array format for rendering
- Handles "building" status with auto-retry every 10 seconds
- Budtender table: multi-column sortable (click header cycles desc → asc → reset, priority numbers shown)

---

## Environment Variables

| Variable | Required | Description |
|----------|----------|-------------|
| `FLOWHUB_CLIENT_ID` | Yes | Flowhub API client ID |
| `FLOWHUB_API_KEY` | Yes | Flowhub API key |
| `REDIS_URL` | Yes | Redis connection string (auto-set by Railway Redis addon) |
| `PORT` | No | Server port (default: 3000, Railway sets 8080) |
| `CACHE_DIR` | No | Disk cache directory (default: `/tmp`, set to `/data` with Railway volume) |
| `DASHBOARD_PASSWORD` | No | Password for API endpoints. If unset, no auth required. |
| `INTERNAL_SECRET` | No | Secret for `/internal/*` endpoints. If unset, no auth required. |
| `CACHE_TTL` | No | In-memory cache TTL in seconds (default: 300) |

---

## Railway Setup

1. **Service**: Node.js, auto-detected from package.json
2. **Redis**: Add via New → Database → Redis. Reference `REDIS_URL` in service variables.
3. **Volume**: Right-click service → Add Volume → mount at `/data`. Set env `CACHE_DIR=/data`.
4. **Environment**: Set `FLOWHUB_CLIENT_ID` and `FLOWHUB_API_KEY`.

### railway.toml
```toml
[build]
builder = "NIXPACKS"

[deploy]
startCommand = "node server/index.js"
healthcheckPath = "/health"
healthcheckTimeout = 300
restartPolicyType = "ON_FAILURE"
restartPolicyMaxRetries = 3
```

---

## Key Design Decisions

### Pacific Timezone Handling
All date logic uses `America/Los_Angeles`. Order `createdAt` timestamps from Flowhub are UTC — converted to Pacific before any date comparison or week bucketing. This prevents orders from showing on the wrong day after 4 PM PST.

### Streaming Bucket Fetch
For 12-week bulk fetches (~11K+ orders per store), orders are processed page-by-page (500/page) and immediately bucketed into lightweight accumulators. Raw order data is discarded after each page. Peak memory: ~500 orders vs ~20,000 with naive approach. Prevents OOM on Railway's 512MB containers.

### Partial Week Handling
Current week is always marked as "in progress":
- Executive tab: no WoW% shown for current week
- Trend chart: current week bar rendered at 50% opacity in neutral gray
- Heatmap: current week row dimmed, shows "IN PROGRESS"
- Velocity: rolling averages exclude partial current week
- WoW momentum: compares last 2 *completed* weeks only

### Sales Calculation
`net_sales = totalPrice - totalDiscounts` per line item, summed across all non-voided orders. Voided orders (`voided === true` or `orderStatus === 'voided'`) are excluded.

---

## Dependencies

```json
{
  "cors": "^2.8.5",
  "dotenv": "^16.3.1",
  "express": "^4.18.2",
  "ioredis": "^5.3.2",
  "node-cache": "^5.1.2",
  "node-fetch": "^2.7.0"
}
```

No build step. No bundler. No frontend framework.
