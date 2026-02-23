# THRIVE Executive Intelligence Dashboard

Live sales analytics dashboard for Thrive Cannabis Marketplace — 7 stores via Flowhub Maui API.

## Architecture

```
Browser → Express (Railway) → Flowhub Auth0 → Bearer token
                            → GET /v1/orders/findByLocationId/{importId}
                            → NodeCache (5 min TTL)
```

## Setup

### 1. Install
```bash
npm install
```

### 2. Configure
```bash
cp .env.example .env
```

`.env` already has the correct `FLOWHUB_CLIENT_ID` and `FLOWHUB_API_KEY`.
Set `DASHBOARD_PASSWORD` to whatever you want as the login key.

### 3. Run locally
```bash
npm run dev
# → http://localhost:3000
```

## Deploy to Railway

1. Push to GitHub
2. New Railway project → "Deploy from GitHub repo"
3. Add environment variables (Settings → Variables):
   - `FLOWHUB_CLIENT_ID`
   - `FLOWHUB_API_KEY`
   - `DASHBOARD_PASSWORD`
   - `NODE_ENV=production`
4. Deploy — Railway auto-detects Node and runs `npm start`

## API Endpoints

| Route | Description |
|-------|-------------|
| `GET /api/dashboard` | This week + last week + today for all stores |
| `GET /api/trend?weeks=12` | 12-week rolling trend, all stores |
| `GET /api/sales?start=&end=` | All stores for date range |
| `GET /api/sales?start=&end=&store=cactus` | Single store |
| `GET /api/products?store=&start=&end=` | Top SKUs |
| `GET /api/categories?store=&start=&end=` | Category breakdown |
| `GET /api/employees?store=&start=&end=` | Budtender performance |
| `POST /api/cache/clear` | Force refresh |

All routes except `/health` and `/api/stores` require `?key=YOUR_PASSWORD`.

## How auth works

On startup the server calls `POST https://flowhub.auth0.com/oauth/token` with
`client_credentials` grant. The Bearer token is cached and auto-refreshed before expiry.
Every API call includes both `Authorization: Bearer <token>` and the raw
`clientId` / `key` headers as documented in the Flowhub Stoplight portal.

## Stores

| Dashboard ID | Flowhub Name |
|---|---|
| cactus | Cactus |
| cheyenne | Cheyenne |
| jackpot | Jackpot |
| main | Main Street |
| reno | Reno |
| sahara | Sahara |
| sammy | Sammy |

Smoke & Mirrors and MBNV are excluded automatically.
Location `importId` values are discovered at runtime via `GET /v0/clientsLocations`.
