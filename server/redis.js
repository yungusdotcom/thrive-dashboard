// server/redis.js
// ============================================================
// Redis Client — Thrive Dashboard
// Stale-while-revalidate cache layer
// ============================================================

const Redis = require('ioredis');

const REDIS_URL = process.env.REDIS_URL || process.env.REDIS_PRIVATE_URL || 'redis://localhost:6379';

let client = null;

function getClient() {
  if (client) return client;
  client = new Redis(REDIS_URL, {
    maxRetriesPerRequest: 3,
    retryStrategy(times) {
      if (times > 5) return null; // stop retrying
      return Math.min(times * 200, 2000);
    },
    lazyConnect: false,
  });
  client.on('connect', () => console.log('✓ Redis connected'));
  client.on('error', (err) => console.error('⚠ Redis error:', err.message));
  return client;
}

// ── JSON get/set ─────────────────────────────────────────────

async function getJSON(key) {
  try {
    const raw = await getClient().get(key);
    return raw ? JSON.parse(raw) : null;
  } catch (err) {
    console.error(`Redis GET ${key}:`, err.message);
    return null;
  }
}

async function setJSON(key, value, ttlSeconds) {
  try {
    const raw = JSON.stringify(value);
    if (ttlSeconds) {
      await getClient().set(key, raw, 'EX', ttlSeconds);
    } else {
      await getClient().set(key, raw);
    }
    return true;
  } catch (err) {
    console.error(`Redis SET ${key}:`, err.message);
    return false;
  }
}

// ── Distributed lock ─────────────────────────────────────────

async function acquireLock(key, ttlSeconds = 90) {
  try {
    const result = await getClient().set(key, '1', 'NX', 'EX', ttlSeconds);
    return result === 'OK';
  } catch (err) {
    console.error(`Redis LOCK ${key}:`, err.message);
    return false;
  }
}

async function releaseLock(key) {
  try {
    await getClient().del(key);
  } catch (err) {
    console.error(`Redis UNLOCK ${key}:`, err.message);
  }
}

// ── Health ────────────────────────────────────────────────────

async function ping() {
  try {
    const result = await getClient().ping();
    return result === 'PONG';
  } catch {
    return false;
  }
}

module.exports = { getClient, getJSON, setJSON, acquireLock, releaseLock, ping };
