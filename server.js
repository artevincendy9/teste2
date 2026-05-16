const express = require('express');
const cors = require('cors');
const fs = require('fs').promises;
const fsSync = require('fs');
const path = require('path');
const crypto = require('crypto');
const multer = require('multer');
const { ProxyAgent, fetch: undiciFetch } = require('undici');

const app = express();
const PORT = process.env.PORT || 5000;

// ─── Safe error message extraction ────────────────────────────────────────────
// Handles Error objects, plain strings, numbers, null/undefined, and anything else
// thrown — guarantees callers always get a non-empty string back.
function toMsg(e, fallback = 'Unknown error') {
  if (!e) return fallback;
  if (typeof e === 'string') return e || fallback;
  if (e instanceof Error) return e.message || e.toString() || fallback;
  if (typeof e.message === 'string') return e.message || fallback;
  try { return JSON.stringify(e); } catch { return fallback; }
}

app.use(cors());
app.use(express.json({ limit: '50mb' }));
app.use(express.static(path.join(__dirname, 'public')));
app.use('/uploads', express.static(path.join(__dirname, 'uploads')));

const storage = multer.diskStorage({
  destination: (req, file, cb) => {
    const dir = req.path.includes('host')
      ? path.join(__dirname, 'uploads/hosted')
      : path.join(__dirname, 'uploads/csv');
    try { fsSync.mkdirSync(dir, { recursive: true }); } catch (e) {}
    cb(null, dir);
  },
  filename: (req, file, cb) => cb(null, Date.now() + '-' + file.originalname)
});
const upload = multer({ storage, limits: { fileSize: 50 * 1024 * 1024 } });

// ─── In-memory state ─────────────────────────────────────────────────────────
let stores = [];
let proxies = [];
let config = {
  processingMode: 'order_create_delivery',
  apiVersion: '2026-04',
  carrier: 'USPS',
  delayMs: 650,
  notifyCustomer: true,
  sendInvoiceEmail: true,
  sendPaidEmail: true,
  marketingConsent: true,
  smsConsent: true,
  taxExempt: false,
  sourceName: 'bulk-api',
  note: 'Order placed via website',
  tags: 'website,customer',
  randomizeTags: true,
  randomizeSource: true,
  randomize: true,
  defaultLineItems: [{ title: 'Premium Digital Package', price: '49.99', quantity: 1, sku: 'PREM-001' }],
  sufioEnabled: false,
  spreadAcrossStores: false,
  rotateStoreEveryN: 0,
  warmupOrders: 0,
  warmupDelayMs: 2000,
  maxOrdersPerHour: 0,
  userPin: '1234',
  adminPin: '6001',
};
let uploadedData = [];
let history = [];
let linkHistory = [];
let linkStore = {};
let hostedFile = null;
let currentJob = null;
let clients = new Set();
let proxyIndex = 0;

let jobState = null; // persisted job state for resume
let appHost = process.env.REPLIT_DEV_DOMAIN ? `https://${process.env.REPLIT_DEV_DOMAIN}` : null;
app.use((req, res, next) => {
  if (!appHost) {
    const fwdHost = req.get('x-forwarded-host') || req.get('host') || '';
    const fwdProto = req.get('x-forwarded-proto') || req.protocol || 'https';
    appHost = `${fwdProto.split(',')[0].trim()}://${fwdHost.split(',')[0].trim()}`;
  }
  next();
});

// ─── Persist helpers ──────────────────────────────────────────────────────────
const DATA_DIR = __dirname;
const fp = (name) => path.join(DATA_DIR, name);

async function loadAll() {
  try { stores = JSON.parse(await fs.readFile(fp('stores.json'), 'utf8')); } catch (e) { stores = []; }
  try { proxies = JSON.parse(await fs.readFile(fp('proxies.json'), 'utf8')); } catch (e) { proxies = []; }
  try { config = { ...config, ...JSON.parse(await fs.readFile(fp('config.json'), 'utf8')) }; } catch (e) {}
  try { uploadedData = JSON.parse(await fs.readFile(fp('data.json'), 'utf8')); } catch (e) { uploadedData = []; }
  try { history = JSON.parse(await fs.readFile(fp('history.json'), 'utf8')); } catch (e) { history = []; }
  try { linkHistory = JSON.parse(await fs.readFile(fp('link-history.json'), 'utf8')); } catch (e) { linkHistory = []; }
  try { linkStore = JSON.parse(await fs.readFile(fp('link-store.json'), 'utf8')); } catch (e) { linkStore = {}; }
  try { hostedFile = JSON.parse(await fs.readFile(fp('hosted-file.json'), 'utf8')); } catch (e) { hostedFile = null; }
  try { jobState = JSON.parse(await fs.readFile(fp('job-state.json'), 'utf8')); } catch (e) { jobState = null; }
}
loadAll();

const save = {
  stores: () => fs.writeFile(fp('stores.json'), JSON.stringify(stores, null, 2)),
  proxies: () => fs.writeFile(fp('proxies.json'), JSON.stringify(proxies, null, 2)),
  config: () => fs.writeFile(fp('config.json'), JSON.stringify(config, null, 2)),
  data: () => fs.writeFile(fp('data.json'), JSON.stringify(uploadedData, null, 2)),
  history: () => fs.writeFile(fp('history.json'), JSON.stringify(history.slice(0, 500), null, 2)),
  linkHistory: () => fs.writeFile(fp('link-history.json'), JSON.stringify(linkHistory.slice(0, 5000), null, 2)),
  linkStore: () => fs.writeFile(fp('link-store.json'), JSON.stringify(linkStore, null, 2)),
  hostedFile: () => fs.writeFile(fp('hosted-file.json'), JSON.stringify(hostedFile, null, 2)),
  jobState: () => fs.writeFile(fp('job-state.json'), JSON.stringify(jobState, null, 2))
};

// Debounced stores save — flushes at most every 2 s during bulk, or immediately on demand
let _storesSaveTimer = null;
function debounceSaveStores(urgentMs = 0) {
  if (urgentMs === 0) { clearTimeout(_storesSaveTimer); _storesSaveTimer = null; return save.stores(); }
  if (_storesSaveTimer) return;
  _storesSaveTimer = setTimeout(() => { _storesSaveTimer = null; save.stores().catch(() => {}); }, urgentMs);
}

// ─── SSE heartbeat (25 s) — prevents proxy/load-balancer idle timeouts ────────
setInterval(() => {
  clients.forEach(c => { try { c.write(': heartbeat\n\n'); } catch (e) { clients.delete(c); } });
}, 25000);

// ─── SSE broadcast ────────────────────────────────────────────────────────────
function broadcast(data) {
  const msg = `data: ${JSON.stringify(data)}\n\n`;
  clients.forEach(c => { try { c.write(msg); } catch (e) { clients.delete(c); } });
}

// ─── Proxy rotation ───────────────────────────────────────────────────────────
function getNextProxy() {
  const active = proxies.filter(p => p.status === 'active');
  if (!active.length) return null;
  const p = active[proxyIndex % active.length];
  proxyIndex++;
  return p;
}

// ─── Browser fingerprint profiles ─────────────────────────────────────────────
// Each profile bundles a UA + its exact matching Sec-Ch-Ua + its platform so
// they can never mismatch. Only proven desktop Chrome on Windows — the most
// common real-world combination for browser-initiated API traffic.
// Versions are deliberately conservative: slightly behind the bleeding edge
// (not all users auto-update instantly) but not old enough to look stale.
const BROWSER_PROFILES = [
  {
    ua: 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
    secChUa: '"Chromium";v="124", "Google Chrome";v="124", "Not-A.Brand";v="24"',
    platform: '"Windows"',
  },
  {
    ua: 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36',
    secChUa: '"Chromium";v="125", "Google Chrome";v="125", "Not-A.Brand";v="8"',
    platform: '"Windows"',
  },
  {
    ua: 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36',
    secChUa: '"Chromium";v="126", "Google Chrome";v="126", "Not-A.Brand";v="24"',
    platform: '"Windows"',
  },
];

// Common US-centric Accept-Language values — matches our Windows Chrome profiles
const ACCEPT_LANG_POOL = [
  'en-US,en;q=0.9',
  'en-US,en;q=0.8',
  'en-US,en;q=0.9,en-GB;q=0.8',
];

// ─── Per-order browser fingerprint ───────────────────────────────────────────
// All API calls within one order share the same UA/IP/cookies so Shopify's
// server-side analytics never sees a single "order" made by 3 different browsers.
let _orderProfile  = null;
let _orderIp       = null;
let _orderCookies  = null;

// US residential ISP first-octets (Comcast, Spectrum, AT&T, Verizon, Cox…)
const US_RESIDENTIAL_PREFIXES = [24, 66, 68, 71, 73, 75, 76, 97, 98, 99, 100, 107, 108, 174];
function randomUsResidentialIp() {
  const a = US_RESIDENTIAL_PREFIXES[Math.floor(Math.random() * US_RESIDENTIAL_PREFIXES.length)];
  const b = Math.floor(Math.random() * 254) + 1;
  const c = Math.floor(Math.random() * 254) + 1;
  const d = Math.floor(Math.random() * 254) + 1;
  return `${a}.${b}.${c}.${d}`;
}

function randomShopifyCookies() {
  // _shopify_y: long-lived visitor token  _shopify_s: short-lived session token
  const y = crypto.randomUUID().replace(/-/g, '');
  const s = crypto.randomUUID().replace(/-/g, '').slice(0, 26);
  const cart = crypto.randomBytes(8).toString('hex');
  return `_shopify_y=${y}; _shopify_s=${s}; cart=${cart}`;
}

// Call once per order to lock in a consistent fingerprint for all sub-requests
function setOrderFingerprint() {
  _orderProfile = BROWSER_PROFILES[Math.floor(Math.random() * BROWSER_PROFILES.length)];
  _orderIp      = randomUsResidentialIp();
  _orderCookies = randomShopifyCookies();
}

// ─── Shopify core fetch (retry + rate-limit aware + proxy support) ─────────────
async function shopifyFetch(storeDomain, accessToken, url, options = {}, retries = 3) {
  // Reuse the per-order locked fingerprint; fall back to a fresh random one
  // (e.g. for one-off calls outside the main order loop like store tests)
  const profile  = _orderProfile  || BROWSER_PROFILES[Math.floor(Math.random() * BROWSER_PROFILES.length)];
  const fwdIp    = _orderIp       || randomUsResidentialIp();
  const cookies  = _orderCookies  || randomShopifyCookies();
  const headers = {
    'X-Shopify-Access-Token': accessToken,
    'Content-Type': 'application/json',
    'Accept': 'application/json',
    'User-Agent': profile.ua,
    'Accept-Language': ACCEPT_LANG_POOL[Math.floor(Math.random() * ACCEPT_LANG_POOL.length)],
    'X-Request-ID': crypto.randomBytes(12).toString('hex'),
    // Sec-Ch-Ua headers must exactly match the UA — never mix versions
    'Sec-Ch-Ua': profile.secChUa,
    'Sec-Ch-Ua-Mobile': '?0',
    'Sec-Ch-Ua-Platform': profile.platform,
    'Sec-Fetch-Dest': 'empty',
    'Sec-Fetch-Mode': 'cors',
    'Sec-Fetch-Site': 'same-origin',
    // Simulate a real customer browser: forwarded IP, page referer, session cookies
    'X-Forwarded-For': fwdIp,
    'X-Real-IP': fwdIp,
    'Referer': `https://${storeDomain}/`,
    'Cookie': cookies,
    ...options.headers
  };

  const proxy = getNextProxy();
  let dispatcher;
  if (proxy) {
    try {
      const auth = proxy.username ? `${encodeURIComponent(proxy.username)}:${encodeURIComponent(proxy.password || '')}@` : '';
      dispatcher = new ProxyAgent(`http://${auth}${proxy.host}:${proxy.port}`);
      const pidx = proxies.findIndex(p => p.id === proxy.id);
      if (pidx >= 0) { proxies[pidx].usageCount = (proxies[pidx].usageCount || 0) + 1; proxies[pidx].lastUsed = new Date().toISOString(); }
    } catch (e) { dispatcher = undefined; }
  }

  const doFetch = dispatcher
    ? (u, o) => undiciFetch(u, { ...o, dispatcher })
    : (u, o) => fetch(u, o);

  let lastError = new Error('Request failed after all retries');
  for (let i = 0; i < retries; i++) {
    try {
      const res = await doFetch(url, { ...options, headers });
      if (res.status === 429) {
        const retryAfter = parseInt(res.headers.get('Retry-After') || '3');
        const wait = (retryAfter + 1) * 1000;
        console.warn(`[Shopify] 429 rate limited — waiting ${retryAfter + 1}s`);
        // Update lastError so that if ALL retries exhaust on 429s, we still throw a real Error
        lastError = new Error(`HTTP 429: Rate limited — waited ${retryAfter + 1}s, exhausted ${retries} retries`);
        lastError.httpStatus = 429;
        await sleep(wait);
        continue;
      }
      // Monitor API call bucket — throttle proactively when nearing limit
      const callLimit = res.headers.get('X-Shopify-Api-Call-Limit') || '';
      if (callLimit) {
        const [used, max] = callLimit.split('/').map(Number);
        if (max && used / max > 0.80) {
          // Over 80% of bucket used — pause to let it refill
          const pauseMs = Math.round(((used / max) - 0.75) * 4000);
          if (pauseMs > 200) await sleep(pauseMs);
        }
      }
      const text = await res.text();
      if (!res.ok) {
        let errMsg = `HTTP ${res.status}`;
        try {
          const p = JSON.parse(text);
          if (p.errors) errMsg = typeof p.errors === 'string' ? p.errors : JSON.stringify(p.errors);
          else if (p.error_description) errMsg = p.error_description;
          else if (p.error) errMsg = p.error;
          else errMsg = `HTTP ${res.status}: ${text.slice(0, 300)}`;
        } catch { errMsg = `HTTP ${res.status}: ${text.slice(0, 300)}`; }
        const err = new Error(errMsg);
        err.httpStatus = res.status;
        // Auth errors surface immediately — never retry at this level
        if (res.status === 401 || res.status === 403) throw err;
        throw err;
      }
      return JSON.parse(text);
    } catch (err) {
      lastError = err instanceof Error ? err : new Error(toMsg(err));
      if (lastError.httpStatus === 401 || lastError.httpStatus === 403) throw lastError;
      // Use short retry delay — don't burn bucket with long retries
      if (i < retries - 1) await sleep(600 * (i + 1));
    }
  }
  throw lastError;
}

function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

// ─── Store health management ───────────────────────────────────────────────────
const storeCooldowns    = new Map(); // storeId → cooldownUntil (epoch ms)
const storeHourlyOrders = new Map(); // storeId → number[] of successful order timestamps (rolling 1 hr)

function pruneStoreHourly(storeId) {
  const now = Date.now();
  const ts = (storeHourlyOrders.get(storeId) || []).filter(t => now - t < 3600000);
  storeHourlyOrders.set(storeId, ts);
  return ts.length;
}
function recordStoreHourlyOrder(storeId) {
  const ts = storeHourlyOrders.get(storeId) || [];
  ts.push(Date.now());
  storeHourlyOrders.set(storeId, ts);
}
function storeHourlyLimitReached(storeId, limit) {
  return limit > 0 && pruneStoreHourly(storeId) >= limit;
}

function isStoreBlocked(errMsg) {
  const m = (errMsg || '').toLowerCase();
  return m.includes('http 401') || m.includes('http 403') ||
         (m.includes('401') && (m.includes('unauthorized') || m.includes('access token'))) ||
         m.includes('403') || m.includes('forbidden') ||
         m.includes('invalid api key') || m.includes('invalid token') ||
         m.includes('shop not found') || m.includes('shop is unavailable') ||
         m.includes('account suspended') || m.includes('deactivated') ||
         m.includes('account is locked') || m.includes('store not found');
}

function getAvailableStore(preferredStoreId = null) {
  const now = Date.now();
  for (const [id, until] of storeCooldowns) { if (now >= until) storeCooldowns.delete(id); }
  const active = stores.filter(s => s.status === 'active' && !storeCooldowns.has(s.id));
  if (!active.length) {
    const anyActive = stores.filter(s => s.status === 'active');
    if (!anyActive.length) throw new Error('No active store configured. Go to Settings → Add Store.');
    const soonestMs = Math.min(...[...storeCooldowns.values()]);
    const waitSecs = Math.ceil((soonestMs - now) / 1000);
    const err = new Error(`ALL_COOLING:${waitSecs}`);
    err.allCooling = true;
    throw err;
  }
  // Sequential failover: prefer the current active store, only switch on failure
  if (preferredStoreId) {
    const preferred = active.find(s => s.id === preferredStoreId);
    if (preferred) return preferred;
  }
  // No preferred store (first call or after failover) — pick first available in order added
  return active[0];
}

async function applyStoreCooldown(storeId, durationMs = 30000) {
  storeCooldowns.set(storeId, Date.now() + durationMs);
  const idx = stores.findIndex(s => s.id === storeId);
  if (idx >= 0) {
    stores[idx].status = 'inactive';
    stores[idx].lastErrorAt = new Date().toISOString();
    await debounceSaveStores(0); // urgent
    broadcast({ type: 'store_disabled', storeId, domain: stores[idx].domain, name: stores[idx].storeName || stores[idx].domain, cooldownSecs: Math.ceil(durationMs / 1000) });
  }
}

// ─── Phone utilities ──────────────────────────────────────────────────────────
function normalizePhone(phone) {
  if (!phone) return null;
  const digits = String(phone).replace(/\D/g, '');
  if (digits.length === 10) return `+1${digits}`;
  if (digits.length === 11 && digits[0] === '1') return `+${digits}`;
  if (digits.length >= 7 && digits.length <= 15) return `+${digits}`;
  return null;
}
function isPhoneNumber(str) {
  if (!str || String(str).includes('@')) return false;
  const digits = String(str).replace(/\D/g, '');
  return digits.length >= 7 && digits.length <= 15;
}

// ─── UA parser ────────────────────────────────────────────────────────────────
function parseUA(ua) {
  if (!ua) return { os: 'Unknown', browser: 'Unknown', device: 'Desktop' };
  const ual = ua.toLowerCase();
  const os = ua.includes('Windows') ? 'Windows' : ua.includes('Mac OS') ? 'macOS' : ua.includes('iPhone') || ua.includes('iPad') ? 'iOS' : ua.includes('Android') ? 'Android' : ua.includes('Linux') ? 'Linux' : 'Unknown';
  const browser = ual.includes('edg/') ? 'Edge' : ual.includes('chrome/') ? 'Chrome' : ual.includes('firefox/') ? 'Firefox' : ual.includes('safari/') ? 'Safari' : ual.includes('opera') ? 'Opera' : 'Unknown';
  const device = ua.includes('Mobile') || ua.includes('Android') || ua.includes('iPhone') ? 'Mobile' : 'Desktop';
  const chromeVer = ua.match(/Chrome\/([\d.]+)/)?.[1]?.split('.')[0];
  const ffVer = ua.match(/Firefox\/([\d.]+)/)?.[1]?.split('.')[0];
  const browserVersion = chromeVer || ffVer || '';
  return { os, browser: browserVersion ? `${browser} ${browserVersion}` : browser, device };
}

// ─── Data generators ──────────────────────────────────────────────────────────
function generateAddress(identifier) {
  const raw = (String(identifier || 'customer').split('@')[0]).replace(/[^a-zA-Z]/g, '') || 'Customer';
  const first = raw.slice(0, 1).toUpperCase() + raw.slice(1, 14);
  const lasts = ['Smith', 'Johnson', 'Williams', 'Brown', 'Davis', 'Miller', 'Wilson', 'Moore', 'Taylor', 'Anderson'];
  const last = lasts[Math.floor(Math.random() * lasts.length)];
  const locs = [
    { city: 'New York', province: 'NY', zip: '10001' },
    { city: 'Los Angeles', province: 'CA', zip: '90001' },
    { city: 'Chicago', province: 'IL', zip: '60601' },
    { city: 'Houston', province: 'TX', zip: '77001' },
    { city: 'Phoenix', province: 'AZ', zip: '85001' },
    { city: 'Seattle', province: 'WA', zip: '98101' },
    { city: 'Miami', province: 'FL', zip: '33101' },
    { city: 'Denver', province: 'CO', zip: '80201' },
    { city: 'Atlanta', province: 'GA', zip: '30301' },
    { city: 'Dallas', province: 'TX', zip: '75201' }
  ];
  const loc = locs[Math.floor(Math.random() * locs.length)];
  const streets = ['Commerce Drive', 'Market Street', 'Oak Avenue', 'Maple Lane', 'Washington Blvd', 'Park Place', 'Broadway', 'Main Street'];
  return {
    first_name: first, last_name: last,
    address1: `${Math.floor(Math.random() * 9000) + 100} ${streets[Math.floor(Math.random() * streets.length)]}`,
    city: loc.city, province: loc.province, country: 'US', zip: loc.zip,
    phone: `+1${Math.floor(Math.random() * 9000000000) + 1000000000}`
  };
}

// Build address: prefers real CSV data from options, falls back to generated values.
// CSV columns supported (case-insensitive variants):
//   first_name / last_name / address_line / city / state / postal_code / country
function buildAddress(options, fallbackId) {
  const fb = generateAddress(fallbackId);
  const get = (...keys) => {
    for (const k of keys) {
      const v = (options[k] || options[k.toLowerCase()] || options[k.toUpperCase()] || '');
      const s = String(v || '').trim();
      if (s) return s;
    }
    return null;
  };
  return {
    first_name: get('first_name', 'First_Name', 'First Name', 'firstname') || fb.first_name,
    last_name:  get('last_name',  'Last_Name',  'Last Name',  'lastname')  || fb.last_name,
    address1:   get('address_line','address1','address','Address','Address Line','street') || fb.address1,
    city:       get('city','City') || fb.city,
    province:   get('state','State','province','Province') || fb.province,
    zip:        get('postal_code','zip','ZIP','Zip','Postal Code','postcode') || fb.zip,
    country:    get('country','Country') || 'US',
  };
}

function generateTracking(carrier) {
  const r = crypto.randomBytes(5).toString('hex').toUpperCase();
  const n = Math.floor(Math.random() * 9000) + 1000;
  const map = {
    'USPS': `9400111899${r}${n}`,
    'UPS': `1Z9999W8${r}${n}`,
    'FedEx': `77491${r}${n}`,
    'DHL': `JD01${r}${n}`,
    'DHL eCommerce': `GM${r}${n}`,
    'DHL Express': `DHL${r}${n}`,
    'Canada Post': `CP${r}${n}CA`,
    'Australia Post': `AP${r}${n}AU`,
    'New Zealand Post': `NZ${r}${n}NZ`,
    'Royal Mail': `RM${r}${n}GB`,
    'PostNL': `NL${r}${n}`,
    'Deutsche Post': `DP${r}${n}DE`,
    'La Poste': `FR${r}${n}`,
    'Japan Post (EN)': `JP${r}${n}JP`,
    'Singapore Post': `SP${r}${n}SG`,
    'China Post': `CP${r}${n}CN`,
    'SF Express': `SF${r}${n}`,
    'Purolator': `PUR${r}${n}`,
    'OnTrac': `OT${r}${n}`,
    'Lasership': `LS${r}${n}`,
    'GLS': `GLS${r}${n}`,
    'TNT': `TNT${r}${n}`,
    'Fastway': `FW${r}${n}`,
    'Sendle': `SN${r}${n}`,
    'Toll IPEC': `TL${r}${n}`,
    'Startrack': `ST${r}${n}`,
    'Aramex': `ARM${r}${n}`,
    'Correios': `BR${r}${n}`,
    '4PX': `4PX${r}${n}`,
    'Cainiao': `CN${r}${n}`,
    'Yanwen': `YW${r}${n}`,
    'Amazon Logistics US': `TBA${r}${n}`,
    'Amazon Logistics UK': `QH${r}${n}`,
    'Passport': `PP${r}${n}`,
    'Seko Logistics': `SK${r}${n}`,
    'Globegistics': `GB${r}${n}`,
  };
  return map[carrier] || `TRK${r}${n}`;
}

function applyTag(str, url) {
  return str ? str.replace(/\{link_maste\}/gi, url || '') : str;
}

function buildEmailMarketingConsent(accepted) {
  if (accepted === false) return { state: 'not_subscribed', opt_in_level: 'single_opt_in' };
  return { state: 'subscribed', opt_in_level: 'single_opt_in', consent_updated_at: new Date().toISOString() };
}
function buildSmsMarketingConsent(accepted) {
  if (accepted === false) return { state: 'not_subscribed', opt_in_level: 'single_opt_in' };
  return { state: 'subscribed', opt_in_level: 'single_opt_in', consent_collected_from: 'OTHER', consent_updated_at: new Date().toISOString() };
}

// ─── Shopify API builders ──────────────────────────────────────────────────────
function isPhoneTakenError(msg) {
  const m = (msg || '').toLowerCase();
  return m.includes('phone has already been taken') || m.includes('phone number is already in use') ||
    m.includes('customer phone_number_change_not_allowed') || (m.includes('phone') && m.includes('already'));
}

async function findCustomerByPhone(store, normalPhone) {
  try {
    const d = await shopifyFetch(store.domain, store.accessToken,
      `https://${store.domain}/admin/api/${config.apiVersion}/customers/search.json?query=phone:${encodeURIComponent(normalPhone)}&limit=1`, { method: 'GET' });
    return d.customers?.[0] || null;
  } catch (e) { return null; }
}

async function findCustomerByEmail(store, email) {
  try {
    const d = await shopifyFetch(store.domain, store.accessToken,
      `https://${store.domain}/admin/api/${config.apiVersion}/customers/search.json?query=email:${encodeURIComponent(email)}&limit=1`, { method: 'GET' });
    return d.customers?.[0] || null;
  } catch (e) { return null; }
}

// Build a clean order payload.
// existingCustomerId: when set, attaches the known customer by id (used in conflict-retry path).
// We do NOT include a nested customer object on normal attempts — Shopify's Orders API
// finds or creates the customer automatically from the top-level email/phone, with zero
// conflict errors on duplicates. Name comes from billing/shipping address.
function buildOrderPayload(email, normalPhone, addr, lineItems, options, total, noteAttributes, withPhone = true, existingCustomerId = null) {
  const sendPaidEmail = (options.sendPaidEmail !== false) && (config.sendPaidEmail !== false);
  const forceEmail = options.forceEmail === true || config.forceEmail === true;
  const notify = forceEmail || ((options.notifyCustomer !== false) && (config.notifyCustomer !== false));
  const usePhone = withPhone && normalPhone;
  // productType: 'digital' means no physical shipping; 'physical' means shipping required
  const productType = options.productType || config.productType || 'physical';
  const isDigital = productType === 'digital';
  return {
    order: {
      ...(email ? { email } : {}),
      ...(usePhone ? { phone: normalPhone } : {}),
      send_receipt: true,
      send_fulfillment_receipt: notify,
      // Only inject customer object when we have a confirmed existing customer id
      ...(existingCustomerId ? { customer: { id: existingCustomerId } } : {}),
      line_items: lineItems.map(li => ({
        title: li.title || 'Product',
        price: parseFloat(li.price || 0).toFixed(2),
        quantity: parseInt(li.quantity || 1),
        ...(li.sku ? { sku: li.sku } : {}),
        // Always requires_shipping: true so Shopify creates fulfillment orders (needed for notifications)
        // Digital products get an "Online" shipping line with $0 cost instead
        requires_shipping: true,
        taxable: li.taxable !== false
      })),
      financial_status: 'paid',
      transactions: [{ kind: 'sale', status: 'success', amount: total.toFixed(2), gateway: randomGateway(options.gateway) }],
      billing_address: { ...addr, name: `${addr.first_name} ${addr.last_name}` },
      shipping_address: { ...addr, name: `${addr.first_name} ${addr.last_name}` },
      note: randomNote(options._note),
      tags: randomizeOrderTags(options.tags),
      currency: 'USD',
      source_name: randomSource(options.sourceName),
      ...(noteAttributes.length ? { note_attributes: noteAttributes } : {}),
      // Digital products: add "Online" shipping line so delivery method shows correctly
      ...(isDigital
        ? { shipping_lines: [{ title: 'Online', price: '0.00', code: 'ONLINE', source: 'shopify' }] }
        : buildShippingLines(options)),
      ...buildDiscountCodes(options)
    }
  };
}

async function createPaidOrder(store, email, phone, lineItems, options, linkUrl) {
  const addr = buildAddress(options, email || phone);
  const normalPhone = phone ? normalizePhone(phone) : null;
  if (normalPhone) addr.phone = normalPhone;
  const total = lineItems.reduce((s, i) => s + parseFloat(i.price || 0) * parseInt(i.quantity || 1), 0);
  const note = applyTag(options.note || config.note, linkUrl);
  const noteAttributes = [
    ...(linkUrl ? [{ name: 'link_maste', value: linkUrl }] : []),
    ...(options.poNumber || config.poNumber ? [{ name: 'PO Number', value: options.poNumber || config.poNumber }] : [])
  ];
  const opts = { ...options, _note: note };

  const postOrder = async (p) => {
    const data = await shopifyFetch(store.domain, store.accessToken,
      `https://${store.domain}/admin/api/${config.apiVersion}/orders.json`,
      { method: 'POST', body: JSON.stringify(p) });
    return data.order;
  };

  // Attempt 1: no customer object — Shopify auto-resolves from email/phone (zero duplicate errors)
  try {
    return await postOrder(buildOrderPayload(email, normalPhone, addr, lineItems, opts, total, noteAttributes, true, null));
  } catch (e) {
    const msg = toMsg(e);
    const m = msg.toLowerCase();
    const phoneTaken = normalPhone && isPhoneTakenError(msg);
    const anyTaken = m.includes('has already been taken') || m.includes('already been taken');

    if (!phoneTaken && !anyTaken) throw (e instanceof Error ? e : new Error(msg)); // unrelated error — surface immediately

    console.warn(`[Order] Conflict (${msg.slice(0, 80)}) — looking up existing customer for retry…`);

    // Identify the conflicting customer and attach by id on retry
    let existingCust = null;
    if (normalPhone && phoneTaken) existingCust = await findCustomerByPhone(store, normalPhone);
    if (!existingCust && email) existingCust = await findCustomerByEmail(store, email);

    // Retry with explicit customer id (and without the conflicting phone if phone caused it)
    return await postOrder(buildOrderPayload(
      email, normalPhone, addr, lineItems, opts, total, noteAttributes,
      !phoneTaken,          // withPhone — drop phone if it caused the conflict
      existingCust?.id ?? null  // existingCustomerId — attach by id if found
    ));
  }
}

function buildShippingLines(options) {
  if (!options.shippingTitle && !config.shippingTitle) return {};
  return { shipping_lines: [{ title: options.shippingTitle || config.shippingTitle || 'Standard Shipping', price: parseFloat(options.shippingPrice || config.shippingPrice || 0).toFixed(2), code: options.shippingCode || 'STANDARD', source: 'shopify-api' }] };
}
function buildDiscountCodes(options) {
  const code = options.discountCode || config.discountCode;
  const amount = options.discountAmount || config.discountAmount;
  const type = options.discountType || config.discountType || 'fixed_amount';
  if (!code && !amount) return {};
  return { discount_codes: [{ code: code || 'BULK-DISCOUNT', amount: parseFloat(amount || 0).toFixed(2), type }] };
}

// ─── Sufio background invoicing ───────────────────────────────────────────────
// Each order gets its own independent background timer so there are zero shared-
// state race conditions and every order is guaranteed to be visited.
// The warmup delay gives Shopify's outbound webhook time to reach Sufio before
// we visit the "view invoice" redirect — otherwise Sufio has no record yet.
const SUFIO_WARMUP_MS  = 7000;  // ms to wait after order creation before first visit
const SUFIO_RETRY_MAX  = 4;     // how many times to attempt the visit
const SUFIO_RETRY_WAIT = 9000;  // ms between retry attempts

async function visitSufioUrl(sufioUrl, orderId) {
  for (let attempt = 1; attempt <= SUFIO_RETRY_MAX; attempt++) {
    try {
      const ctrl = new AbortController();
      const timer = setTimeout(() => ctrl.abort(), 20000);
      const r = await fetch(sufioUrl, {
        method: 'GET',
        redirect: 'follow',
        signal: ctrl.signal,
        headers: {
          'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
          'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
          'Accept-Language': 'en-US,en;q=0.9',
          'Cache-Control': 'no-cache',
          'Connection': 'keep-alive'
        }
      });
      clearTimeout(timer);
      console.log(`[Sufio] ✅ Order ${orderId} attempt ${attempt} → HTTP ${r.status}`);
      return; // done — don't retry on success
    } catch (e) {
      console.warn(`[Sufio] ⚠ Order ${orderId} attempt ${attempt} failed: ${toMsg(e)}`);
      if (attempt < SUFIO_RETRY_MAX) await sleep(SUFIO_RETRY_WAIT);
    }
  }
  console.warn(`[Sufio] ❌ Order ${orderId} — all ${SUFIO_RETRY_MAX} attempts exhausted`);
}

function callSufioInvoice(store, orderId) {
  if (!config.sufioEnabled) return null;
  const shopDomain = (store.sufioShopDomain || '').trim() || store.domain;
  const sufioUrl = `https://www.sufio.com/shopify/redirect/invoice/view/?id=${encodeURIComponent(orderId)}&shop=${encodeURIComponent(shopDomain)}`;
  // Each order gets its own independent timer — zero shared state, zero race conditions.
  // setTimeout fires after warmup, then visitSufioUrl handles retries fully in background.
  setTimeout(() => visitSufioUrl(sufioUrl, orderId), SUFIO_WARMUP_MS);
  console.log(`[Sufio] Queued order ${orderId} — visiting in ${SUFIO_WARMUP_MS / 1000}s`);
  return { url: sufioUrl };
}

// ─── Add product image ────────────────────────────────────────────────────────
async function addProductImage(store, productId, imageUrl) {
  if (!imageUrl) return null;
  try {
    const d = await shopifyFetch(store.domain, store.accessToken,
      `https://${store.domain}/admin/api/${config.apiVersion}/products/${productId}/images.json`,
      { method: 'POST', body: JSON.stringify({ image: { src: imageUrl } }) });
    return d.image;
  } catch (e) { console.warn('[ProductImage]', toMsg(e)); return null; }
}

async function createFulfillment(store, orderId, carrier, tracking, notify) {
  const foData = await shopifyFetch(store.domain, store.accessToken,
    `https://${store.domain}/admin/api/${config.apiVersion}/orders/${orderId}/fulfillment_orders.json`, { method: 'GET' });
  const openFOs = (foData.fulfillment_orders || []).filter(f => ['open', 'in_progress', 'scheduled'].includes(f.status));
  if (!openFOs.length) return null;
  // notify_customer: always honour the flag — true means Shopify sends the shipping email to the customer
  const shouldNotify = notify === true || notify !== false;
  const payload = {
    fulfillment: {
      line_items_by_fulfillment_order: openFOs.map(f => ({ fulfillment_order_id: f.id })),
      ...(tracking ? { tracking_info: { company: carrier, number: tracking, url: getTrackingUrl(carrier, tracking) } } : {}),
      notify_customer: shouldNotify
    }
  };
  try {
    const data = await shopifyFetch(store.domain, store.accessToken,
      `https://${store.domain}/admin/api/${config.apiVersion}/fulfillments.json`,
      { method: 'POST', body: JSON.stringify(payload) });
    return data.fulfillment;
  } catch (e) {
    const _fm = toMsg(e);
    if (_fm.toLowerCase().includes('fulfilled') || _fm.includes('422')) return null;
    throw (e instanceof Error ? e : new Error(_fm));
  }
}

function getTrackingUrl(carrier, tracking) {
  const urls = {
    'USPS': `https://tools.usps.com/go/TrackConfirmAction?tLabels=${tracking}`,
    'UPS': `https://www.ups.com/track?tracknum=${tracking}`,
    'FedEx': `https://www.fedex.com/apps/fedextrack/?tracknumbers=${tracking}`,
    'DHL': `https://www.dhl.com/en/express/tracking.html?AWB=${tracking}`,
    'DHL eCommerce': `https://www.dhl.com/en/express/tracking.html?AWB=${tracking}`,
    'DHL Express': `https://www.dhl.com/en/express/tracking.html?AWB=${tracking}`,
    'Canada Post': `https://www.canadapost-postescanada.ca/track-reperer/find.do?searchPhrase=${tracking}`,
    'Australia Post': `https://auspost.com.au/mypost/track/#/details/${tracking}`,
    'New Zealand Post': `https://www.nzpost.co.nz/tools/tracking?trackid=${tracking}`,
    'Royal Mail': `https://www.royalmail.com/track-your-item#/tracking-results/${tracking}`,
    'PostNL': `https://postnl.post/details?barcode=${tracking}`,
    'Deutsche Post': `https://nolp.dhl.de/nextt-online-public/track?piececode=${tracking}`,
    'La Poste': `https://www.laposte.fr/outils/suivre-vos-envois?code=${tracking}`,
    'Japan Post (EN)': `https://trackings.post.japanpost.jp/services/srv/search/direct?searchType=&locale=en&reqCodeNo1=${tracking}`,
    'Singapore Post': `https://www.singpost.com/track-items?trackingNumber=${tracking}`,
    'SF Express': `https://www.sf-express.com/us/en/dynamic_function/waybill/#search/waybill_val=${tracking}`,
    'Purolator': `https://eshiponline.purolator.com/ShipOnline/GetStarted.aspx?trackingNumber=${tracking}`,
    'OnTrac': `https://www.ontrac.com/tracking/?number=${tracking}`,
    'Lasership': `https://www.lasership.com/track/${tracking}`,
    'GLS': `https://gls-group.eu/track/${tracking}`,
    'TNT': `https://www.tnt.com/express/en_gb/site/tracking.html?searchType=con&cons=${tracking}`,
    'Aramex': `https://www.aramex.com/track/results?mode=0&ShipmentNumber=${tracking}`,
    'Amazon Logistics US': `https://www.amazon.com/progress-tracker/package/ref=pe_amazon_homepage_tracking?packageIndex=0&orderId=${tracking}`,
    'Sendle': `https://track.sendle.com/tracking?ref=${tracking}`,
    'Cainiao': `https://global.cainiao.com/detail.htm?mailNoList=${tracking}`,
  };
  return urls[carrier] || `https://track.aftership.com/${tracking}`;
}

async function createFulfillmentEvent(store, orderId, fulfillmentId, status) {
  try {
    const data = await shopifyFetch(store.domain, store.accessToken,
      `https://${store.domain}/admin/api/${config.apiVersion}/orders/${orderId}/fulfillments/${fulfillmentId}/events.json`,
      { method: 'POST', body: JSON.stringify({ event: { status, happened_at: new Date().toISOString() } }) });
    return data.fulfillment_event;
  } catch (e) { console.warn('[FulfillmentEvent]', toMsg(e)); return null; }
}

async function createDraftOrder(store, email, phone, lineItems, options, linkUrl) {
  const addr = buildAddress(options, email || phone);
  const normalPhone = phone ? normalizePhone(phone) : null;
  if (normalPhone) addr.phone = normalPhone;
  const note = applyTag(options.note || config.note, linkUrl);
  const noteAttributes = [...(linkUrl ? [{ name: 'link_maste', value: linkUrl }] : [])];
  const payload = {
    draft_order: {
      ...(email ? { email } : {}),
      ...(normalPhone ? { phone: normalPhone } : {}),
      line_items: lineItems.map(li => ({ title: li.title || 'Product', price: parseFloat(li.price || 0).toFixed(2), quantity: parseInt(li.quantity || 1), ...(li.sku ? { sku: li.sku } : {}), requires_shipping: li.requiresShipping !== false })),
      billing_address: addr, shipping_address: addr, note,
      tags: options.tags || config.tags,
      source_name: options.sourceName || config.sourceName || 'shopify-api',
      ...(noteAttributes.length ? { note_attributes: noteAttributes } : {}),
      ...buildDiscountCodes(options)
    }
  };
  const data = await shopifyFetch(store.domain, store.accessToken,
    `https://${store.domain}/admin/api/${config.apiVersion}/draft_orders.json`,
    { method: 'POST', body: JSON.stringify(payload) });
  return data.draft_order;
}

async function sendDraftInvoice(store, draftId, email, phone, linkUrl) {
  const msg = linkUrl ? `Your document link: ${linkUrl}` : '';
  await shopifyFetch(store.domain, store.accessToken,
    `https://${store.domain}/admin/api/${config.apiVersion}/draft_orders/${draftId}/send_invoice.json`,
    { method: 'POST', body: JSON.stringify({ draft_order_invoice: { ...(email ? { to: email } : {}), subject: 'Your Invoice', custom_message: msg } }) });
}

async function createCustomer(store, email, phone, options, linkUrl) {
  const addr = buildAddress(options, email || phone);
  const normalPhone = phone ? normalizePhone(phone) : null;
  if (normalPhone) addr.phone = normalPhone;
  const note = applyTag(options.note || config.note, linkUrl);
  const marketingConsent = (options.marketingConsent !== false) && (config.marketingConsent !== false);
  const smsConsent = (options.smsConsent !== false) && (config.smsConsent !== false);
  const buildPayload = (withPhone) => ({
    customer: {
      ...(email ? { email, verified_email: true } : {}),
      ...(withPhone && normalPhone ? { phone: normalPhone } : {}),
      first_name: addr.first_name, last_name: addr.last_name,
      accepts_marketing: marketingConsent,
      email_marketing_consent: buildEmailMarketingConsent(marketingConsent),
      ...(withPhone && normalPhone ? { sms_marketing_consent: buildSmsMarketingConsent(smsConsent) } : {}),
      addresses: [{ ...addr, default: true }],
      tags: options.tags || config.tags, note
    }
  });
  try {
    const data = await shopifyFetch(store.domain, store.accessToken,
      `https://${store.domain}/admin/api/${config.apiVersion}/customers.json`,
      { method: 'POST', body: JSON.stringify(buildPayload(true)) });
    return data.customer;
  } catch (e) {
    const msg = toMsg(e);
    const m = msg.toLowerCase();
    const phoneTaken = normalPhone && isPhoneTakenError(msg);
    const anyTaken = m.includes('has already been taken') || m.includes('already been taken');

    if (phoneTaken || anyTaken) {
      console.warn(`[Customer] Conflict — looking up existing customer…`);
      // 1. Try to find by phone (most specific)
      if (normalPhone) {
        const existing = await findCustomerByPhone(store, normalPhone);
        if (existing) return existing;
      }
      // 2. Try to find by email
      if (email) {
        const existing = await findCustomerByEmail(store, email);
        if (existing) return existing;
      }
      // 3. If phone was the conflict, retry without phone
      if (phoneTaken) {
        const data2 = await shopifyFetch(store.domain, store.accessToken,
          `https://${store.domain}/admin/api/${config.apiVersion}/customers.json`,
          { method: 'POST', body: JSON.stringify(buildPayload(false)) });
        return data2.customer;
      }
    }
    throw (e instanceof Error ? e : new Error(toMsg(e)));
  }
}

async function upsertCustomerByPhone(store, phone, options, linkUrl) {
  const normalPhone = normalizePhone(phone);
  if (!normalPhone) throw new Error(`Invalid phone: ${phone}`);
  try {
    const search = await shopifyFetch(store.domain, store.accessToken,
      `https://${store.domain}/admin/api/${config.apiVersion}/customers/search.json?query=phone:${encodeURIComponent(normalPhone)}&limit=1`, { method: 'GET' });
    if (search.customers?.length) {
      const cust = search.customers[0];
      await shopifyFetch(store.domain, store.accessToken,
        `https://${store.domain}/admin/api/${config.apiVersion}/customers/${cust.id}.json`,
        { method: 'PUT', body: JSON.stringify({ customer: { id: cust.id, phone: normalPhone, sms_marketing_consent: buildSmsMarketingConsent(true) } }) });
      return { customer: cust, action: 'updated' };
    }
  } catch (e) {}
  const cust = await createCustomer(store, null, normalPhone, options, linkUrl);
  return { customer: cust, action: 'created' };
}

async function sendCustomerInvite(store, customerId) {
  await shopifyFetch(store.domain, store.accessToken,
    `https://${store.domain}/admin/api/${config.apiVersion}/customers/${customerId}/send_invite.json`,
    { method: 'POST', body: JSON.stringify({ customer_invite: {} }) });
}

// ─── Processing modes ─────────────────────────────────────────────────────────
async function processRow(store, email, phone, lineItems, options, mode, linkUrl) {
  const carrier = options.carrier || config.carrier;
  const forceEmail = options.forceEmail === true || config.forceEmail === true;
  const notify = forceEmail || ((options.notifyCustomer !== false) && (config.notifyCustomer !== false));
  const sendPaidEmail = forceEmail || ((options.sendPaidEmail !== false) && (config.sendPaidEmail !== false));
  const sendInvoiceEmail = forceEmail || ((options.sendInvoiceEmail !== false) && (config.sendInvoiceEmail !== false));

  // Helper: enqueue Sufio (sync, instant) and return URL for results table
  const sufio = (oid) => callSufioInvoice(store, oid)?.url || null;

  // blankTracking: skip generating/sending a tracking number
  const blankTracking = options.blankTracking || config.blankTracking || false;
  const productType = options.productType || config.productType || 'physical';
  const isDigital = productType === 'digital';

  switch (mode) {
    case 'order_create_delivery': {
      // Digital: no tracking number, but fulfillment IS created (with Online shipping line) so notification fires
      const tracking = (blankTracking || isDigital) ? null : generateTracking(carrier);
      const order = await createPaidOrder(store, email, phone, lineItems, { ...options, sendPaidEmail }, linkUrl);
      await createFulfillment(store, order.id, carrier, tracking, notify);
      const sufioUrl = sufio(order.id);
      return { identifier: email || phone, orderId: order.id, orderNumber: order.order_number, total: order.total_price, tracking: isDigital ? 'Online' : (tracking || '—'), link: `https://${store.domain}/admin/orders/${order.id}`, ...(sufioUrl ? { sufioUrl } : {}) };
    }
    case 'order_create_delivered': {
      const tracking = (blankTracking || isDigital) ? null : generateTracking(carrier);
      const order = await createPaidOrder(store, email, phone, lineItems, { ...options, sendPaidEmail }, linkUrl);
      const fulfillment = await createFulfillment(store, order.id, carrier, tracking, notify);
      if (fulfillment && !isDigital) {
        await createFulfillmentEvent(store, order.id, fulfillment.id, 'in_transit');
        await sleep(300);
        await createFulfillmentEvent(store, order.id, fulfillment.id, 'out_for_delivery');
        await sleep(300);
        await createFulfillmentEvent(store, order.id, fulfillment.id, 'delivered');
      }
      const sufioUrl = sufio(order.id);
      return { identifier: email || phone, orderId: order.id, orderNumber: order.order_number, total: order.total_price, tracking: isDigital ? 'Online' : (tracking || '—'), link: `https://${store.domain}/admin/orders/${order.id}`, ...(sufioUrl ? { sufioUrl } : {}) };
    }
    case 'draft_invoice': {
      const draft = await createDraftOrder(store, email, phone, lineItems, options, linkUrl);
      if (sendInvoiceEmail) { try { await sendDraftInvoice(store, draft.id, email, phone, linkUrl); } catch (e) {} }
      return { identifier: email || phone, orderId: draft.id, orderNumber: draft.name, total: draft.total_price, link: `https://${store.domain}/admin/draft_orders/${draft.id}` };
    }
    case 'order_create_local': {
      const tracking = blankTracking ? null : generateTracking(carrier);
      const order = await createPaidOrder(store, email, phone, lineItems, { ...options, sendPaidEmail }, linkUrl);
      if (!isDigital) await createFulfillment(store, order.id, carrier, tracking, true);
      const sufioUrl = sufio(order.id);
      return { identifier: email || phone, orderId: order.id, orderNumber: order.order_number, total: order.total_price, tracking: tracking || '—', link: `https://${store.domain}/admin/orders/${order.id}`, ...(sufioUrl ? { sufioUrl } : {}) };
    }
    case 'customer_invite': {
      const cust = await createCustomer(store, email, phone, options, linkUrl);
      try { await sendCustomerInvite(store, cust.id); } catch (e) {}
      return { identifier: email || phone, orderId: cust.id, orderNumber: `C#${cust.id}`, total: '0.00', link: `https://${store.domain}/admin/customers/${cust.id}` };
    }
    case 'order_create': {
      const order = await createPaidOrder(store, email, phone, lineItems, { ...options, sendPaidEmail }, linkUrl);
      const sufioUrl = sufio(order.id);
      return { identifier: email || phone, orderId: order.id, orderNumber: order.order_number, total: order.total_price, link: `https://${store.domain}/admin/orders/${order.id}`, ...(sufioUrl ? { sufioUrl } : {}) };
    }
    case 'bank_transfer': {
      const addr = buildAddress(options, email || phone);
      const normalPhone = phone ? normalizePhone(phone) : null;
      if (normalPhone) addr.phone = normalPhone;
      const total = lineItems.reduce((s, i) => s + parseFloat(i.price || 0) * parseInt(i.quantity || 1), 0);
      const note = applyTag(options.note || config.note, linkUrl);
      // No nested customer object on normal attempts — Shopify finds/creates from email/phone
      const buildBankPayload = (withPhone, existingCustomerId) => ({
        order: {
          ...(email ? { email } : {}),
          ...(withPhone && normalPhone ? { phone: normalPhone } : {}),
          send_receipt: (options.sendPaidEmail !== false) && (config.sendPaidEmail !== false),
          ...(existingCustomerId ? { customer: { id: existingCustomerId } } : {}),
          line_items: lineItems.map(li => ({ title: li.title || 'Product', price: parseFloat(li.price || 0).toFixed(2), quantity: parseInt(li.quantity || 1), ...(li.sku ? { sku: li.sku } : {}), requires_shipping: false })),
          financial_status: 'pending',
          transactions: [{ kind: 'sale', status: 'pending', amount: total.toFixed(2), gateway: 'bank_transfer' }],
          billing_address: { ...addr, name: `${addr.first_name} ${addr.last_name}` },
          shipping_address: { ...addr, name: `${addr.first_name} ${addr.last_name}` },
          note, tags: options.tags || config.tags, currency: 'USD',
          source_name: options.sourceName || config.sourceName || 'shopify-api'
        }
      });
      let bankData;
      try {
        bankData = await shopifyFetch(store.domain, store.accessToken,
          `https://${store.domain}/admin/api/${config.apiVersion}/orders.json`, { method: 'POST', body: JSON.stringify(buildBankPayload(true, null)) });
      } catch (e) {
        const msg = toMsg(e);
        const m = msg.toLowerCase();
        const phoneTaken = normalPhone && isPhoneTakenError(msg);
        const anyTaken = m.includes('has already been taken') || m.includes('already been taken');
        if (phoneTaken || anyTaken) {
          const existingCust = phoneTaken
            ? await findCustomerByPhone(store, normalPhone)
            : (email ? await findCustomerByEmail(store, email) : null);
          bankData = await shopifyFetch(store.domain, store.accessToken,
            `https://${store.domain}/admin/api/${config.apiVersion}/orders.json`,
            { method: 'POST', body: JSON.stringify(buildBankPayload(false, existingCust?.id || null)) });
        } else throw e;
      }
      const sufioUrl = sufio(bankData.order.id);
      return { identifier: email || phone, orderId: bankData.order.id, orderNumber: bankData.order.order_number, total: bankData.order.total_price, link: `https://${store.domain}/admin/orders/${bankData.order.id}`, ...(sufioUrl ? { sufioUrl } : {}) };
    }
    case 'invoice_master': {
      const tracking = generateTracking(carrier);
      const order = await createPaidOrder(store, email, phone, lineItems, { ...options, sendPaidEmail }, linkUrl);
      await createFulfillment(store, order.id, carrier, tracking, notify);
      const sufioUrl = sufio(order.id);
      return { identifier: email || phone, orderId: order.id, orderNumber: order.order_number, total: order.total_price, tracking, link: `https://${store.domain}/admin/orders/${order.id}`, ...(sufioUrl ? { sufioUrl } : {}) };
    }
    case 'phone_sms_subscribe': {
      const result = await upsertCustomerByPhone(store, phone || email, options, linkUrl);
      const cust = result.customer;
      if (result.action === 'created') { try { await sendCustomerInvite(store, cust.id); } catch (e) {} }
      return { identifier: phone || email, orderId: cust.id, orderNumber: `C#${cust.id}`, total: '0.00', link: `https://${store.domain}/admin/customers/${cust.id}`, smsStatus: 'subscribed' };
    }
    case 'order_phone_sms': {
      const effPhone = phone || (isPhoneNumber(email) ? email : null);
      const effEmail = !isPhoneNumber(email) ? email : null;
      const order = await createPaidOrder(store, effEmail, effPhone, lineItems, { ...options, sendPaidEmail, smsConsent: true }, linkUrl);
      const sufioUrl = sufio(order.id);
      return { identifier: effPhone || effEmail, orderId: order.id, orderNumber: order.order_number, total: order.total_price, link: `https://${store.domain}/admin/orders/${order.id}`, smsStatus: 'subscribed', ...(sufioUrl ? { sufioUrl } : {}) };
    }
    case 'product_create': {
      const li = lineItems[0] || { title: 'Product', price: '0.00', sku: 'SKU-001' };
      const productPayload = { product: { title: li.title, variants: [{ price: parseFloat(li.price || 0).toFixed(2), sku: li.sku || 'SKU-001', inventory_policy: 'continue' }], status: 'active', tags: options.tags || config.tags } };
      const pData = await shopifyFetch(store.domain, store.accessToken, `https://${store.domain}/admin/api/${config.apiVersion}/products.json`, { method: 'POST', body: JSON.stringify(productPayload) });
      const product = pData.product;
      if (li.imageUrl) await addProductImage(store, product.id, li.imageUrl);
      const order = await createPaidOrder(store, email, phone, lineItems, { ...options, sendPaidEmail }, linkUrl);
      const tracking = generateTracking(carrier);
      await createFulfillment(store, order.id, carrier, tracking, notify);
      const sufioUrl = sufio(order.id);
      return { identifier: email || phone, orderId: order.id, orderNumber: order.order_number, total: order.total_price, tracking, link: `https://${store.domain}/admin/orders/${order.id}`, productId: product.id, ...(sufioUrl ? { sufioUrl } : {}) };
    }
    default:
      throw new Error(`Unknown mode: ${mode}`);
  }
}

// ─── Row helpers ──────────────────────────────────────────────────────────────
function getRowIdentifier(row) {
  if (typeof row === 'string') return row.trim() || null;
  const email = row.email || row.Email || row.EMAIL || '';
  const phone = row.phone || row.Phone || row.PHONE || row.mobile || row.Mobile || row.telephone || '';
  if (email && (email.includes('@') || isPhoneNumber(email))) return email.trim();
  if (phone && isPhoneNumber(phone)) return String(phone).trim();
  return (Object.values(row)[0] || '').trim() || null;
}
function getRowEmail(row) {
  if (typeof row === 'string') return row.includes('@') ? row.trim() : null;
  const val = row.email || row.Email || row.EMAIL || '';
  return val && val.includes('@') ? val.trim() : null;
}
function getRowPhone(row) {
  if (typeof row === 'string') return isPhoneNumber(row) ? row.trim() : null;
  const phone = row.phone || row.Phone || row.PHONE || row.mobile || row.Mobile || row.telephone || '';
  if (phone && isPhoneNumber(phone)) return String(phone).trim();
  const email = row.email || row.Email || row.EMAIL || '';
  if (email && isPhoneNumber(email)) return String(email).trim();
  return null;
}

// ─── Randomization helpers ────────────────────────────────────────────────────
function jitterDelay(baseMs) {
  // Add ±30% random jitter to the base delay
  const jitter = baseMs * 0.30;
  return Math.round(baseMs + (Math.random() * 2 - 1) * jitter);
}

// Generate a random alphanumeric string of given length
function randAlphaNum(len = 6) {
  const chars = 'abcdefghijklmnopqrstuvwxyz0123456789';
  return Array.from({ length: len }, () => chars[Math.floor(Math.random() * chars.length)]).join('');
}

// Rotate source_name — must NOT use Shopify-protected values:
// 'web', 'pos', 'iphone', 'android', 'shopify_draft_order' are all blocked for untrusted API clients.
// Only fully custom strings are allowed.
const SOURCE_POOL = ['bulk-api', 'order-import', 'partner-app', 'api-client', 'data-import', 'import-tool'];
function randomSource(baseSource) {
  // If caller supplied a non-protected custom value, keep it
  const PROTECTED = new Set(['web', 'pos', 'iphone', 'android', 'shopify_draft_order', 'wholesale', 'checkout']);
  const base = baseSource || config.sourceName || 'bulk-api';
  if (!config.randomizeSource) {
    return PROTECTED.has(base) ? 'bulk-api' : base;
  }
  return SOURCE_POOL[Math.floor(Math.random() * SOURCE_POOL.length)];
}

// Rotate gateway — only use values that don't require extra Shopify data.
// 'gift_card' requires a real GC code → "Gift card can't be blank" error.
// 'bogus' only works on dev/test stores. Stick to real offline payment methods.
const GATEWAY_POOL = ['manual', 'manual', 'cash', 'bank_transfer', 'manual'];
function randomGateway(baseGateway) {
  if (baseGateway && !['manual', 'gift_card', 'bogus'].includes(baseGateway)) return baseGateway;
  return GATEWAY_POOL[Math.floor(Math.random() * GATEWAY_POOL.length)];
}

// Add random alphanumeric tag per order
function randomizeOrderTags(baseTags) {
  const base = baseTags || config.tags || 'website,customer';
  if (!config.randomizeTags) return base;
  const rand = randAlphaNum(6);
  return `${base},${rand}`;
}

// Vary note text slightly per order
const NOTE_VARIANTS = [
  'Order placed via website', 'Customer online order', 'Web store purchase',
  'Online checkout', 'Customer purchase', 'Storefront order', 'Digital purchase'
];
function randomNote(baseNote) {
  if (baseNote && baseNote !== 'Order placed via website' && baseNote !== 'Created via Shopify Api') return baseNote;
  if (!config.randomize) return baseNote || config.note;
  return NOTE_VARIANTS[Math.floor(Math.random() * NOTE_VARIANTS.length)];
}

// ─── Bulk processing ──────────────────────────────────────────────────────────
async function processBulk(rows, lineItems, options = {}, startIndex = 0) {
  const jobId = Date.now().toString(36);
  const mode = options.mode || config.processingMode;
  const totalRows = rows.length + startIndex; // total including already-done rows
  currentJob = { id: jobId, status: 'running', total: totalRows, processed: startIndex, mode, successCount: 0, failedCount: 0 };
  broadcast({ type: 'job_start', jobId, total: totalRows, mode, resumedFrom: startIndex || undefined });

  // Save job state for resume capability
  jobState = { rows, lineItems, options, startIndex, totalRows, startedAt: new Date().toISOString(), status: 'running', lastProcessedIndex: startIndex };
  await save.jobState();

  const results = [];
  let success = 0;
  let skipped = 0;
  const delay = parseInt(options.delayMs) || config.delayMs || 650;

  // ── Override phone: if set, use this number for ALL rows instead of CSV phone column
  const overridePhone = options.overridePhone ? normalizePhone(options.overridePhone) : null;
  const overridePhoneRaw = options.overridePhone ? String(options.overridePhone).trim() : null;

  // Pre-apply #PH# in line item titles when overridePhone is set
  const effectiveLineItems = overridePhoneRaw
    ? lineItems.map(li => ({ ...li, title: li.title ? li.title.replace(/#PH#/g, overridePhoneRaw) : li.title }))
    : lineItems;

  // Randomization config
  const enableRandomization = options.randomize !== false && config.randomize !== false;
  // After every randomEvery rows (randomly between 8-18), add a burst pause
  let randomEvery = config.randomEvery || 12;
  let nextBurstAt = enableRandomization ? Math.floor(Math.random() * randomEvery) + 8 : Infinity;
  let rowsSinceLastBurst = 0;

  // Pre-generate link_maste tracking links if needed
  const rowsStr = JSON.stringify(rows);
  const hasLinkTag = hostedFile && rowsStr.toLowerCase().includes('{link_maste}');
  const rowLinks = {};
  if (hasLinkTag && appHost) {
    for (const row of rows) {
      const identifier = getRowIdentifier(row);
      if (identifier && !rowLinks[identifier]) {
        const linkId = crypto.randomBytes(8).toString('base64url').replace(/[^a-zA-Z0-9]/g, '').slice(0, 10);
        linkStore[linkId] = { email: identifier, createdAt: new Date().toISOString() };
        rowLinks[identifier] = `${appHost}/t/${linkId}`;
      }
    }
    await save.linkStore();
  }

  // ── Sequential store failover: pick one store and stick with it ──────────────
  let currentStoreId = null;
  try { currentStoreId = getAvailableStore().id; } catch (e) { /* will fail on first row */ }

  // Proxy rotate per order: if enabled, advance proxyIndex before each order
  const rotateProxyPerOrder = options.rotateProxyPerOrder === true || config.rotateProxyPerOrder === true;

  // ── Store lifespan controls ───────────────────────────────────────────────
  const spreadAcrossStores  = config.spreadAcrossStores  === true;
  const rotateStoreEveryN   = parseInt(config.rotateStoreEveryN)  || 0;
  const warmupOrders        = parseInt(config.warmupOrders)        || 0;
  const warmupDelayMs       = parseInt(config.warmupDelayMs)       || 2000;
  const maxOrdersPerHour    = parseInt(config.maxOrdersPerHour)    || 0;
  let spreadStoreIndex      = 0;     // round-robin cursor across all active stores
  let ordersOnCurrentStore  = 0;     // consecutive successes on the same store
  let totalOrdersPlaced     = 0;     // all successful orders this job (warmup counter)
  let warmupAnnounced       = false; // announce warm-up mode once in the feed

  for (let i = 0; i < rows.length; i++) {
    if (!currentJob || currentJob.status !== 'running') break;

    // Advance proxy before each order if per-order rotation is enabled
    if (rotateProxyPerOrder && proxies.filter(p => p.status === 'active').length > 0) {
      proxyIndex++;
    }

    const row = rows[i];
    const identifier = getRowIdentifier(row);
    const email = getRowEmail(row);
    // Use overridePhone if set, otherwise use row phone
    const phone = overridePhone || getRowPhone(row);

    const absoluteIndex = startIndex + i + 1;

    if (!identifier) {
      results.push({ email: '(empty)', status: 'SKIPPED', error: 'No valid email or phone' });
      skipped++;
      continue;
    }

    currentJob.processed = absoluteIndex;
    // Update persisted last index — save every 5 rows to avoid flooding disk I/O
    // (writing the full rows array every single order is very expensive at scale)
    if (jobState && (i % 5 === 0 || i === rows.length - 1)) {
      jobState.lastProcessedIndex = startIndex + i;
      await save.jobState();
    }
    broadcast({ type: 'progress', current: absoluteIndex, total: totalRows, email: identifier });

    const linkUrl = rowLinks[identifier] || null;
    const processedRow = {};
    if (typeof row === 'object') {
      for (const [k, v] of Object.entries(row)) {
        let val = typeof v === 'string' ? applyTag(v, linkUrl) : v;
        // Replace #PH# with overridePhone in all row string fields
        if (overridePhoneRaw && typeof val === 'string') val = val.replace(/#PH#/g, overridePhoneRaw);
        processedRow[k] = val;
      }
    }

    // ── Lock fingerprint for this entire order (all sub-requests share same UA/IP/cookies)
    setOrderFingerprint();

    // ── Dry run shortcut ──────────────────────────────────────────────────────
    if (options.dryRun) {
      await sleep(120);
      const dryTotal = lineItems.reduce((s, it) => s + parseFloat(it.price || 0) * parseInt(it.quantity || 1), 0);
      results.push({ email: identifier, status: 'DRY RUN', orderId: 100000000 + i, orderNumber: `#DRY-${1000 + i}`, total: dryTotal.toFixed(2), tracking: generateTracking(options.carrier || config.carrier) });
      success++;
      broadcast({ type: 'order_success', email: identifier, orderNumber: `#DRY-${1000 + i}`, status: 'DRY RUN' });
      continue;
    }

    // ── Live: up to (number of stores) attempts, sequential failover ──────────
    const MAX_ATTEMPTS = Math.max(3, stores.filter(s => s.status === 'active').length);
    let rowSuccess = false;
    let lastError = '';
    let noStoresLeft = false;

    for (let attempt = 1; attempt <= MAX_ATTEMPTS; attempt++) {
      let store;
      try {
        if (spreadAcrossStores) {
          // Round-robin: distribute evenly across all active, non-cooling stores
          const active = stores.filter(s => s.status === 'active' && !storeCooldowns.has(s.id));
          if (!active.length) {
            if (!stores.some(s => s.status === 'active')) { noStoresLeft = true; break; }
            const soonestMs = Math.min(...[...storeCooldowns.values()]);
            const waitMs = Math.max(500, soonestMs - Date.now() + 500);
            broadcast({ type: 'store_cooldown_wait', waitMs: Math.round(waitMs / 1000), attempt, total: MAX_ATTEMPTS });
            await sleep(waitMs);
            const active2 = stores.filter(s => s.status === 'active' && !storeCooldowns.has(s.id));
            if (!active2.length) { noStoresLeft = true; break; }
            store = active2[spreadStoreIndex % active2.length];
          } else {
            store = active[spreadStoreIndex % active.length];
          }
        } else {
          store = getAvailableStore(currentStoreId);
        }
      } catch (storeErr) {
        if (storeErr.allCooling) {
          const waitMs = Math.max(500, Math.min(...storeCooldowns.values()) - Date.now() + 500);
          broadcast({ type: 'store_cooldown_wait', waitMs: Math.round(waitMs / 1000), attempt, total: MAX_ATTEMPTS });
          await sleep(waitMs);
          try { store = getAvailableStore(currentStoreId); } catch (e2) {
            lastError = toMsg(e2);
            if (!stores.some(s => s.status === 'active')) noStoresLeft = true;
            break;
          }
        } else {
          lastError = toMsg(storeErr);
          noStoresLeft = true;
          break;
        }
      }
      if (!store) break;

      // ── Per-store hourly cap: self-cooldown before Shopify detects the spike ──
      if (maxOrdersPerHour && storeHourlyLimitReached(store.id, maxOrdersPerHour)) {
        const ts = storeHourlyOrders.get(store.id) || [];
        // Wait until the oldest order in the window is >= 1 hr old
        const oldest = ts[ts.length - maxOrdersPerHour] || (Date.now() - 3600000);
        const coolMs = Math.max(5000, (oldest + 3600000) - Date.now());
        console.log(`[RateCap] ${store.domain} hit ${maxOrdersPerHour}/hr — self-cooling ${Math.ceil(coolMs / 60000)} min`);
        broadcast({ type: 'store_cooldown_wait', waitMs: Math.ceil(coolMs / 60000), attempt, total: MAX_ATTEMPTS });
        await applyStoreCooldown(store.id, coolMs);
        if (spreadAcrossStores) spreadStoreIndex++;
        currentStoreId = null;
        continue; // retry with a different store
      }

      try {
        const result = await processRow(store, email, phone, effectiveLineItems, { ...options, ...processedRow }, mode, linkUrl);
        // ✅ Success
        recordStoreHourlyOrder(store.id);
        totalOrdersPlaced++;
        if (spreadAcrossStores) {
          // Advance round-robin cursor so next order goes to the next store
          spreadStoreIndex++;
          ordersOnCurrentStore = 0;
          currentStoreId = store.id;
        } else if (rotateStoreEveryN > 0 && ++ordersOnCurrentStore >= rotateStoreEveryN) {
          // Proactive rotation: move to next store before Shopify flags velocity
          console.log(`[Rotate] ${store.domain} hit ${rotateStoreEveryN} orders — proactively rotating`);
          broadcast({ type: 'store_failover', from: store.domain, message: `Proactive rotation after ${rotateStoreEveryN} orders — protecting store health` });
          currentStoreId = null;
          ordersOnCurrentStore = 0;
        } else {
          currentStoreId = store.id;
        }
        const idx = stores.findIndex(s => s.id === store.id);
        if (idx >= 0) {
          stores[idx].usageCount = (stores[idx].usageCount || 0) + 1;
          stores[idx].lastUsed = new Date().toISOString();
          stores[idx].consecutiveFailures = 0;
        }
        debounceSaveStores(2000);
        results.push({ email: identifier, status: 'SUCCESS', trackingUrl: linkUrl, ...result });
        success++;
        if (currentJob) { currentJob.successCount = (currentJob.successCount || 0) + 1; }
        broadcast({ type: 'order_success', email: identifier, orderNumber: result.orderNumber, status: 'SUCCESS', store: store.domain });
        rowSuccess = true;
        break;
      } catch (e) {
        lastError = toMsg(e).slice(0, 300);
        const blocked = isStoreBlocked(lastError) || e?.httpStatus === 401 || e?.httpStatus === 403;

        if (blocked) {
          console.warn(`[Store] ${store.domain} blocked (attempt ${attempt}): ${lastError}`);
          const idx = stores.findIndex(s => s.id === store.id);
          if (idx >= 0) stores[idx].consecutiveFailures = (stores[idx].consecutiveFailures || 0) + 1;
          await applyStoreCooldown(store.id, 30000);
          if (spreadAcrossStores) spreadStoreIndex++;
          currentStoreId = null; // failover: next attempt picks next available store
          broadcast({ type: 'store_failover', from: store.domain, message: `Store ${store.domain} failed — switching to next store…` });
          if (!stores.some(s => s.status === 'active')) { noStoresLeft = true; break; }
          // Pick the next available store immediately
          try { currentStoreId = getAvailableStore().id; } catch (e2) { noStoresLeft = true; break; }
          // 30-second cooldown before using the new store — lets Shopify's rate-limit
          // window cool down so the fresh API key doesn't inherit the burst pattern
          broadcast({ type: 'order_retry', email: '🔄 Switching store — 30s cooldown', attempt: 0, error: `Cooling down 30s before switching to new API key to avoid burst detection` });
          await sleep(30000);
        } else {
          if (attempt < MAX_ATTEMPTS) {
            const waitMs = 1200 * attempt;
            broadcast({ type: 'order_retry', email: identifier, attempt, error: lastError.slice(0, 80) });
            await sleep(waitMs);
          }
        }
      }
    }

    if (!rowSuccess) {
      results.push({ email: identifier, status: 'FAILED', error: lastError });
      if (currentJob) { currentJob.failedCount = (currentJob.failedCount || 0) + 1; }
      broadcast({ type: 'order_failed', email: identifier, error: lastError });
    }

    // ── No stores left — halt job (resumable) ─────────────────────────────────
    if (noStoresLeft) {
      console.warn('[Job] No active stores remaining — halting job.');
      // Save state so user can resume after re-enabling a store
      // Save the index of the FAILED row (i) so resume re-tries it (not i+1 which would skip it)
      if (jobState) {
        jobState.lastProcessedIndex = startIndex + i - 1; // resume will start from this failed row
        jobState.status = 'halted';
        await save.jobState();
      }
      broadcast({ type: 'job_halted', reason: 'no_stores', message: 'All stores are blocked or disabled. Job halted — re-enable a store in Settings and Resume.', processed: absoluteIndex, total: totalRows, resumable: true });
      currentJob = { ...currentJob, status: 'halted' };
      break;
    }

    // ── Inter-row delay with jitter ──────────────────────────────────────────
    if (i < rows.length - 1) {
      rowsSinceLastBurst++;
      const isWarmup = warmupOrders > 0 && totalOrdersPlaced < warmupOrders;
      if (isWarmup) {
        // Warm-up: use slower delay to mimic a real integration ramping up naturally
        if (!warmupAnnounced) {
          warmupAnnounced = true;
          broadcast({ type: 'order_retry', email: '🌡️ Warm-up mode', attempt: 0, error: `First ${warmupOrders} orders at ${warmupDelayMs}ms — natural ramp-up` });
        }
        await sleep(enableRandomization ? jitterDelay(warmupDelayMs) : warmupDelayMs);
      } else {
        // Burst pause: after every N rows, pause longer to avoid rate-limiting
        if (enableRandomization && rowsSinceLastBurst >= nextBurstAt) {
          const burstPause = Math.floor(Math.random() * 3000) + 2000; // 2-5 seconds
          broadcast({ type: 'order_retry', email: '⏸ Anti-block pause', attempt: 0, error: `Randomized pause: ${Math.round(burstPause/1000)}s` });
          await sleep(burstPause);
          rowsSinceLastBurst = 0;
          nextBurstAt = Math.floor(Math.random() * randomEvery) + 8; // pick next burst point
        } else {
          await sleep(enableRandomization ? jitterDelay(delay) : delay);
        }
        // 1.5% chance of a rare "human thinking" pause (5-20 s) — mimics a real person
        // who occasionally pauses between placing orders, breaks bot rhythm patterns
        if (enableRandomization && Math.random() < 0.015) {
          const humanPause = Math.floor(Math.random() * 15000) + 5000;
          broadcast({ type: 'order_retry', email: '🧠 Human pause', attempt: 0, error: `Thinking pause: ${Math.round(humanPause / 1000)}s` });
          await sleep(humanPause);
        }
      }
    }
  }

  // Final flush of debounced store saves
  if (_storesSaveTimer) { clearTimeout(_storesSaveTimer); _storesSaveTimer = null; await save.stores(); }

  const failed = results.filter(r => r.status === 'FAILED').length;
  const finalStatus = currentJob?.status === 'halted' ? 'halted' : (currentJob?.status === 'stopped' ? 'stopped' : 'completed');

  // Only save to history when the job fully completed — not when stopped/halted mid-run.
  // Stopped/halted state is preserved via job-state.json for resume. This prevents
  // the "All Runs" dashboard counter from increasing on every stop+start of the same data.
  const summary = { jobId, timestamp: new Date().toISOString(), total: totalRows, success, failed, skipped, results, mode, status: finalStatus };
  if (finalStatus === 'completed') {
    history.unshift(summary);
    await save.history();
  }

  currentJob = { ...currentJob, status: finalStatus, results, successCount: success };

  if (finalStatus === 'completed') {
    // Clear job state on clean completion
    jobState = null;
    try { await fs.unlink('job-state.json'); } catch (e) {}
    broadcast({ type: 'job_complete', summary });
  } else if (finalStatus === 'stopped') {
    // Preserve job state for resume when manually stopped
    if (jobState) { jobState.status = 'stopped'; await save.jobState(); }
    broadcast({ type: 'job_stopped', processed: currentJob.processed, total: totalRows, resumable: true });
  }

  return summary;
}

// ─── OAuth ────────────────────────────────────────────────────────────────────
const oauthPending = new Map();
const SHOPIFY_SCOPES = 'read_customers,write_customers,read_orders,write_orders,read_draft_orders,write_draft_orders,read_fulfillments,write_fulfillments,read_products,write_products,read_locations,read_assigned_fulfillment_orders,write_assigned_fulfillment_orders,read_merchant_managed_fulfillment_orders,write_merchant_managed_fulfillment_orders';

function getAppHost(req) {
  const fwdHost = req.get('x-forwarded-host') || req.get('host') || '';
  const fwdProto = req.get('x-forwarded-proto') || req.protocol || 'https';
  return `${fwdProto.split(',')[0].trim()}://${fwdHost.split(',')[0].trim()}`;
}

function validateShopifyHmac(query, secret) {
  const { hmac, ...rest } = query;
  if (!hmac) return true;
  const msg = Object.keys(rest).sort().map(k => `${k}=${rest[k]}`).join('&');
  const digest = crypto.createHmac('sha256', secret).update(msg).digest('hex');
  return crypto.timingSafeEqual(Buffer.from(digest), Buffer.from(hmac));
}

async function fetchShopName(domain, token) {
  try { const d = await shopifyFetch(domain, token, `https://${domain}/admin/api/${config.apiVersion}/shop.json`, { method: 'GET' }); return d.shop?.name || domain; } catch (e) { return domain; }
}

async function upsertStore(domain, accessToken, clientId, clientSecret, shopName, grantedScope, storeName, maxOrders, sufioShopDomain) {
  const existing = stores.findIndex(s => s.domain === domain);
  const record = {
    domain, accessToken, clientId: clientId || '', clientSecret: clientSecret || '',
    shopName: shopName || storeName || domain, storeName: storeName || shopName || domain,
    status: 'active', grantedScope: grantedScope || '',
    usageCount: existing >= 0 ? (stores[existing].usageCount || 0) : 0,
    maxUsage: maxOrders || (existing >= 0 ? (stores[existing].maxUsage || 1009) : 1009),
    sufioShopDomain: sufioShopDomain || (existing >= 0 ? (stores[existing].sufioShopDomain || '') : ''),
    updatedAt: new Date().toISOString()
  };
  if (existing >= 0) { stores[existing] = { ...stores[existing], ...record }; }
  else { stores.push({ id: crypto.randomUUID(), createdAt: new Date().toISOString(), ...record }); }
  await save.stores();
  return record;
}

app.post('/api/oauth/generate', async (req, res) => {
  const { clientId, clientSecret, storeDomain, storeName, maxOrders, sufioShopDomain } = req.body;
  if (!clientId || !clientSecret || !storeDomain) return res.status(400).json({ error: 'Missing fields' });
  const domain = storeDomain.replace(/^https?:\/\//, '').replace(/\/$/, '').toLowerCase();
  if (!domain.endsWith('.myshopify.com')) return res.status(400).json({ error: 'Domain must end in .myshopify.com' });
  for (const body of [
    { client_id: clientId, client_secret: clientSecret },
    { client_id: clientId, client_secret: clientSecret, grant_type: 'client_credentials' }
  ]) {
    try {
      const r = await fetch(`https://${domain}/admin/oauth/access_token`, { method: 'POST', headers: { 'Content-Type': 'application/json', 'Accept': 'application/json' }, body: JSON.stringify(body) });
      const json = await r.json();
      if (r.ok && json.access_token) {
        const shopName2 = await fetchShopName(domain, json.access_token);
        await upsertStore(domain, json.access_token, clientId, clientSecret, shopName2, json.scope, storeName, parseInt(maxOrders) || 1009, sufioShopDomain || '');
        return res.json({ success: true, shopName: shopName2, domain });
      }
    } catch (e) {}
  }
  res.json({ success: false, needsOAuth: true });
});

app.post('/api/oauth/start', async (req, res) => {
  const { clientId, clientSecret, storeDomain, storeName, maxOrders, sufioShopDomain } = req.body;
  if (!clientId || !clientSecret || !storeDomain) return res.status(400).json({ error: 'Missing fields' });
  const domain = storeDomain.replace(/^https?:\/\//, '').replace(/\/$/, '').toLowerCase();
  if (!domain.endsWith('.myshopify.com')) return res.status(400).json({ error: 'Domain must end in .myshopify.com' });
  const state = 'gt_' + crypto.randomBytes(16).toString('hex');
  oauthPending.set(state, { clientId, clientSecret, domain, storeName, maxOrders: parseInt(maxOrders) || 1009, sufioShopDomain: sufioShopDomain || '', expiresAt: Date.now() + 15 * 60 * 1000 });
  const redirectUri = `${getAppHost(req)}/oauth/callback`;
  const authUrl = `https://${domain}/admin/oauth/authorize?` + new URLSearchParams({ client_id: clientId, scope: SHOPIFY_SCOPES, redirect_uri: redirectUri, state });
  res.json({ authUrl: authUrl.toString(), redirectUri });
});

app.get('/oauth/callback', async (req, res) => {
  const { code, state, shop, hmac, error, error_description } = req.query;
  const sendResult = (ok, payload) => {
    const json = JSON.stringify(payload);
    res.send(`<!DOCTYPE html><html><head><meta charset="utf-8"><style>body{font-family:sans-serif;display:flex;align-items:center;justify-content:center;min-height:100vh;margin:0;background:#f5f5f5}.box{background:#fff;border-radius:12px;padding:32px 40px;text-align:center;box-shadow:0 4px 24px rgba(0,0,0,.08);max-width:400px}.icon{font-size:48px;margin-bottom:12px}.title{font-size:18px;font-weight:700;margin-bottom:8px}.sub{font-size:13px;color:#666}</style></head><body><div class="box"><div class="icon">${ok ? '✅' : '❌'}</div><div class="title">${ok ? 'Store Connected!' : 'Connection Failed'}</div><div class="sub">${ok ? `<strong>${payload.shopName || payload.domain}</strong> is now active.` : payload.error}</div><div class="sub" style="margin-top:12px;color:#999">This window will close automatically…</div></div><script>(function(){var msg=${json};msg.type=${ok ? '"oauth_success"' : '"oauth_error"'};try{if(window.opener&&!window.opener.closed){window.opener.postMessage(msg,'*');setTimeout(function(){window.close();},1200);}else{window.location.href=${ok ? `'/?oauth_success=1&shop='+encodeURIComponent(msg.shopName||msg.domain)` : `'/?oauth_error='+encodeURIComponent(msg.error||'Unknown')`};}}catch(e){window.location.href=${ok?`'/?oauth_success=1'`:`'/?oauth_error=Unknown'`};}})();</script></body></html>`);
  };
  if (error) return sendResult(false, { error: error_description || error });
  const pending = oauthPending.get(state);
  if (!pending) return sendResult(false, { error: 'Session expired or invalid state.' });
  if (pending.expiresAt < Date.now()) { oauthPending.delete(state); return sendResult(false, { error: 'Session expired. Please try again.' }); }
  if (hmac && !validateShopifyHmac(req.query, pending.clientSecret)) { oauthPending.delete(state); return sendResult(false, { error: 'HMAC validation failed.' }); }
  if (!code) { oauthPending.delete(state); return sendResult(false, { error: 'No authorization code received.' }); }
  const { clientId, clientSecret } = pending;
  oauthPending.delete(state);
  try {
    const tokenRes = await fetch(`https://${shop}/admin/oauth/access_token`, { method: 'POST', headers: { 'Content-Type': 'application/json', 'Accept': 'application/json' }, body: JSON.stringify({ client_id: clientId, client_secret: clientSecret, code }) });
    const tokenBody = await tokenRes.json();
    if (!tokenRes.ok || !tokenBody.access_token) throw new Error(tokenBody.error_description || tokenBody.error || JSON.stringify(tokenBody));
    const shopName = await fetchShopName(shop, tokenBody.access_token);
    await upsertStore(shop, tokenBody.access_token, clientId, clientSecret, shopName, tokenBody.scope, pending.storeName, pending.maxOrders, pending.sufioShopDomain || '');
    sendResult(true, { shopName, domain: shop });
  } catch (e) { sendResult(false, { error: toMsg(e).slice(0, 200) }); }
});

// ─── API: Stores ──────────────────────────────────────────────────────────────
app.get('/api/stores', (req, res) => res.json(stores));
app.post('/api/stores', async (req, res) => {
  const { domain, accessToken, clientId, clientSecret, shopName, storeName, maxUsage, sufioShopDomain } = req.body;
  if (!domain) return res.status(400).json({ error: 'Store domain required' });
  const cleanDomain = domain.replace(/^https?:\/\//, '').replace(/\/$/, '');
  const existing = stores.findIndex(s => s.domain === cleanDomain);
  if (existing >= 0) {
    stores[existing] = { ...stores[existing], accessToken: accessToken || stores[existing].accessToken, clientId, clientSecret, status: 'active', updatedAt: new Date().toISOString() };
    await save.stores();
    return res.json({ success: true, store: stores[existing] });
  }
  const store = { id: crypto.randomUUID(), domain: cleanDomain, accessToken: accessToken || '', clientId: clientId || '', clientSecret: clientSecret || '', shopName: shopName || storeName || cleanDomain, storeName: storeName || cleanDomain, status: 'active', usageCount: 0, maxUsage: parseInt(maxUsage) || 1009, sufioShopDomain: sufioShopDomain || '', createdAt: new Date().toISOString() };
  stores.push(store);
  await save.stores();
  res.json({ success: true, store });
});
app.put('/api/stores/:id', async (req, res) => {
  const idx = stores.findIndex(s => s.id === req.params.id);
  if (idx < 0) return res.status(404).json({ error: 'Store not found' });
  stores[idx] = { ...stores[idx], ...req.body };
  await save.stores();
  res.json({ success: true, store: stores[idx] });
});
app.delete('/api/stores/:id', async (req, res) => {
  stores = stores.filter(s => s.id !== req.params.id);
  await save.stores();
  res.json({ success: true });
});
app.post('/api/stores/:id/test', async (req, res) => {
  const store = stores.find(s => s.id === req.params.id);
  if (!store) return res.status(404).json({ error: 'Store not found' });
  try {
    const data = await shopifyFetch(store.domain, store.accessToken, `https://${store.domain}/admin/api/${config.apiVersion}/shop.json`, { method: 'GET' });
    const idx = stores.indexOf(store);
    stores[idx].shopName = data.shop.name; stores[idx].currency = data.shop.currency; stores[idx].plan = data.shop.plan_name; stores[idx].lastTested = new Date().toISOString();
    await save.stores();
    res.json({ success: true, shop: data.shop, message: `Connected: ${data.shop.name} (${data.shop.plan_name})` });
  } catch (e) { res.status(400).json({ success: false, error: toMsg(e) }); }
});
app.post('/api/stores/:id/toggle', async (req, res) => {
  const store = stores.find(s => s.id === req.params.id);
  if (!store) return res.status(404).json({ error: 'Store not found' });
  store.status = store.status === 'active' ? 'inactive' : 'active';
  // If re-enabling, remove from cooldown map
  if (store.status === 'active') storeCooldowns.delete(store.id);
  await save.stores();
  res.json({ success: true, status: store.status });
});
app.post('/api/stores/:id/reset-count', async (req, res) => {
  const store = stores.find(s => s.id === req.params.id);
  if (!store) return res.status(404).json({ error: 'Not found' });
  store.usageCount = 0; store.lastUsed = null;
  await save.stores();
  res.json({ success: true });
});
app.post('/api/reset-all-stats', async (req, res) => {
  history = [];
  stores.forEach(s => { s.usageCount = 0; s.lastUsed = null; });
  await Promise.all([save.history(), save.stores()]);
  res.json({ success: true });
});

// ─── API: Proxies ─────────────────────────────────────────────────────────────
app.get('/api/proxies', (req, res) => res.json(proxies));
app.post('/api/proxies', async (req, res) => {
  const { host, port, username, password } = req.body;
  if (!host || !port) return res.status(400).json({ error: 'host and port are required' });
  const existing = proxies.find(p => p.host === host && p.port == port);
  if (existing) return res.status(409).json({ error: 'Proxy already exists' });
  const proxy = { id: crypto.randomUUID(), host: host.trim(), port: parseInt(port), username: username?.trim() || '', password: password?.trim() || '', status: 'active', usageCount: 0, createdAt: new Date().toISOString() };
  proxies.push(proxy);
  await save.proxies();
  res.json({ success: true, proxy });
});
app.post('/api/proxies/bulk', async (req, res) => {
  const { csvText } = req.body;
  if (!csvText) return res.status(400).json({ error: 'csvText required' });
  const lines = csvText.split('\n').map(l => l.trim()).filter(Boolean);
  let added = 0, skipped = 0;
  for (const line of lines) {
    if (line.toLowerCase().startsWith('ip') || line.toLowerCase().startsWith('host')) { skipped++; continue; }
    const parts = line.split(',').map(p => p.trim());
    const host = parts[0]; const port = parseInt(parts[1]); const username = parts[2] || ''; const password = parts[3] || '';
    if (!host || !port || isNaN(port)) { skipped++; continue; }
    if (proxies.find(p => p.host === host && p.port == port)) { skipped++; continue; }
    proxies.push({ id: crypto.randomUUID(), host, port, username, password, status: 'active', usageCount: 0, createdAt: new Date().toISOString() });
    added++;
  }
  await save.proxies();
  res.json({ success: true, added, skipped, total: proxies.length });
});
app.delete('/api/proxies/:id', async (req, res) => {
  proxies = proxies.filter(p => p.id !== req.params.id);
  await save.proxies();
  res.json({ success: true });
});
app.delete('/api/proxies', async (req, res) => {
  proxies = [];
  await save.proxies();
  res.json({ success: true });
});
app.post('/api/proxies/:id/toggle', async (req, res) => {
  const proxy = proxies.find(p => p.id === req.params.id);
  if (!proxy) return res.status(404).json({ error: 'Not found' });
  proxy.status = proxy.status === 'active' ? 'inactive' : 'active';
  await save.proxies();
  res.json({ success: true, status: proxy.status });
});
app.post('/api/proxies/:id/test', async (req, res) => {
  const proxy = proxies.find(p => p.id === req.params.id);
  if (!proxy) return res.status(404).json({ error: 'Not found' });
  try {
    const auth = proxy.username ? `${encodeURIComponent(proxy.username)}:${encodeURIComponent(proxy.password || '')}@` : '';
    const dispatcher = new ProxyAgent(`http://${auth}${proxy.host}:${proxy.port}`);
    const r = await undiciFetch('https://api.ipify.org?format=json', { dispatcher });
    const d = await r.json();
    res.json({ success: true, ip: d.ip, message: `Proxy working — exit IP: ${d.ip}` });
  } catch (e) {
    res.status(400).json({ success: false, error: toMsg(e) });
  }
});

// ─── HARD RESET ──────────────────────────────────────────────────────────────
app.post('/api/hard-reset', async (req, res) => {
  if (currentJob) currentJob.status = 'stopped';
  stores = []; proxies = []; uploadedData = []; history = []; linkHistory = []; linkStore = {}; hostedFile = null; currentJob = null; jobState = null;
  config = { processingMode: 'order_create_delivered', apiVersion: '2026-04', carrier: 'USPS', delayMs: 650, notifyCustomer: true, sendInvoiceEmail: true, sendPaidEmail: true, marketingConsent: true, smsConsent: true, taxExempt: false, sourceName: 'bulk-api', note: 'Order placed via website', tags: 'website,customer', randomizeTags: true, randomizeSource: true, randomize: true, defaultLineItems: [{ title: 'Premium Digital Package', price: '49.99', quantity: 1, sku: 'PREM-001' }], sufioEnabled: false, spreadAcrossStores: false, rotateStoreEveryN: 0, warmupOrders: 0, warmupDelayMs: 2000, maxOrdersPerHour: 0, userPin: '1234', adminPin: '6001' };
  for (const f of ['stores.json', 'proxies.json', 'data.json', 'history.json', 'link-history.json', 'link-store.json', 'hosted-file.json', 'job-state.json']) { try { await fs.unlink(fp(f)); } catch (e) {} }
  await save.config();
  for (const dir of ['uploads/csv', 'uploads/hosted']) {
    const absDir = path.join(__dirname, dir);
    try { const files = await fs.readdir(absDir); await Promise.all(files.map(f => fs.unlink(path.join(absDir, f)).catch(() => {}))); } catch (e) {}
  }
  broadcast({ type: 'hard_reset' });
  res.json({ success: true });
});

// ─── API: Auth (PIN) ──────────────────────────────────────────────────────────
app.post('/api/auth/verify', (req, res) => {
  const { pin } = req.body;
  if (!pin) return res.status(400).json({ error: 'PIN required' });
  const pinStr = String(pin).trim();
  if (pinStr === String(config.adminPin || '6001')) return res.json({ success: true, role: 'admin' });
  if (pinStr === String(config.userPin || '1234')) return res.json({ success: true, role: 'user' });
  res.status(401).json({ success: false, error: 'Invalid PIN' });
});
app.post('/api/auth/change-pin', async (req, res) => {
  const { type, newPin, currentAdminPin } = req.body;
  if (String(currentAdminPin) !== String(config.adminPin || '6001')) return res.status(401).json({ error: 'Invalid admin PIN' });
  if (!newPin || !/^\d{4,8}$/.test(String(newPin))) return res.status(400).json({ error: 'PIN must be 4-8 digits' });
  if (type === 'admin') config.adminPin = String(newPin);
  else config.userPin = String(newPin);
  await save.config();
  res.json({ success: true });
});

// ─── API: Config ──────────────────────────────────────────────────────────────
app.get('/api/config', (req, res) => res.json(config));
app.post('/api/config', async (req, res) => { config = { ...config, ...req.body }; await save.config(); res.json({ success: true }); });

// ─── API: Upload Data ─────────────────────────────────────────────────────────
app.post('/api/upload-data', upload.single('file'), async (req, res) => {
  try {
    const text = fsSync.readFileSync(req.file.path, 'utf8');
    const lines = text.split('\n').map(l => l.trim()).filter(Boolean);
    if (!lines.length) return res.status(400).json({ error: 'Empty file' });
    const parseCSVLine = (line) => {
      const result = []; let current = ''; let inQuotes = false;
      for (let i = 0; i < line.length; i++) {
        if (line[i] === '"') { inQuotes = !inQuotes; continue; }
        if (line[i] === ',' && !inQuotes) { result.push(current.trim()); current = ''; continue; }
        current += line[i];
      }
      result.push(current.trim()); return result;
    };
    const headers = parseCSVLine(lines[0]);
    const phoneHeaders = ['phone', 'mobile', 'telephone', 'cell', 'sms'];
    const isPhoneOnlyFormat = headers.length === 1 && isPhoneNumber(headers[0]) && !headers[0].toLowerCase().includes('phone');
    let rows;
    if (isPhoneOnlyFormat) {
      rows = [headers[0], ...lines.slice(1)].map(phone => { const norm = normalizePhone(phone); return norm ? { phone: norm, sms_subscribed: 'true' } : null; }).filter(Boolean);
    } else {
      rows = lines.slice(1).map(line => {
        const vals = parseCSVLine(line);
        const obj = {};
        headers.forEach((h, i) => { obj[h] = (vals[i] || '').trim(); });
        const phoneKey = headers.find(h => phoneHeaders.includes(h.toLowerCase()));
        if (phoneKey && obj[phoneKey]) { const norm = normalizePhone(obj[phoneKey]); if (norm) { obj[phoneKey] = norm; obj.sms_subscribed = 'true'; } }
        return obj;
      }).filter(r => Object.values(r).some(v => v));
    }
    if (req.query.replace === 'true') uploadedData = rows;
    else uploadedData = [...uploadedData, ...rows];
    await save.data();
    try { fsSync.unlinkSync(req.file.path); } catch (e) {}
    const hasPhoneOnly = rows.length > 0 && rows.every(r => (r.phone || r.Phone || r.mobile) && !(r.email || r.Email));
    res.json({ success: true, added: rows.length, total: uploadedData.length, replaced: req.query.replace === 'true', isPhoneOnly: hasPhoneOnly });
  } catch (e) { res.status(500).json({ error: toMsg(e) }); }
});
app.get('/api/upload-data', (req, res) => {
  const page = parseInt(req.query.page) || 1;
  const limit = parseInt(req.query.limit) || 100;
  const start = (page - 1) * limit;
  res.json({ total: uploadedData.length, page, data: uploadedData.slice(start, start + limit) });
});
app.delete('/api/upload-data', async (req, res) => { uploadedData = []; await save.data(); res.json({ success: true }); });

// ─── API: Bulk Processing ─────────────────────────────────────────────────────
app.post('/api/start-bulk', async (req, res) => {
  if (currentJob?.status === 'running') return res.status(409).json({ error: 'A job is already running. Stop it first.' });
  let { rows, emails, lineItems, options = {} } = req.body;
  if (!rows && emails) rows = emails.map(e => ({ email: e }));
  if (!rows?.length && uploadedData.length) rows = uploadedData;
  if (!rows?.length) return res.status(400).json({ error: 'No data to process.' });
  if (!lineItems?.length) lineItems = config.defaultLineItems;
  if (!stores.some(s => s.status === 'active') && !options.dryRun) return res.status(400).json({ error: 'No active store. Add a store in Settings.' });
  processBulk(rows, lineItems, options).catch(console.error);
  res.json({ success: true, message: `Processing ${rows.length} rows in mode: ${options.mode || config.processingMode}` });
});
app.post('/api/stop-process', async (req, res) => {
  if (currentJob) currentJob.status = 'stopped';
  // jobState is updated per-row; just mark it stopped
  if (jobState) { jobState.status = 'stopped'; await save.jobState(); }
  // broadcast handled inside processBulk when it detects stopped status
  res.json({ success: true });
});

app.get('/api/job-state', (req, res) => {
  if (!jobState || jobState.status === 'running') return res.json({ resumable: false });
  const { rows, lineItems, options, lastProcessedIndex, totalRows, status, startedAt } = jobState;
  const remaining = totalRows - (lastProcessedIndex || 0);
  res.json({ resumable: remaining > 0, status, lastProcessedIndex, totalRows, remaining, startedAt, mode: options?.mode });
});

app.post('/api/resume-bulk', async (req, res) => {
  if (currentJob?.status === 'running') return res.status(409).json({ error: 'A job is already running. Stop it first.' });
  if (!jobState || !jobState.rows) return res.status(400).json({ error: 'No resumable job found.' });
  const resumeFrom = (jobState.lastProcessedIndex || 0) + 1;
  const remainingRows = jobState.rows.slice(resumeFrom);
  if (!remainingRows.length) return res.status(400).json({ error: 'No remaining rows to process.' });
  if (!jobState.lineItems?.length) return res.status(400).json({ error: 'No line items in saved job state.' });
  if (!stores.some(s => s.status === 'active') && !jobState.options?.dryRun) return res.status(400).json({ error: 'No active store. Re-enable a store in Settings first.' });
  processBulk(remainingRows, jobState.lineItems, jobState.options, resumeFrom).catch(console.error);
  res.json({ success: true, message: `Resuming from row ${resumeFrom + 1} — ${remainingRows.length} rows remaining`, resumeFrom, remaining: remainingRows.length });
});
app.get('/api/progress', (req, res) => {
  res.setHeader('Content-Type', 'text/event-stream');
  res.setHeader('Cache-Control', 'no-cache');
  res.setHeader('Connection', 'keep-alive');
  res.setHeader('X-Accel-Buffering', 'no');
  res.flushHeaders();
  clients.add(res);
  if (currentJob) res.write(`data: ${JSON.stringify({ type: 'current_job', job: currentJob })}\n\n`);
  req.on('close', () => clients.delete(res));
});

// ─── API: History ─────────────────────────────────────────────────────────────
app.get('/api/history', (req, res) => res.json(history));
app.delete('/api/history', async (req, res) => { history = []; await save.history(); res.json({ success: true }); });

// ─── API: Link Maste ──────────────────────────────────────────────────────────
app.post('/api/host-file', upload.single('file'), async (req, res) => {
  try {
    if (hostedFile?.path) try { fsSync.unlinkSync(hostedFile.path); } catch (e) {}
    hostedFile = { name: req.file.filename, originalName: req.file.originalname, path: req.file.path, uploadedAt: new Date().toISOString(), visits: 0 };
    await save.hostedFile();
    res.json({ success: true, file: hostedFile });
  } catch (e) { res.status(500).json({ error: toMsg(e) }); }
});
app.get('/api/hosted-file', (req, res) => res.json(hostedFile));
app.delete('/api/hosted-file', async (req, res) => {
  if (hostedFile?.path) try { fsSync.unlinkSync(hostedFile.path); } catch (e) {}
  hostedFile = null; linkStore = {};
  await Promise.all([save.hostedFile(), save.linkStore()]);
  res.json({ success: true });
});
app.get('/t/:linkId', async (req, res) => {
  const link = linkStore[req.params.linkId];
  if (!link || !hostedFile) return res.status(404).send('Link not found or expired');
  const ua = req.headers['user-agent'] || '';
  const uaParsed = parseUA(ua);
  linkHistory.unshift({ linkId: req.params.linkId, email: link.email, ip: req.ip?.replace('::ffff:', '') || '—', ua, os: uaParsed.os, browser: uaParsed.browser, device: uaParsed.device, country: req.headers['cf-ipcountry'] || req.headers['x-country-code'] || '—', city: req.headers['cf-ipcity'] || '—', time: new Date().toISOString() });
  if (hostedFile) hostedFile.visits = (hostedFile.visits || 0) + 1;
  await Promise.all([save.linkHistory(), save.hostedFile()]);
  res.redirect(`/uploads/hosted/${hostedFile.name}`);
});
app.get('/api/link-history', (req, res) => res.json(linkHistory));
app.delete('/api/link-history', async (req, res) => { linkHistory = []; linkStore = {}; await Promise.all([save.linkHistory(), save.linkStore()]); res.json({ success: true }); });
app.get('/api/link-stats', (req, res) => {
  const totalClicks = linkHistory.length;
  const uniqueEmails = new Set(linkHistory.map(h => h.email).filter(Boolean)).size;
  const uniqueIPs = new Set(linkHistory.map(h => h.ip).filter(Boolean)).size;
  const countries = [...new Set(linkHistory.map(h => h.country).filter(c => c && c !== '—'))];
  res.json({ totalClicks, uniqueEmails, countries: countries.length, uniqueIPs, countriesList: countries });
});

// ─── Misc ─────────────────────────────────────────────────────────────────────
app.get('/api/stats', (req, res) => {
  const last = history[0] || null;
  const allTotal = history.reduce((s, j) => s + (j.total || 0), 0);
  const allSuccess = history.reduce((s, j) => s + (j.success || 0), 0);
  const allFailed = history.reduce((s, j) => s + (j.failed || 0), 0);
  res.json({
    total: last?.total || 0,
    success: last?.success || 0,
    failed: last?.failed || 0,
    skipped: last?.skipped || 0,
    lastRunMode: last?.mode || null,
    lastRunAt: last?.timestamp || null,
    hasHistory: history.length > 0,
    allTotal, allSuccess, allFailed,
    activeApis: stores.filter(s => s.status === 'active').length,
    uploadedData: uploadedData.length,
    totalStores: stores.length,
    activeProxies: proxies.filter(p => p.status === 'active').length,
    coolingStores: storeCooldowns.size
  });
});
app.get('/health', (req, res) => res.json({ status: 'ok', version: '4.0.0', stores: stores.length, proxies: proxies.length, uploadedRows: uploadedData.length }));
app.listen(PORT, () => console.log(`Shopify Api running on port ${PORT}`));
