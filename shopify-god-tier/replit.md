# God Tier Automator v4.1

A professional Shopify bulk order automation tool. Multi-store management, OAuth auto-token flow, CSV upload, link tracking, and 8 processing modes.

## Run & Operate
- **Start**: `cd shopify-god-tier && npm install && node server.js` (port 5000)
- **User PIN**: 1234 | **Admin PIN**: 6001
- No required env vars — config persisted in `config.json`

## Stack
- Node.js + Express, single-page HTML frontend (no framework)
- Server-Sent Events (SSE) for real-time progress
- Multer for CSV/file uploads
- Shopify REST Admin API 2025-10

## Where things live
- `server.js` — all API routes, bulk processor, OAuth, SSE
- `public/index.html` — complete single-file frontend (sidebar, 6 pages)
- `config.json` — global settings (auto-created)
- `stores.json` — store list (auto-created)
- `data.json` — uploaded CSV rows
- `history.json` — last 50 job summaries
- `link-store.json` / `link-history.json` — link tracking
- `uploads/` — CSV + hosted file storage

## Architecture decisions
- All config/state in flat JSON files on disk; no external DB needed
- OAuth auto-generate: tries client_credentials POST first, falls back to popup OAuth flow
- Sequential store failover: stay on one store until it fails, then switch; cooldown 30s on blocked stores
- Randomization: ±25% jitter on per-row delays + burst pause every 8–18 rows to avoid Shopify rate-limiting
- Digital product mode sets `requires_shipping: false` on all line items; skips fulfillment creation

## Product
- **Dashboard** — Live stats + SSE activity feed; Quick Actions link to Run Orders
- **Upload Data** — Drag & drop CSV upload with preview
- **Run Orders** — Mode selector, Physical/Digital product toggle, 35+ carrier dropdown, blank tracking option, randomized delay toggle, line items builder, dry run, live progress + results table
- **Settings** — Store management (add via OAuth or direct token), global config (carrier, delay, blank tracking, randomization, product type defaults)
- **Admin Panel** — System overview + danger zone; store management links to Settings
- **Link Maste Host** — Host a file; `{link_maste}` tag generates unique per-email tracking URLs

## Processing Modes
1. `order_create_delivery` — Paid order + fulfillment + out-for-delivery
2. `draft_invoice` — Draft order + invoice email
3. `order_create_local` — Paid order + local delivery fulfillment
4. `customer_invite` — Create customer + invite email
5. `order_create` — Simple paid order
6. `bank_transfer` — Pending order with bank transfer gateway
7. `invoice_master` — Paid order + fulfillment + invoice + paid emails
8. `order_create_delivered` — Paid order + full delivery event sequence

## Gotchas
- Domain must end in `.myshopify.com` for OAuth generate to work
- Blank tracking + Digital mode both suppress fulfillment tracking; they can combine
- `renderStores()` renders to `#storesList` (Settings page only); Admin Panel just links to Settings
- `currentProductType` is a JS global set by `setProductType()` on the Run Orders page
