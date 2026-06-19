# Live Quotes

A Cloudflare Workers app that streams live stock quotes from Yahoo Finance using Hono, yahoo-finance2, and Server-Sent Events (SSE).

This project relies on the `yahoo-finance2` library for all quote retrieval and market state data. The package is community-maintained Yahoo Finance client available at https://github.com/gadicc/yahoo-finance2.

## Features

- `GET /` redirects to the static `index.htm` landing page
- `GET /live-quotes` streams live quote updates via SSE
- Supports one or more comma-separated symbols via `s`
- Supports custom polling interval via `i` (milliseconds)
- Uses `yahoo-finance2` to fetch latest market, pre-market, and post-market prices
- Powered by `yahoo-finance2` for live quote retrieval and market state detection
- Includes global error handling with Chanfana API exception support

## Project structure

- `src/index.ts` — main Worker application
- `static/index.htm` — static client page served from the asset directory
- `wrangler.jsonc` — Cloudflare Workers configuration
- `package.json` — scripts, dependencies, and dev dependencies

## Requirements

- Node.js
- npm
- Cloudflare Wrangler

## Install

```bash
npm install
```

## Local development

```bash
npm run dev
```

Then open the local Worker URL printed by Wrangler.

## Usage

### Open the UI

Visit:

```text
/
```

This redirects to the static `index.htm` client.

### Subscribe to quote events

Request:

```text
/live-quotes?s=AAPL,MSFT&i=2000
```

Query parameters:

- `s` — comma-separated symbol list (default: `AAPL`)
- `i` — poll interval in milliseconds (default: `1000`)

### SSE event payload

Each SSE event is emitted with type `quote` and JSON payload containing:

- `t` — timestamp
- `symbol` — requested ticker symbol
- `price` — current price
- `change` — numeric change amount
- `changePercent` — percentage change
- `state` — market state (`PRE`, `REGULAR`, or `POST`)

## Scripts

- `npm run dev` — run Wrangler in development mode
- `npm run deploy` — deploy the Worker using Wrangler
- `npm test` — run a dry-run deploy plus Vitest tests

## Cloudflare configuration

The project uses:

- `compatibility_date: 2026-05-11`
- `nodejs_compat` compatibility flag
- static assets from `./static`

## Notes

- No external environment variables are required for the basic quote stream.
- The app relies on the `yahoo-finance2` package for live quote retrieval.

## License

MIT
