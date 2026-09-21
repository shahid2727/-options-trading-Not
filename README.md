# Telegram Options Opportunity Bot V10.0

V10.0 is a direct stability/diagnostics upgrade of the supplied V9.9 codebase. It remains **alert-only**: no brokerage execution and no order placement.

## V9.9 strategy preserved
The existing V9.9 multi-timeframe analysis, regime filter, scoring weights, candidate thresholds, premium/risk model, confidence calculation, TP/SL model, SPXW support, symbol list, and alert cooldown are preserved. V10.0 does not lower the V9.9 minimum score or liquidity thresholds.

## V10.0 stability changes
- Telegram long polling runs in its own thread and has independent diagnostics.
- Scanner runs in a killable worker process. `SCAN_TIMEOUT_SECONDS` prevents an indefinitely stuck scan.
- Scanner progress reports: `fetching_bars`, `fetching_options`, `scoring`, `alerts`, `complete`/`failed`.
- `/status`, `/health`, and `/` do not wait for the scanner.
- Provider diagnostics include HTTP status, endpoint path, retry count, and timeout/connection failures without exposing secrets.
- Alpaca requests use a reusable HTTP session, bounded retries/backoff, and configurable timeout.
- Options snapshot pagination is supported so the scanner is not limited to the first page of contracts.
- Session labels use `America/New_York`: 04:00–09:30 ET pre-market, 09:30–16:00 regular, 16:00–20:00 after-hours.
- Options `data_mode` is taken from `ALPACA_OPTIONS_FEED`; it is not inferred from the underlying extended-hours session.

## Environment variables
Required secrets (keep in Render, never commit them):
- `TELEGRAM_BOT_TOKEN`
- `TELEGRAM_CHAT_ID`
- `ALPACA_API_KEY`
- `ALPACA_API_SECRET`

V10.0 controls:
```text
TELEGRAM_COMMANDS_ENABLED=true
PREMARKET_ENABLED=true
REGULAR_ENABLED=true
AFTERHOURS_ENABLED=true
SCAN_TIMEOUT_SECONDS=120
ALPACA_HTTP_TIMEOUT=8
ALPACA_RETRIES=2
OPTIONS_MAX_PAGES=20
```

The V9.9 variables remain supported, including `STOCK_SYMBOLS`, `INDEX_ROOTS=SPXW`, `SCAN_INTERVAL_SECONDS`, `MIN_SCORE`, `MIN_VOLUME`, `MIN_OI`, `MAX_SPREAD_PCT`, premium limits, `ALERT_COOLDOWN_SECONDS`, and `MAX_ALERTS`.

## Telegram commands
- `/start`
- `/help`
- `/status`
- `/scan`
- `/top`

`/scan` does not start a second scan while one is running.

## HTTP endpoints
- `/`
- `/health`
- `/status`
- `/scan` (protected by `SCAN_SECRET` when configured)
- `/scan/status` (protected by `SCAN_SECRET` when configured)
- `/telegram-test` (optional `TELEGRAM_TEST_SECRET`)
- `/webhook` (optional `WEBHOOK_SECRET`, alert-only)

## Data accuracy
The bot does not equate an extended-hours underlying quote with a live options quote. The alert includes the configured options `Data Mode` such as `INDICATIVE`, `DELAYED`, or another configured feed name. Verify your Alpaca entitlement before treating option prices as real-time.

## Render
Use a Render Web Service with the included Dockerfile. Render provides `PORT`; the application reads it automatically. No paid libraries or brokerage services are required.

No API keys, Telegram tokens, or other secrets belong in GitHub.
