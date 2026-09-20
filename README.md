# Options Opportunity Bot V8.2

V8.2 adds diagnostic visibility to the V8.1 scanner while keeping the same Render/Telegram endpoints.

## Highlights
- Intraday 5-minute momentum, 30-minute momentum, volume surge and breakout signals.
- Nasdaq coverage through QQQ and NDX, plus SPXW and liquid U.S. equities.
- Score, premium, OI, option volume, spread, delta, entry, stop and targets.
- `last_top` now includes whether a candidate is intraday.
- Scanner diagnostics report symbols checked, option-chain activity, and how many contracts pass each filter.
- Set `DIAGNOSTIC_MODE=true` to keep scored candidates in diagnostic output even if they are below alert thresholds. This is for testing; Telegram still sends only the configured top results.

## Render environment
Keep your existing Telegram values and `SCAN_SECRET`. Optional settings are in `.env.example`.

## Endpoints
- `/health`
- `/scan?secret=SCAN_SECRET`
- `/scan/status?secret=SCAN_SECRET`
- `/telegram-test?secret=TELEGRAM_TEST_SECRET`

This bot is an alert/research tool. It does not place brokerage orders.


## V8.3 changes
- Falls back to the latest daily close when `fast_info.last_price` is unavailable, so scans can discover option chains outside market hours.
- Diagnostic mode can inspect recent 5-minute history even while the market is closed.
- Adds `option_chain_empty` diagnostic count.
- Keep `DIAGNOSTIC_MODE=true` while validating the deployment; set it to `false` after validation if desired.
- This bot is alert/research only and does not place brokerage orders.
