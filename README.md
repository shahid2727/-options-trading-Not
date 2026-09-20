# Options Opportunity Bot V7.2

Alert/research-only U.S. options scanner. No brokerage order execution.

## Features
- Stocks + SPXW scan target (SPXW is resolved through `^SPX` because market-data providers may not expose `SPXW` as a normal ticker).
- Pre-market 04:00–09:30 ET, regular 09:30–16:00 ET, optional after-hours 16:00–20:00 ET.
- Background `/scan` to prevent Render gateway timeouts.
- `/scan/status` for scan progress/results.
- Telegram alerts, max 5 by default.
- Risk/entry/SL/TP estimates are research estimates, not execution instructions.

## Endpoints
- `/health`
- `/status`
- `/scan?secret=YOUR_TEST_SECRET`
- `/scan/status?secret=YOUR_TEST_SECRET`

## Render environment
Keep your existing secrets. Optional settings:
`PREMARKET_ENABLED=true`, `REGULAR_ENABLED=true`, `AFTERHOURS_ENABLED=false`, `SCAN_INTERVAL_SECONDS=300`, `MAX_ALERTS=5`, `RISK_BUDGET=100`.


### V7.3 endpoints
- `/health` — service and scanner status.
- `/scan?secret=YOUR_SCAN_SECRET` — starts a background scan and returns immediately.
- `/scan/status?secret=YOUR_SCAN_SECRET` — scan status.
- `/telegram-test?secret=YOUR_TELEGRAM_TEST_SECRET` — sends a Telegram test message.

Set `SCAN_SECRET` in Render separately from `TELEGRAM_TEST_SECRET`. Do not put secrets in GitHub.
