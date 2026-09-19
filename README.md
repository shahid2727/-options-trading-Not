# Options Opportunity Bot V5

Alert/research bot for U.S. options. It does **not** place brokerage orders.

## V5 additions
- Automatic scanner loop during U.S. regular market hours.
- Telegram alerts without requiring a TradingView webhook.
- Configurable scan interval and ticker universe.
- Alert de-duplication/cooldown.
- `/health` and `/status` endpoints.
- Existing entry, stop-loss, TP1/TP2/TP3, R:R and position-risk calculations.
- TradingView webhook remains available.

## Render
The Docker service runs Gunicorn with one worker. Keep one worker so the background scanner does not duplicate alerts.

### Required environment variables
- `TELEGRAM_BOT_TOKEN`
- `TELEGRAM_CHAT_ID`

### Optional settings
- `RISK_BUDGET=100`
- `SCAN_INTERVAL_SECONDS=300` (5 minutes)
- `SCAN_SYMBOLS=SPY,QQQ,...`
- `MAX_ALERTS_PER_SCAN=5`
- `ALERT_COOLDOWN_MINUTES=30`
- `MIN_ALERT_SCORE=7`
- `MARKET_ONLY=true`

## Important limitation
Render Free can sleep/stop idle services. Therefore V5's in-process scanner is automatic while the service is running, but **not a guaranteed 24/7 market scanner on the Free plan**. For continuous operation, use an always-on/paid service or a dedicated worker.

## Data limitation
The current scanner uses yfinance. Option Greeks such as delta may be unavailable from the feed; the risk module therefore uses a conservative proxy when needed. For production-grade real-time options scanning, replace the market-data layer with a dedicated options-data provider.


## V5.1 fixes
- Reads Telegram credentials directly from Render environment variables at send time.
- Logs only whether Telegram is configured; never logs the bot token.
- Handles NaN/invalid option-chain values without aborting a symbol scan.
- `/health` and `/status` expose a non-secret `telegram_configured` flag.
