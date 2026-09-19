# Options Opportunity Bot V6

Alert/research bot for U.S. options. **It does not place brokerage orders.**

## V6 features
- Automatic scan every 5 minutes while the Render service is running.
- Option quality scoring on a 0–100 scale.
- Underlying 5-day and 20-day momentum.
- Volume-ratio / volume-surge detection.
- 20-day breakout proximity for calls and puts.
- Liquidity filters: volume, OI, premium and spread.
- Entry zone, stop loss, TP1/TP2/TP3, R:R and risk-budget sizing.
- Telegram alerts with deduplication/cooldown.
- Manual `/scan?secret=...` endpoint.
- `/health`, `/status`, `/telegram-test`.
- TradingView `/webhook` remains available.
- Telegram API errors are now returned safely without exposing the bot token.

## Render environment
Required:
- `TELEGRAM_BOT_TOKEN`
- `TELEGRAM_CHAT_ID`

Recommended:
- `TELEGRAM_TEST_SECRET` (temporary; remove after testing)
- `RISK_BUDGET=100`
- `SCAN_INTERVAL_SECONDS=300`
- `MAX_ALERTS_PER_SCAN=5`
- `ALERT_COOLDOWN_MINUTES=30`
- `MIN_ALERT_SCORE=7`
- `MARKET_ONLY=true`
- `RUN_SCANNER=true`

## Test Telegram
Open:
`https://YOUR-SERVICE.onrender.com/telegram-test?secret=YOUR_TELEGRAM_TEST_SECRET`

A successful response contains `"ok": true` and sends a test message.

## Manual scan
Open:
`https://YOUR-SERVICE.onrender.com/scan?secret=YOUR_TELEGRAM_TEST_SECRET`

This forces a scan and also sends the top candidates through Telegram.

## Important limitations
- Render Free can sleep/stop idle services, so the in-process scanner is not guaranteed to run continuously 24/7.
- yfinance is not a dedicated real-time options feed. Greeks such as delta may be unavailable.
- Alerts are research signals, not guarantees of profit and not automatic trade execution.
