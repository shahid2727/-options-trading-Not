# Options Opportunity Bot V9 Free

Free-first options scanner using Alpaca Basic market data, Telegram and an optional TradingView webhook.

## What it does
- Intraday confirmation: 5-minute EMA 9/21, VWAP, RSI, MACD, volume surge and breakout.
- Option filters: premium, spread, volume, open interest, DTE and delta when supplied.
- Score 0-100 and top 5 alerts.
- Entry zone, stop, TP1/TP2/TP3 and risk sizing.
- SPXW is handled through SPX as the underlying, matching Alpaca's index-option model.
- QQQ and selected liquid Nasdaq names are included.
- Telegram alerts only; no automatic brokerage orders.
- `/webhook` accepts TradingView alerts and forwards them to Telegram.

## Important free-data limitation
Alpaca Basic provides a free indicative options feed; option trades are delayed and quotes are modified. Equity real-time coverage on Basic is IEX. The bot labels alerts with `Data: INDICATIVE` and does not claim the option quote is OPRA real-time.

NDX index options are not supported by Alpaca's current index-options offering, so NDX is not included in the default scanner. QQQ is included instead.

## Environment variables
Copy `.env.example` to your Render environment variables. Required:
- `ALPACA_API_KEY`
- `ALPACA_API_SECRET`
- `TELEGRAM_BOT_TOKEN`
- `TELEGRAM_CHAT_ID`

Recommended secrets:
- `SCAN_SECRET`
- `TELEGRAM_TEST_SECRET`
- `WEBHOOK_SECRET`

## Endpoints
- `/health`
- `/status`
- `/scan?secret=...`
- `/scan/status?secret=...`
- `/telegram-test?secret=...`
- `POST /webhook?secret=...`

TradingView webhook URL:
`https://YOUR-RENDER-URL/webhook?secret=YOUR_WEBHOOK_SECRET`

The webhook is for alerts/analysis only and never places trades.

## Telegram commands
With `TELEGRAM_COMMANDS_ENABLED=true`, the bot listens for commands from the configured `TELEGRAM_CHAT_ID`:
- `/status` — current bot/scan status
- `/scan` — start a manual background scan
- `/top` — show the best results from the last scan
- `/help` — show commands
Only the configured Telegram chat ID is accepted. The bot never places trades.
