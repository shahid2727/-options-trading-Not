# Options Opportunity Bot — V2

Research/alert scanner for US equity/index options. **It does not place trades.**

## What it does
- Scans a configurable watchlist.
- Uses Yahoo Finance option chains as a fallback/public-data layer.
- Optionally consumes an Unusual Whales flow endpoint through an isolated adapter.
- Filters by contract premium, volume, OI, volume/OI, DTE and bid/ask spread.
- Produces a transparent 0–10 opportunity score.
- Sends top alerts to Telegram.
- Accepts TradingView webhooks at `/webhook` for technical triggers.
- Includes `/health` endpoint.
- Supports Docker.

## Important limitation
Data-provider APIs and endpoint/field names can change. Unusual Whales requires your own API access and may use plan-specific endpoints. Set `UW_FLOW_PATH` to the endpoint documented for your account if necessary. The Yahoo layer is a fallback, not a substitute for real-time institutional options flow.

## Setup

### Local
```bash
python -m venv .venv
# Windows: .venv\\Scripts\\activate
# macOS/Linux: source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
python bot.py
```

Open `http://localhost:8080/health`.

### Telegram
Create a bot using Telegram's BotFather, put the token in `TELEGRAM_BOT_TOKEN`, and put the destination chat ID in `TELEGRAM_CHAT_ID`.

### TradingView
Create an alert and use the webhook URL:
`https://YOUR_PUBLIC_HOST/webhook`

Add HTTP header:
`X-Webhook-Secret: YOUR_WEBHOOK_SECRET`

Use `tradingview_alert_message.json` as the JSON body. Never put broker credentials in the TradingView alert.

### Docker
```bash
cp .env.example .env
# edit .env
docker compose up -d --build
```

## Default strategy
The default scanner is deliberately conservative about liquidity and only alerts contracts between $0.50 and $1.50. It rewards:
- large premium,
- high volume/OI,
- high contract volume,
- contract price inside the configured range,
- DTE inside the configured range,
- tighter spreads.

This score is **not a probability of profit** and is not a guarantee. Options can lose 100% of the premium.

## Suggested production architecture
For genuinely real-time execution-grade data, add a paid OPRA/market-data feed and a broker API separately. Keep this bot in alert/paper mode until the scanner is backtested and monitored.
