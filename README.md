# Options Opportunity Bot V7.1

Alert/research scanner only. No brokerage orders are executed.

## What changed
- Scans configured equities plus **SPXW** via the SPX index underlying (`^SPX`).
- SPXW is handled separately and filters for option contract symbols containing `SPXW` when the data provider exposes that field.
- SPXW Global Trading Hours are recognized (8:15 PM–9:25 AM ET); regular SPX/SPXW hours are 9:30 AM–4:15 PM ET. Cboe also has a 4:15–5:00 PM ET curb session.
- Equity pre-market scans are setup/watch alerts; U.S. equity options are not treated as executable during the stock pre-market.
- Uses provider Greeks when valid and Black-Scholes delta approximation when missing/invalid.
- SPX/SPXW option tick-size aware risk plan: $0.05 below $3 and $0.10 at/above $3.
- Better liquidity, volume/OI, spread, momentum and breakout scoring.
- Top 5 alerts only by default, with deduplication.
- Telegram errors should never expose the bot token.

## Render environment
Set the variables in `.env.example`. Never commit real Telegram tokens.

## Endpoints
- GET `/health`
- GET `/status`
- GET `/scan?secret=YOUR_TELEGRAM_TEST_SECRET`
- GET `/telegram-test?secret=YOUR_TELEGRAM_TEST_SECRET`
- POST `/webhook` with `X-Webhook-Secret`

## Important
`yfinance` is a research-data source and may have delayed, incomplete, or missing option Greeks/quotes. Verify bid/ask and the live contract on your broker before acting. This bot does not guarantee returns and does not place orders.
