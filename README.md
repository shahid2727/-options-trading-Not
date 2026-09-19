# Options Opportunity Bot V2

Alert/research bot for U.S. options. It screens option chains and produces a structured trade plan with **entry zone, stop-loss, TP1, TP2, TP3 and an exit-management plan**.

## Trade-plan logic

For an option premium `P`:

- Entry zone: `0.95P – 1.05P`
- Stop-loss: `0.70P`
- TP1: `1.50P`
- TP2: `2.00P`
- TP3: `3.00P`

Exit management:

1. At TP1, consider partial profit and move stop toward breakeven.
2. At TP2, consider another partial profit and trail the remainder.
3. At TP3, consider closing the remainder.
4. If the stop-loss is hit, exit the position.
5. Review/close before expiry rather than letting the contract expire by accident.

These are configurable screening rules, not guarantees or investment advice. The bot does **not** place brokerage orders.

## Local setup

1. Install Python 3.11+
2. Copy `.env.example` to `.env`
3. Fill Telegram credentials if desired.
4. `pip install -r requirements.txt`
5. `python bot.py`
6. Open `http://localhost:10000/health`

## TradingView webhook

URL: `https://YOUR-DOMAIN/webhook`

Header: `X-Webhook-Secret: YOUR_SECRET`

Example body:

```json
{"ticker":"NVDA","price":178.25,"signal":"BREAKOUT"}
```

## Render

Push the folder to GitHub and create a Render Web Service from the repository, or use `render.yaml`.

## Next upgrades

- Dedicated real-time options-data API instead of relying on Yahoo alone.
- Underlying technical levels (support/resistance/ATR) to improve stops and exits.
- Persistent database and signal outcome tracking.
- Historical backtesting.
- Dashboard.
- Paper-trading execution.
- Broker integration only after paper-trading validation.


## V4 trade plan
Each alert includes an entry zone, option stop loss, underlying reference stop, TP1/TP2/TP3, expected profit per contract in dollars and percent, R:R, and a suggested whole-contract size based on `RISK_BUDGET`. The options multiplier of 100 is used for dollar P/L. These are screening/reference calculations, not guaranteed outcomes.
