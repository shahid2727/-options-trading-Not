# Options Opportunity Bot V8

Research/alert bot for U.S. listed options. It does not place brokerage orders.

## V8 highlights
- Pre-market 04:00–09:30 ET, regular 09:30–16:00 ET, optional after-hours 16:00–20:00 ET.
- Score 0–100 using momentum, volume surge, breakout proximity, liquidity, spread, OI, option volume, delta availability, DTE and premium.
- Filters and risk budget are configurable with environment variables.
- Best 5 candidates are sent to Telegram.
- Background scans avoid blocking the web endpoint.
- `/health`, `/scan`, `/scan/status`, `/telegram-test` endpoints.
- SPXW is represented through the configured SPXW symbol path; verify the upstream option-chain source before treating it as a guaranteed live SPXW feed.
- Pre-market stock option quotes can be incomplete depending on upstream data availability.

## Environment
See `.env.example`.

`SCAN_SECRET` protects `/scan` and `/scan/status`.
`TELEGRAM_TEST_SECRET` protects `/telegram-test`.
Never commit real secrets.
