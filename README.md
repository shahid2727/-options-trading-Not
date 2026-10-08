# Options Opportunity Bot V15.0.0

Alert-only Telegram Options Scanner / Analyzer. **No brokerage order execution is implemented.**

## What was rebuilt

- Full multi-symbol scanner for SPY, QQQ, IWM, NVDA, AMD, TSLA, AAPL, AMZN, META, MSFT, GOOGL, MU, AVGO, PLTR and SMCI.
- SPXW handled as an index-option chain under the SPX underlier, with SPY used only for technical context.
- NDX/Nasdaq is detected and reported honestly when the provider does not expose NDX index options; QQQ remains the technical proxy. No fake NDX contracts are generated.
- Options-chain pagination follows `next_page_token` and is bounded by an overall timeout/page safety limit. Early strike, expiration and type filters reduce unnecessary pages.
- OPRA is preferred when configured; indicative data is a bounded fallback when the account/feed rejects OPRA.
- Trading-API contract discovery is an optional fallback when market-data chain discovery fails.
- Quote handling: Bid/Ask mid first, recent Last fallback, quote source/timestamp/age/status retained.
- Missing Greeks, volume or OI are treated as N/A/neutral where possible; contracts without a usable quote remain visible in diagnostics instead of silently disappearing.
- Premium display/alert band is $0.20–$5.00.
- Composite 0–100 score uses direction, market alignment, momentum, trend, volume/RVOL, spread, OI, contract volume, delta, IV, DTE, premium, technical levels, gap/expected-move context and regime/conflict penalties.
- HERO is independently gated; a high raw score alone cannot create HERO.
- MOONSHOT requires multiple simultaneous conditions and cannot qualify only because the option is cheap.
- On-demand `/analyze` works for tickers/company names outside the scanner universe, e.g. `BE`, `Bloom Energy`, `TSLA`, `NVIDIA`.
- On-demand contract recognition supports OCC symbols and shorthand such as `SPXW 7760C`, `NVDA 200C`, and `NVDA 2026-10-15 200C`.
- `/status` remains non-blocking while the scanner runs and exposes real provider/worker errors.
- Telegram polling is independent from scanner execution.
- One symbol/provider failure cannot terminate the complete scan.
- Multi-timeframe cache access is thread-safe; no shared mutable cache pointer is used during parallel symbol scans.
- Manual `/scan` is allowed outside market hours for diagnostics; automatic scheduling still respects the phase settings.

## Commands

`/start` `/help` `/status` `/scan` `/top` `/hero` `/heroes` `/moonshot` `/watchlist` `/analyze TICKER_OR_COMPANY` `/contract CONTRACT` `/watch CONTRACT` `/unwatch CONTRACT` `/clearwatch` `/diagnostics`

Examples:

- `/analyze BE`
- `/analyze Bloom Energy`
- `/analyze NVDA`
- `/contract SPXW 7760C`
- `/contract NVDA 200C`
- `/contract NVDA 2026-10-15 200C`
- `/contract NVDA261015C00200000`

## Render

The included Dockerfile runs `python bot.py` on Render's `PORT` (default 10000). Set the environment variables from `.env.example` in Render. Do not commit secrets.

Important provider settings:

- `ALPACA_OPTIONS_FEED=opra` (falls back to `indicative` if enabled and OPRA is unavailable)
- `ALPACA_UNDERLYING_FEED=iex` by default; change only if your Alpaca subscription supports another underlying feed.
- `OPTIONS_CHAIN_FALLBACK=true`
- `ALPACA_TRADING_BASE_URL` must match the account environment if contract-discovery fallback is used.

## Provider limitation: NDX

Alpaca's current index-options documentation lists SPX/SPXW support but does not list NDX/NQX as supported index-option products. The bot therefore attempts the configured provider path, records the actual provider error, and uses QQQ only as a technical proxy rather than fabricating NDX contracts.

## Safety

This project analyzes and alerts only. It does not submit, modify, cancel or execute brokerage orders. Model-derived Entry/TP/SL levels are not guaranteed fills or outcomes.
