# Options Opportunity Bot V14

SPXW final fix: technical indicators use SPY IEX bars while the real SPXW option chain is fetched from Alpaca under the SPX options root. SPXW failures are isolated from other symbols.

Includes Entry Low, Entry Zone, Stop Loss, TP1/TP2/TP3, suggested contracts, and profit targets in alerts.

Run with the existing Docker/Render configuration and environment variables. Do not commit API keys or Telegram tokens.


V10.5 fixes Alpaca SPXW discovery by using the option-chain root_symbol=SPXW filter under the SPX underlier, rather than relying on contract-string prefixes. /status now exposes Prefix and BadContract rejection counts.


### V13.3 chain recovery
The scanner first uses Alpaca market-data option chains. If that request fails, it can fall back to Alpaca option-contract discovery followed by batched snapshots. Set `OPTIONS_CHAIN_FALLBACK=true`. `ALPACA_TRADING_BASE_URL` should match the account environment (paper or live).

## V13.4 Explosive Momentum Detector
The scanner now separately scores high-momentum option setups using multi-timeframe alignment, breakouts, volume expansion, price acceleration, delta sensitivity, liquidity and near-term expiry. `explosive_score` is a setup-detection score, not a guarantee of profit. Alerts prioritize explosive setups first.


## V13.5 fixes
- OPRA -> indicative fallback when the configured options feed is rejected.
- More detailed provider/HTTP diagnostics in `/status`.
- One market-close HERO alert per trading day when a candidate exists.


## V14 setup engine
- Preserves the existing Alpaca/OPRA/IEX scanner, Telegram polling, worker timeout, SPXW proxy-chain architecture, caching, and alert-only risk controls.
- Separates hard contract/quote safety filters from soft setup factors.
- Premium, 4H alignment, regime, RSI, VWAP, volume, momentum and score are soft factors unless an explicit safety setting is enabled.
- Adds explainable `HERO`, `STRONG`, and `WATCH` tiers with configurable thresholds.
- Scores underlying direction once per symbol and reuses the indicators across its option chain.
- CALLs and PUTs are directionally evaluated independently; SPXW uses separate weighting while using SPY IEX bars as its technical proxy and the real SPX option chain.
- Adds per-symbol rejection/tier diagnostics and scan-wide no-setup reasons.
- Alert cooldown is `symbol + direction + contract`; `MAX_ALERTS_PER_SCAN=5` prioritizes HERO, then STRONG, then WATCH.
- End-of-day can send up to three HERO setups, including SPXW.
- Quote validation rejects zero/invalid bid/ask and explicit stale quotes; missing timestamps are not treated as stale unless `REQUIRE_QUOTE_TIMESTAMP=true`.
- Entry/stop/target levels are model-derived from current bid/ask and available ATR. When the required volatility data is unavailable, the alert reports the target as unavailable instead of inventing a number.
- The bot never places brokerage orders.

## V14 pipeline validation
V14 separates chain receipt, contract normalization, quote-stage validation, and scoring. Set `DEBUG_SCANNER=true`, `DEBUG_SAMPLE=true`, or `DEBUG_BYPASS_SCORING=true` to diagnose data flow without changing production scoring thresholds.


## V14.2 reconciliation fix
- Final scan counters and HERO/STRONG/WATCH totals are recomputed from actual result rows and per-symbol diagnostics.
- Prevents a state/aggregation mismatch where candidates existed but summary counters displayed zero.


V14.8: bounded options-feed retries, batch sizing, per-request timeout, overall chain deadline, partial-batch fallback, and feed telemetry. Scoring thresholds unchanged.
