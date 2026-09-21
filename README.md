# Options Opportunity Bot V10.4

SPXW final fix: technical indicators use SPY IEX bars while the real SPXW option chain is fetched from Alpaca under the SPX options root. SPXW failures are isolated from other symbols.

Includes Entry Low, Entry Zone, Stop Loss, TP1/TP2/TP3, suggested contracts, and profit targets in alerts.

Run with the existing Docker/Render configuration and environment variables. Do not commit API keys or Telegram tokens.


V10.5 fixes Alpaca SPXW discovery by using the option-chain root_symbol=SPXW filter under the SPX underlier, rather than relying on contract-string prefixes. /status now exposes Prefix and BadContract rejection counts.


### V13.3 chain recovery
The scanner first uses Alpaca market-data option chains. If that request fails, it can fall back to Alpaca option-contract discovery followed by batched snapshots. Set `OPTIONS_CHAIN_FALLBACK=true`. `ALPACA_TRADING_BASE_URL` should match the account environment (paper or live).

## V13.4 Explosive Momentum Detector
The scanner now separately scores high-momentum option setups using multi-timeframe alignment, breakouts, volume expansion, price acceleration, delta sensitivity, liquidity and near-term expiry. `explosive_score` is a setup-detection score, not a guarantee of profit. Alerts prioritize explosive setups first.
