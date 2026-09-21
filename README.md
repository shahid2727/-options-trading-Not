# Options Opportunity Bot V10.4

SPXW final fix: technical indicators use SPY IEX bars while the real SPXW option chain is fetched from Alpaca under the SPX options root. SPXW failures are isolated from other symbols.

Includes Entry Low, Entry Zone, Stop Loss, TP1/TP2/TP3, suggested contracts, and profit targets in alerts.

Run with the existing Docker/Render configuration and environment variables. Do not commit API keys or Telegram tokens.


V10.5 fixes Alpaca SPXW discovery by using the option-chain root_symbol=SPXW filter under the SPX underlier, rather than relying on contract-string prefixes. /status now exposes Prefix and BadContract rejection counts.


## V13.2 changes
- Lowers the relaxed opportunity threshold to 40.
- Adds a controlled discovery fallback when normal filters return zero candidates.
- Discovery results are tagged `discovery_fallback` and limited to the top 3 per symbol.
- Keeps the existing risk, spread, liquidity, Telegram and timeout protections.
