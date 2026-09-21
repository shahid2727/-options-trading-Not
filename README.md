# Options Opportunity Bot V10.4

SPXW final fix: technical indicators use SPY IEX bars while the real SPXW option chain is fetched from Alpaca under the SPX options root. SPXW failures are isolated from other symbols.

Includes Entry Low, Entry Zone, Stop Loss, TP1/TP2/TP3, suggested contracts, and profit targets in alerts.

Run with the existing Docker/Render configuration and environment variables. Do not commit API keys or Telegram tokens.
