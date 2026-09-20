# Options Opportunity Bot V9.7

Research/alert-only US options scanner. No automatic trading.

## V9.7 highlights
- Multi-timeframe analysis: 4H, 1H, 15M, 5M.
- Market regime filter: TRENDING / MIXED / SIDEWAYS / CHOPPY.
- Technical scoring, confidence estimate, breakout and volume confirmation.
- Contract Entry / SL / TP1 / TP2 / TP3 and underlying levels.
- Broader affordable contract range: default $0.30-$10.00, configurable.
- Special **1000%+ analyzed upside** alert when a technically-derived underlying target implies a modeled option premium upside >= 1000% and the contract is within the affordable ceiling.
- The 1000%+ figure is a scenario estimate, not a guaranteed return or probability. Option prices can diverge materially because delta, IV, theta and liquidity change.
- Alpaca Basic: IEX underlying feed is real-time; free options feed is indicative/delayed. The bot labels this.

## Deploy
1. Upload these files to GitHub.
2. Let Render auto-deploy.
3. Keep existing secrets in Render; do not commit them.
4. Verify `/health`, then run `/scan` with your existing scan secret.

No brokerage order execution is implemented.


## V9.8 changes
- Telegram `/status`, `/scan`, and `/top` are available to the configured admin chat without entering `SCAN_SECRET`.
- `/scan` and `/scan/status` HTTP endpoints remain protected by `SCAN_SECRET`.
- Public Telegram `/start`, `/help`, and `/privacy` remain available.
- Removed a duplicated Market/4H line from opportunity alerts.
- No brokerage order execution.
