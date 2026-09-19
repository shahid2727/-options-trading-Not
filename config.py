import os
from dataclasses import dataclass, field


def csv_list(value: str):
    return [x.strip().upper() for x in value.split(',') if x.strip()]


@dataclass
class Config:
    min_premium: float = float(os.getenv('MIN_PREMIUM', '0.50'))
    max_premium: float = float(os.getenv('MAX_PREMIUM', '1.50'))
    min_volume: int = int(os.getenv('MIN_VOLUME', '500'))
    min_oi: int = int(os.getenv('MIN_OI', '200'))
    min_score: float = float(os.getenv('MIN_SCORE', '7'))
    max_spread_pct: float = float(os.getenv('MAX_SPREAD_PCT', '15'))
    min_dte: int = int(os.getenv('MIN_DTE', '1'))
    max_dte: int = int(os.getenv('MAX_DTE', '30'))
    risk_budget: float = float(os.getenv('RISK_BUDGET', '100'))
    webhook_secret: str = os.getenv('TRADINGVIEW_WEBHOOK_SECRET', '')
    scan_interval_seconds: int = int(os.getenv('SCAN_INTERVAL_SECONDS', '300'))
    scan_symbols: list = field(default_factory=lambda: csv_list(os.getenv(
        'SCAN_SYMBOLS',
        'SPY,QQQ,IWM,NVDA,AMD,TSLA,AAPL,AMZN,META,MSFT,GOOGL,MU,AVGO,PLTR,SMCI'
    )))
    max_alerts_per_scan: int = int(os.getenv('MAX_ALERTS_PER_SCAN', '5'))
    alert_cooldown_minutes: int = int(os.getenv('ALERT_COOLDOWN_MINUTES', '30'))
    min_alert_score: float = float(os.getenv('MIN_ALERT_SCORE', '7'))
    market_only: bool = os.getenv('MARKET_ONLY', 'true').lower() in ('1', 'true', 'yes', 'on')
    run_scanner: bool = os.getenv('RUN_SCANNER', 'true').lower() in ('1', 'true', 'yes', 'on')


cfg = Config()
