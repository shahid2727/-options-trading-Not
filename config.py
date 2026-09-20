import os
from dataclasses import dataclass, field

def csv_list(value: str):
    return [x.strip().upper() for x in value.split(',') if x.strip()]

def env_bool(name, default='true'):
    return os.getenv(name, default).strip().lower() in ('1','true','yes','on')

@dataclass
class Config:
    min_premium: float = float(os.getenv('MIN_PREMIUM','0.50'))
    max_premium: float = float(os.getenv('MAX_PREMIUM','1.50'))
    min_volume: int = int(os.getenv('MIN_VOLUME','500'))
    min_oi: int = int(os.getenv('MIN_OI','200'))
    min_score: float = float(os.getenv('MIN_SCORE','7'))
    min_alert_score: float = float(os.getenv('MIN_ALERT_SCORE','7'))
    max_spread_pct: float = float(os.getenv('MAX_SPREAD_PCT','10'))
    min_dte: int = int(os.getenv('MIN_DTE','1'))
    max_dte: int = int(os.getenv('MAX_DTE','30'))
    risk_budget: float = float(os.getenv('RISK_BUDGET','100'))
    webhook_secret: str = os.getenv('TRADINGVIEW_WEBHOOK_SECRET','').strip()
    telegram_test_secret: str = os.getenv('TELEGRAM_TEST_SECRET','').strip()
    scan_interval_seconds: int = int(os.getenv('SCAN_INTERVAL_SECONDS','300'))
    scan_symbols: list = field(default_factory=lambda: csv_list(os.getenv('SCAN_SYMBOLS','SPY,QQQ,IWM,NVDA,AMD,TSLA,AAPL,AMZN,META,MSFT,GOOGL,MU,AVGO,PLTR,SMCI')))
    scan_spxw: bool = env_bool('SCAN_SPXW','true')
    spxw_underlying: str = os.getenv('SPXW_UNDERLYING','^SPX').strip()
    max_alerts_per_scan: int = int(os.getenv('MAX_ALERTS_PER_SCAN','5'))
    alert_cooldown_minutes: int = int(os.getenv('ALERT_COOLDOWN_MINUTES','30'))
    premarket_enabled: bool = env_bool('PREMARKET_ENABLED','true')
    regular_enabled: bool = env_bool('REGULAR_ENABLED','true')
    afterhours_enabled: bool = env_bool('AFTERHOURS_ENABLED','false')
    spxw_gth_enabled: bool = env_bool('SPXW_GTH_ENABLED','true')
    market_only: bool = env_bool('MARKET_ONLY','true')
    run_scanner: bool = env_bool('RUN_SCANNER','true')
    notify_premarket: bool = env_bool('NOTIFY_PREMARKET','true')
    notify_regular: bool = env_bool('NOTIFY_REGULAR','true')
    top_n: int = int(os.getenv('TOP_N','5'))
    min_volume_ratio: float = float(os.getenv('MIN_VOLUME_RATIO','1.10'))

cfg = Config()
