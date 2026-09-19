import os
from dataclasses import dataclass
@dataclass
class Config:
    min_premium: float=float(os.getenv('MIN_PREMIUM','0.50'))
    max_premium: float=float(os.getenv('MAX_PREMIUM','1.50'))
    min_volume: int=int(os.getenv('MIN_VOLUME','500'))
    min_oi: int=int(os.getenv('MIN_OI','200'))
    min_score: float=float(os.getenv('MIN_SCORE','7'))
    max_spread_pct: float=float(os.getenv('MAX_SPREAD_PCT','15'))
    min_dte: int=int(os.getenv('MIN_DTE','1'))
    max_dte: int=int(os.getenv('MAX_DTE','30'))
    webhook_secret: str=os.getenv('TRADINGVIEW_WEBHOOK_SECRET','')
cfg=Config()
