import os, math, statistics
import yfinance as yf
from datetime import datetime

SYMBOLS=[s.strip().upper() for s in os.getenv('SYMBOLS','SPY,QQQ,IWM,NVDA,AMD,TSLA,AAPL,AMZN,META,MSFT,GOOGL,MU,AVGO,PLTR,SMCI,SPXW').split(',') if s.strip()]
RISK=float(os.getenv('RISK_BUDGET','100')); MIN_PREMIUM=float(os.getenv('MIN_PREMIUM','0.30')); MAX_PREMIUM=float(os.getenv('MAX_PREMIUM','2.00'))

def num(v,default=0.0):
    try:
        if v is None or not math.isfinite(float(v)): return default
        return float(v)
    except Exception: return default

def scan_one(symbol,phase):
    # yfinance does not consistently expose a distinct SPXW symbol. Use ^SPX as the underlying
    # when SPXW is requested, and label qualifying index-option results as SPXW.
    ticker_symbol='^SPX' if symbol=='SPXW' else symbol
    t=yf.Ticker(ticker_symbol)
    try: price=num(t.fast_info.get('last_price'),0)
    except Exception: price=0
    if not price: return []
    try: hist=t.history(period='3mo',interval='1d',prepost=True)
    except Exception: return []
    if hist.empty: return []
    close=hist['Close'].dropna()
    ret5=(close.iloc[-1]/close.iloc[-6]-1)*100 if len(close)>6 else 0
    ret20=(close.iloc[-1]/close.iloc[-21]-1)*100 if len(close)>21 else 0
    vols=hist['Volume'].dropna().tail(20)
    vol_ratio=num(hist['Volume'].iloc[-1]/statistics.mean(vols),1) if len(vols)>0 and statistics.mean(vols)>0 else 1
    hi=float(close.tail(20).max()); lo=float(close.tail(20).min()); breakout=price>=hi*0.995 or price<=lo*1.005
    out=[]
    try: exps=t.options
    except Exception: return []
    for exp in exps[:12]:
        try:
            d=(datetime.strptime(exp,'%Y-%m-%d').date()-datetime.now().date()).days
            if d<0 or d>30: continue
            chain=t.option_chain(exp)
            for side,df in [('C',chain.calls),('P',chain.puts)]:
                if df is None or df.empty: continue
                for _,r in df.iterrows():
                    bid=num(r.get('bid')); ask=num(r.get('ask')); last=num(r.get('lastPrice'))
                    premium=(bid+ask)/2 if bid>0 and ask>0 else last
                    if premium<MIN_PREMIUM or premium>MAX_PREMIUM: continue
                    oi=int(num(r.get('openInterest'))); vol=int(num(r.get('volume')))
                    spread=(ask-bid)/premium*100 if premium and ask>=bid else 999
                    max_spread=float(os.getenv('MAX_SPREAD_PCT','15'))
                    if oi<int(os.getenv('MIN_OI','100')) or vol<int(os.getenv('MIN_VOLUME','50')) or spread>max_spread: continue
                    delta=num(r.get('delta'),float('nan'))
                    if not math.isfinite(delta) or abs(delta)<0.01: delta=0.25 if side=='C' else -0.25
                    score=45 + min(15,max(0,vol_ratio-1)*10) + min(12,oi/400) + min(12,abs(ret5)*1.2) + min(8,abs(ret20)*0.4) + (8 if breakout else 0)
                    if spread<8: score+=5
                    if symbol=='SPXW': score+=3
                    score=min(100,score)
                    entry_low=round(premium*.97,2); entry_high=round(premium*1.03,2)
                    risk=max(entry_high*.25, premium*.15, 0.10)
                    stop=max(0.05,entry_low-risk)
                    tp1=entry_high+risk; tp2=entry_high+2*risk; tp3=entry_high+3*risk
                    contracts=max(0,int(R//(risk*100)))
                    out.append({'market':'SPXW' if symbol=='SPXW' else 'STOCK','symbol':symbol,'contract':str(int(r.get('strike',0)))+side,'expiration':exp,'dte':d,'premium':round(premium,2),'entry_low':entry_low,'entry_high':entry_high,'stop_loss':round(stop,2),'tp1':round(tp1,2),'tp2':round(tp2,2),'tp3':round(tp3,2),'risk_per_contract':round(risk,2),'risk_dollars_per_contract':round(risk*100,2),'suggested_contracts':contracts,'risk_budget':R,'score':round(score,1),'volume':vol,'open_interest':oi,'spread_pct':round(spread,1),'ret5':round(ret5,2),'ret20':round(ret20,2),'volume_ratio':round(vol_ratio,2),'breakout':breakout,'delta':round(delta,3),'reasons':['volume','OI','liquidity','momentum']+(['breakout'] if breakout else [])+(['SPXW index'] if symbol=='SPXW' else [])})
        except Exception: continue
    return out

def scan_all(phase):
    results=[]
    for s in SYMBOLS:
        try: results.extend(scan_one(s,phase))
        except Exception: continue
    results.sort(key=lambda x:(x['score'],x['volume'],x['open_interest']),reverse=True)
    return results
