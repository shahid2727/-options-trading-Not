import os, math, statistics
from datetime import datetime, date
import yfinance as yf

SYMBOLS=[s.strip().upper() for s in os.getenv('SYMBOLS','SPY,QQQ,NDX,IWM,NVDA,AMD,TSLA,AAPL,AMZN,META,MSFT,GOOGL,MU,AVGO,PLTR,SMCI,SPXW').split(',') if s.strip()]
RISK=float(os.getenv('RISK_BUDGET','100')); MIN_PREMIUM=float(os.getenv('MIN_PREMIUM','0.30')); MAX_PREMIUM=float(os.getenv('MAX_PREMIUM','2.00'))
MIN_SCORE=float(os.getenv('MIN_SCORE','65')); INTRADAY_MIN_SCORE=float(os.getenv('INTRADAY_MIN_SCORE','70'))
INTRADAY_ENABLED=os.getenv('INTRADAY_ENABLED','true').lower()=='true'; DIAGNOSTIC_MODE=os.getenv('DIAGNOSTIC_MODE','false').lower()=='true'
MIN_OI=int(os.getenv('MIN_OI','100')); MIN_VOLUME=int(os.getenv('MIN_VOLUME','50')); MAX_SPREAD=float(os.getenv('MAX_SPREAD_PCT','15')); MAX_DTE=int(os.getenv('MAX_DTE','30')); MIN_DTE=int(os.getenv('MIN_DTE','0'))


def num(v, default=0.0):
    try:
        x=float(v); return x if math.isfinite(x) else default
    except Exception: return default

def pct(a,b): return (a/b-1)*100 if b else 0.0

def estimate_delta(spot,strike,dte,side,premium):
    m=(spot-strike)/spot*100 if spot else 0
    base=max(0.10,min(0.55,0.25+max(-0.12,min(0.12,m/100*1.5))))
    return base if side=='C' else -base

def scan_one(symbol,phase,diag=None):
    ticker_symbol='^SPX' if symbol=='SPXW' else ('^NDX' if symbol=='NDX' else symbol)
    if diag is not None: diag['symbols_seen']+=1; diag['by_symbol'].setdefault(symbol,{'chain_expirations':0,'raw_contracts':0,'premium_pass':0,'liquidity_pass':0,'scored':0})
    t=yf.Ticker(ticker_symbol)
    try: spot=num(t.fast_info.get('last_price'))
    except Exception: spot=0
    try:
        hist=t.history(period='3mo',interval='1d',prepost=True)
    except Exception:
        if diag is not None: diag['history_errors']+=1
        return []
    # Outside regular hours fast_info may not expose last_price. Fall back to
    # the latest daily close so diagnostics and option-chain discovery can
    # still run when the market is CLOSED.
    except Exception as e:
        if diag is not None: diag['history_errors']+=1
        return []
    if hist.empty or 'Close' not in hist: return []
    close=hist['Close'].dropna(); volume=hist['Volume'].dropna() if 'Volume' in hist else None
    if len(close)<25: return []
    ret5=pct(float(close.iloc[-1]),float(close.iloc[-6])); ret20=pct(float(close.iloc[-1]),float(close.iloc[-21]))
    avg_vol=float(volume.tail(20).mean()) if volume is not None and len(volume)>=5 else 0; latest_vol=float(volume.iloc[-1]) if volume is not None and len(volume) else 0
    vol_ratio=latest_vol/avg_vol if avg_vol>0 else 1
    hi=float(close.tail(20).max()); lo=float(close.tail(20).min()); breakout=spot>=hi*0.995 or spot<=lo*1.005
    intraday=False; intraday_score=0; intraday_reasons=[]
    if INTRADAY_ENABLED and (phase in ('PRE_MARKET','REGULAR','AFTER_HOURS') or DIAGNOSTIC_MODE):
        try:
            ih=t.history(period='1d',interval='5m',prepost=True)
            if not ih.empty and len(ih)>=8:
                ic=ih['Close'].dropna(); iv=ih['Volume'].dropna() if 'Volume' in ih else None
                last=float(ic.iloc[-1]); r5=pct(last,float(ic.iloc[-2])); r30=pct(last,float(ic.iloc[-7])); surge=(float(iv.iloc[-1])/float(iv.iloc[-7:-1].mean())) if iv is not None and len(iv)>=7 and float(iv.iloc[-7:-1].mean())>0 else 1
                if abs(r5)>=0.35: intraday_score+=10; intraday_reasons.append('5m momentum')
                if abs(r30)>=0.75: intraday_score+=10; intraday_reasons.append('30m momentum')
                if surge>=2: intraday_score+=12; intraday_reasons.append('intraday volume surge')
                day_hi=float(ic.tail(12).max()); day_lo=float(ic.tail(12).min())
                if last>=day_hi*0.998 or last<=day_lo*1.002: intraday_score+=12; intraday_reasons.append('intraday breakout')
                intraday=intraday_score>0
        except Exception:
            pass
    try: exps=t.options
    except Exception:
        if diag is not None: diag['option_chain_errors']+=1
        return []
    out=[]; today=date.today()
    if not exps and diag is not None: diag['option_chain_empty']+=1
    for exp in exps[:20]:
        try:
            d=(datetime.strptime(exp,'%Y-%m-%d').date()-today).days
            if d<MIN_DTE or d>MAX_DTE: continue
            chain=t.option_chain(exp)
            if diag is not None: diag['by_symbol'][symbol]['chain_expirations']+=1
            for side,df in [('C',chain.calls),('P',chain.puts)]:
                if df is None or df.empty: continue
                for _,r in df.iterrows():
                    if diag is not None: diag['by_symbol'][symbol]['raw_contracts']+=1
                    bid=num(r.get('bid')); ask=num(r.get('ask')); last=num(r.get('lastPrice')); premium=(bid+ask)/2 if bid>0 and ask>0 else last
                    if premium<MIN_PREMIUM or premium>MAX_PREMIUM: continue
                    if diag is not None: diag['by_symbol'][symbol]['premium_pass']+=1
                    oi=int(num(r.get('openInterest'))); vol=int(num(r.get('volume'))); spread=(ask-bid)/premium*100 if premium>0 and ask>=bid else 999
                    if oi<MIN_OI or vol<MIN_VOLUME or spread>MAX_SPREAD: continue
                    if diag is not None: diag['by_symbol'][symbol]['liquidity_pass']+=1
                    strike=num(r.get('strike')); delta=num(r.get('delta'),float('nan'))
                    if not math.isfinite(delta) or abs(delta)<0.01: delta=estimate_delta(spot,strike,d,side,premium)
                    s=35.; reasons=[]
                    if vol_ratio>=2: s+=15; reasons.append('volume surge')
                    elif vol_ratio>=1.3: s+=9; reasons.append('above-average volume')
                    if abs(ret5)>=3: s+=12; reasons.append('strong 5d momentum')
                    elif abs(ret5)>=1: s+=7; reasons.append('5d momentum')
                    if abs(ret20)>=6: s+=10; reasons.append('strong 20d momentum')
                    elif abs(ret20)>=2: s+=5; reasons.append('20d momentum')
                    if breakout: s+=12; reasons.append('breakout proximity')
                    if oi>=1000: s+=6; reasons.append('high OI')
                    elif oi>=300: s+=3; reasons.append('solid OI')
                    if vol>=1000: s+=6; reasons.append('high option volume')
                    elif vol>=250: s+=3; reasons.append('solid option volume')
                    if spread<=5: s+=6; reasons.append('tight spread')
                    elif spread<=10: s+=3; reasons.append('acceptable spread')
                    if 0.18<=abs(delta)<=0.50: s+=4; reasons.append('usable delta')
                    if d<=7: s+=3; reasons.append('near-term catalyst window')
                    if premium<=1: s+=2; reasons.append('low premium')
                    if symbol in ('SPXW','NDX'): s+=3; reasons.append(symbol+' index')
                    bullish=ret5>=0 or ret20>=0
                    if (side=='C' and bullish) or (side=='P' and not bullish): s+=5; reasons.append('directional alignment')
                    else: s-=4
                    if intraday: s=min(100,s+intraday_score); reasons+=intraday_reasons
                    s=min(100,max(0,s)); threshold=INTRADAY_MIN_SCORE if intraday else MIN_SCORE
                    if s<threshold and not DIAGNOSTIC_MODE: continue
                    if diag is not None: diag['by_symbol'][symbol]['scored']+=1
                    entry_low=round(premium*.97,2); entry_high=round(premium*1.03,2); risk=max(entry_high*.25,premium*.15,0.10); stop=max(0.05,entry_low-risk)
                    out.append({'market':'SPXW' if symbol=='SPXW' else ('NDX' if symbol=='NDX' else 'STOCK'),'symbol':symbol,'contract':f'{int(strike)}{side}','expiration':exp,'dte':d,'premium':round(premium,2),'entry_low':entry_low,'entry_high':entry_high,'stop_loss':round(stop,2),'tp1':round(entry_high+risk,2),'tp2':round(entry_high+2*risk,2),'tp3':round(entry_high+3*risk,2),'risk_dollars_per_contract':round(risk*100,2),'suggested_contracts':max(0,int(R//(risk*100))),'score':round(s,1),'volume':vol,'open_interest':oi,'spread_pct':round(spread,1),'ret5':round(ret5,2),'ret20':round(ret20,2),'volume_ratio':round(vol_ratio,2),'breakout':breakout,'intraday':intraday,'intraday_score':intraday_score,'delta':round(delta,3),'reasons':reasons})
        except Exception: continue
    return out

def scan_all(phase, diagnostics=False):
    results=[]; diag={'symbols_seen':0,'symbols_no_price':0,'history_errors':0,'option_chain_errors':0,'option_chain_empty':0,'by_symbol':{}}
    for s in SYMBOLS:
        try: results.extend(scan_one(s,phase,diag if diagnostics else None))
        except Exception: continue
    results.sort(key=lambda x:(x['score'],x['intraday'],x['volume_ratio'],x['volume'],x['open_interest']),reverse=True)
    seen=set(); clean=[]
    for x in results:
        k=(x['symbol'],x['contract'],x['expiration'])
        if k not in seen: seen.add(k); clean.append(x)
    return (clean,diag) if diagnostics else clean
