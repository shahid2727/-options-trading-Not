from datetime import datetime, timezone
import math
from statistics import NormalDist
import pandas as pd
import yfinance as yf
from config import cfg
from risk import build_trade_plan, position_size

def num(x, default=0.0):
    try:
        if x is None or pd.isna(x): return default
        v=float(x); return default if not math.isfinite(v) else v
    except Exception: return default

def latest_underlying_price(t):
    for kwargs in ({'period':'2d','interval':'5m','prepost':True,'auto_adjust':False},{'period':'5d','interval':'1d','auto_adjust':False}):
        try:
            h=t.history(**kwargs)
            if not h.empty:
                c=pd.to_numeric(h['Close'],errors='coerce').dropna()
                if not c.empty and num(c.iloc[-1])>0: return num(c.iloc[-1])
        except Exception: pass
    try: return num(t.fast_info.last_price)
    except Exception: return 0.0

def market_snapshot(t):
    try:
        hist=t.history(period='90d',interval='1d',auto_adjust=False)
        if hist.empty: return (0,None,None,0,0,0,0,0)
        h=pd.to_numeric(hist['High'],errors='coerce'); l=pd.to_numeric(hist['Low'],errors='coerce'); c=pd.to_numeric(hist['Close'],errors='coerce'); v=pd.to_numeric(hist.get('Volume',pd.Series(index=hist.index)),errors='coerce')
        pc=c.shift(1); tr=pd.concat([(h-l),(h-pc).abs(),(l-pc).abs()],axis=1).max(axis=1)
        atr=num(tr.rolling(14).mean().iloc[-1]); support=num(l.tail(20).min(),None); resistance=num(h.tail(20).max(),None)
        ret5=num((c.iloc[-1]/c.iloc[-6]-1)*100) if len(c)>=6 else 0; ret20=num((c.iloc[-1]/c.iloc[-21]-1)*100) if len(c)>=21 else 0
        av=num(v.tail(20).mean()); vr=num(v.iloc[-1]/av) if av>0 else 0
        ph=num(h.iloc[-21:-1].max()) if len(h)>=22 else 0; pl=num(l.iloc[-21:-1].min()) if len(l)>=22 else 0; last=num(c.iloc[-1])
        return atr,support,resistance,ret5,ret20,vr,int(ph and last>=ph*.995),int(pl and last<=pl*1.005)
    except Exception: return (0,None,None,0,0,0,0,0)

def bs_delta(spot,strike,dte,iv,side):
    s,k,vol=num(spot),num(strike),num(iv); t=max(float(dte),1)/365
    if s<=0 or k<=0 or vol<=0: return 0.25
    try:
        d1=(math.log(s/k)+(0.04+0.5*vol*vol)*t)/(vol*math.sqrt(t)); nd=NormalDist().cdf(d1)
        return max(.05,min(.95,nd if side=='C' else 1-nd))
    except Exception: return .25

def option_score(row,side,m,dte):
    vol=int(num(row.get('volume'))); oi=int(num(row.get('openInterest'))); bid=num(row.get('bid')); ask=num(row.get('ask')); last=num(row.get('lastPrice'))
    premium=((bid+ask)/2 if bid>0 and ask>0 else last); spread=((ask-bid)/premium*100) if premium>0 and ask>=bid else 100
    delta=abs(num(row.get('delta'))); score=0; reasons=[]
    if vol>=cfg.min_volume: score+=15; reasons.append('volume')
    if oi>=cfg.min_oi: score+=10; reasons.append('OI')
    if oi and vol/oi>=1: score+=12; reasons.append('volume/OI')
    elif oi and vol/oi>=.5: score+=6
    if spread<=cfg.max_spread_pct: score+=15; reasons.append('tight spread')
    elif spread<=cfg.max_spread_pct*1.25: score+=5
    if cfg.min_premium<=premium<=cfg.max_premium: score+=10; reasons.append('premium')
    if .20<=delta<=.85: score+=8; reasons.append('delta')
    dr5=m['ret5'] if side=='C' else -m['ret5']; dr20=m['ret20'] if side=='C' else -m['ret20']
    if dr5>=3: score+=8; reasons.append('momentum')
    elif dr5>=1: score+=4
    if dr20>=5: score+=5; reasons.append('trend')
    elif dr20>=2: score+=2
    if m['vol_ratio']>=1.5: score+=6; reasons.append('volume surge')
    elif m['vol_ratio']>=1.2: score+=3
    if (side=='C' and m['breakout_up']) or (side=='P' and m['breakout_down']): score+=8; reasons.append('breakout')
    if dte<=3: reasons.append('short DTE')
    return min(score,100),premium,spread,reasons

def is_spxw_contract(contract_symbol):
    s=str(contract_symbol or '').upper()
    return s.startswith('SPXW') or 'SPXW' in s

def scan_one(symbol, product='STOCK'):
    out=[]
    t=yf.Ticker(symbol); price=latest_underlying_price(t)
    if price<=0: return out
    atr,support,resistance,ret5,ret20,vr,bu,bd=market_snapshot(t); m={'ret5':ret5,'ret20':ret20,'vol_ratio':vr,'breakout_up':bu,'breakout_down':bd}
    try: expirations=t.options
    except Exception: expirations=[]
    now=datetime.now(timezone.utc)
    for exp in expirations:
        try: dte=max(0,(datetime.fromisoformat(str(exp)).replace(tzinfo=timezone.utc)-now).days)
        except Exception: continue
        if not cfg.min_dte<=dte<=cfg.max_dte: continue
        try: chain=t.option_chain(exp)
        except Exception: continue
        for side,df in [('C',chain.calls),('P',chain.puts)]:
            for _,row in df.iterrows():
                try:
                    r=row.to_dict(); cs=r.get('contractSymbol','')
                    if product=='SPXW' and not is_spxw_contract(cs): continue
                    strike=num(r.get('strike'),None)
                    if not strike: continue
                    iv=num(r.get('impliedVolatility')); raw_delta=num(r.get('delta')); delta=raw_delta if .05<=abs(raw_delta)<=.99 else bs_delta(price,strike,dte,iv,side)
                    score,premium,spread,reasons=option_score({**r,'delta':delta},side,m,dte)
                    vol=int(num(r.get('volume'))); oi=int(num(r.get('openInterest')))
                    if not (premium>0 and vol>=cfg.min_volume and oi>=cfg.min_oi and cfg.min_premium<=premium<=cfg.max_premium and spread<=cfg.max_spread_pct and score>=cfg.min_score*10): continue
                    if vr and vr<cfg.min_volume_ratio and score<85: continue
                    plan=build_trade_plan(premium,price,side,delta,atr,support,resistance,product=product)
                    mult=100; size=position_size(cfg.risk_budget,plan.risk_per_contract,mult)
                    out.append({'symbol':'SPXW' if product=='SPXW' else symbol,'underlying_symbol':symbol,'product':product,'contract':f'{strike:g}{side}','contract_symbol':cs,'expiration':exp,'dte':dte,'underlying_price':price,'premium':premium,'volume':vol,'open_interest':oi,'spread_pct':spread,'delta':delta,'delta_source':'provider' if .05<=abs(raw_delta)<=.99 else 'BS-approx','iv':iv,'atr':atr,'support':support,'resistance':resistance,'ret5':ret5,'ret20':ret20,'volume_ratio':vr,'breakout':bool(bu if side=='C' else bd),'score':score,'entry_low':plan.entry_low,'entry_high':plan.entry_high,'stop_loss':plan.stop_loss,'tp1':plan.tp1,'tp2':plan.tp2,'tp3':plan.tp3,'stop_underlying':plan.stop_underlying,'tp1_underlying':plan.tp1_underlying,'tp2_underlying':plan.tp2_underlying,'tp3_underlying':plan.tp3_underlying,'risk_per_contract':plan.risk_per_contract,'risk_dollars_per_contract':round(plan.risk_per_contract*mult,2),'reward_tp1':plan.reward_tp1,'reward_tp2':plan.reward_tp2,'reward_tp3':plan.reward_tp3,'profit_pct_tp1':round(plan.reward_tp1/plan.entry_high*100,1),'profit_pct_tp2':round(plan.reward_tp2/plan.entry_high*100,1),'profit_pct_tp3':round(plan.reward_tp3/plan.entry_high*100,1),'rr_tp1':plan.rr_tp1,'rr_tp2':plan.rr_tp2,'rr_tp3':plan.rr_tp3,'risk_budget':cfg.risk_budget,'suggested_contracts':size,'max_loss_position':round(size*plan.risk_per_contract*mult,2),'tp1_profit_position':round(size*plan.reward_tp1*mult,2),'tp2_profit_position':round(size*plan.reward_tp2*mult,2),'tp3_profit_position':round(size*plan.reward_tp3*mult,2),'plan_method':plan.method,'reasons':reasons})
                except Exception: continue
    return out

def scan_symbols(symbols):
    out=[]
    for symbol in symbols:
        try: out.extend(scan_one(symbol,'STOCK'))
        except Exception: pass
    if cfg.scan_spxw:
        try: out.extend(scan_one(cfg.spxw_underlying,'SPXW'))
        except Exception: pass
    return sorted(out,key=lambda x:(x['score'],x['volume_ratio'],x['open_interest']),reverse=True)
