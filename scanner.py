from datetime import datetime, timezone
import math
import pandas as pd
import yfinance as yf
from config import cfg
from risk import build_trade_plan, position_size

def num(x, default=0.0):
    try:
        if x is None or pd.isna(x):
            return default
        value = float(x)
        return default if not math.isfinite(value) else value
    except Exception:
        return default

def score_row(row):
    score=0.0; reasons=[]
    vol=int(num(row.get('volume'))); oi=int(num(row.get('openInterest')))
    bid=num(row.get('bid')); ask=num(row.get('ask')); last=num(row.get('lastPrice'))
    premium=last or ((bid+ask)/2 if bid and ask else 0)
    spread=((ask-bid)/premium*100) if premium>0 and ask>=bid else 100
    if vol>=cfg.min_volume: score+=1.5; reasons.append('volume')
    if oi>=cfg.min_oi: score+=1.0; reasons.append('OI')
    if oi and vol/oi>=1: score+=1.5; reasons.append('volume/OI')
    if spread<=cfg.max_spread_pct: score+=1.5; reasons.append('liquidity')
    if cfg.min_premium<=premium<=cfg.max_premium: score+=1; reasons.append('premium range')
    delta=abs(num(row.get('delta')))
    if delta>=0.20: score+=1.0; reasons.append('usable delta')
    iv=num(row.get('impliedVolatility'))
    if iv>0: score+=0.5; reasons.append('IV available')
    return min(score,10),premium,spread,reasons

def market_levels(t):
    try:
        hist=t.history(period='60d', interval='1d', auto_adjust=False)
        if hist.empty: return 0, None, None
        h=hist['High'].astype(float); l=hist['Low'].astype(float); c=hist['Close'].astype(float)
        prev_c=c.shift(1)
        tr=(h-l).combine((h-prev_c).abs(), max).combine((l-prev_c).abs(), max)
        atr=num(tr.rolling(14).mean().iloc[-1], 0) if len(tr)>=14 else num(tr.mean(), 0)
        support=num(l.tail(20).min(), None); resistance=num(h.tail(20).max(), None)
        return atr, support, resistance
    except Exception:
        return 0, None, None

def scan_symbols(symbols):
    out=[]
    for symbol in symbols:
        try:
            t=yf.Ticker(symbol); price=num(getattr(t.fast_info,'last_price',0))
            if price <= 0:
                continue
            atr,support,resistance=market_levels(t)
            for exp in t.options:
                try:
                    exp_dt=datetime.fromisoformat(str(exp)).replace(tzinfo=timezone.utc)
                    dte=(exp_dt-datetime.now(timezone.utc)).days
                except Exception:
                    continue
                if not cfg.min_dte<=dte<=cfg.max_dte: continue
                try:
                    chain=t.option_chain(exp)
                except Exception:
                    continue
                for side,df in [('C',chain.calls),('P',chain.puts)]:
                    for _,row in df.iterrows():
                        try:
                            r=row.to_dict()
                            strike=num(r.get('strike'), None)
                            if strike is None or strike <= 0:
                                continue
                            score,premium,spread,reasons=score_row(r)
                            vol=int(num(r.get('volume'))); oi=int(num(r.get('openInterest')))
                            if not (premium>0 and vol>=cfg.min_volume and oi>=cfg.min_oi and cfg.min_premium<=premium<=cfg.max_premium and spread<=cfg.max_spread_pct and score>=cfg.min_score): continue
                            delta=num(r.get('delta'))
                            plan=build_trade_plan(premium,price,side,delta,atr,support,resistance)
                            out.append({
                                'symbol':symbol,'contract':f"{strike:g}{side}",'expiration':exp,'dte':dte,
                                'underlying_price':price,'premium':premium,'volume':vol,'open_interest':oi,'spread_pct':spread,
                                'delta':delta,'iv':num(r.get('impliedVolatility')),'atr':atr,'support':support,'resistance':resistance,
                                'score':score,'entry_low':plan.entry_low,'entry_high':plan.entry_high,'stop_loss':plan.stop_loss,
                                'tp1':plan.tp1,'tp2':plan.tp2,'tp3':plan.tp3,'stop_underlying':plan.stop_underlying,
                                'tp1_underlying':plan.tp1_underlying,'tp2_underlying':plan.tp2_underlying,'tp3_underlying':plan.tp3_underlying,
                                'risk_per_contract':plan.risk_per_contract,'risk_dollars_per_contract':round(plan.risk_per_contract*100,2),'reward_tp1':plan.reward_tp1,'reward_tp2':plan.reward_tp2,'reward_tp3':plan.reward_tp3,
                                'profit_pct_tp1':round(plan.reward_tp1/plan.entry_high*100,1),'profit_pct_tp2':round(plan.reward_tp2/plan.entry_high*100,1),'profit_pct_tp3':round(plan.reward_tp3/plan.entry_high*100,1),
                                'rr_tp1':plan.rr_tp1,'rr_tp2':plan.rr_tp2,'rr_tp3':plan.rr_tp3,
                                'risk_budget':cfg.risk_budget,'suggested_contracts':position_size(cfg.risk_budget,plan.risk_per_contract),
                                'max_loss_position':round(position_size(cfg.risk_budget,plan.risk_per_contract)*plan.risk_per_contract*100,2),
                                'tp1_profit_position':round(position_size(cfg.risk_budget,plan.risk_per_contract)*plan.reward_tp1*100,2),
                                'tp2_profit_position':round(position_size(cfg.risk_budget,plan.risk_per_contract)*plan.reward_tp2*100,2),
                                'tp3_profit_position':round(position_size(cfg.risk_budget,plan.risk_per_contract)*plan.reward_tp3*100,2),
                                'plan_method':plan.method,
                                'exit_rules':['TP1 partial + stop toward breakeven','TP2 partial + trail','TP3 close remainder','Stop-loss = full exit','Review/close before expiry'],
                                'reasons':reasons
                            })
                        except Exception:
                            continue
        except Exception:
            continue
    return sorted(out,key=lambda x:x['score'],reverse=True)
