from dataclasses import dataclass
import math

@dataclass
class TradePlan:
    entry_low: float; entry_high: float; stop_loss: float; tp1: float; tp2: float; tp3: float
    risk_per_contract: float; reward_tp1: float; reward_tp2: float; reward_tp3: float
    rr_tp1: float; rr_tp2: float; rr_tp3: float
    stop_underlying: float; tp1_underlying: float; tp2_underlying: float; tp3_underlying: float
    method: str

def tick_for(p, multiplier=100):
    # SPX/SPXW: $0.05 tick below $3, $0.10 at/above $3.
    return 0.05 if multiplier == 100 and p < 3 else 0.10 if multiplier == 100 else 0.01

def floor_tick(x, tick):
    return max(tick, math.floor((x + 1e-9)/tick)*tick)

def ceil_tick(x, tick):
    return max(tick, math.ceil((x - 1e-9)/tick)*tick)

def build_trade_plan(premium, underlying_price, side, delta=0.0, atr=0.0, support=None, resistance=None, product='STOCK'):
    p=max(float(premium),0.01); u=max(float(underlying_price),0.01)
    d=min(max(abs(float(delta or 0.0)),0.10),0.90)
    atr=max(float(atr or 0.0),u*0.005)
    multiplier=100
    tick=tick_for(p,multiplier) if product=='SPXW' else 0.01
    entry_low=floor_tick(max(tick,p*0.97),tick); entry_high=ceil_tick(max(entry_low,p*1.03),tick)
    if side=='C':
        sr_stop=float(support) if support and support<u else u-atr
        stop_u=max(0.01,min(u-0.5*atr,sr_stop)); direction=1
    else:
        sr_stop=float(resistance) if resistance and resistance>u else u+atr
        stop_u=max(u+0.5*atr,sr_stop); direction=-1
    # Risk floor avoids meaningless $0.01 stops while keeping risk bounded.
    risk=max(entry_high*0.30,p*0.20)
    stop=floor_tick(max(tick,entry_high-risk),tick)
    risk=max(entry_high-stop,tick)
    tp1=ceil_tick(entry_high+risk,tick); tp2=ceil_tick(entry_high+2*risk,tick); tp3=ceil_tick(entry_high+3*risk,tick)
    def utarget(tp):
        move=(tp-p)/d
        return round(max(0.01,u+direction*move),2)
    return TradePlan(entry_low,entry_high,stop,tp1,tp2,tp3,risk,tp1-entry_high,tp2-entry_high,tp3-entry_high,
                     1,2,3,round(stop_u,2),utarget(tp1),utarget(tp2),utarget(tp3),
                     'ATR + S/R + delta (provider or Black-Scholes approximation)')

def position_size(risk_budget,risk_per_contract,multiplier=100):
    try:
        rb=float(risk_budget); rpc=float(risk_per_contract)*float(multiplier)
        return math.floor(rb/rpc) if rb>0 and rpc>0 else 0
    except Exception: return 0
