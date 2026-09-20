import os, math, statistics
from datetime import datetime, timezone, timedelta
import requests

ALPACA = 'https://data.alpaca.markets/v2'
OPTIONS = 'https://data.alpaca.markets/v1beta1/options'
STOCKS = [s.strip().upper() for s in os.getenv('STOCK_SYMBOLS','QQQ,NVDA,AMD,TSLA,AAPL,AMZN,META,MSFT,GOOGL,MU,AVGO,PLTR,SMCI,SPY,IWM').split(',') if s.strip()]
INDEX_ROOTS = [s.strip().upper() for s in os.getenv('INDEX_ROOTS','SPXW').split(',') if s.strip()]
RISK = float(os.getenv('RISK_BUDGET','100'))
MIN_PREMIUM = float(os.getenv('MIN_PREMIUM','0.30')); MAX_PREMIUM = float(os.getenv('MAX_PREMIUM','10.00'))
UPSIDE_ALERT_PCT = float(os.getenv('UPSIDE_ALERT_PCT','1000'))
MAX_AFFORDABLE_PREMIUM = float(os.getenv('MAX_AFFORDABLE_PREMIUM','10.00'))
MAX_SPREAD = float(os.getenv('MAX_SPREAD_PCT','15')); MIN_OI = int(os.getenv('MIN_OI','50')); MIN_VOL = int(os.getenv('MIN_VOLUME','20'))
MIN_SCORE = max(70.0, float(os.getenv('MIN_SCORE','70')))
ALERT_COOLDOWN = int(os.getenv('ALERT_COOLDOWN_SECONDS','900'))


def headers():
    k=os.getenv('ALPACA_API_KEY'); s=os.getenv('ALPACA_API_SECRET')
    return {'APCA-API-KEY-ID':k,'APCA-API-SECRET-KEY':s} if k and s else {}

def req(url, params=None, timeout=15):
    last=None
    for attempt in range(3):
        try:
            r=requests.get(url, headers=headers(), params=params or {}, timeout=timeout)
            if r.status_code == 429 and attempt < 2:
                import time; time.sleep(1.5*(attempt+1)); continue
            r.raise_for_status(); return r.json()
        except Exception as e:
            last=e
            if attempt < 2:
                import time; time.sleep(0.8*(attempt+1))
    raise last

def num(v,d=0.0):
    try:
        x=float(v); return x if math.isfinite(x) else d
    except Exception: return d

def ema(values, n):
    if not values: return 0.0
    a=2/(n+1); e=values[0]
    for v in values[1:]: e=a*v+(1-a)*e
    return e

def rsi(values,n=14):
    if len(values)<n+1:return 50.0
    gains=[]; losses=[]
    for a,b in zip(values[-n-1:-1], values[-n:]):
        d=b-a; gains.append(max(d,0)); losses.append(max(-d,0))
    ag=sum(gains)/n; al=sum(losses)/n
    if al==0:return 100.0
    return 100-(100/(1+ag/al))

def atr_bars(bars,n=14):
    if len(bars)<n+1:return 0.0
    trs=[]
    prev=num(bars[-n-1].get('c'))
    for b in bars[-n:]:
        h=num(b.get('h')); l=num(b.get('l')); c=num(b.get('c'))
        trs.append(max(h-l, abs(h-prev), abs(l-prev))); prev=c
    return statistics.mean(trs) if trs else 0.0

def adx(bars,n=14):
    if len(bars)<n*2+1:return 0.0
    highs=[num(b.get('h')) for b in bars]; lows=[num(b.get('l')) for b in bars]; closes=[num(b.get('c')) for b in bars]
    trs=[]; plus=[]; minus=[]
    for i in range(1,len(bars)):
        up=highs[i]-highs[i-1]; down=lows[i-1]-lows[i]
        trs.append(max(highs[i]-lows[i],abs(highs[i]-closes[i-1]),abs(lows[i]-closes[i-1])))
        plus.append(up if up>down and up>0 else 0); minus.append(down if down>up and down>0 else 0)
    vals=[]
    for i in range(n,len(trs)+1):
        tr=sum(trs[i-n:i]); p=sum(plus[i-n:i]); m=sum(minus[i-n:i])
        if tr<=0: continue
        pdi=100*p/tr; mdi=100*m/tr; vals.append(100*abs(pdi-mdi)/max(pdi+mdi,1e-9))
    return statistics.mean(vals[-n:]) if vals else 0.0

def fetch_bars(symbol,timeframe,days=45,limit=1000):
    end_dt=datetime.now(timezone.utc); start_dt=end_dt-timedelta(days=days)
    data=req(f'{ALPACA}/stocks/{symbol}/bars', {'timeframe':timeframe,'start':start_dt.isoformat().replace('+00:00','Z'),'end':end_dt.isoformat().replace('+00:00','Z'),'limit':limit,'feed':'iex','sort':'asc'})
    return data.get('bars') or []

def fetch_bars_batch(symbols,timeframe,days=45,limit=10000):
    end_dt=datetime.now(timezone.utc); start_dt=end_dt-timedelta(days=days)
    data=req(f'{ALPACA}/stocks/bars', {'symbols':','.join(symbols),'timeframe':timeframe,'start':start_dt.isoformat().replace('+00:00','Z'),'end':end_dt.isoformat().replace('+00:00','Z'),'limit':limit,'feed':'iex','sort':'asc'})
    return data.get('bars') or {}

def frame_indicators(symbol,timeframe,days=45,cache=None):
    bars=(cache or {}).get(symbol) if cache is not None else None
    if bars is None: bars=fetch_bars(symbol,timeframe,days)
    closes=[num(b.get('c')) for b in bars if num(b.get('c'))>0]
    if len(closes)<55: raise RuntimeError(f'not enough {timeframe} bars for {symbol}: {len(closes)}')
    e20=ema(closes[-100:],20); e50=ema(closes[-100:],50); rr=rsi(closes,14); a=atr_bars(bars,14); ax=adx(bars,14)
    mf=ema(closes[-80:],12); ms=ema(closes[-80:],26); macd=mf-ms
    pmf=ema(closes[-81:-1],12); pms=ema(closes[-81:-1],26); pmacd=pmf-pms
    slope=closes[-1]-closes[-6]
    return {'last':closes[-1],'ema20':e20,'ema50':e50,'rsi':rr,'atr':a,'adx':ax,'macd':macd,'macd_delta':macd-pmacd,'slope':slope,'bars':bars}

def session_vwap(bars):
    # Use today's US date; bars may be UTC but timestamps identify the same session.
    from zoneinfo import ZoneInfo
    ny=ZoneInfo('America/New_York'); today=datetime.now(ny).date(); pv=vv=0.0
    for b in bars:
        try: dt=datetime.fromisoformat(str(b.get('t')).replace('Z','+00:00')).astimezone(ny)
        except Exception: continue
        if dt.date()!=today: continue
        h=num(b.get('h')); l=num(b.get('l')); c=num(b.get('c')); v=num(b.get('v'))
        tp=(h+l+c)/3 if h and l and c else c
        pv+=tp*v; vv+=v
    return pv/vv if vv else 0.0

def regime(f4,f1,f15,f5):
    # 4H ADX + EMA separation + 1H/15M agreement identify trend vs range/chop.
    sep=abs(f4['ema20']-f4['ema50'])/max(f4['last'],1e-9)*100
    aligned_up=f4['last']>f4['ema20']>f4['ema50'] and f1['last']>f1['ema20']>f1['ema50']
    aligned_dn=f4['last']<f4['ema20']<f4['ema50'] and f1['last']<f1['ema20']<f1['ema50']
    if f4['adx']<17 and sep<0.35: return 'SIDEWAYS'
    if f4['adx']<20 and not (aligned_up or aligned_dn): return 'CHOPPY'
    if aligned_up or aligned_dn: return 'TRENDING'
    return 'MIXED'

def multi_tf(symbol,caches=None):
    caches=caches or {}
    f5=frame_indicators(symbol,'5Min',7,caches.get('5m')); f15=frame_indicators(symbol,'15Min',20,caches.get('15m')); f1=frame_indicators(symbol,'1Hour',45,caches.get('1h')); f4=frame_indicators(symbol,'4Hour',180,caches.get('4h'))
    vwap=session_vwap(f5['bars']) or f5['last']
    closes=[num(b.get('c')) for b in f5['bars']]; vols=[num(b.get('v')) for b in f5['bars']]
    avgvol=statistics.mean(vols[-21:-1]) if len(vols)>21 else max(statistics.mean(vols[:-1]),1)
    vr=vols[-1]/avgvol if avgvol else 1
    recent_high=max(closes[-21:-1]); recent_low=min(closes[-21:-1]); last=closes[-1]
    breakout='UP' if last>recent_high else ('DOWN' if last<recent_low else 'NO')
    reg=regime(f4,f1,f15,f5)
    return {'5m':f5,'15m':f15,'1h':f1,'4h':f4,'vwap':vwap,'volume_ratio':vr,'breakout':breakout,'regime':reg,'recent_high':recent_high,'recent_low':recent_low}

def option_chain(underlying, side=None):
    params={'feed':os.getenv('ALPACA_OPTIONS_FEED','indicative'),'limit':1000}
    if side: params['type']=side.lower()
    return req(f'{OPTIONS}/snapshots/{underlying}',params)

def parse_contract(sym, details):
    exp=details.get('expiration_date') or details.get('expirationDate'); strike=num(details.get('strike_price') or details.get('strikePrice')); typ=(details.get('type') or '').upper()
    return exp,strike,typ

def score_setup(m, direction, spread, delta, vol, oi):
    f5,f15,f1,f4=m['5m'],m['15m'],m['1h'],m['4h']; score=0; reasons=[]
    up4=f4['last']>f4['ema20']>f4['ema50'] and f4['macd_delta']>=0 and f4['rsi']>=52
    dn4=f4['last']<f4['ema20']<f4['ema50'] and f4['macd_delta']<=0 and f4['rsi']<=48
    up1=f1['last']>f1['ema20']>f1['ema50']; dn1=f1['last']<f1['ema20']<f1['ema50']
    up15=f15['last']>f15['ema20']; dn15=f15['last']<f15['ema20']
    up5=f5['last']>m['vwap'] and f5['ema20']>f5['ema50'] and f5['macd_delta']>0 and f5['rsi']>=52
    dn5=f5['last']<m['vwap'] and f5['ema20']<f5['ema50'] and f5['macd_delta']<0 and f5['rsi']<=48
    aligned4=up4 if direction=='CALL' else dn4; aligned1=up1 if direction=='CALL' else dn1; aligned15=up15 if direction=='CALL' else dn15; aligned5=up5 if direction=='CALL' else dn5
    if aligned4: score+=20; reasons.append('4H trend aligned')
    if aligned1: score+=12; reasons.append('1H aligned')
    if aligned15: score+=8; reasons.append('15M aligned')
    if aligned5: score+=15; reasons.append('5M confirmation')
    if m['breakout']==('UP' if direction=='CALL' else 'DOWN'): score+=10; reasons.append('breakout')
    if m['volume_ratio']>=1.5: score+=10; reasons.append('volume surge')
    elif m['volume_ratio']>=1.15: score+=5
    if spread<8: score+=6; reasons.append('tight spread')
    elif spread<12: score+=3
    if vol>=100 and oi>=500: score+=5; reasons.append('strong option liquidity')
    elif vol>=MIN_VOL and oi>=MIN_OI: score+=2
    if abs(delta)>=0.30: score+=4
    if m['regime']=='SIDEWAYS': score-=25; reasons.append('sideways market risk')
    elif m['regime']=='CHOPPY': score-=15; reasons.append('choppy market risk')
    elif m['regime']=='TRENDING': score+=5; reasons.append('trend regime')
    return max(0,min(100,score)), reasons, aligned4, aligned1, aligned15, aligned5

def scan_underlying(symbol, session, contract_prefix=None, caches=None):
    m=multi_tf(symbol,caches); chain=option_chain(symbol); rows=chain.get('snapshots') or {}; out=[]; today=datetime.now(timezone.utc).date()
    for contract,snap in rows.items():
        if contract_prefix and not contract.startswith(contract_prefix): continue
        details=snap.get('details') or {}; exp,strike,typ=parse_contract(contract,details)
        if not exp or not strike or typ not in ('CALL','PUT'): continue
        try:dte=(datetime.fromisoformat(exp).date()-today).days
        except Exception:continue
        if dte<0 or dte>30:continue
        quote=snap.get('latestQuote') or {}; trade=snap.get('latestTrade') or {}; greeks=snap.get('greeks') or {}
        bid=num(quote.get('bp')); ask=num(quote.get('ap')); last=num(trade.get('p')); premium=(bid+ask)/2 if bid>0 and ask>0 else last
        if premium<MIN_PREMIUM or premium>MAX_PREMIUM or premium>MAX_AFFORDABLE_PREMIUM:continue
        spread=((ask-bid)/premium*100) if premium and ask>=bid else 999
        daily=snap.get('dailyBar') or snap.get('daily_bar') or {}; vol=int(num(daily.get('v'))); oi=int(num(details.get('open_interest') or details.get('openInterest')))
        if vol<=0:vol=int(num(trade.get('s'))) if trade else 0
        if spread>MAX_SPREAD or vol<MIN_VOL or oi<MIN_OI:continue
        delta=num(greeks.get('delta'),0.25 if typ=='CALL' else -0.25); direction=typ
        score,reasons,a4,a1,a15,a5=score_setup(m,direction,spread,delta,vol,oi)
        if score<MIN_SCORE or not a4 or m['regime'] in ('SIDEWAYS','CHOPPY'):continue
        risk=max(premium*.20,0.10); entry_low=round(premium*.97,2); entry_high=round(premium*1.03,2); stop=round(max(.05,entry_low-risk),2)
        tp1=round(entry_high+risk,2); tp2=round(entry_high+2*risk,2); tp3=round(entry_high+3*risk,2)
        contracts=max(0,int(RISK//(risk*100))); max_loss=round(risk*100*contracts,2)
        reward1=round(max(0,tp1-entry_high)*100*contracts,2); reward2=round(max(0,tp2-entry_high)*100*contracts,2); reward3=round(max(0,tp3-entry_high)*100*contracts,2)
        atr_points=max(m['5m']['atr'],m['5m']['last']*0.001)
        u_entry=round(m['5m']['last'],2)
        if direction=='CALL': u_stop=round(u_entry-atr_points,2); u_tp1=round(u_entry+atr_points,2); u_tp2=round(u_entry+2*atr_points,2); u_tp3=round(u_entry+3*atr_points,2)
        else: u_stop=round(u_entry+atr_points,2); u_tp1=round(u_entry-atr_points,2); u_tp2=round(u_entry-2*atr_points,2); u_tp3=round(u_entry-3*atr_points,2)
        confidence=round(min(94,max(50,50+score*.44)),0)
        tp1_conf=round(min(95,confidence+4)); tp2_conf=round(max(25,confidence-9)); tp3_conf=round(max(15,confidence-20))
        if direction=='CALL':
            projected_underlying=round(max(u_tp3, m['recent_high'] + 2*atr_points),2)
            underlying_move=max(0.0, projected_underlying-u_entry)
        else:
            projected_underlying=round(min(u_tp3, m['recent_low'] - 2*atr_points),2)
            underlying_move=max(0.0, u_entry-projected_underlying)
        projected_premium=max(0.05, premium + abs(delta)*underlying_move)
        projected_upside_pct=round(max(0.0,(projected_premium/premium-1)*100),1) if premium>0 else 0.0
        extreme_upside=projected_upside_pct>=UPSIDE_ALERT_PCT and premium<=MAX_AFFORDABLE_PREMIUM
        if extreme_upside: reasons.append(f'projected upside > {UPSIDE_ALERT_PCT:.0f}%')
        out.append({'signal':direction,'market':'OPTIONS','symbol':symbol,'contract':contract,'dte':dte,'premium':round(premium,2),'entry_low':entry_low,'entry_high':entry_high,'stop_loss':stop,'tp1':tp1,'tp2':tp2,'tp3':tp3,'risk_dollars_per_contract':round(risk*100,2),'suggested_contracts':contracts,'max_loss':max_loss,'expected_profit_tp1':reward1,'expected_profit_tp2':reward2,'expected_profit_tp3':reward3,'underlying_entry':u_entry,'underlying_stop_loss':u_stop,'underlying_tp1':u_tp1,'underlying_tp2':u_tp2,'underlying_tp3':u_tp3,'atr_5m_points':round(atr_points,2),'score':round(score,1),'confidence':confidence,'tp1_confidence':tp1_conf,'tp2_confidence':tp2_conf,'tp3_confidence':tp3_conf,'projected_underlying_target':projected_underlying,'projected_premium':round(projected_premium,2),'projected_upside_pct':projected_upside_pct,'extreme_upside':extreme_upside,'market_regime':m['regime'],'volume':vol,'open_interest':oi,'spread_pct':round(spread,1),'delta':round(delta,3),'underlying':u_entry,'vwap_state':'BULLISH' if u_entry>m['vwap'] else 'BEARISH','ema_state':'BULLISH' if m['5m']['ema20']>m['5m']['ema50'] else 'BEARISH','rsi':round(m['5m']['rsi'],1),'macd_state':'BULLISH' if m['5m']['macd_delta']>0 else 'BEARISH','volume_ratio':round(m['volume_ratio'],2),'breakout':m['breakout'],'trend_4h':'BULLISH' if m['4h']['last']>m['4h']['ema20']>m['4h']['ema50'] else 'BEARISH' if m['4h']['last']<m['4h']['ema20']<m['4h']['ema50'] else 'NEUTRAL','adx_4h':round(m['4h']['adx'],1),'rsi_4h':round(m['4h']['rsi'],1),'reasons':reasons,'session':session,'data_mode':os.getenv('ALPACA_OPTIONS_FEED','indicative').upper()})
    return out, {'chain_items':len(rows),'scored':len(out),'underlying':symbol,'indicator_source':'Alpaca IEX multi-timeframe 5m/15m/1h/4h','option_source':'Alpaca options '+os.getenv('ALPACA_OPTIONS_FEED','indicative'),'market_regime':m['regime'],'trend_4h':'BULLISH' if m['4h']['last']>m['4h']['ema20']>m['4h']['ema50'] else 'BEARISH' if m['4h']['last']<m['4h']['ema20']<m['4h']['ema50'] else 'NEUTRAL'}

def scan_all(session):
    if not headers():raise RuntimeError('Missing ALPACA_API_KEY / ALPACA_API_SECRET')
    results=[]; diagnostics={}; symbols=list(dict.fromkeys(STOCKS+(['SPX'] if 'SPXW' in INDEX_ROOTS else [])))
    periods={'5m':('5Min',7),'15m':('15Min',20),'1h':('1Hour',45),'4h':('4Hour',180)}; caches={}
    for key,(tf,days) in periods.items():
        try:caches[key]=fetch_bars_batch(symbols,tf,days)
        except Exception as e:
            # Keep the scan alive if one timeframe fails; affected symbols will report a diagnostic.
            caches[key]={}; diagnostics[f'__{key}']={'error':f'{type(e).__name__}: {e}'}
    for sym in STOCKS:
        try:
            r,d=scan_underlying(sym,session,caches=caches); results.extend(r); diagnostics[sym]=d
        except Exception as e:diagnostics[sym]={'error':f'{type(e).__name__}: {e}'}
    if 'SPXW' in INDEX_ROOTS:
        try:
            r,d=scan_underlying('SPX',session,contract_prefix='SPXW',caches=caches)
            for x in r:x['symbol']='SPXW'
            results.extend(r); diagnostics['SPXW']=d
        except Exception as e:diagnostics['SPXW']={'error':f'{type(e).__name__}: {e}'}
    results.sort(key=lambda x:(x['extreme_upside'],x['projected_upside_pct'],x['score'],x['confidence'],x['volume'],x['open_interest']),reverse=True)
    return results,diagnostics

def provider_status():
    feed=os.getenv('ALPACA_OPTIONS_FEED','indicative')
    return {'name':'Alpaca','configured':bool(os.getenv('ALPACA_API_KEY') and os.getenv('ALPACA_API_SECRET')),'options_feed':feed,'underlying_feed':'iex','note':'Free Alpaca options feed is indicative/delayed; IEX equity feed is real-time.' if feed=='indicative' else 'OPRA options feed requires the appropriate subscription.'}
