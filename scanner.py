import os, math, statistics
from datetime import datetime, timezone
import requests

ALPACA = 'https://data.alpaca.markets/v2'
OPTIONS = 'https://data.alpaca.markets/v1beta1/options'
STOCKS = [s.strip().upper() for s in os.getenv('STOCK_SYMBOLS','QQQ,NVDA,AMD,TSLA,AAPL,AMZN,META,MSFT,GOOGL,MU,AVGO,PLTR,SMCI,SPY,IWM').split(',') if s.strip()]
INDEX_ROOTS = [s.strip().upper() for s in os.getenv('INDEX_ROOTS','SPXW').split(',') if s.strip()]
RISK = float(os.getenv('RISK_BUDGET','100'))
MIN_PREMIUM = float(os.getenv('MIN_PREMIUM','0.30')); MAX_PREMIUM = float(os.getenv('MAX_PREMIUM','2.00'))
MAX_SPREAD = float(os.getenv('MAX_SPREAD_PCT','15')); MIN_OI = int(os.getenv('MIN_OI','50')); MIN_VOL = int(os.getenv('MIN_VOLUME','20'))
MIN_SCORE = float(os.getenv('MIN_SCORE','65'))


def headers():
    k=os.getenv('ALPACA_API_KEY'); s=os.getenv('ALPACA_API_SECRET')
    return {'APCA-API-KEY-ID':k,'APCA-API-SECRET-KEY':s} if k and s else {}

def req(url, params=None, timeout=12):
    r=requests.get(url, headers=headers(), params=params or {}, timeout=timeout)
    r.raise_for_status(); return r.json()

def num(v,d=0.0):
    try:
        x=float(v); return x if math.isfinite(x) else d
    except Exception: return d

def ema(values, n):
    if not values: return 0
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

def indicators(symbol):
    # Alpaca Basic: IEX real-time equity data. Bars are used for intraday confirmation.
    end=datetime.now(timezone.utc).isoformat().replace('+00:00','Z')
    data=req(f'{ALPACA}/stocks/{symbol}/bars', {'timeframe':'5Min','start':None,'end':end,'limit':100,'feed':'iex'})
    bars=data.get('bars') or []
    closes=[num(x.get('c')) for x in bars if num(x.get('c'))>0]
    vols=[num(x.get('v')) for x in bars]
    if len(closes)<25: raise RuntimeError(f'not enough bars for {symbol}: {len(closes)}')
    last=closes[-1]; e9=ema(closes[-30:],9); e21=ema(closes[-30:],21); rr=rsi(closes,14)
    typical=[]; pv=0; vv=0
    for b in bars:
        h=num(b.get('h')); l=num(b.get('l')); c=num(b.get('c')); v=num(b.get('v'))
        tp=(h+l+c)/3 if h and l and c else c
        pv += tp*v; vv += v
    vwap=pv/vv if vv else last
    avgvol=statistics.mean(vols[-21:-1]) if len(vols)>21 else statistics.mean(vols[:-1])
    vr=(vols[-1]/avgvol) if avgvol else 1
    recent_high=max(closes[-21:-1]); recent_low=min(closes[-21:-1])
    breakout='UP' if last>=recent_high else ('DOWN' if last<=recent_low else 'NO')
    macd_fast=ema(closes[-35:],12); macd_slow=ema(closes[-35:],26); macd=macd_fast-macd_slow
    prev_fast=ema(closes[-36:-1],12); prev_slow=ema(closes[-36:-1],26); prev_macd=prev_fast-prev_slow
    return {'underlying':last,'ema9':e9,'ema21':e21,'rsi':rr,'vwap':vwap,'volume_ratio':vr,'breakout':breakout,'macd':macd,'macd_state':'BULLISH' if macd>prev_macd else 'BEARISH'}

def option_chain(underlying, side=None):
    params={'feed':os.getenv('ALPACA_OPTIONS_FEED','indicative'),'limit':1000}
    if side: params['type']=side.lower()
    return req(f'{OPTIONS}/snapshots/{underlying}',params)

def parse_contract(sym, details):
    exp=details.get('expiration_date') or details.get('expirationDate')
    strike=num(details.get('strike_price') or details.get('strikePrice'))
    typ=(details.get('type') or '').upper()
    return exp,strike,typ

def scan_underlying(symbol, session, contract_prefix=None):
    ind=indicators(symbol)
    chain=option_chain(symbol)
    rows=chain.get('snapshots') or {}
    out=[]
    today=datetime.now(timezone.utc).date()
    for contract, snap in rows.items():
        if contract_prefix and not contract.startswith(contract_prefix): continue
        details=snap.get('details') or {}
        exp,strike,typ=parse_contract(contract,details)
        if not exp or not strike or typ not in ('CALL','PUT'): continue
        try: dte=(datetime.fromisoformat(exp).date()-today).days
        except Exception: continue
        if dte<0 or dte>30: continue
        quote=snap.get('latestQuote') or {}; trade=snap.get('latestTrade') or {}; greeks=snap.get('greeks') or {}
        bid=num(quote.get('bp')); ask=num(quote.get('ap')); last=num(trade.get('p'))
        premium=(bid+ask)/2 if bid>0 and ask>0 else last
        if premium<MIN_PREMIUM or premium>MAX_PREMIUM: continue
        spread=((ask-bid)/premium*100) if premium and ask>=bid else 999
        vol=int(num(trade.get('s'))); oi=int(num(details.get('open_interest') or details.get('openInterest')))
        if spread>MAX_SPREAD or vol<MIN_VOL or oi<MIN_OI: continue
        delta=num(greeks.get('delta'), 0.25 if typ=='CALL' else -0.25)
        bullish=ind['underlying']>ind['vwap'] and ind['ema9']>ind['ema21'] and ind['macd_state']=='BULLISH' and ind['rsi']>=52
        bearish=ind['underlying']<ind['vwap'] and ind['ema9']<ind['ema21'] and ind['macd_state']=='BEARISH' and ind['rsi']<=48
        direction='CALL' if typ=='CALL' else 'PUT'
        aligned=(bullish if direction=='CALL' else bearish)
        score=45
        score += min(15,max(0,(ind['volume_ratio']-1)*10))
        score += min(12,abs(ind['rsi']-50)*0.6)
        score += 10 if aligned else 0
        score += 8 if ind['breakout']==('UP' if direction=='CALL' else 'DOWN') else 0
        score += 5 if spread<8 else 0
        score += min(5,abs(delta)*5)
        score=min(100,score)
        if score<MIN_SCORE: continue
        risk=max(premium*.20,0.10); entry_low=round(premium*.97,2); entry_high=round(premium*1.03,2)
        stop=round(max(.05,entry_low-risk),2); tp1=round(entry_high+risk,2); tp2=round(entry_high+2*risk,2); tp3=round(entry_high+3*risk,2)
        contracts=max(0,int(RISK//(risk*100)))
        reasons=['liquidity','momentum']
        if aligned: reasons.append('intraday alignment')
        if ind['breakout']!='NO': reasons.append('breakout')
        if ind['volume_ratio']>=1.5: reasons.append('volume surge')
        out.append({'signal':direction,'market':'OPTIONS','symbol':symbol,'contract':contract,'dte':dte,'premium':round(premium,2),'entry_low':entry_low,'entry_high':entry_high,'stop_loss':stop,'tp1':tp1,'tp2':tp2,'tp3':tp3,'risk_dollars_per_contract':round(risk*100,2),'suggested_contracts':contracts,'score':round(score,1),'volume':vol,'open_interest':oi,'spread_pct':round(spread,1),'delta':round(delta,3),'underlying':round(ind['underlying'],2),'vwap_state':'BULLISH' if ind['underlying']>ind['vwap'] else 'BEARISH','ema_state':'BULLISH' if ind['ema9']>ind['ema21'] else 'BEARISH','rsi':round(ind['rsi'],1),'macd_state':ind['macd_state'],'volume_ratio':round(ind['volume_ratio'],2),'breakout':ind['breakout'],'reasons':reasons,'session':session,'data_mode':os.getenv('ALPACA_OPTIONS_FEED','indicative').upper()})
    return out, {'chain_items':len(rows),'scored':len(out),'underlying':symbol,'indicator_source':'Alpaca IEX 5m','option_source':'Alpaca options '+os.getenv('ALPACA_OPTIONS_FEED','indicative')}

def scan_all(session):
    if not headers(): raise RuntimeError('Missing ALPACA_API_KEY / ALPACA_API_SECRET')
    results=[]; diagnostics={}
    symbols=STOCKS[:]
    for sym in symbols:
        try:
            r,d=scan_underlying(sym,session); results.extend(r); diagnostics[sym]=d
        except Exception as e: diagnostics[sym]={'error':f'{type(e).__name__}: {e}'}
    # SPXW uses SPX as the underlier in Alpaca's contract model. NDX is not supported by Alpaca index options.
    if 'SPXW' in INDEX_ROOTS:
        try:
            r,d=scan_underlying('SPX',session,contract_prefix='SPXW')
            for x in r: x['symbol']='SPXW'
            results.extend(r); diagnostics['SPXW']=d
        except Exception as e: diagnostics['SPXW']={'error':f'{type(e).__name__}: {e}'}
    results.sort(key=lambda x:(x['score'],x['volume'],x['open_interest']),reverse=True)
    return results,diagnostics

def provider_status():
    return {'name':'Alpaca','configured':bool(os.getenv('ALPACA_API_KEY') and os.getenv('ALPACA_API_SECRET')),
            'options_feed':os.getenv('ALPACA_OPTIONS_FEED','indicative'),
            'underlying_feed':'iex','free_options_mode':os.getenv('ALPACA_OPTIONS_FEED','indicative')=='indicative',
            'note':'Free Basic options feed is indicative/delayed; IEX equity feed is real-time.'}
