import os, math, statistics, time
from datetime import datetime, timezone, timedelta
from typing import Callable, Optional
import requests

ALPACA = 'https://data.alpaca.markets/v2'
OPTIONS = 'https://data.alpaca.markets/v1beta1/options'
CONTRACTS = os.getenv('ALPACA_CONTRACTS_URL', 'https://paper-api.alpaca.markets/v2/options/contracts')
STOCKS = [s.strip().upper() for s in os.getenv('STOCK_SYMBOLS','QQQ,NVDA,AMD,TSLA,AAPL,AMZN,META,MSFT,GOOGL,MU,AVGO,PLTR,SMCI,SPY,IWM').split(',') if s.strip()]
INDEX_ROOTS = [s.strip().upper() for s in os.getenv('INDEX_ROOTS','SPXW').split(',') if s.strip()]
RISK = float(os.getenv('RISK_BUDGET','100'))
MIN_PREMIUM = float(os.getenv('MIN_PREMIUM','0.30')); MAX_PREMIUM = float(os.getenv('MAX_PREMIUM','10.00'))
UPSIDE_ALERT_PCT = float(os.getenv('UPSIDE_ALERT_PCT','1000'))
MAX_AFFORDABLE_PREMIUM = float(os.getenv('MAX_AFFORDABLE_PREMIUM','10.00'))
MAX_SPREAD = float(os.getenv('MAX_SPREAD_PCT','15')); MIN_OI = int(os.getenv('MIN_OI','50')); MIN_VOL = int(os.getenv('MIN_VOLUME','20'))
MIN_SCORE = max(70.0, float(os.getenv('MIN_SCORE','70')))
ALERT_COOLDOWN = int(os.getenv('ALERT_COOLDOWN_SECONDS','900'))
HTTP_TIMEOUT = max(1, float(os.getenv('ALPACA_HTTP_TIMEOUT','8')))
RETRIES = max(0, int(os.getenv('ALPACA_RETRIES','2')))

_session = requests.Session()
_progress: Optional[Callable[..., None]] = None

class ProviderRequestError(RuntimeError):
    def __init__(self, message, *, provider='Alpaca', endpoint='', status_code=None, retry_count=0, cause=None):
        super().__init__(message)
        self.provider = provider
        self.endpoint = endpoint
        self.status_code = status_code
        self.retry_count = retry_count
        self.cause = cause


def set_progress_callback(callback):
    global _progress
    _progress = callback


def progress(stage, **fields):
    cb = _progress
    if cb:
        try:
            cb(stage, **fields)
        except Exception:
            pass


def headers():
    k=os.getenv('ALPACA_API_KEY'); s=os.getenv('ALPACA_API_SECRET')
    return {'APCA-API-KEY-ID':k,'APCA-API-SECRET-KEY':s} if k and s else {}


def req(url, params=None, timeout=None):
    timeout = HTTP_TIMEOUT if timeout is None else timeout
    last=None
    endpoint=url.replace(ALPACA, '').replace(OPTIONS, '/options')
    for attempt in range(RETRIES + 1):
        try:
            r=_session.get(url, headers=headers(), params=params or {}, timeout=timeout)
            if r.status_code in (401, 403):
                raise ProviderRequestError(f'Alpaca HTTP {r.status_code}', endpoint=endpoint, status_code=r.status_code, retry_count=attempt)
            if r.status_code == 429:
                if attempt < RETRIES:
                    retry_after = r.headers.get('Retry-After')
                    try: delay=min(5.0, max(0.5, float(retry_after)))
                    except Exception: delay=min(5.0, 1.0 * (attempt + 1))
                    progress('provider_retry', endpoint=endpoint, status_code=429, retry=attempt + 1)
                    time.sleep(delay)
                    continue
                raise ProviderRequestError('Alpaca HTTP 429 rate limited', endpoint=endpoint, status_code=429, retry_count=attempt)
            if 500 <= r.status_code <= 599:
                if attempt < RETRIES:
                    progress('provider_retry', endpoint=endpoint, status_code=r.status_code, retry=attempt + 1)
                    time.sleep(min(4.0, 0.8 * (attempt + 1)))
                    continue
                raise ProviderRequestError(f'Alpaca HTTP {r.status_code}', endpoint=endpoint, status_code=r.status_code, retry_count=attempt)
            r.raise_for_status()
            try:
                data=r.json()
            except ValueError as e:
                raise ProviderRequestError('Alpaca returned invalid JSON', endpoint=endpoint, status_code=r.status_code, retry_count=attempt, cause=e)
            if data is None:
                raise ProviderRequestError('Alpaca returned an empty response', endpoint=endpoint, status_code=r.status_code, retry_count=attempt)
            return data
        except ProviderRequestError as e:
            last=e
            if e.status_code in (401,403,429) or attempt >= RETRIES:
                raise
        except (requests.Timeout, requests.ConnectionError) as e:
            last=e
            if attempt < RETRIES:
                progress('provider_retry', endpoint=endpoint, status_code='timeout/connection', retry=attempt + 1)
                time.sleep(min(4.0, 0.8 * (attempt + 1)))
                continue
        except requests.RequestException as e:
            last=e
            # Preserve HTTP status when requests raises HTTPError, so diagnostics
            # can distinguish 400/401/403/404/429/5xx instead of showing '?'.
            response = getattr(e, 'response', None)
            status = getattr(response, 'status_code', None)
            if attempt >= RETRIES:
                label = f'Alpaca HTTP {status}' if status else (
                    'Alpaca timeout' if isinstance(e, requests.Timeout) else
                    'Alpaca connection error' if isinstance(e, requests.ConnectionError) else
                    f'Alpaca {type(e).__name__}'
                )
                raise ProviderRequestError(
                    label, endpoint=endpoint, status_code=status or label,
                    retry_count=attempt, cause=e
                )
        except Exception as e:
            last=e
            if attempt >= RETRIES: raise
        if attempt < RETRIES:
            time.sleep(min(4.0, 0.8 * (attempt + 1)))
    raise last or RuntimeError('Alpaca request failed')


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
    # Alpaca's multi-symbol historical bars endpoint is paginated and the
    # limit applies to the total number of bars across all symbols, not per
    # symbol. Always follow next_page_token so one symbol cannot crowd out
    # another. This fixes sparse per-symbol caches without changing strategy.
    end_dt=datetime.now(timezone.utc); start_dt=end_dt-timedelta(days=days)
    merged={s:[] for s in symbols}; token=None
    max_pages=max(1,int(os.getenv('BARS_MAX_PAGES','20'))); pages=0
    while pages < max_pages:
        params={'symbols':','.join(symbols),'timeframe':timeframe,
                'start':start_dt.isoformat().replace('+00:00','Z'),
                'end':end_dt.isoformat().replace('+00:00','Z'),
                'limit':limit,'feed':os.getenv('ALPACA_UNDERLYING_FEED','iex'),'sort':'asc'}
        if token: params['page_token']=token
        data=req(f'{ALPACA}/stocks/bars', params)
        page=data.get('bars') or {}
        for sym, rows in page.items():
            merged.setdefault(sym,[]).extend(rows or [])
        pages += 1
        token=data.get('next_page_token')
        if not token: break
    return merged

def frame_indicators(symbol,timeframe,days=45,cache=None):
    bars=(cache or {}).get(symbol) if cache is not None else None
    closes=[num(b.get('c')) for b in (bars or []) if num(b.get('c'))>0]
    # IEX can be sparse for some symbols/timeframes. If the batched cache has
    # fewer than the minimum indicator history, retry that symbol directly
    # with a longer lookback. This does not change any scoring threshold.
    if len(closes)<55:
        fallback_days=max(days, 45 if timeframe=='15Min' else 30 if timeframe=='5Min' else days)
        bars=fetch_bars(symbol,timeframe,fallback_days)
        closes=[num(b.get('c')) for b in bars if num(b.get('c'))>0]
    if len(closes)<55: raise RuntimeError(f'not enough {timeframe} bars for {symbol}: {len(closes)}')
    e20=ema(closes[-100:],20); e50=ema(closes[-100:],50); rr=rsi(closes,14); a=atr_bars(bars,14); ax=adx(bars,14)
    mf=ema(closes[-80:],12); ms=ema(closes[-80:],26); macd=mf-ms
    pmf=ema(closes[-81:-1],12); pms=ema(closes[-81:-1],26); pmacd=pmf-pms
    slope=closes[-1]-closes[-6]
    return {'last':closes[-1],'ema20':e20,'ema50':e50,'rsi':rr,'atr':a,'adx':ax,'macd':macd,'macd_delta':macd-pmacd,'slope':slope,'bars':bars}

def session_vwap(bars):
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

def fetch_contract_metadata(underlyings, min_date, max_date):
    """Fetch option contract metadata once per scan to obtain daily open interest.

    Alpaca option snapshots expose latest trade/quote/greeks, while contract
    metadata exposes open_interest and its date. This lookup is read-only and
    does not submit or execute brokerage orders.
    """
    underlyings=[u for u in dict.fromkeys(underlyings) if u]
    if not underlyings:
        return {}, {'pages':0,'contracts':0}
    params={
        'underlying_symbols': ','.join(underlyings),
        'status':'active',
        'expiration_date_gte':min_date.isoformat(),
        'expiration_date_lte':max_date.isoformat(),
        'limit':10000,
    }
    merged={}; token=None; pages=0
    max_pages=max(1,int(os.getenv('CONTRACTS_MAX_PAGES','20')))
    while pages < max_pages:
        q=dict(params)
        if token: q['page_token']=token
        data=req(CONTRACTS,q)
        rows=data.get('option_contracts') or data.get('contracts') or []
        for c in rows:
            sym=str(c.get('symbol') or '').upper()
            if sym:
                merged[sym]=c
        pages += 1
        token=data.get('next_page_token') or data.get('page_token')
        if not token: break
    return merged, {'pages':pages,'contracts':len(merged)}

def option_chain(underlying, side=None):
    params={'feed':os.getenv('ALPACA_OPTIONS_FEED','indicative'),'limit':1000}
    if side: params['type']=side.lower()
    merged={}; page_token=None; pages=0; max_pages=max(1,int(os.getenv('OPTIONS_MAX_PAGES','20')))
    while pages < max_pages:
        p=dict(params)
        if page_token: p['page_token']=page_token
        data=req(f'{OPTIONS}/snapshots/{underlying}',p)
        rows=data.get('snapshots') or {}
        merged.update(rows)
        pages += 1
        page_token=data.get('next_page_token')
        if not page_token: break
    return {'snapshots':merged,'pages':pages}

def parse_contract(sym, details):
    # Alpaca option-chain snapshots are keyed by OCC contract symbols. The
    # snapshot payload does not reliably include expiration/strike/type in
    # `details`, so fall back to parsing the OCC symbol itself. This avoids
    # treating every valid snapshot as an invalid contract.
    details = details or {}
    exp = details.get('expiration_date') or details.get('expirationDate')
    strike = num(details.get('strike_price') or details.get('strikePrice'))
    typ = (details.get('type') or details.get('contract_type') or '').upper()
    if exp and strike and typ in ('CALL','PUT'):
        return exp, strike, typ
    import re
    m = re.match(r'^(.+?)(\d{6})([CP])(\d{8})$', str(sym).strip().upper())
    if not m:
        return None, 0.0, ''
    root, yymmdd, cp, strike_raw = m.groups()
    try:
        yy, mm, dd = int(yymmdd[:2]), int(yymmdd[2:4]), int(yymmdd[4:6])
        exp = f"{2000 + yy:04d}-{mm:02d}-{dd:02d}"
        strike = int(strike_raw) / 1000.0
    except (ValueError, TypeError):
        return None, 0.0, ''
    typ = 'CALL' if cp == 'C' else 'PUT'
    return exp, strike, typ

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

def scan_underlying(symbol, session, contract_prefix=None, caches=None, contract_meta=None):
    m=multi_tf(symbol,caches); progress('fetching_options', symbol=symbol); chain=option_chain(symbol); rows=chain.get('snapshots') or {}; out=[]; today=datetime.now(timezone.utc).date()
    rejection_counts={'invalid_contract':0,'dte':0,'premium':0,'spread':0,'volume':0,'open_interest':0,'score':0,'trend_alignment':0,'regime':0,'score_evaluated':0,'passed_all_filters':0}
    score_values=[]
    progress('scoring', symbol=symbol, contracts=len(rows))
    for contract,snap in rows.items():
        if contract_prefix and not contract.startswith(contract_prefix): continue
        details=snap.get('details') or {}; exp,strike,typ=parse_contract(contract,details)
        if not exp or not strike or typ not in ('CALL','PUT'):
            rejection_counts['invalid_contract'] += 1
            continue
        try:dte=(datetime.fromisoformat(exp).date()-today).days
        except Exception:continue
        if dte<0 or dte>30:
            rejection_counts['dte'] += 1
            continue
        quote=snap.get('latestQuote') or {}; trade=snap.get('latestTrade') or {}; greeks=snap.get('greeks') or {}
        bid=num(quote.get('bp')); ask=num(quote.get('ap')); last=num(trade.get('p')); premium=(bid+ask)/2 if bid>0 and ask>0 else last
        if premium<MIN_PREMIUM or premium>MAX_PREMIUM or premium>MAX_AFFORDABLE_PREMIUM:
            rejection_counts['premium'] += 1
            continue
        spread=((ask-bid)/premium*100) if premium and ask>=bid else 999
        daily=snap.get('dailyBar') or snap.get('daily_bar') or {}; vol=int(num(daily.get('v'))); meta=(contract_meta or {}).get(contract,{})
        oi=int(num(details.get('open_interest') or details.get('openInterest') or snap.get('open_interest') or snap.get('openInterest') or meta.get('open_interest')))
        if vol<=0:vol=int(num(trade.get('s'))) if trade else 0
        if spread>MAX_SPREAD:
            rejection_counts['spread'] += 1
            continue
        if vol<MIN_VOL:
            rejection_counts['volume'] += 1
            continue
        if oi<MIN_OI:
            rejection_counts['open_interest'] += 1
            continue
        delta=num(greeks.get('delta'),0.25 if typ=='CALL' else -0.25); direction=typ
        score,reasons,a4,a1,a15,a5=score_setup(m,direction,spread,delta,vol,oi)
        rejection_counts['score_evaluated'] += 1
        score_values.append(score)
        if score<MIN_SCORE:
            rejection_counts['score'] += 1
            continue
        if not a4:
            rejection_counts['trend_alignment'] += 1
            continue
        if m['regime'] in ('SIDEWAYS','CHOPPY'):
            rejection_counts['regime'] += 1
            continue
        rejection_counts['passed_all_filters'] += 1
        risk=max(premium*.20,0.10); entry_low=round(premium*.97,2); entry_high=round(premium*1.03,2); stop=round(max(.05,entry_low-risk),2)
        tp1=round(entry_high+risk,2); tp2=round(entry_high+2*risk,2); tp3=round(entry_high+3*risk,2)
        contracts=max(0,int(RISK//(risk*100))); max_loss=round(risk*100*contracts,2)
        reward1=round(max(0,tp1-entry_high)*100*contracts,2); reward2=round(max(0,tp2-entry_high)*100*contracts,2); reward3=round(max(0,tp3-entry_high)*100*contracts,2)
        atr_points=max(m['5m']['atr'],m['5m']['last']*0.001); u_entry=round(m['5m']['last'],2)
        if direction=='CALL': u_stop=round(u_entry-atr_points,2); u_tp1=round(u_entry+atr_points,2); u_tp2=round(u_entry+2*atr_points,2); u_tp3=round(u_entry+3*atr_points,2)
        else: u_stop=round(u_entry+atr_points,2); u_tp1=round(u_entry-atr_points,2); u_tp2=round(u_entry-2*atr_points,2); u_tp3=round(u_entry-3*atr_points,2)
        confidence=round(min(94,max(50,50+score*.44)),0)
        tp1_conf=round(min(95,confidence+4)); tp2_conf=round(max(25,confidence-9)); tp3_conf=round(max(15,confidence-20))
        if direction=='CALL': projected_underlying=round(max(u_tp3, m['recent_high'] + 2*atr_points),2); underlying_move=max(0.0, projected_underlying-u_entry)
        else: projected_underlying=round(min(u_tp3, m['recent_low'] - 2*atr_points),2); underlying_move=max(0.0, u_entry-projected_underlying)
        projected_premium=max(0.05, premium + abs(delta)*underlying_move); projected_upside_pct=round(max(0.0,(projected_premium/premium-1)*100),1) if premium>0 else 0.0
        extreme_upside=projected_upside_pct>=UPSIDE_ALERT_PCT and premium<=MAX_AFFORDABLE_PREMIUM
        if extreme_upside: reasons.append(f'projected upside > {UPSIDE_ALERT_PCT:.0f}%')
        out.append({'signal':direction,'market':'OPTIONS','symbol':symbol,'contract':contract,'dte':dte,'premium':round(premium,2),'entry_low':entry_low,'entry_high':entry_high,'stop_loss':stop,'tp1':tp1,'tp2':tp2,'tp3':tp3,'risk_dollars_per_contract':round(risk*100,2),'suggested_contracts':contracts,'max_loss':max_loss,'expected_profit_tp1':reward1,'expected_profit_tp2':reward2,'expected_profit_tp3':reward3,'underlying_entry':u_entry,'underlying_stop_loss':u_stop,'underlying_tp1':u_tp1,'underlying_tp2':u_tp2,'underlying_tp3':u_tp3,'atr_5m_points':round(atr_points,2),'score':round(score,1),'confidence':confidence,'tp1_confidence':tp1_conf,'tp2_confidence':tp2_conf,'tp3_confidence':tp3_conf,'projected_underlying_target':projected_underlying,'projected_premium':round(projected_premium,2),'projected_upside_pct':projected_upside_pct,'extreme_upside':extreme_upside,'market_regime':m['regime'],'volume':vol,'open_interest':oi,'spread_pct':round(spread,1),'delta':round(delta,3),'underlying':u_entry,'vwap_state':'BULLISH' if u_entry>m['vwap'] else 'BEARISH','ema_state':'BULLISH' if m['5m']['ema20']>m['5m']['ema50'] else 'BEARISH','rsi':round(m['5m']['rsi'],1),'macd_state':'BULLISH' if m['5m']['macd_delta']>0 else 'BEARISH','volume_ratio':round(m['volume_ratio'],2),'breakout':m['breakout'],'trend_4h':'BULLISH' if m['4h']['last']>m['4h']['ema20']>m['4h']['ema50'] else 'BEARISH' if m['4h']['last']<m['4h']['ema20']<m['4h']['ema50'] else 'NEUTRAL','trend_1h':'BULLISH' if m['1h']['last']>m['1h']['ema20']>m['1h']['ema50'] else 'BEARISH' if m['1h']['last']<m['1h']['ema20']<m['1h']['ema50'] else 'NEUTRAL','trend_15m':'BULLISH' if m['15m']['last']>m['15m']['ema20'] else 'BEARISH' if m['15m']['last']<m['15m']['ema20'] else 'NEUTRAL','trend_5m':'BULLISH' if m['5m']['last']>m['vwap'] and m['5m']['ema20']>m['5m']['ema50'] else 'BEARISH' if m['5m']['last']<m['vwap'] and m['5m']['ema20']<m['5m']['ema50'] else 'NEUTRAL','adx_4h':round(m['4h']['adx'],1),'rsi_4h':round(m['4h']['rsi'],1),'reasons':reasons,'session':session,'data_mode':options_data_mode()})
    return out, {'chain_items':len(rows),'chain_pages':int(chain.get('pages',0) or 0),'scored':rejection_counts.get('score_evaluated',0),'rejections':rejection_counts,'final_candidates':len(out),'score_min':min(score_values) if score_values else None,'score_max':max(score_values) if score_values else None,'score_avg':round(statistics.mean(score_values),2) if score_values else None,'score_at_or_above_threshold':sum(1 for v in score_values if v>=MIN_SCORE),'underlying':symbol,'indicator_source':'Alpaca historical multi-timeframe bars','option_source':'Alpaca options '+os.getenv('ALPACA_OPTIONS_FEED','indicative'),'market_regime':m['regime'],'trend_4h':'BULLISH' if m['4h']['last']>m['4h']['ema20']>m['4h']['ema50'] else 'BEARISH' if m['4h']['last']<m['4h']['ema20']<m['4h']['ema50'] else 'NEUTRAL','trend_1h':'BULLISH' if m['1h']['last']>m['1h']['ema20']>m['1h']['ema50'] else 'BEARISH' if m['1h']['last']<m['1h']['ema20']<m['1h']['ema50'] else 'NEUTRAL','trend_15m':'BULLISH' if m['15m']['last']>m['15m']['ema20'] else 'BEARISH' if m['15m']['last']<m['15m']['ema20'] else 'NEUTRAL','trend_5m':'BULLISH' if m['5m']['last']>m['vwap'] and m['5m']['ema20']>m['5m']['ema50'] else 'BEARISH' if m['5m']['last']<m['vwap'] and m['5m']['ema20']<m['vwap'] else 'NEUTRAL'}

def scan_all(session):
    if not headers():raise RuntimeError('Missing ALPACA_API_KEY / ALPACA_API_SECRET')
    results=[]; diagnostics={}; symbols=list(dict.fromkeys(STOCKS+(['SPX'] if 'SPXW' in INDEX_ROOTS else [])))
    periods={'5m':('5Min',7),'15m':('15Min',20),'1h':('1Hour',45),'4h':('4Hour',180)}; caches={}
    progress('fetching_bars', symbols_total=len(symbols))
    for key,(tf,days) in periods.items():
        try:
            caches[key]=fetch_bars_batch(symbols,tf,days); progress('fetching_bars', timeframe=key, symbols=len(caches[key]))
        except Exception as e:
            caches[key]={}; diagnostics[f'__{key}']={'error':f'{type(e).__name__}: {e}','provider':'Alpaca','endpoint':getattr(e,'endpoint',''),'status_code':getattr(e,'status_code',None),'retry_count':getattr(e,'retry_count',None)}
    symbols_scanned=0; contracts_scanned=0
    contract_meta={}
    try:
        min_date=datetime.now(timezone.utc).date(); max_date=min_date+timedelta(days=30)
        progress('fetching_contract_metadata', symbols=len(STOCKS)+ (1 if 'SPXW' in INDEX_ROOTS else 0))
        meta_underlyings=list(STOCKS)+(['SPX'] if 'SPXW' in INDEX_ROOTS else [])
        contract_meta, meta_diag=fetch_contract_metadata(meta_underlyings,min_date,max_date)
        diagnostics['__contract_metadata__']={'contracts':meta_diag['contracts'],'pages':meta_diag['pages'],'provider':'Alpaca','endpoint':CONTRACTS}
    except Exception as e:
        diagnostics['__contract_metadata__']={'error':f'{type(e).__name__}: {e}','provider':'Alpaca','endpoint':getattr(e,'endpoint',CONTRACTS),'status_code':getattr(e,'status_code',None),'retry_count':getattr(e,'retry_count',None)}
    for sym in STOCKS:
        try:
            progress('scoring', symbol=sym, symbols_scanned=symbols_scanned, contracts_scanned=contracts_scanned)
            r,d=scan_underlying(sym,session,caches=caches,contract_meta=contract_meta); results.extend(r); diagnostics[sym]=d
            symbols_scanned += 1; contracts_scanned += int(d.get('chain_items',0)); progress('scoring', symbol=sym, symbols_scanned=symbols_scanned, contracts_scanned=contracts_scanned, candidates=len(results))
        except Exception as e:
            diagnostics[sym]={'error':f'{type(e).__name__}: {e}','provider':'Alpaca','endpoint':getattr(e,'endpoint',''),'status_code':getattr(e,'status_code',None),'retry_count':getattr(e,'retry_count',None)}
            symbols_scanned += 1
    if 'SPXW' in INDEX_ROOTS:
        try:
            progress('scoring', symbol='SPXW', symbols_scanned=symbols_scanned, contracts_scanned=contracts_scanned)
            # SPX is an index, not an equity ticker. Keep SPXW support but do
            # not fabricate SPX bars from SPY; if the configured stock-bars
            # feed cannot provide SPX history, report it and skip safely.
            r,d=scan_underlying('SPX',session,contract_prefix='SPXW',caches={},contract_meta=contract_meta)
            for x in r:x['symbol']='SPXW'
            results.extend(r); diagnostics['SPXW']=d; contracts_scanned += int(d.get('chain_items',0)); progress('scoring', symbol='SPXW', symbols_scanned=symbols_scanned+1, contracts_scanned=contracts_scanned, candidates=len(results))
        except Exception as e:
            diagnostics['SPXW']={'error':f'{type(e).__name__}: {e}','provider':'Alpaca','endpoint':getattr(e,'endpoint',''),'status_code':getattr(e,'status_code',None),'retry_count':getattr(e,'retry_count',None)}
    progress('sorting', candidates=len(results))
    results.sort(key=lambda x:(x['extreme_upside'],x['projected_upside_pct'],x['score'],x['confidence'],x['volume'],x['open_interest']),reverse=True)
    diagnostics['__meta__']={'symbols_scanned':symbols_scanned,'contracts_scanned':contracts_scanned,'candidates':len(results),'provider':provider_status(),'contract_metadata_count':len(contract_meta)}
    return results,diagnostics

def options_data_mode():
    feed=str(os.getenv('ALPACA_OPTIONS_FEED','indicative')).strip().lower()
    return {'opra':'LIVE','live':'LIVE','delayed':'DELAYED','indicative':'INDICATIVE'}.get(feed,'UNKNOWN')

def provider_status():
    feed=os.getenv('ALPACA_OPTIONS_FEED','indicative')
    mode=str(feed).lower()
    return {'name':'Alpaca','configured':bool(os.getenv('ALPACA_API_KEY') and os.getenv('ALPACA_API_SECRET')),'options_feed':feed,'underlying_feed':'iex','data_mode':options_data_mode(),'note':'Free Alpaca options data may be delayed/indicative; IEX equity feed is real-time.' if mode in ('indicative','delayed','snapshot') else 'Options feed mode is configured explicitly; verify entitlement before treating it as real-time.'}
