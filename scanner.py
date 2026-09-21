import os, math, statistics, time, threading, re
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone, timedelta
from typing import Callable, Optional
import requests

ALPACA = 'https://data.alpaca.markets/v2'
OPTIONS = 'https://data.alpaca.markets/v1beta1/options'
STOCKS = [s.strip().upper() for s in os.getenv('STOCK_SYMBOLS','QQQ,NVDA,AMD,TSLA,AAPL,AMZN,META,MSFT,GOOGL,MU,AVGO,PLTR,SMCI,SPY,IWM').split(',') if s.strip()]
INDEX_ROOTS = [s.strip().upper() for s in os.getenv('INDEX_ROOTS','SPXW').split(',') if s.strip()]
RISK = float(os.getenv('RISK_BUDGET','100'))
MIN_PREMIUM = float(os.getenv('MIN_PREMIUM','0.20')); MAX_PREMIUM = float(os.getenv('MAX_PREMIUM','25.00'))
UPSIDE_ALERT_PCT = float(os.getenv('UPSIDE_ALERT_PCT','1000'))
MAX_AFFORDABLE_PREMIUM = float(os.getenv('MAX_AFFORDABLE_PREMIUM','25.00'))
MAX_SPREAD = float(os.getenv('MAX_SPREAD_PCT','25')); MIN_OI = int(os.getenv('MIN_OI','10')); MIN_VOL = int(os.getenv('MIN_VOLUME','5'))
MIN_SCORE = max(45.0, float(os.getenv('MIN_SCORE','50')))
REQUIRE_4H_ALIGNMENT = os.getenv('REQUIRE_4H_ALIGNMENT','false').strip().lower() in ('1','true','yes','on')
BLOCK_SIDEWAYS_CHOPPY = os.getenv('BLOCK_SIDEWAYS_CHOPPY','false').strip().lower() in ('1','true','yes','on')
FALLBACK_MIN_SCORE = max(45.0, float(os.getenv('FALLBACK_MIN_SCORE','45')))
EXPLOSIVE_MIN_SCORE = max(65.0, float(os.getenv('EXPLOSIVE_MIN_SCORE','72')))
EXPLOSIVE_MIN_VOLUME_RATIO = max(1.0, float(os.getenv('EXPLOSIVE_MIN_VOLUME_RATIO','1.5')))
EXPLOSIVE_MAX_DTE = max(0, int(os.getenv('EXPLOSIVE_MAX_DTE','14')))
RELAXED_MAX_SPREAD = float(os.getenv('RELAXED_MAX_SPREAD_PCT','35'))
RELAXED_MIN_OI = int(os.getenv('RELAXED_MIN_OI','5'))
RELAXED_MIN_VOL = int(os.getenv('RELAXED_MIN_VOLUME','1'))
AFTER_HOURS_MAX_PREMIUM = float(os.getenv('AFTER_HOURS_MAX_PREMIUM','300.00'))
AFTER_HOURS_MAX_SPREAD = float(os.getenv('AFTER_HOURS_MAX_SPREAD_PCT','150'))
AFTER_HOURS_MIN_OI = int(os.getenv('AFTER_HOURS_MIN_OI','0'))
AFTER_HOURS_MIN_VOL = int(os.getenv('AFTER_HOURS_MIN_VOLUME','0'))
ALERT_COOLDOWN = int(os.getenv('ALERT_COOLDOWN_SECONDS','900'))
HTTP_TIMEOUT = max(1, float(os.getenv('ALPACA_HTTP_TIMEOUT','8')))
RETRIES = max(0, int(os.getenv('ALPACA_RETRIES','2')))
OPTIONS_PAGE_LIMIT = max(100, min(1000, int(os.getenv('OPTIONS_PAGE_LIMIT','1000'))))
OPTIONS_MAX_PAGES = max(1, int(os.getenv('OPTIONS_MAX_PAGES','12')))
OPTIONS_MIN_INTERVAL = max(0.05, float(os.getenv('OPTIONS_MIN_INTERVAL_SECONDS','0.20')))
STOCKS_PAGE_LIMIT = max(100, min(10000, int(os.getenv('STOCKS_PAGE_LIMIT','10000'))))
STOCKS_MAX_PAGES = max(1, int(os.getenv('STOCKS_MAX_PAGES','5')))
_options_rate_lock = threading.Lock()
_options_last_request = 0.0

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


def _pace_options_request(url):
    global _options_last_request
    if not url.startswith(OPTIONS):
        return
    with _options_rate_lock:
        now=time.monotonic()
        wait=OPTIONS_MIN_INTERVAL-(now-_options_last_request)
        if wait>0: time.sleep(wait)
        _options_last_request=time.monotonic()


def req(url, params=None, timeout=None):
    timeout = HTTP_TIMEOUT if timeout is None else timeout
    last=None
    endpoint=url.replace(ALPACA, '').replace(OPTIONS, '/options')
    for attempt in range(RETRIES + 1):
        try:
            _pace_options_request(url)
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
            if r.status_code >= 400:
                body = (r.text or '')[:500].replace('\n',' ')
                raise ProviderRequestError(f'Alpaca HTTP {r.status_code}: {body}', endpoint=endpoint, status_code=r.status_code, retry_count=attempt)
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
            if attempt >= RETRIES: raise ProviderRequestError(type(e).__name__, endpoint=endpoint, retry_count=attempt, cause=e)
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

def fetch_bars_batch(symbols,timeframe,days=45,limit=None):
    """Fetch multi-symbol stock bars with pagination and explicit progress.

    Alpaca's stock-bars endpoint can paginate even when multiple symbols are
    requested. The previous implementation only consumed the first page, which
    could leave some symbols with incomplete/missing bars and trigger expensive
    per-symbol refetches later in the scan.
    """
    end_dt=datetime.now(timezone.utc); start_dt=end_dt-timedelta(days=days)
    page_limit=min(STOCKS_PAGE_LIMIT, int(limit or STOCKS_PAGE_LIMIT))
    merged={}; page_token=None; pages=0
    while pages < STOCKS_MAX_PAGES:
        params={
            'symbols':','.join(symbols),
            'timeframe':timeframe,
            'start':start_dt.isoformat().replace('+00:00','Z'),
            'end':end_dt.isoformat().replace('+00:00','Z'),
            'limit':page_limit,
            'feed':'iex',
            'sort':'asc'
        }
        if page_token:
            params['page_token']=page_token
        progress('fetching_bars_page', timeframe=timeframe, page=pages+1, symbols_total=len(symbols))
        data=req(f'{ALPACA}/stocks/bars', params)
        rows=data.get('bars') or {}
        for sym, bars in rows.items():
            merged.setdefault(sym, []).extend(bars or [])
        pages += 1
        page_token=data.get('next_page_token')
        if not page_token:
            break
    progress('fetching_bars_page', timeframe=timeframe, page=pages, symbols=len(merged), complete=True)
    return merged

def _bar_dt(value):
    try:
        return datetime.fromisoformat(str(value).replace('Z','+00:00')).astimezone(timezone.utc)
    except Exception:
        return None

def _aggregate_bars(bars, minutes):
    # Build higher-timeframe OHLCV bars from a lower timeframe without pandas.
    # Grouping is UTC based and preserves chronological order.
    if not bars or minutes <= 0:
        return []
    step=minutes*60
    groups={}
    for b in bars:
        dt=_bar_dt(b.get('t'))
        if dt is None:
            continue
        epoch=int(dt.timestamp())
        bucket=(epoch//step)*step
        groups.setdefault(bucket,[]).append(b)
    out=[]
    for bucket, rows in sorted(groups.items()):
        rows=sorted(rows,key=lambda x:str(x.get('t','')))
        valid=[r for r in rows if num(r.get('c'))>0]
        if not valid:
            continue
        o=num(valid[0].get('o')); h=max(num(r.get('h')) for r in valid); l=min(num(r.get('l')) for r in valid); c=num(valid[-1].get('c'))
        v=sum(max(0,num(r.get('v'))) for r in valid)
        item={'t':datetime.fromtimestamp(bucket,tz=timezone.utc).isoformat().replace('+00:00','Z'),
              'o':o,'h':h,'l':l,'c':c,'v':v}
        # Carry common optional fields when present.
        for key in ('n','vw'):
            vals=[num(r.get(key),0) for r in valid if r.get(key) is not None]
            if vals:
                item[key]=sum(vals)/len(vals) if key=='vw' else int(sum(vals))
        out.append(item)
    return out

def _bars_for_symbol(symbol,timeframe,days,cache):
    # Prefer the requested cache. If it is missing/incomplete, fetch the symbol
    # directly rather than turning a partial multi-symbol response into a hard
    # provider error.
    bars=(cache or {}).get(symbol) if cache is not None else None
    bars=bars or []
    if len([b for b in bars if num(b.get('c'))>0]) >= 55:
        return bars, 'cache'
    try:
        fetched=fetch_bars(symbol,timeframe,days,limit=2000)
        if len(fetched) >= len(bars):
            bars=fetched
        if len([b for b in bars if num(b.get('c'))>0]) >= 55:
            if cache is not None: cache[symbol]=bars
            return bars, 'live_fetch'
    except Exception as e:
        progress('bars_symbol_fallback_failed',symbol=symbol,timeframe=timeframe,error=str(e)[:300])
    return bars, 'cache_partial'

def frame_indicators(symbol,timeframe,days=45,cache=None):
    bars,source=_bars_for_symbol(symbol,timeframe,days,cache)
    closes=[num(b.get('c')) for b in bars if num(b.get('c'))>0]
    if len(closes)<55:
        # Higher timeframes are rebuilt from lower ones when the provider's
        # native timeframe is missing or too short.
        lower={'15Min':('5Min',7,15),'1Hour':('15Min',20,60),'4Hour':('1Hour',45,240)}.get(timeframe)
        if lower:
            ltf,ldays,minutes=lower
            lcache=None
            # The caller normally provides a multi-timeframe cache. Access it
            # through the temporary attribute installed by multi_tf.
            allc=getattr(frame_indicators,'_all_caches',{}) or {}
            lcache=allc.get({'5Min':'5m','15Min':'15m','1Hour':'1h'}.get(ltf,''))
            lower_bars,lower_source=_bars_for_symbol(symbol,ltf,ldays,lcache)
            if len([b for b in lower_bars if num(b.get('c'))>0])>=55:
                bars=_aggregate_bars(lower_bars,minutes)
                source=f'aggregated_{ltf}'
                closes=[num(b.get('c')) for b in bars if num(b.get('c'))>0]
                if cache is not None and len(closes)>=55:
                    cache[symbol]=bars
        
    if len(closes)<55:
        raise RuntimeError(f'not enough {timeframe} bars for {symbol}: {len(closes)}')
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
    frame_indicators._all_caches=caches
    try:
        f5=frame_indicators(symbol,'5Min',7,caches.get('5m')); f15=frame_indicators(symbol,'15Min',20,caches.get('15m')); f1=frame_indicators(symbol,'1Hour',45,caches.get('1h')); f4=frame_indicators(symbol,'4Hour',180,caches.get('4h'))
    finally:
        frame_indicators._all_caches={}
    vwap=session_vwap(f5['bars']) or f5['last']
    closes=[num(b.get('c')) for b in f5['bars']]; vols=[num(b.get('v')) for b in f5['bars']]
    avgvol=statistics.mean(vols[-21:-1]) if len(vols)>21 else max(statistics.mean(vols[:-1]),1)
    vr=vols[-1]/avgvol if avgvol else 1
    recent_high=max(closes[-21:-1]); recent_low=min(closes[-21:-1]); last=closes[-1]
    breakout='UP' if last>recent_high else ('DOWN' if last<recent_low else 'NO')
    reg=regime(f4,f1,f15,f5)
    return {'5m':f5,'15m':f15,'1h':f1,'4h':f4,'vwap':vwap,'volume_ratio':vr,'breakout':breakout,'regime':reg,'recent_high':recent_high,'recent_low':recent_low}

def _options_feeds():
    """Return configured feed first, then safe public fallback when available."""
    configured = str(os.getenv('ALPACA_OPTIONS_FEED','indicative')).strip().lower()
    feeds = [configured]
    # OPRA can require a paid entitlement. If it fails, indicative is still
    # useful for discovery/scoring and prevents the entire scan from becoming
    # Chain=0. We keep OPRA first so entitled accounts still get live data.
    if configured == 'opra' and str(os.getenv('OPTIONS_FEED_FALLBACK','true')).lower() in ('1','true','yes','on'):
        feeds.append('indicative')
    elif configured in ('live', 'delayed') and str(os.getenv('OPTIONS_FEED_FALLBACK','true')).lower() in ('1','true','yes','on'):
        feeds.append('indicative')
    return list(dict.fromkeys(feeds))


def _option_contracts_fallback(underlying, side=None, root_symbol=None):
    """Fallback universe from Alpaca Trading API, then hydrate quotes/greeks via snapshots.

    The market-data chain endpoint can fail independently of the contracts endpoint.
    Keeping these paths separate lets the scanner continue when chain discovery has a
    transient/provider-specific problem, while preserving the same indicative feed.
    """
    today=datetime.now(timezone.utc).date()
    trading_base=os.getenv('ALPACA_TRADING_BASE_URL','https://paper-api.alpaca.markets/v2').rstrip('/')
    params={
        'underlying_symbols':underlying,
        'status':'active',
        'expiration_date_gte':today.isoformat(),
        'expiration_date_lte':(today+timedelta(days=30)).isoformat(),
        'limit':int(os.getenv('OPTIONS_CONTRACTS_PAGE_LIMIT','10000')),
    }
    if side: params['type']=side.lower()
    if root_symbol: params['root_symbol']=root_symbol
    contracts=[]; page_token=None; pages=0
    while pages < int(os.getenv('OPTIONS_CONTRACTS_MAX_PAGES','3')):
        q=dict(params)
        if page_token: q['page_token']=page_token
        data=req(f'{trading_base}/options/contracts',q)
        rows=data.get('option_contracts') or data.get('contracts') or []
        contracts.extend(rows)
        pages += 1
        page_token=data.get('page_token') or data.get('next_page_token')
        if not page_token: break

    symbols=[]; metadata={}
    for c in contracts:
        sym=str(c.get('symbol') or '').upper()
        if not sym: continue
        symbols.append(sym)
        metadata[sym]={
            'details':{
                'expiration_date':c.get('expiration_date'),
                'strike_price':c.get('strike_price'),
                'type':c.get('type'),
                'root_symbol':c.get('root_symbol'),
                'underlying_symbol':c.get('underlying_symbol'),
                'open_interest':c.get('open_interest'),
                'openInterest':c.get('open_interest'),
            }
        }

    merged={}; batch_size=100
    for i in range(0,len(symbols),batch_size):
        batch=symbols[i:i+batch_size]
        last_error=None
        data=None
        for feed in _options_feeds():
            try:
                data=req(f'{OPTIONS}/snapshots', {
                    'symbols':','.join(batch),
                    'feed':feed,
                    'limit':len(batch),
                })
                break
            except Exception as e:
                last_error=e
                progress('options_feed_retry', underlying=underlying, feed=feed,
                         error=str(e)[:300])
        if data is None:
            raise last_error or ProviderRequestError('No options snapshot feed available',
                                                     endpoint='/options/snapshots')
        snaps=data.get('snapshots') or {}
        for sym in batch:
            base=metadata.get(sym,{}); snap=snaps.get(sym) or {}
            if snap:
                snap.setdefault('details',{}).update(base.get('details') or {})
                # Record the effective feed without changing the configured
                # provider status shown to the user.
                snap.setdefault('_scanner_meta', {})['feed_used'] = feed
                merged[sym]=snap
    return {'snapshots':merged,'pages':pages,'fallback':True,'contracts_discovered':len(symbols)}

def option_chain(underlying, side=None, root_symbol=None):
    # Restrict snapshots to the next 30 calendar days. This dramatically reduces
    # pagination and Alpaca rate-limit pressure while matching the scanner DTE rule.
    today=datetime.now(timezone.utc).date()
    base_params={
        'limit':min(1000, OPTIONS_PAGE_LIMIT),
        'expiration_date_gte':today.isoformat(),
        'expiration_date_lte':(today+timedelta(days=30)).isoformat(),
    }
    if side: base_params['type']=side.lower()
    if root_symbol: base_params['root_symbol']=root_symbol
    merged={}; page_token=None; pages=0; primary_error=None; feed_used=None
    try:
        for feed in _options_feeds():
            try:
                merged={}; page_token=None; pages=0
                while pages < OPTIONS_MAX_PAGES:
                    q=dict(base_params); q['feed']=feed
                    if page_token: q['page_token']=page_token
                    data=req(f'{OPTIONS}/snapshots/{underlying}',q)
                    rows=data.get('snapshots') or {}
                    merged.update(rows)
                    pages += 1
                    page_token=data.get('next_page_token')
                    if not page_token: break
                feed_used=feed
                if merged or feed == _options_feeds()[-1]:
                    break
            except Exception as e:
                primary_error=e
                progress('options_feed_retry', underlying=underlying, feed=feed,
                         error=str(e)[:300])
                continue
        if merged:
            for snap in merged.values():
                if isinstance(snap,dict):
                    snap.setdefault('_scanner_meta', {})['feed_used'] = feed_used
            return {'snapshots':merged,'pages':pages,'fallback':False,
                    'feed_used':feed_used,
                    'primary_error':str(primary_error)[:500] if primary_error else None}
        if primary_error:
            raise primary_error
        return {'snapshots':{},'pages':pages,'fallback':False,'feed_used':feed_used}
    except Exception as primary_error:
        if str(os.getenv('OPTIONS_CHAIN_FALLBACK','true')).lower() not in ('1','true','yes','on'):
            raise
        progress('options_chain_fallback', underlying=underlying, error=str(primary_error)[:300])
        try:
            result=_option_contracts_fallback(underlying,side=side,root_symbol=root_symbol)
            result['primary_error']=str(primary_error)[:500]
            return result
        except Exception as fallback_error:
            raise ProviderRequestError(
                f'Option chain failed; fallback also failed. primary={primary_error}; fallback={fallback_error}',
                endpoint=getattr(fallback_error,'endpoint',f'/options/snapshots/{underlying}'),
                status_code=getattr(fallback_error,'status_code',None),
                retry_count=getattr(fallback_error,'retry_count',None),
                cause=fallback_error
            )

def parse_contract(sym, details):
    """Parse Alpaca option details, with OCC-symbol fallback.

    Alpaca can return incomplete/variant contract metadata for index options.
    SPX/SPXW OCC symbols encode YYMMDD + C/P + strike*1000, so use that
    information when metadata fields are absent.
    """
    details = details or {}
    exp = details.get('expiration_date') or details.get('expirationDate')
    strike = num(details.get('strike_price') or details.get('strikePrice'))
    typ = (details.get('type') or details.get('option_type') or '').upper()

    # OCC-style index option symbol, e.g. SPXW260921C06600000
    m = re.search(r'(?:SPXW|SPX)(\d{6})([CP])(\d{8})$', str(sym).upper())
    if m:
        yy, mm, dd = int(m.group(1)[:2]), int(m.group(1)[2:4]), int(m.group(1)[4:6])
        try:
            exp = f"20{yy:02d}-{mm:02d}-{dd:02d}"
        except Exception:
            pass
        typ = 'CALL' if m.group(2) == 'C' else 'PUT'
        if strike <= 0:
            strike = int(m.group(3)) / 1000.0

    # A few APIs return "call"/"put" and occasionally omit the strike in
    # details while exposing it in the symbol.
    if typ in ('C', 'CALL'):
        typ = 'CALL'
    elif typ in ('P', 'PUT'):
        typ = 'PUT'

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

def explosive_setup_score(m, direction, spread, delta, vol, oi, dte):
    """Score contracts for explosive momentum setups (0-100).

    This is a detection score, not a prediction. It rewards simultaneous
    price acceleration, breakout, volume expansion, multi-timeframe alignment,
    option sensitivity/liquidity and near-term expiry while penalizing wide
    spreads and choppy/sideways regimes.
    """
    f5,f15,f1,f4=m['5m'],m['15m'],m['1h'],m['4h']
    score=0.0; flags=[]
    bullish=direction=='CALL'
    trend5=(f5['last']>m['vwap'] and f5['ema20']>f5['ema50'] and f5['macd_delta']>0) if bullish else (f5['last']<m['vwap'] and f5['ema20']<f5['ema50'] and f5['macd_delta']<0)
    trend15=(f15['last']>f15['ema20'] and f15['macd_delta']>0) if bullish else (f15['last']<f15['ema20'] and f15['macd_delta']<0)
    trend1=(f1['last']>f1['ema20']>f1['ema50']) if bullish else (f1['last']<f1['ema20']<f1['ema50'])
    breakout=m['breakout']==('UP' if bullish else 'DOWN')
    volume=m['volume_ratio']>=EXPLOSIVE_MIN_VOLUME_RATIO
    strong_volume=m['volume_ratio']>=2.0
    acceleration=abs(f5['slope']) >= max(f5['atr']*0.35, f5['last']*0.0008)
    delta_ok=abs(delta)>=0.35
    tight=spread<=12
    liquid=vol>=20 and oi>=20
    near_term=dte<=EXPLOSIVE_MAX_DTE
    trending=m['regime']=='TRENDING'
    if trend5: score+=22; flags.append('5M momentum')
    if trend15: score+=14; flags.append('15M momentum')
    if trend1: score+=10; flags.append('1H alignment')
    if breakout: score+=18; flags.append('BREAKOUT')
    if volume: score+=12; flags.append('VOLUME EXPANSION')
    if strong_volume: score+=5; flags.append('VOLUME SPIKE')
    if acceleration: score+=8; flags.append('PRICE ACCELERATION')
    if delta_ok: score+=5; flags.append('GAMMA/DELTA SENSITIVITY')
    if tight: score+=4; flags.append('LIQUID SPREAD')
    if liquid: score+=4; flags.append('OPTION LIQUIDITY')
    if near_term: score+=3; flags.append('NEAR-TERM EXPIRY')
    if trending: score+=5; flags.append('TREND REGIME')
    if m['regime']=='SIDEWAYS': score-=20
    elif m['regime']=='CHOPPY': score-=10
    return round(max(0,min(100,score)),1), flags


def scan_underlying(symbol, session, contract_prefix=None, caches=None, option_underlying=None):
    # `symbol` is the technical-data symbol; `option_underlying` can override the
    # Alpaca options root. This is used for SPXW: SPY supplies IEX technical bars
    # while SPX supplies the actual SPXW option chain.
    m=multi_tf(symbol,caches); chain_root=option_underlying or symbol; progress('fetching_options', symbol=chain_root); chain=option_chain(chain_root, root_symbol=None); rows=chain.get('snapshots') or {}; out=[]; today=datetime.now(timezone.utc).date()
    rej={'prefix':0,'bad_contract':0,'dte':0,'premium':0,'spread':0,'liquidity':0,'score':0,'alignment':0,'regime':0,'relaxed_candidates':0,'after_hours_candidates':0}
    progress('scoring', symbol=symbol, contracts=len(rows))
    for contract,snap in rows.items():
        details=snap.get('details') or {}
        exp,strike,typ=parse_contract(contract,details)
        # SPXW is often returned by Alpaca inside the SPX option chain.
        # Depending on the endpoint/snapshot payload, root_symbol may be SPX,
        # SPXW, or omitted. For SPXW scans, do not require a literal SPXW
        # prefix (that was causing every contract to be rejected).
        if contract_prefix:
            root=(details.get('root_symbol') or details.get('rootSymbol') or details.get('underlying_symbol') or details.get('underlyingSymbol') or '').upper().strip()
            if option_underlying=='SPX':
                # Alpaca documents that SPXW weekly contracts are listed under
                # the SPX underlier. Snapshot payloads may expose the root as SPX,
                # SPXW, or omit it, so a literal prefix filter can incorrectly
                # discard the entire chain. For SPX scans, accept the SPX chain
                # here and identify the weekly product using metadata/expiry when
                # available. This keeps candidates flowing even when the snapshot
                # omits the weekly root metadata.
                is_spx_family = root in ('', 'SPX', 'SPXW') or contract.upper().startswith(('SPX','SPXW'))
                if not is_spx_family:
                    rej['prefix']+=1; continue
            elif not contract.upper().startswith(contract_prefix):
                rej['prefix']+=1; continue
        if not exp or not strike or typ not in ('CALL','PUT'): rej['bad_contract']+=1; continue
        try:dte=(datetime.fromisoformat(exp).date()-today).days
        except Exception: rej['bad_contract']+=1; continue
        if dte<0 or dte>30: rej['dte']+=1; continue
        quote=snap.get('latestQuote') or {}; trade=snap.get('latestTrade') or {}; greeks=snap.get('greeks') or {}
        bid=num(quote.get('bp')); ask=num(quote.get('ap')); last=num(trade.get('p')); premium=(bid+ask)/2 if bid>0 and ask>0 else last
        is_spxw = bool(contract_prefix and option_underlying=='SPX')
        # SPXW contracts are structurally more expensive than single-stock
        # options, so use a separate envelope. User env vars still override.
        premium_floor = float(os.getenv('SPXW_MIN_PREMIUM','0.50')) if is_spxw else MIN_PREMIUM
        premium_cap = float(os.getenv('SPXW_MAX_PREMIUM','75.00')) if is_spxw else MAX_PREMIUM
        affordable_cap = float(os.getenv('SPXW_MAX_AFFORDABLE_PREMIUM','75.00')) if is_spxw else MAX_AFFORDABLE_PREMIUM
        spread_cap = float(os.getenv('SPXW_MAX_SPREAD_PCT','35')) if is_spxw else MAX_SPREAD
        oi_floor = int(os.getenv('SPXW_MIN_OI','5')) if is_spxw else MIN_OI
        vol_floor = int(os.getenv('SPXW_MIN_VOLUME','1')) if is_spxw else MIN_VOL
        after_hours = str(session).upper() in ('AFTER_HOURS','CLOSED')
        ah_cap = AFTER_HOURS_MAX_PREMIUM if after_hours else premium_cap
        ah_affordable = AFTER_HOURS_MAX_PREMIUM if after_hours else affordable_cap
        if premium<premium_floor or premium>ah_cap or premium>ah_affordable: rej['premium']+=1; continue
        spread=((ask-bid)/premium*100) if premium and ask>=bid else 999
        daily=snap.get('dailyBar') or snap.get('daily_bar') or {}; vol=int(num(daily.get('v'))); oi=int(num(details.get('open_interest') or details.get('openInterest')))
        if vol<=0:vol=int(num(trade.get('s'))) if trade else 0
        strict_liquidity = spread<=spread_cap and vol>=vol_floor and oi>=oi_floor
        relaxed_liquidity = spread<=max(spread_cap, RELAXED_MAX_SPREAD) and vol>=min(vol_floor, RELAXED_MIN_VOL) and oi>=min(oi_floor, RELAXED_MIN_OI)
        # After-hours option quotes can be stale and spreads/OI/volume can be
        # incomplete. Do not throw away every technically valid contract just
        # because the market is closed; score liquidity as a risk factor instead.
        after_hours_liquidity = after_hours and spread<=AFTER_HOURS_MAX_SPREAD and vol>=AFTER_HOURS_MIN_VOL and oi>=AFTER_HOURS_MIN_OI
        # Momentum lane: do not discard a potentially explosive setup solely because
        # displayed OI/volume are stale or incomplete.  We still require a bounded
        # spread and a strong underlying/contract score before allowing an alert.
        momentum_liquidity = (spread <= float(os.getenv('MOMENTUM_MAX_SPREAD_PCT','150'))
                              and (after_hours or vol >= int(os.getenv('MOMENTUM_MIN_VOLUME','0')))
                              and (after_hours or oi >= int(os.getenv('MOMENTUM_MIN_OI','0'))))
        if not strict_liquidity and not relaxed_liquidity and not after_hours_liquidity and not momentum_liquidity:
            rej['spread' if spread>RELAXED_MAX_SPREAD else 'liquidity']+=1; continue
        delta=num(greeks.get('delta'),0.25 if typ=='CALL' else -0.25); direction=typ
        score,reasons,a4,a1,a15,a5=score_setup(m,direction,spread,delta,vol,oi)
        explosive_score, explosive_flags = explosive_setup_score(m,direction,spread,delta,vol,oi,dte)
        explosive_ok = (explosive_score >= EXPLOSIVE_MIN_SCORE and (relaxed_liquidity or after_hours_liquidity or momentum_liquidity) and m['regime'] not in ('SIDEWAYS','CHOPPY'))
        strict_ok = score>=MIN_SCORE and (a4 or not REQUIRE_4H_ALIGNMENT) and (m['regime'] not in ('SIDEWAYS','CHOPPY') or not BLOCK_SIDEWAYS_CHOPPY)
        relaxed_ok = score>=FALLBACK_MIN_SCORE and (relaxed_liquidity or after_hours_liquidity or momentum_liquidity) and (a4 or not REQUIRE_4H_ALIGNMENT) and (m['regime'] not in ('SIDEWAYS','CHOPPY') or not BLOCK_SIDEWAYS_CHOPPY)
        after_hours_ok = after_hours and score>=FALLBACK_MIN_SCORE and (after_hours_liquidity or momentum_liquidity) and (a4 or not REQUIRE_4H_ALIGNMENT) and (m['regime'] not in ('SIDEWAYS','CHOPPY') or not BLOCK_SIDEWAYS_CHOPPY)
        if not strict_ok and not relaxed_ok and not explosive_ok and not after_hours_ok:
            if score<MIN_SCORE: rej['score']+=1
            if REQUIRE_4H_ALIGNMENT and not a4: rej['alignment']+=1
            if BLOCK_SIDEWAYS_CHOPPY and m['regime'] in ('SIDEWAYS','CHOPPY'): rej['regime']+=1
            continue
        relaxed = not strict_ok and not explosive_ok and not after_hours_ok
        if explosive_ok:
            reasons.append('💥 EXPLOSIVE MOMENTUM SETUP')
            reasons.extend(explosive_flags)
        elif after_hours_ok:
            rej['after_hours_candidates']+=1
            reasons.append('🌙 AFTER-HOURS HERO FALLBACK — verify live spread at open')
        elif relaxed:
            rej['relaxed_candidates']+=1
            reasons.append('RELAXED LIQUIDITY/SCORE FALLBACK')
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
        extreme_upside=projected_upside_pct>=UPSIDE_ALERT_PCT and premium<=affordable_cap
        if extreme_upside: reasons.append(f'projected upside > {UPSIDE_ALERT_PCT:.0f}%')
        out.append({'signal':direction,'market':'OPTIONS','symbol':symbol,'contract':contract,'dte':dte,'premium':round(premium,2),'bid':round(bid,2),'ask':round(ask,2),'entry_low':entry_low,'entry_high':entry_high,'stop_loss':stop,'tp1':tp1,'tp2':tp2,'tp3':tp3,'risk_dollars_per_contract':round(risk*100,2),'suggested_contracts':contracts,'affordable':contracts>0,'max_loss':max_loss,'expected_profit_tp1':reward1,'expected_profit_tp2':reward2,'expected_profit_tp3':reward3,'underlying_entry':u_entry,'underlying_stop_loss':u_stop,'underlying_tp1':u_tp1,'underlying_tp2':u_tp2,'underlying_tp3':u_tp3,'atr_5m_points':round(atr_points,2),'score':round(score,1),'explosive_score':explosive_score,'explosive_setup':bool(explosive_ok),'explosive_flags':explosive_flags,'confidence':confidence,'tp1_confidence':tp1_conf,'tp2_confidence':tp2_conf,'tp3_confidence':tp3_conf,'projected_underlying_target':projected_underlying,'projected_premium':round(projected_premium,2),'projected_upside_pct':projected_upside_pct,'extreme_upside':extreme_upside,'market_regime':m['regime'],'volume':vol,'open_interest':oi,'spread_pct':round(spread,1),'delta':round(delta,3),'underlying':u_entry,'indicator_symbol':symbol,'option_underlying':chain_root,'vwap_state':'BULLISH' if u_entry>m['vwap'] else 'BEARISH','ema_state':'BULLISH' if m['5m']['ema20']>m['5m']['ema50'] else 'BEARISH','rsi':round(m['5m']['rsi'],1),'macd_state':'BULLISH' if m['5m']['macd_delta']>0 else 'BEARISH','volume_ratio':round(m['volume_ratio'],2),'breakout':m['breakout'],'trend_4h':'BULLISH' if m['4h']['last']>m['4h']['ema20']>m['4h']['ema50'] else 'BEARISH' if m['4h']['last']<m['4h']['ema20']<m['4h']['ema50'] else 'NEUTRAL','trend_1h':'BULLISH' if m['1h']['last']>m['1h']['ema20']>m['1h']['ema50'] else 'BEARISH' if m['1h']['last']<m['1h']['ema20']<m['1h']['ema50'] else 'NEUTRAL','trend_15m':'BULLISH' if m['15m']['last']>m['15m']['ema20'] else 'BEARISH' if m['15m']['last']<m['15m']['ema20'] else 'NEUTRAL','trend_5m':'BULLISH' if m['5m']['last']>m['vwap'] and m['5m']['ema20']>m['5m']['ema50'] else 'BEARISH' if m['5m']['last']<m['vwap'] and m['5m']['ema20']<m['5m']['ema50'] else 'NEUTRAL','adx_4h':round(m['4h']['adx'],1),'rsi_4h':round(m['4h']['rsi'],1),'reasons':reasons,'session':session,'data_mode':options_data_mode()})
    return out, {'chain_items':len(rows),'scored':len(out), 'parse_note':'OCC fallback enabled', 'chain_fallback':bool(chain.get('fallback')), 'chain_pages':chain.get('pages',0), 'contracts_discovered':chain.get('contracts_discovered',0), 'primary_chain_error':chain.get('primary_error'),'underlying':chain_root,'indicator_source':'Alpaca IEX multi-timeframe 5m/15m/1h/4h','option_source':'Alpaca options '+os.getenv('ALPACA_OPTIONS_FEED','indicative'),'market_regime':m['regime'],'rejections':rej,'config':{'after_hours_max_premium':AFTER_HOURS_MAX_PREMIUM,'after_hours_max_spread':AFTER_HOURS_MAX_SPREAD,'momentum_max_spread':float(os.getenv('MOMENTUM_MAX_SPREAD_PCT','150')),'explosive_min_score':EXPLOSIVE_MIN_SCORE,'explosive_min_volume_ratio':EXPLOSIVE_MIN_VOLUME_RATIO,'explosive_max_dte':EXPLOSIVE_MAX_DTE,'spxw_max_premium':float(os.getenv('SPXW_MAX_PREMIUM','75.00')),'spxw_max_spread':float(os.getenv('SPXW_MAX_SPREAD_PCT','35')),'spxw_min_oi':int(os.getenv('SPXW_MIN_OI','5')),'spxw_min_volume':int(os.getenv('SPXW_MIN_VOLUME','1')),'min_score':MIN_SCORE,'fallback_min_score':FALLBACK_MIN_SCORE,'require_4h_alignment':REQUIRE_4H_ALIGNMENT,'block_sideways_choppy':BLOCK_SIDEWAYS_CHOPPY,'max_spread':MAX_SPREAD,'relaxed_max_spread':RELAXED_MAX_SPREAD,'min_oi':MIN_OI,'relaxed_min_oi':RELAXED_MIN_OI,'min_volume':MIN_VOL,'relaxed_min_volume':RELAXED_MIN_VOL},'trend_4h':'BULLISH' if m['4h']['last']>m['4h']['ema20']>m['4h']['ema50'] else 'BEARISH' if m['4h']['last']<m['4h']['ema20']<m['4h']['ema50'] else 'NEUTRAL','trend_1h':'BULLISH' if m['1h']['last']>m['1h']['ema20']>m['1h']['ema50'] else 'BEARISH' if m['1h']['last']<m['1h']['ema20']<m['1h']['ema50'] else 'NEUTRAL','trend_15m':'BULLISH' if m['15m']['last']>m['15m']['ema20'] else 'BEARISH' if m['15m']['last']<m['15m']['ema20'] else 'NEUTRAL','trend_5m':'BULLISH' if m['5m']['last']>m['vwap'] and m['5m']['ema20']>m['5m']['ema50'] else 'BEARISH' if m['5m']['last']<m['vwap'] and m['5m']['ema20']<m['5m']['ema50'] else 'NEUTRAL'}

def scan_all(session):
    if not headers():raise RuntimeError('Missing ALPACA_API_KEY / ALPACA_API_SECRET')
    results=[]; diagnostics={}; symbols=list(dict.fromkeys(STOCKS+(['SPX'] if 'SPXW' in INDEX_ROOTS else [])))
    periods={'5m':('5Min',7),'15m':('15Min',20),'1h':('1Hour',45),'4h':('4Hour',180)}; caches={}
    progress('fetching_bars', symbols_total=len(symbols), timeframes=len(periods), completed=0)
    # Fetch the four timeframes concurrently. Each request still has its own
    # HTTP timeout/retry policy, but a slow timeframe no longer blocks the
    # other three before they can start.
    def _fetch_period(item):
        key,(tf,days)=item
        progress('fetching_bars', timeframe=key, state='started', symbols_total=len(symbols))
        try:
            data=fetch_bars_batch(symbols,tf,days)
            return key,data,None
        except Exception as e:
            return key,{},e

    completed=0
    with ThreadPoolExecutor(max_workers=min(4,len(periods))) as ex:
        futures=[ex.submit(_fetch_period,item) for item in periods.items()]
        for fut in as_completed(futures):
            key,data,err=fut.result()
            caches[key]=data
            completed += 1
            if err is not None:
                diagnostics[f'__{key}']={'error':f'{type(err).__name__}: {err}','provider':'Alpaca','endpoint':getattr(err,'endpoint',''),'status_code':getattr(err,'status_code',None),'retry_count':getattr(err,'retry_count',None)}
                progress('fetching_bars', timeframe=key, state='failed', symbols=len(data), completed=completed, error=str(err))
            else:
                progress('fetching_bars', timeframe=key, state='complete', symbols=len(data), completed=completed)

    # Repair partial/missing timeframe caches before scoring. Native 4H data can
    # be sparse; lower timeframe aggregation is more reliable than dropping the
    # symbol from the scan.
    repair_plan=[('15m','5m',15,7),('1h','15m',60,20),('4h','1h',240,45)]
    for target,source_tf,minutes,src_days in repair_plan:
        target_cache=caches.setdefault(target,{})
        source_cache=caches.setdefault(source_tf,{})
        for sym in symbols:
            good=len([b for b in (target_cache.get(sym) or []) if num(b.get('c'))>0])
            if good>=55:
                continue
            src=source_cache.get(sym) or []
            if len([b for b in src if num(b.get('c'))>0])<55:
                try:
                    fetched=fetch_bars(sym, {'5m':'5Min','15m':'15Min','1h':'1Hour'}[source_tf], src_days, limit=2000)
                    if fetched:
                        source_cache[sym]=fetched; src=fetched
                except Exception as e:
                    progress('bars_repair_fetch_failed',symbol=sym,target=target,source=source_tf,error=str(e)[:300])
            agg=_aggregate_bars(src,minutes)
            if len([b for b in agg if num(b.get('c'))>0])>=55:
                target_cache[sym]=agg
                progress('bars_repaired',symbol=sym,target=target,source=source_tf,bars=len(agg))

    progress('fetching_bars_complete', symbols=len(symbols), completed=completed)
    symbols_scanned=0; contracts_scanned=0
    for sym in STOCKS:
        try:
            progress('scoring', symbol=sym, symbols_scanned=symbols_scanned, contracts_scanned=contracts_scanned)
            r,d=scan_underlying(sym,session,caches=caches); results.extend(r); diagnostics[sym]=d
            symbols_scanned += 1; contracts_scanned += int(d.get('chain_items',0)); progress('scoring', symbol=sym, symbols_scanned=symbols_scanned, contracts_scanned=contracts_scanned, candidates=len(results))
        except Exception as e:
            diagnostics[sym]={'error':f'{type(e).__name__}: {e}','provider':'Alpaca',
                              'endpoint':getattr(e,'endpoint',''),
                              'status_code':getattr(e,'status_code',None),
                              'retry_count':getattr(e,'retry_count',None),
                              'detail':repr(e)}
            symbols_scanned += 1
    if 'SPXW' in INDEX_ROOTS:
        try:
            progress('scoring', symbol='SPXW', symbols_scanned=symbols_scanned, contracts_scanned=contracts_scanned)
            # SPX has no normal equity/IEX bar stream. Use SPY only for technical
            # context, but fetch the real SPXW chain from the SPX options root.
            proxy_caches={k:dict(v or {}) for k,v in caches.items()}
            r,d=scan_underlying('SPY',session,contract_prefix='SPXW',caches=proxy_caches,option_underlying='SPX')
            for x in r:
                x['symbol']='SPXW'; x['indicator_proxy']='SPY'; x['option_underlying']='SPX'
            results.extend(r); diagnostics['SPXW']=d; contracts_scanned += int(d.get('chain_items',0)); symbols_scanned += 1; progress('scoring', symbol='SPXW', symbols_scanned=symbols_scanned, contracts_scanned=contracts_scanned, candidates=len(results))
        except Exception as e:
            diagnostics['SPXW']={'error':f'{type(e).__name__}: {e}','provider':'Alpaca','endpoint':getattr(e,'endpoint',''),'status_code':getattr(e,'status_code',None),'retry_count':getattr(e,'retry_count',None),'detail':repr(e)}
    progress('sorting', candidates=len(results))
    results.sort(key=lambda x:(x.get('explosive_setup',False),x.get('explosive_score',0),x['extreme_upside'],x['projected_upside_pct'],x['score'],x['confidence'],x['volume'],x['open_interest']),reverse=True)
    diagnostics['__meta__']={'symbols_scanned':symbols_scanned,'contracts_scanned':contracts_scanned,'candidates':len(results),'provider':provider_status()}
    return results,diagnostics

def options_data_mode():
    feed=str(os.getenv('ALPACA_OPTIONS_FEED','indicative')).strip().lower()
    return {'opra':'LIVE','live':'LIVE','delayed':'DELAYED','indicative':'INDICATIVE'}.get(feed,'UNKNOWN')

def provider_status():
    feed=os.getenv('ALPACA_OPTIONS_FEED','indicative')
    mode=str(feed).lower()
    return {'name':'Alpaca','configured':bool(os.getenv('ALPACA_API_KEY') and os.getenv('ALPACA_API_SECRET')),
            'options_feed':feed,'effective_fallback':'indicative' if feed.lower()=='opra' else feed,
            'underlying_feed':'iex','data_mode':options_data_mode(),
            'note':'OPRA is attempted first; if the account/feed rejects it, the scanner can fall back to indicative discovery.' if feed.lower()=='opra' else
                  ('Free Alpaca options data may be delayed/indicative; IEX equity feed is real-time.' if mode in ('indicative','delayed','snapshot')
                   else 'Options feed mode is configured explicitly; verify entitlement before treating it as real-time.') }
