import os, math, statistics, time, threading, re
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone, timedelta
from typing import Callable, Optional
import requests

ALPACA = 'https://data.alpaca.markets/v2'
OPTIONS = 'https://data.alpaca.markets/v1beta1/options'
STOCKS = [s.strip().upper() for s in os.getenv('STOCK_SYMBOLS','QQQ,NVDA,AMD,TSLA,AAPL,AMZN,META,MSFT,GOOGL,MU,AVGO,PLTR,SMCI,SPY,IWM').split(',') if s.strip()]
INDEX_ROOTS = [s.strip().upper() for s in os.getenv('INDEX_ROOTS','SPXW,NDX').split(',') if s.strip()]
DEFAULT_STOCKS = ['SPY','QQQ','IWM','NVDA','AMD','TSLA','AAPL','AMZN','META','MSFT','GOOGL','MU','AVGO','PLTR','SMCI']
COMPANY_ALIASES = {
    'TESLA':'TSLA','APPLE':'AAPL','AMAZON':'AMZN','NVIDIA':'NVDA','MICROSOFT':'MSFT','GOOGLE':'GOOGL','ALPHABET':'GOOGL',
    'META PLATFORMS':'META','FACEBOOK':'META','ADVANCED MICRO DEVICES':'AMD','PALANTIR':'PLTR','BROADCOM':'AVGO','MICRON':'MU',
    'BLOOM ENERGY':'BE','BLOOM':'BE','BERKSHIRE':'BRK.B','NETFLIX':'NFLX','INTEL':'INTC','SUPERMICRO':'SMCI',
    'NASDAQ':'NDX','NASDAQ 100':'NDX','NASDAQ100':'NDX','ناسداك':'NDX','ناسداك 100':'NDX',
    'S&P 500':'SPXW','SP500':'SPXW','SPX':'SPXW','ستاندرد اند بورز':'SPXW',
}
RISK = float(os.getenv('RISK_BUDGET','100'))
MIN_PREMIUM = float(os.getenv('MIN_PREMIUM','0.20')); MAX_PREMIUM = float(os.getenv('MAX_PREMIUM','5.00'))
# V14.3.1 scoring/config aliases. Existing V13.8 names remain supported.
MIN_OPTION_VOLUME = int(os.getenv('MIN_OPTION_VOLUME', os.getenv('MIN_VOLUME','5')))
MIN_OPEN_INTEREST = int(os.getenv('MIN_OPEN_INTEREST', os.getenv('MIN_OI','10')))
MAX_SPREAD_PCT = float(os.getenv('MAX_SPREAD_PCT','35'))
MIN_SCORE_HERO = float(os.getenv('MIN_SCORE_HERO', os.getenv('HERO_SCORE','90')))
MIN_SCORE_STRONG = float(os.getenv('MIN_SCORE_STRONG', os.getenv('STRONG_SCORE','80')))
MIN_SCORE_WATCH = float(os.getenv('MIN_SCORE_WATCH', os.getenv('WATCH_SCORE','50')))
MAX_QUOTE_AGE = int(os.getenv('MAX_QUOTE_AGE', os.getenv('QUOTE_STALE_SECONDS','300')))
MAX_CONCURRENCY = max(1, int(os.getenv('MAX_CONCURRENCY','4')))
DEBUG_SCANNER = os.getenv('DEBUG_SCANNER','false').strip().lower() in ('1','true','yes','on')
DEBUG_BYPASS_SCORING = os.getenv('DEBUG_BYPASS_SCORING','false').strip().lower() in ('1','true','yes','on')
DEBUG_SAMPLE = os.getenv('DEBUG_SAMPLE','false').strip().lower() in ('1','true','yes','on')
UPSIDE_ALERT_PCT = float(os.getenv('UPSIDE_ALERT_PCT','1000'))
MAX_AFFORDABLE_PREMIUM = float(os.getenv('MAX_AFFORDABLE_PREMIUM','25.00'))
MOONSHOT_MIN_PREMIUM = max(0.25, float(os.getenv('MOONSHOT_MIN_PREMIUM','0.25')))
MOONSHOT_MIN_UPSIDE_PCT = float(os.getenv('MOONSHOT_MIN_UPSIDE_PCT','1000'))
MOONSHOT_MIN_SCORE = float(os.getenv('MOONSHOT_MIN_SCORE','65'))
MOONSHOT_MIN_EXPLOSIVE_SCORE = float(os.getenv('MOONSHOT_MIN_EXPLOSIVE_SCORE','78'))
MAX_SPREAD = MAX_SPREAD_PCT; MIN_OI = MIN_OPEN_INTEREST; MIN_VOL = MIN_OPTION_VOLUME
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
ALERT_COOLDOWN = int(os.getenv('ALERT_COOLDOWN_SECONDS','300'))
HTTP_TIMEOUT = max(1, float(os.getenv('ALPACA_HTTP_TIMEOUT','8')))
HTTP_CONNECT_TIMEOUT = max(1, float(os.getenv('ALPACA_CONNECT_TIMEOUT','3')))
RETRIES = max(0, int(os.getenv('ALPACA_RETRIES','2')))
STOCKS_PAGE_TIMEOUT = max(1, float(os.getenv('STOCKS_PAGE_TIMEOUT_SECONDS','12')))
OPTIONS_PAGE_LIMIT = max(100, min(1000, int(os.getenv('OPTIONS_PAGE_LIMIT','1000'))))
OPTIONS_MAX_PAGES = max(1, int(os.getenv('OPTIONS_MAX_PAGES','100')))
OPTIONS_MIN_INTERVAL = max(0.05, float(os.getenv('OPTIONS_MIN_INTERVAL_SECONDS','0.20')))
OPTIONS_SNAPSHOT_BATCH_SIZE = max(10, min(50, int(os.getenv('OPTIONS_SNAPSHOT_BATCH_SIZE','25'))))
OPTIONS_FEED_RETRIES = max(0, int(os.getenv('OPTIONS_FEED_RETRIES','1')))
OPTIONS_FEED_TIMEOUT = max(1.0, float(os.getenv('OPTIONS_FEED_TIMEOUT_SECONDS','6')))
OPTIONS_CHAIN_TIMEOUT = max(10.0, float(os.getenv('OPTIONS_CHAIN_TIMEOUT_SECONDS','30')))
STOCKS_PAGE_LIMIT = max(100, min(10000, int(os.getenv('STOCKS_PAGE_LIMIT','10000'))))
STOCKS_MAX_PAGES = max(1, int(os.getenv('STOCKS_MAX_PAGES','5')))
# V13.8 setup classification / risk controls. These are soft-scoring thresholds
# except the explicit hard limits below.
HERO_SCORE = MIN_SCORE_HERO
STRONG_SCORE = MIN_SCORE_STRONG
WATCH_SCORE = MIN_SCORE_WATCH
WATCH_MIN_DIRECTION = int(os.getenv('WATCH_MIN_DIRECTION_EVIDENCE','2'))
HARD_MAX_SPREAD = float(os.getenv('HARD_MAX_SPREAD_PCT','150'))
HARD_MAX_PREMIUM_MULTIPLIER = float(os.getenv('HARD_MAX_PREMIUM_MULTIPLIER','10'))
QUOTE_STALE_SECONDS = int(os.getenv('QUOTE_STALE_SECONDS','300'))
AFTER_HOURS_STALE_SECONDS = int(os.getenv('AFTER_HOURS_STALE_SECONDS','3600'))
REQUIRE_QUOTE_TIMESTAMP = os.getenv('REQUIRE_QUOTE_TIMESTAMP','false').strip().lower() in ('1','true','yes','on')
MAX_DTE = max(0, int(os.getenv('MAX_DTE','21')))
MIN_DTE = max(0, int(os.getenv('MIN_DTE','0')))
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
    # Progress updates are telemetry only and must never abort a market scan.
    # Normalize through a single dictionary so future callers cannot accidentally
    # forward duplicate keyword fields to the callback.
    cb = _progress
    if not cb:
        return
    payload = dict(fields or {})
    payload.pop('_stage', None)
    try:
        cb(stage, **payload)
    except TypeError as e:
        # A telemetry callback must not turn a successful scan into a fatal scan
        # error. Retry once with only the canonical, non-conflicting fields.
        try:
            safe = dict(payload)
            safe.pop('candidates', None)
            if 'candidates' in payload:
                safe['candidates'] = payload['candidates']
            cb(stage, **safe)
        except Exception:
            pass
    except Exception:
        pass


def debug_rejection(reason, **fields):
    if DEBUG_SCANNER:
        progress('contract_rejected', reason=reason, **fields)


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


def req(url, params=None, timeout=None, retries=None):
    timeout = HTTP_TIMEOUT if timeout is None else timeout
    max_retries = RETRIES if retries is None else max(0, int(retries))
    last=None
    endpoint=url.replace(ALPACA, '').replace(OPTIONS, '/options')
    for attempt in range(max_retries + 1):
        try:
            _pace_options_request(url)
            r=_session.get(url, headers=headers(), params=params or {}, timeout=(HTTP_CONNECT_TIMEOUT, timeout))
            if r.status_code in (401, 403):
                raise ProviderRequestError(f'Alpaca HTTP {r.status_code}', endpoint=endpoint, status_code=r.status_code, retry_count=attempt)
            if r.status_code == 429:
                if attempt < max_retries:
                    retry_after = r.headers.get('Retry-After')
                    try: delay=min(5.0, max(0.5, float(retry_after)))
                    except Exception: delay=min(5.0, 1.0 * (attempt + 1))
                    progress('provider_retry', endpoint=endpoint, status_code=429, retry=attempt + 1)
                    time.sleep(delay)
                    continue
                raise ProviderRequestError('Alpaca HTTP 429 rate limited', endpoint=endpoint, status_code=429, retry_count=attempt)
            if 500 <= r.status_code <= 599:
                if attempt < max_retries:
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
            if e.status_code in (401,403,429) or attempt >= max_retries:
                raise
        except (requests.Timeout, requests.ConnectionError) as e:
            last=e
            if attempt < max_retries:
                progress('provider_retry', endpoint=endpoint, status_code='timeout/connection', retry=attempt + 1)
                time.sleep(min(4.0, 0.8 * (attempt + 1)))
                continue
        except requests.RequestException as e:
            last=e
            if attempt >= max_retries: raise ProviderRequestError(type(e).__name__, endpoint=endpoint, retry_count=attempt, cause=e)
        except Exception as e:
            last=e
            if attempt >= max_retries: raise
        if attempt < max_retries:
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
    data=req(f'{ALPACA}/stocks/{symbol}/bars', {'timeframe':timeframe,'start':start_dt.isoformat().replace('+00:00','Z'),'end':end_dt.isoformat().replace('+00:00','Z'),'limit':limit,'feed':os.getenv('ALPACA_UNDERLYING_FEED','iex'),'sort':'asc'}, timeout=STOCKS_PAGE_TIMEOUT)
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
            'feed':os.getenv('ALPACA_UNDERLYING_FEED','iex'),
            'sort':'asc'
        }
        if page_token:
            params['page_token']=page_token
        page_no=pages+1
        started=time.monotonic()
        progress('fetching_bars_page', timeframe=timeframe, page=page_no,
                 symbols_total=len(symbols), symbols=len(merged), state='started')
        try:
            data=req(f'{ALPACA}/stocks/bars', params, timeout=STOCKS_PAGE_TIMEOUT)
        except Exception as e:
            elapsed=round(time.monotonic()-started,2)
            # A failed/slow page must never freeze the complete market scan.
            # Return any data already collected and let the scanner continue
            # with its per-symbol fallback/repair path.
            progress('bars_page_failed', timeframe=timeframe, page=page_no,
                     symbols=len(merged), elapsed=elapsed,
                     error=f'{type(e).__name__}: {e}'[:400])
            break
        elapsed=round(time.monotonic()-started,2)
        rows=data.get('bars') or {}
        page_rows=0
        for sym, bars in rows.items():
            vals=bars or []
            merged.setdefault(sym, []).extend(vals)
            page_rows += len(vals)
        pages += 1
        page_token=data.get('next_page_token')
        progress('fetching_bars_page', timeframe=timeframe, page=page_no,
                 symbols=len(merged), rows=page_rows, elapsed=elapsed,
                 state='complete', has_next=bool(page_token))
        if not page_token:
            break
    progress('fetching_bars_complete_page', timeframe=timeframe, page=pages,
             symbols=len(merged), pages=pages)
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

def frame_indicators(symbol,timeframe,days=45,cache=None,all_caches=None):
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
            allc=all_caches or {}
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
    f5=frame_indicators(symbol,'5Min',7,caches.get('5m'),caches); f15=frame_indicators(symbol,'15Min',20,caches.get('15m'),caches); f1=frame_indicators(symbol,'1Hour',45,caches.get('1h'),caches); f4=frame_indicators(symbol,'4Hour',180,caches.get('4h'),caches)
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
    configured = str(os.getenv('ALPACA_OPTIONS_FEED','opra')).strip().lower()
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
    while pages < int(os.getenv('OPTIONS_CONTRACTS_MAX_PAGES','20')):
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

    merged={}; batch_size=OPTIONS_SNAPSHOT_BATCH_SIZE; failed_batches=0
    for i in range(0,len(symbols),batch_size):
        batch=symbols[i:i+batch_size]
        data=None; feed_used=None; batch_errors=[]
        for feed in _options_feeds():
            for attempt in range(OPTIONS_FEED_RETRIES + 1):
                try:
                    progress('options_feed_fetch', underlying=underlying, feed=feed,
                             batch=(i//batch_size)+1, batch_total=(len(symbols)+batch_size-1)//batch_size,
                             batch_size=len(batch), attempt=attempt+1)
                    data=req(f'{OPTIONS}/snapshots', {
                        'symbols':','.join(batch),
                        'feed':feed,
                    }, timeout=OPTIONS_FEED_TIMEOUT, retries=0)
                    feed_used=feed
                    break
                except Exception as e:
                    batch_errors.append(f'{feed}[{attempt+1}]: {str(e)[:180]}')
                    if attempt < OPTIONS_FEED_RETRIES:
                        progress('options_feed_retry', underlying=underlying, feed=feed,
                                 batch=(i//batch_size)+1, retry=attempt+1, error=str(e)[:300])
            if data is not None:
                break
        if data is None:
            failed_batches += 1
            progress('options_feed_batch_failed', underlying=underlying,
                     batch=(i//batch_size)+1, error=' | '.join(batch_errors)[:500])
            continue
        snaps=data.get('snapshots') or {}
        for sym in batch:
            base=metadata.get(sym,{}); snap=snaps.get(sym) or {}
            if snap:
                snap.setdefault('details',{}).update(base.get('details') or {})
                snap.setdefault('_scanner_meta', {})['feed_used'] = feed_used
                merged[sym]=snap
    if not merged and failed_batches:
        raise ProviderRequestError('No option snapshots available after bounded feed retries',
                                   endpoint='/options/snapshots')
    return {'snapshots':merged,'pages':pages,'fallback':True,'contracts_discovered':len(symbols),
            'failed_batches':failed_batches,'feed_used':feed_used}

def option_chain(underlying, side=None, root_symbol=None, strike_hint=None, expiration_hint=None, type_hint=None):
    """Fetch option snapshots with hard bounded time/retries; never let one feed hang the scan."""
    today=datetime.now(timezone.utc).date()
    base_params={
        'limit':min(1000, OPTIONS_PAGE_LIMIT),
        'expiration_date_gte':today.isoformat(),
        'expiration_date_lte':(today+timedelta(days=30)).isoformat(),
    }
    if side: base_params['type']=side.lower()
    elif type_hint in ('CALL','PUT'): base_params['type']=type_hint.lower()
    if root_symbol: base_params['root_symbol']=root_symbol
    if expiration_hint: base_params['expiration_date']=str(expiration_hint)
    if strike_hint is not None:
        sh=float(strike_hint); width=max(0.01, sh*0.0025)
        base_params['strike_price_gte']=max(0,sh-width); base_params['strike_price_lte']=sh+width
    merged={}; page_token=None; pages=0; primary_error=None; feed_used=None
    deadline=time.monotonic()+OPTIONS_CHAIN_TIMEOUT
    try:
        for feed in _options_feeds():
            try:
                merged={}; page_token=None; pages=0
                while pages < OPTIONS_MAX_PAGES and time.monotonic() < deadline:
                    q=dict(base_params); q['feed']=feed
                    if page_token: q['page_token']=page_token
                    remaining=max(1.0, min(OPTIONS_FEED_TIMEOUT, deadline-time.monotonic()))
                    progress('options_feed_fetch', underlying=underlying, feed=feed,
                             page=pages+1, attempt=1, timeout=round(remaining,1))
                    data=req(f'{OPTIONS}/snapshots/{underlying}',q,timeout=remaining, retries=0)
                    rows=data.get('snapshots') or {}
                    merged.update(rows)
                    pages += 1
                    page_token=data.get('next_page_token')
                    if not page_token: break
                if merged:
                    feed_used=feed
                    break
            except Exception as e:
                primary_error=e
                progress('options_feed_retry', underlying=underlying, feed=feed,
                         error=str(e)[:300], elapsed=round(time.monotonic()-(deadline-OPTIONS_CHAIN_TIMEOUT),1))
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
        raise ProviderRequestError('Options feed returned no snapshots', endpoint=f'/options/snapshots/{underlying}')
    except Exception as primary_error:
        if str(os.getenv('OPTIONS_CHAIN_FALLBACK','false')).lower() not in ('1','true','yes','on'):
            raise
        progress('options_chain_fallback', underlying=underlying, error=str(primary_error)[:300])
        try:
            result=_option_contracts_fallback(underlying,side=side,root_symbol=root_symbol)
            result['primary_error']=str(primary_error)[:500]
            return result
        except Exception as fallback_error:
            progress('options_chain_failed', underlying=underlying,
                     error=f'primary={str(primary_error)[:220]} fallback={str(fallback_error)[:220]}')
            raise ProviderRequestError(
                f'Option chain failed; fallback also failed. primary={primary_error}; fallback={fallback_error}',
                endpoint=getattr(fallback_error,'endpoint',f'/options/snapshots/{underlying}'),
                status_code=getattr(fallback_error,'status_code',None),
                retry_count=getattr(fallback_error,'retry_count',None),
                cause=fallback_error
            )

def _optional_float(v):
    """Return None for missing/non-finite values; preserve legitimate zero."""
    if v is None or isinstance(v, bool):
        return None
    try:
        x=float(v)
        return x if math.isfinite(x) else None
    except (TypeError, ValueError):
        return None


def _optional_int(v):
    x=_optional_float(v)
    if x is None:
        return None
    try:
        return int(x)
    except Exception:
        return None


def parse_contract(sym, details):
    """Parse Alpaca contract metadata and OCC symbols for equity + index options."""
    details = details or {}
    exp = details.get('expiration_date') or details.get('expirationDate')
    strike_raw = details.get('strike_price') if details.get('strike_price') is not None else details.get('strikePrice')
    strike = _optional_float(strike_raw)
    typ = str(details.get('type') or details.get('option_type') or '').upper().strip()

    # OCC option symbols: ROOT + YYMMDD + C/P + 8-digit strike*1000.
    # Roots may be 1-6 letters (and Alpaca index roots such as SPXW).
    m = re.match(r'^([A-Z]{1,6})(\d{6})([CP])(\d{8})$', str(sym).upper().strip())
    if m:
        yy, mm, dd = int(m.group(2)[:2]), int(m.group(2)[2:4]), int(m.group(2)[4:6])
        try:
            exp = f"20{yy:02d}-{mm:02d}-{dd:02d}"
        except Exception:
            pass
        typ = 'CALL' if m.group(3) == 'C' else 'PUT'
        if strike is None:
            strike = int(m.group(4)) / 1000.0

    if typ in ('C','CALL'):
        typ='CALL'
    elif typ in ('P','PUT'):
        typ='PUT'

    return exp, strike, typ


def normalize_contract(raw, contract_symbol=None):
    """Normalize snapshot/contract variants without hiding parsing failures."""
    raw = raw or {}
    symbol = str(contract_symbol or raw.get('symbol') or raw.get('option_symbol') or raw.get('optionSymbol') or '').upper().strip()
    details = raw.get('details') if isinstance(raw.get('details'), dict) else {}
    if not details:
        details = raw
    exp, strike, typ = parse_contract(symbol, details)
    underlying = (details.get('underlying_symbol') or details.get('underlyingSymbol') or
                  details.get('underlying') or details.get('root_symbol') or details.get('rootSymbol'))
    underlying = str(underlying or '').upper().strip() or None
    if not symbol:
        return None, 'missing_symbol'
    if not exp:
        return None, 'invalid_expiration'
    if strike is None or strike <= 0:
        return None, 'invalid_strike'
    if not typ:
        return None, 'missing_type'
    if typ not in ('CALL','PUT'):
        return None, 'invalid_type'
    return {'symbol':symbol,'underlying':underlying,'expiration':exp,'strike':strike,'option_type':typ,'details':details}, None

def _direction_state(m, direction):
    """Return directional evidence without making 4H/regime a hard gate."""
    bullish = direction == 'CALL'
    f5, f15, f1, f4 = m['5m'], m['15m'], m['1h'], m['4h']
    checks = {
        '5M': (f5['last'] > m['vwap'] and f5['ema20'] > f5['ema50'] and f5['macd_delta'] >= 0) if bullish
              else (f5['last'] < m['vwap'] and f5['ema20'] < f5['ema50'] and f5['macd_delta'] <= 0),
        '15M': (f15['last'] > f15['ema20'] and f15['macd_delta'] >= 0) if bullish
               else (f15['last'] < f15['ema20'] and f15['macd_delta'] <= 0),
        '1H': (f1['last'] > f1['ema20'] > f1['ema50']) if bullish
              else (f1['last'] < f1['ema20'] < f1['ema50']),
        '4H': (f4['last'] > f4['ema20'] > f4['ema50'] and f4['macd_delta'] >= 0) if bullish
              else (f4['last'] < f4['ema20'] < f4['ema50'] and f4['macd_delta'] <= 0),
        'VWAP': m['5m']['last'] > m['vwap'] if bullish else m['5m']['last'] < m['vwap'],
        'BREAKOUT': m['breakout'] == ('UP' if bullish else 'DOWN'),
    }
    positive=sum(bool(v) for v in checks.values())
    opposite_checks = {
        '5M': (f5['last'] < m['vwap'] and f5['ema20'] < f5['ema50'] and f5['macd_delta'] < 0) if bullish
              else (f5['last'] > m['vwap'] and f5['ema20'] > f5['ema50'] and f5['macd_delta'] > 0),
        '15M': (f15['last'] < f15['ema20'] and f15['macd_delta'] < 0) if bullish
               else (f15['last'] > f15['ema20'] and f15['macd_delta'] > 0),
        '1H': (f1['last'] < f1['ema20'] < f1['ema50']) if bullish
              else (f1['last'] > f1['ema20'] > f1['ema50']),
        '4H': (f4['last'] < f4['ema20'] < f4['ema50']) if bullish
              else (f4['last'] > f4['ema20'] > f4['ema50']),
        'VWAP': m['5m']['last'] < m['vwap'] if bullish else m['5m']['last'] > m['vwap'],
    }
    opposite=sum(bool(v) for v in opposite_checks.values())
    return checks, positive, opposite


def score_setup(m, direction, spread, delta, vol, oi, dte=0, strike=None, is_spxw=False,
                premium=None, iv=None, theta=None, market_bias='NEUTRAL', gap_pct=0.0,
                expected_move_pct=None):
    """Composite 0-100 setup score using independent market, technical and option factors."""
    f5, f15, f1, f4 = m['5m'], m['15m'], m['1h'], m['4h']
    checks, positive, opposite = _direction_state(m, direction)
    reasons=[]; c={}
    bullish=direction=='CALL'
    # Direction + market confirmation: 20
    c['direction']=round(min(20.0, positive/6.0*15.0),1)
    if market_bias in ('BULLISH','BEARISH'):
        aligned=(market_bias=='BULLISH') == bullish
        c['market_alignment']=5.0 if aligned else -3.0
        reasons.append('Market bias confirms direction') if aligned else reasons.append('⚠️ Market bias opposes direction')
    else:
        c['market_alignment']=2.0
    # Momentum: 12
    mom=sum(bool(checks[k]) for k in ('5M','15M','VWAP','BREAKOUT'))
    c['momentum']=round(min(12.0,mom*3.0),1)
    if checks['BREAKOUT']: reasons.append('Breakout/breakdown trigger')
    if checks['VWAP']: reasons.append('Price aligned with VWAP')
    # Trend: 10
    trend=sum(bool(checks[k]) for k in ('4H','1H','15M'))
    c['trend']=round(min(10.0,trend*10/3),1)
    if checks['4H'] and checks['1H']: reasons.append('Multi-timeframe trend alignment')
    # Volume / RVOL: 10
    vr=max(0.0,float(m.get('volume_ratio',1.0) or 1.0))
    c['volume']=round(min(10.0,max(0.0,(vr-0.7)*5.0)),1)
    if vr>=2: reasons.append(f'High RVOL {vr:.1f}x')
    elif vr>=1.5: reasons.append(f'Positive RVOL {vr:.1f}x')
    # Liquidity spread: 10
    sp=spread if spread is not None else 40.0
    c['spread']=10.0 if sp<=8 else 8.0 if sp<=15 else 6.0 if sp<=25 else 3.0 if sp<=35 else 0.0
    if sp<=15: reasons.append('Usable bid/ask spread')
    # OI/activity: 8
    oi_n=max(0,oi or 0); vol_n=max(0,vol or 0)
    c['open_interest']=6.0 if oi_n>=500 else 5.0 if oi_n>=100 else 3.5 if oi_n>=25 else 1.5 if oi_n>0 else 0.5
    c['contract_volume']=2.0 if vol_n>=100 else 1.5 if vol_n>=25 else 1.0 if vol_n>=5 else 0.0
    if oi_n>=100: reasons.append('Meaningful open interest')
    # Delta: 6
    da=abs(delta or 0)
    c['delta']=6.0 if .45<=da<=.70 else 5.0 if .35<=da<=.80 else 3.0 if .20<=da<=.85 else 1.0
    if .35<=da<=.75: reasons.append('Actionable delta')
    # IV: 5 (high IV is not automatically good; score near a usable range)
    ivn=float(iv or 0)
    if ivn<=0: c['iv']=2.0
    elif ivn<=0.80: c['iv']=4.0
    elif ivn<=1.20: c['iv']=3.0
    elif ivn<=2.0: c['iv']=2.0
    else: c['iv']=1.0
    # DTE: 4
    d=int(dte or 0)
    c['dte']=4.0 if 3<=d<=14 else 3.0 if d in (0,1,2,15,16,17,18,19,20,21) else 1.0
    # Premium: 4, with the requested hard display band handled separately
    p=float(premium or 0)
    c['premium']=4.0 if .75<=p<=3.0 else 3.0 if .5<=p<=4.0 else 2.0
    # Technical levels / expected move / gap: 5
    atr_pct=(f5['atr']/max(f5['last'],1e-9))*100
    level=0.0
    if checks['BREAKOUT']: level+=2.0
    elif abs(f5['last']-m['recent_high'])/max(f5['last'],1e-9)<.003 or abs(f5['last']-m['recent_low'])/max(f5['last'],1e-9)<.003: level+=1.5
    if expected_move_pct is not None and atr_pct>0 and float(expected_move_pct)>atr_pct*.5: level+=1.0
    if abs(gap_pct)>=1.0 and ((gap_pct>0)==bullish): level+=1.0
    c['technical_levels']=min(5.0,level+min(1.0,max(0.0,atr_pct)))
    if abs(gap_pct)>=1.0: reasons.append(f'Gap {gap_pct:+.1f}%')
    # Regime penalty/bonus is included in a bounded 100-point total
    c['regime']=5.0 if m['regime']=='TRENDING' else 2.0 if m['regime']=='MIXED' else -2.0 if m['regime']=='CHOPPY' else -4.0
    if m['regime']=='TRENDING': reasons.append('Trending market regime')
    # Opposite direction penalty: keep both sides available, but make conflict expensive
    c['conflict']=-min(6.0,max(0.0,opposite-positive+1)*2.0)
    if opposite>positive: reasons.append('⚠️ Opposing technical evidence')
    raw=sum(c.values())
    score=max(0.0,min(100.0,raw))
    return round(score,1), reasons, checks['4H'], checks['1H'], checks['15M'], checks['5M'], c, positive, opposite

def premium_quality(premium, is_spxw=False):
    """Soft premium quality score. Security caps are handled separately."""
    if premium <= 0: return 0.0, 'invalid premium'
    budget_per_contract=max(RISK,1.0)
    # Reward usable premiums without claiming cheap = better. Very cheap options
    # get a small penalty for fragility; expensive options get a budget penalty.
    if premium < 0.50: score=2.0; label='very low premium / high sensitivity'
    elif premium < 1.00: score=5.0; label='low premium'
    elif premium <= max(5.0, budget_per_contract/100*0.75): score=8.0; label='balanced premium'
    elif premium <= max(10.0, budget_per_contract/100*1.5): score=6.0; label='higher premium'
    else: score=3.0; label='high premium / capital intensive'
    return score, label


def _quote_timestamp(snap, quote, trade):
    for source in (quote, trade, snap):
        for key in ('t','timestamp','time','updated_at','updatedAt'):
            value=source.get(key) if isinstance(source,dict) else None
            if value:
                try:
                    return datetime.fromisoformat(str(value).replace('Z','+00:00')).astimezone(timezone.utc)
                except Exception:
                    continue
    return None


def _classify(score, positive, opposite, risk_flags=None):
    risk_flags = risk_flags or []
    # Direction evidence influences score, but does not silently delete candidates.
    if score >= HERO_SCORE and not risk_flags:
        return 'HERO'
    if score >= STRONG_SCORE:
        return 'STRONG'
    if score >= WATCH_SCORE:
        return 'WATCH'
    return None

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



def moonshot_qualification(premium, projected_upside_pct, score, explosive_score, dte, spread=None,
                           volume=0, oi=0, rvol=1.0, market_regime='MIXED'):
    """MOONSHOT requires multiple simultaneous quality signals; cheap alone is never enough."""
    try:
        p=float(premium or 0); up=float(projected_upside_pct or 0); sc=float(score or 0)
        ex=float(explosive_score or 0); d=int(dte or 0); sp=float(spread) if spread is not None else 999
        v=float(volume or 0); o=float(oi or 0); rv=float(rvol or 0)
    except Exception:
        return False, []
    flags=[]
    if not (MIN_PREMIUM <= p <= MAX_PREMIUM): return False, []
    if not (0<=d<=EXPLOSIVE_MAX_DTE): return False, []
    if sp>35: return False, []
    if rv<1.5: return False, []
    if v<5 and o<25: return False, []
    if sc<60 or ex<70: return False, []
    if market_regime in ('SIDEWAYS','CHOPPY'): return False, []
    if up>=150: flags.append('model-upside>=150%')
    else: flags.append('high convexity setup')
    if p<=1.50: flags.append('low/medium premium')
    if rv>=2: flags.append('RVOL>=2x')
    if ex>=80: flags.append('explosive>=80')
    if sp<=20: flags.append('spread<=20%')
    if d<=7: flags.append('DTE<=7')
    return True, flags

def scan_underlying(symbol, session, contract_prefix=None, caches=None, option_underlying=None, market_context=None, strike_hint=None, expiration_hint=None, type_hint=None):
    """Scan one underlying. Validation is staged and every rejection is observable."""
    m = multi_tf(symbol, caches)
    _attach_gap_and_expected_move(m)
    chain_root = option_underlying or symbol
    is_spxw = bool(contract_prefix and option_underlying == 'SPX')
    progress('fetching_options', symbol=chain_root)
    market_context = market_context or {'bias':'NEUTRAL','breadth':None,'vix':None}
    chain = option_chain(chain_root, root_symbol=(contract_prefix if is_spxw else None), strike_hint=strike_hint, expiration_hint=expiration_hint, type_hint=type_hint)
    rows = chain.get('snapshots') or {}
    received = len(rows)
    qualified, scored_rows = [], []
    today = datetime.now(timezone.utc).date()

    rej = {k: 0 for k in (
        'bad_contract_schema','missing_symbol','invalid_expiration','expired','dte','invalid_strike',
        'missing_type','invalid_type','missing_quote','invalid_bid','invalid_ask','invalid_last',
        'zero_volume','zero_open_interest','spread_too_wide','premium_too_low','premium_too_high',
        'delta','technical','score','prefix','quote_stale','quote_fallback','no_bid','no_ask',
        'missing_last','missing_volume','missing_oi','invalid_quote','liquidity','price','spread',
        'alignment','regime','direction','premium','hard_premium','after_hours_candidates'
    )}
    quote_fresh = quote_stale = quote_with_data = 0
    quote_stage = 0
    quotes_received = 0
    bid_present = ask_present = last_present = volume_present = oi_present = 0
    normalized_count = 0
    dte_rejected = 0
    sample_done = False
    tier_counts = {'HERO':0,'STRONG':0,'WATCH':0}
    potential = 0
    hard_spread = float(os.getenv('ABSOLUTE_MAX_SPREAD_PCT','500'))
    configured_cap = float(os.getenv('SPXW_MAX_PREMIUM','75.00')) if is_spxw else max(MAX_PREMIUM,20.0)
    hard_premium_cap = max(configured_cap * max(2.0,HARD_MAX_PREMIUM_MULTIPLIER),100.0)

    progress('scoring', symbol=symbol, contracts=received)
    if DEBUG_SCANNER:
        print(f'DEBUG {symbol} | chain={received} | feed={chain.get("feed_used") or "—"}')

    for idx,(contract,snap) in enumerate(rows.items()):
        snap = snap if isinstance(snap,dict) else {}
        details = snap.get('details') if isinstance(snap.get('details'),dict) else {}
        normalized, norm_reason = normalize_contract(snap, contract)
        if DEBUG_SCANNER and idx < 2:
            print(f'DEBUG {symbol} Raw contract sample #{idx+1}: {contract} {snap if idx < 2 else ""}')
            print(f'DEBUG {symbol} Normalized: {normalized if normalized else norm_reason}')
        if normalized is None:
            rej['bad_contract_schema'] += 1
            if norm_reason in rej: rej[norm_reason] += 1
            debug_rejection(norm_reason or 'bad_contract_schema',symbol=symbol,contract=contract)
            continue
        normalized_count += 1
        exp = normalized['expiration']; strike = normalized['strike']; typ = normalized['option_type']
        if contract_prefix:
            root = str(details.get('root_symbol') or details.get('rootSymbol') or normalized.get('underlying') or '').upper().strip()
            if is_spxw:
                contract_upper = str(contract).upper().strip()
                # SPXW is a distinct weekly root. Do not let ordinary SPX
                # contracts leak into the SPXW result set. The Alpaca chain
                # endpoint is requested with root_symbol=SPXW as the primary
                # protection; this check is the defensive second layer.
                if not (root == 'SPXW' or contract_upper.startswith('SPXW')):
                    rej['prefix'] += 1; continue
            elif not str(contract).upper().startswith(contract_prefix):
                rej['prefix'] += 1; continue

        try:
            exp_date = datetime.fromisoformat(str(exp).replace('Z','+00:00')).date()
        except Exception:
            rej['invalid_expiration'] += 1
            continue
        dte = (exp_date - today).days
        if dte < 0:
            rej['expired'] += 1
            continue
        if dte < MIN_DTE or dte > MAX_DTE:
            rej['dte'] += 1; dte_rejected += 1
            continue
        quote_stage += 1

        quote = snap.get('latestQuote') or {}
        trade = snap.get('latestTrade') or {}
        greeks = snap.get('greeks') or {}
        bid = _optional_float(quote.get('bp'))
        ask = _optional_float(quote.get('ap'))
        last = _optional_float(trade.get('p'))
        if bid is not None: bid_present += 1
        if ask is not None: ask_present += 1
        if last is not None: last_present += 1
        if bid is None: rej['no_bid'] += 1
        if ask is None: rej['no_ask'] += 1
        if last is None: rej['missing_last'] += 1
        if bid is not None and bid < 0: rej['invalid_bid'] += 1; bid=None
        if ask is not None and ask < 0: rej['invalid_ask'] += 1; ask=None
        if last is not None and last < 0: rej['invalid_last'] += 1; last=None

        premium = None; quote_source='FALLBACK'
        if bid is not None and ask is not None and bid > 0 and ask > 0 and ask >= bid:
            premium=(bid+ask)/2.0; quote_source='BID_ASK'
            quotes_received += 1
        elif last is not None and last > 0:
            premium=last; quote_source='LAST'; quotes_received += 1; rej['quote_fallback'] += 1
        else:
            rej['missing_quote'] += 1
            # Preserve the contract in diagnostics instead of silently dropping it.
            scored_rows.append({'signal':typ,'tier':None,'symbol':symbol,'contract':contract,'underlying':m['5m']['last'],
                                'option_underlying':chain_root,'expiration':exp,'dte':dte,'strike':round(strike,2),
                                'premium':None,'bid':bid,'ask':ask,'last':last,'volume':0,'open_interest':0,
                                'spread_pct':None,'score':FALLBACK_MIN_SCORE,'qualification':'DATA_INCOMPLETE',
                                'quote_source':'NONE','quote_status':'INVALID','quote_is_stale':False,
                                'delta':None,'theta':None,'iv':None,'iv_rank':None,
                                'reasons':['Quote unavailable; contract retained for diagnostics and on-demand analysis.']})
            continue
        quote_with_data += 1

        qdt=_quote_timestamp(snap,quote,trade)
        quote_age_sec=None; stale=False
        if qdt is not None:
            quote_age_sec=max(0.0,(datetime.now(timezone.utc)-qdt).total_seconds())
            stale_limit=AFTER_HOURS_STALE_SECONDS if str(session).upper() in ('AFTER_HOURS','CLOSED') else MAX_QUOTE_AGE
            stale=quote_age_sec>stale_limit
        else:
            stale=bool(REQUIRE_QUOTE_TIMESTAMP)
        if stale: quote_stale += 1; rej['quote_stale'] += 1
        else: quote_fresh += 1

        if bid is not None and ask is not None and bid > 0 and ask > 0:
            if ask < bid:
                rej['invalid_quote'] += 1
                spread=None
            else:
                mid=(bid+ask)/2.0
                spread=(ask-bid)/max(mid,1e-9)*100.0
        else:
            spread=None
        if spread is not None and spread > hard_spread:
            rej['spread_too_wide'] += 1; rej['spread'] += 1
            continue
        if premium <= 0:
            rej['price'] += 1
            continue

        preferred_floor=MIN_PREMIUM
        preferred_cap=MAX_PREMIUM
        if premium < preferred_floor:
            rej['premium_too_low'] += 1; rej['premium'] += 1; continue
        if premium > preferred_cap:
            rej['premium_too_high'] += 1; rej['premium'] += 1; continue
        if premium > hard_premium_cap:
            rej['hard_premium'] += 1; continue

        daily=snap.get('dailyBar') or snap.get('daily_bar') or {}
        vol=_optional_int(daily.get('v'))
        if vol is None:
            vol=_optional_int(trade.get('s')) if trade else None
        oi_raw=details.get('open_interest') if details.get('open_interest') is not None else details.get('openInterest')
        if oi_raw is None: oi_raw=snap.get('openInterest')
        oi=_optional_int(oi_raw)
        if vol is None: rej['missing_volume'] += 1
        elif vol == 0: rej['zero_volume'] += 1
        else: volume_present += 1
        if oi is None: rej['missing_oi'] += 1
        elif oi == 0: rej['zero_open_interest'] += 1
        else: oi_present += 1

        if DEBUG_SAMPLE and not sample_done:
            mid=(bid+ask)/2.0 if bid is not None and ask is not None and bid>0 and ask>0 else premium
            print('SAMPLE CONTRACT', {'Underlying':chain_root,'Contract':contract,'Expiration':exp,'DTE':dte,'Strike':strike,'Type':typ,'Bid':bid,'Ask':ask,'Last':last,'Mid':mid,'Spread':spread,'Volume':vol,'Open Interest':oi,'Stock Price':m['5m']['last'],'Distance From Strike':round(abs(m['5m']['last']-strike),4),'Passed':'YES','contract_structure':'YES','expiration':'YES','quote':'YES','liquidity':'SOFT','premium':'SOFT','technical':'SOFT'})
            sample_done=True

        potential += 1
        if DEBUG_BYPASS_SCORING:
            scored_rows.append({'signal':typ,'tier':None,'symbol':symbol,'contract':contract,'underlying':m['5m']['last'],'option_underlying':chain_root,'expiration':exp,'dte':dte,'strike':round(strike,2),'premium':round(premium,2),'bid':round(bid,2) if bid is not None and bid>0 else None,'ask':round(ask,2) if ask is not None and ask>0 else None,'last':round(last,2) if last is not None and last>0 else None,'volume':vol or 0,'open_interest':oi or 0,'spread_pct':round(spread,1) if spread is not None else None,'score':None,'qualification':'VALIDATION_ONLY','quote_source':quote_source,'quote_age_sec':round(quote_age_sec,1) if quote_age_sec is not None else None,'quote_is_stale':stale,'reasons':['DEBUG_BYPASS_SCORING']})
            continue

        delta_raw=greeks.get('delta')
        theta=_optional_float(greeks.get('theta'))
        iv=_optional_float(greeks.get('iv'))
        greeks_available=delta_raw is not None and _optional_float(delta_raw) is not None
        delta=_optional_float(delta_raw)
        if delta is None:
            delta=0.25 if typ=='CALL' else -0.25
            rej['delta'] += 1
        gap_pct=float(m.get('gap_pct',0.0) or 0.0)
        expected_move_pct=float(m.get('expected_move_pct',0.0) or 0.0) if m.get('expected_move_pct') is not None else None
        score,reasons,a4,a1,a15,a5,components,positive,opposite=score_setup(
            m,typ,spread if spread is not None else 40.0,delta,vol or 0,oi or 0,dte,strike,is_spxw=is_spxw,
            premium=premium,iv=iv,theta=theta,market_bias=str(market_context.get('bias') or 'NEUTRAL'),
            gap_pct=gap_pct,expected_move_pct=expected_move_pct)
        pscore,plabel=premium_quality(premium,is_spxw)
        components['premium_quality']=round(pscore,1)
        explosive_score,explosive_flags=explosive_setup_score(m,typ,spread if spread is not None else 20.0,delta,vol or 0,oi or 0,dte)
        risk_flags=[]
        if spread is not None and spread>max(MAX_SPREAD,RELAXED_MAX_SPREAD): risk_flags.append('wide spread')
        if (vol or 0)<MIN_VOL and (oi or 0)<MIN_OI: risk_flags.append('thin option liquidity')
        # Staleness is surfaced as a data-quality warning, but is not mixed
        # into the setup-quality tier calculation. Alert dispatch must still
        # enforce freshness separately before sending an actionable entry.
        if stale: reasons.append('⚠️ Quote stale/undated — verify live price before entry')
        if not greeks_available: reasons.append('Greeks unavailable — neutral score treatment')
        if vol is None: reasons.append('Volume unavailable — neutral score treatment')
        if oi is None: reasons.append('Open interest unavailable — neutral score treatment')
        if premium<preferred_floor: reasons.append('Low premium — higher sensitivity risk')
        elif premium>preferred_cap: reasons.append('High premium — capital intensive')
        if explosive_score>=EXPLOSIVE_MIN_SCORE and m['regime'] not in ('SIDEWAYS','CHOPPY'):
            reasons.append('💥 Momentum/volume expansion setup'); reasons.extend(explosive_flags)
        # HERO requires both score and independent confirmation; a wide spread, missing quote freshness,
        # opposing direction, or thin liquidity prevents HERO even when the raw score is high.
        hero_blockers=[]
        if spread is None or spread>20: hero_blockers.append('spread')
        if stale: hero_blockers.append('stale_quote')
        if (vol or 0)<MIN_VOL and (oi or 0)<MIN_OI: hero_blockers.append('liquidity')
        if opposite>=positive: hero_blockers.append('direction_conflict')
        if market_context.get('bias') in ('BULLISH','BEARISH') and ((market_context['bias']=='BULLISH') != (typ=='CALL')): hero_blockers.append('market_conflict')
        tier=_classify(score,positive,opposite,hero_blockers)
        if REQUIRE_4H_ALIGNMENT and not a4:
            if tier=='HERO': tier='STRONG'
            elif tier=='STRONG': tier='WATCH'
        if BLOCK_SIDEWAYS_CHOPPY and m['regime'] in ('SIDEWAYS','CHOPPY'): tier=None
        if tier in tier_counts: tier_counts[tier]+=1
        if tier is None:
            rej['score']+=1
            if positive<WATCH_MIN_DIRECTION: rej['direction']+=1
            if REQUIRE_4H_ALIGNMENT and not a4: rej['alignment']+=1
            if BLOCK_SIDEWAYS_CHOPPY and m['regime'] in ('SIDEWAYS','CHOPPY'): rej['regime']+=1
        if tier=='WATCH': reasons.append('🟡 WATCH — monitor; not a full entry signal')
        elif tier=='STRONG': reasons.append('🟢 STRONG setup')
        elif tier=='HERO': reasons.append('🏆 HERO setup')
        else: reasons.append('Below configured WATCH threshold')

        entry_low=round(bid,2) if bid is not None and bid>0 else round(premium,2); entry_high=round(ask,2) if ask is not None and ask>0 else round(premium,2); entry=premium
        atr_points=num(m['5m'].get('atr')); u_entry=round(m['5m']['last'],2)
        if atr_points>0 and u_entry>0:
            if typ=='CALL': u_stop=round(u_entry-atr_points,2); u_tp1=round(u_entry+atr_points,2); u_tp2=round(u_entry+2*atr_points,2); u_tp3=round(u_entry+3*atr_points,2); projected_underlying=u_tp3; underlying_move=max(0,projected_underlying-u_entry)
            else: u_stop=round(u_entry+atr_points,2); u_tp1=round(u_entry-atr_points,2); u_tp2=round(u_entry-2*atr_points,2); u_tp3=round(u_entry-3*atr_points,2); projected_underlying=u_tp3; underlying_move=max(0,u_entry-projected_underlying)
        else: u_stop=u_tp1=u_tp2=u_tp3=projected_underlying=None; underlying_move=0
        projected_premium=max(0.01,premium+abs(delta)*underlying_move) if underlying_move>0 and abs(delta)>0 else None
        projected_upside_pct=round(max(0,(projected_premium/premium-1)*100),1) if projected_premium is not None else None
        risk=max(((ask-bid)*1.5 if bid is not None and ask is not None and bid>0 and ask>0 else premium*0.20),0.05); stop=round(max(0.01,entry-risk),2); tp1=round(entry+risk,2); tp2=round(entry+2*risk,2); tp3=round(entry+3*risk,2)
        contracts=max(0,int(RISK//(risk*100))); max_loss=round(risk*100*contracts,2); reward1=round(max(0,tp1-entry)*100*contracts,2); reward2=round(max(0,tp2-entry)*100*contracts,2); reward3=round(max(0,tp3-entry)*100*contracts,2)
        confidence=round(min(97,max(50,score*.92)),0)
        trend4='BULLISH' if m['4h']['last']>m['4h']['ema20']>m['4h']['ema50'] else 'BEARISH' if m['4h']['last']<m['4h']['ema20']<m['4h']['ema50'] else 'NEUTRAL'; trend1='BULLISH' if m['1h']['last']>m['1h']['ema20']>m['1h']['ema50'] else 'BEARISH' if m['1h']['last']<m['1h']['ema20']<m['1h']['ema50'] else 'NEUTRAL'; trend15='BULLISH' if m['15m']['last']>m['15m']['ema20'] else 'BEARISH' if m['15m']['last']<m['15m']['ema20'] else 'NEUTRAL'; trend5='BULLISH' if m['5m']['last']>m['vwap'] and m['5m']['ema20']>m['5m']['ema50'] else 'BEARISH' if m['5m']['last']<m['vwap'] and m['5m']['ema20']<m['5m']['ema50'] else 'NEUTRAL'
        moonshot, moonshot_flags = moonshot_qualification(
            premium, projected_upside_pct, score, explosive_score, dte, spread, vol or 0, oi or 0,
            m.get('volume_ratio',1.0), m.get('regime','MIXED')
        )
        if moonshot and not tier:
            tier = 'MOONSHOT'
            tier_counts['WATCH'] += 0
        item={'signal':typ,'tier':tier,'market':'OPTIONS','symbol':symbol,'contract':contract,'dte':dte,'strike':round(strike,2),'expiration':exp,'premium':round(premium,2),'bid':round(bid,2) if bid is not None and bid>0 else None,'ask':round(ask,2) if ask is not None and ask>0 else None,'entry_low':entry_low,'entry_high':entry_high,'entry':round(entry,2),'stop_loss':stop,'tp1':tp1,'tp2':tp2,'tp3':tp3,'risk_dollars_per_contract':round(risk*100,2),'suggested_contracts':contracts,'affordable':contracts>0,'max_loss':max_loss,'expected_profit_tp1':reward1,'expected_profit_tp2':reward2,'expected_profit_tp3':reward3,'expected_loss_pct':round(risk/max(entry,0.01)*100,1),'expected_profit_pct':round((tp1/entry-1)*100,1),'risk_reward':round((tp1-entry)/risk,2) if risk else None,'underlying_entry':u_entry,'underlying_stop_loss':u_stop,'underlying_tp1':u_tp1,'underlying_tp2':u_tp2,'underlying_tp3':u_tp3,'atr_5m_points':round(atr_points,2) if atr_points else None,'score':score,'score_components':components,'direction_evidence':positive,'opposite_evidence':opposite,'explosive_score':explosive_score,'explosive_setup':bool(explosive_score>=EXPLOSIVE_MIN_SCORE and positive>=3),'moonshot':moonshot,'moonshot_flags':moonshot_flags,'explosive_flags':explosive_flags,'confidence':confidence,'projected_underlying_target':projected_underlying,'projected_premium':round(projected_premium,2) if projected_premium is not None else None,'projected_upside_pct':projected_upside_pct,'market_regime':m['regime'],'volume':vol or 0,'open_interest':oi or 0,'spread_pct':round(spread,1) if spread is not None else None,'delta':round(delta,3),'underlying':u_entry,'indicator_symbol':symbol,'indicator_proxy':symbol if not is_spxw else 'SPY','option_underlying':chain_root,'vwap_state':'BULLISH' if u_entry>m['vwap'] else 'BEARISH','ema_state':'BULLISH' if m['5m']['ema20']>m['5m']['ema50'] else 'BEARISH','rsi':round(m['5m']['rsi'],1),'macd_state':'BULLISH' if m['5m']['macd_delta']>0 else 'BEARISH','volume_ratio':round(m['volume_ratio'],2),'breakout':m['breakout'],'trend_4h':trend4,'trend_1h':trend1,'trend_15m':trend15,'trend_5m':trend5,'support':round(m.get('recent_low',0),2),'resistance':round(m.get('recent_high',0),2),'call_put_bias':typ,'gap_pct':gap_pct,'expected_move_pct':expected_move_pct,'adx_4h':round(m['4h']['adx'],1),'rsi_4h':round(m['4h']['rsi'],1),'reasons':reasons,'session':session,'data_mode':options_data_mode(),'premium_quality':round(pscore,1),'premium_quality_label':plabel,'quote_timestamp':qdt.isoformat() if qdt else None,'quote_source':quote_source,'quote_age_sec':round(quote_age_sec,1) if quote_age_sec is not None else None,'quote_is_stale':stale,'quote_status':('LIVE' if qdt and quote_age_sec is not None and quote_age_sec<=60 else 'FRESH' if not stale else 'STALE'),'greeks_available':greeks_available,'iv':iv,'theta':theta,'iv_rank':None,'expected_move_pct':expected_move_pct,'gap_pct':gap_pct,'market_bias':market_context.get('bias','NEUTRAL'),'market_breadth':market_context.get('breadth'),'vix':market_context.get('vix'),'qualification':tier or 'BELOW_WATCH'}
        scored_rows.append(item)
        if tier: qualified.append(item)

    scored_rows.sort(key=lambda x:(float(x.get('score') or 0),float(x.get('explosive_score') or 0),float(x.get('volume') or 0),float(x.get('open_interest') or 0)),reverse=True)
    # Keep a richer global diagnostic pool so /top and /status can explain why
    # the best contracts stopped at WATCH instead of forcing scoring changes.
    top_n=max(10,int(os.getenv('TOP_DIAGNOSTIC_COUNT','10')))
    top_pool=scored_rows[:max(3,int(os.getenv('TOP_POOL_PER_UNDERLYING','3')))]
    diagnostic_pool=scored_rows[:top_n]
    for row in diagnostic_pool:
        sc=float(row.get('score') or 0)
        row['score_gap_to_strong']=round(max(0.0,STRONG_SCORE-sc),1)
        row['score_gap_to_hero']=round(max(0.0,HERO_SCORE-sc),1)
        comps=row.get('score_components') or {}
        row['score_component_summary']={k:round(float(v),1) for k,v in comps.items() if isinstance(v,(int,float))}
        row['diagnostic_next_tier']='HERO' if sc < HERO_SCORE and sc >= STRONG_SCORE else ('STRONG' if sc < STRONG_SCORE and sc >= WATCH_SCORE else ('WATCH' if sc >= WATCH_SCORE else 'BELOW_WATCH'))
    no_setup_reasons=[]
    if not rows: no_setup_reasons.append('No option-chain contracts returned')
    if normalized_count==0: no_setup_reasons.append('No contracts passed normalization')
    if quote_stage==0 and normalized_count>0: no_setup_reasons.append('No contracts reached quote stage')
    if quote_with_data==0 and quote_stage>0: no_setup_reasons.append('No usable quotes')
    if potential and not qualified: no_setup_reasons.append('Contracts scored but none reached WATCH threshold')
    diag={'chain_items':received,'contracts_received':received,'normalized':normalized_count,'potential_setups':potential,'scored':len([x for x in scored_rows if x.get('score') is not None]),'quote_stage':quote_stage,'quotes_received':quotes_received,'scored_rows':len(scored_rows),'rejections':rej,'quote_metrics':{'with_data':quote_with_data,'fresh':quote_fresh,'stale':quote_stale,'bid_present':bid_present,'ask_present':ask_present,'last_present':last_present,'volume_present':volume_present,'oi_present':oi_present},'no_setup_reasons':no_setup_reasons,'chain_fallback':bool(chain.get('fallback')),'chain_pages':chain.get('pages',0),'contracts_discovered':chain.get('contracts_discovered',0),'primary_chain_error':chain.get('primary_error'),'underlying':chain_root,'underlying_price':m['5m']['last'],'trend_4h':('BULLISH' if m['4h']['last']>m['4h']['ema20']>m['4h']['ema50'] else 'BEARISH' if m['4h']['last']<m['4h']['ema20']<m['4h']['ema50'] else 'NEUTRAL'),'trend_1h':('BULLISH' if m['1h']['last']>m['1h']['ema20']>m['1h']['ema50'] else 'BEARISH' if m['1h']['last']<m['1h']['ema20']<m['1h']['ema50'] else 'NEUTRAL'),'momentum':('BULLISH' if m['5m']['macd_delta']>0 and m['5m']['last']>m['vwap'] else 'BEARISH' if m['5m']['macd_delta']<0 and m['5m']['last']<m['vwap'] else 'NEUTRAL'),'volume_ratio':m.get('volume_ratio'),'gap_pct':m.get('gap_pct',0.0),'market_bias':market_context.get('bias','NEUTRAL'),'market_breadth':market_context.get('breadth'),'vix':market_context.get('vix'),'option_source':'Alpaca options '+str(chain.get('feed_used') or os.getenv('ALPACA_OPTIONS_FEED','opra')),'top_candidates':top_pool,'diagnostic_top_candidates':diagnostic_pool,'tier_counts':tier_counts,'dte_rejected':dte_rejected,'feed_used':chain.get('feed_used')}
    # Authoritative tier counts come from the actual qualified rows. This avoids
    # any counter drift if a row is classified after the local counter update.
    tier_counts = {
        'HERO': sum(1 for x in qualified if x.get('tier') == 'HERO'),
        'STRONG': sum(1 for x in qualified if x.get('tier') == 'STRONG'),
        'WATCH': sum(1 for x in qualified if x.get('tier') == 'WATCH'),
    }
    diag['tier_counts'] = tier_counts
    diag['hero']=tier_counts['HERO']; diag['strong']=tier_counts['STRONG']; diag['watch']=tier_counts['WATCH']
    return qualified, diag

def fetch_stock_snapshot(symbol):
    """Best-effort latest stock snapshot; never required for the whole scan."""
    try:
        return req(f'{ALPACA}/stocks/{symbol}/snapshot', {'feed': os.getenv('ALPACA_UNDERLYING_FEED','iex')}, timeout=STOCKS_PAGE_TIMEOUT, retries=1)
    except Exception as e:
        progress('snapshot_failed',symbol=symbol,error=str(e)[:240])
        return {}


def _trend_from_frame(f):
    return 'BULLISH' if f['last']>f['ema20']>f['ema50'] and f['macd_delta']>=0 else 'BEARISH' if f['last']<f['ema20']<f['ema50'] and f['macd_delta']<=0 else 'NEUTRAL'


def market_context_from_caches(caches):
    """Build a resilient market regime from SPY/QQQ/IWM plus optional VIX/VIXY proxy."""
    rows=[]
    for sym in ('SPY','QQQ','IWM'):
        try:
            m=multi_tf(sym,caches)
            rows.append((_trend_from_frame(m['1h']),m))
        except Exception as e:
            progress('market_symbol_failed',symbol=sym,error=str(e)[:200])
    bull=sum(1 for t,_ in rows if t=='BULLISH'); bear=sum(1 for t,_ in rows if t=='BEARISH')
    breadth=round((bull-bear)/max(1,len(rows))*100,1) if rows else None
    bias='BULLISH' if bull>bear and bull>=2 else 'BEARISH' if bear>bull and bear>=2 else 'NEUTRAL'
    vix=None; vix_source=None
    for sym in ('VIX','VIXY'):
        try:
            end_dt=datetime.now(timezone.utc); start_dt=end_dt-timedelta(days=3)
            data=req(f'{ALPACA}/stocks/{sym}/bars', {'timeframe':'1Hour','start':start_dt.isoformat().replace('+00:00','Z'),'end':end_dt.isoformat().replace('+00:00','Z'),'limit':100,'feed':os.getenv('ALPACA_UNDERLYING_FEED','iex'),'sort':'asc'}, timeout=3, retries=0)
            bars=data.get('bars') or []; closes=[num(b.get('c')) for b in bars if num(b.get('c'))>0]
            if closes:
                vix=round(closes[-1],2); vix_source=sym; break
        except Exception:
            continue
    return {'bias':bias,'breadth':breadth,'vix':vix,'vix_source':vix_source,'symbols':len(rows),'bullish':bull,'bearish':bear}


def _attach_gap_and_expected_move(m):
    # Derive gap from the first/last daily bars when available; expected move is
    # a conservative ATR-based proxy when an options-implied move is unavailable.
    try:
        bars=m['1h']['bars']; closes=[num(b.get('c')) for b in bars if num(b.get('c'))>0]
        if len(closes)>=2:
            prev=closes[-2]; last=closes[-1]; m['gap_pct']=round((last/prev-1)*100,2) if prev else 0.0
        else: m['gap_pct']=0.0
        m['expected_move_pct']=round((m['1h']['atr']/max(m['1h']['last'],1e-9))*100,2)
    except Exception:
        m['gap_pct']=0.0; m['expected_move_pct']=None
    return m


def resolve_underlying(value):
    q=re.sub(r'\s+',' ',str(value or '').strip().upper())
    q=COMPANY_ALIASES.get(q,q)
    # Strip common punctuation from ticker-like requests only.
    if q.endswith('$'): q=q[:-1]
    if re.fullmatch(r'[A-Z][A-Z0-9.\-]{0,7}',q): return q
    return COMPANY_ALIASES.get(q,q)


def _parse_contract_query(query):
    q=re.sub(r'\s+',' ',str(query or '').strip().upper())
    # Exact OCC symbol
    m=re.search(r'([A-Z]{1,6}\d{6}[CP]\d{8})',q)
    if m:
        sym=m.group(1); root=re.match(r'([A-Z]{1,6})',sym).group(1)
        exp,strike,typ=parse_contract(sym,{})
        under='SPX' if root=='SPXW' else ('NDX' if root.startswith('NDX') else root)
        return {'contract':sym,'root':root,'underlying':under,'expiration':exp,'strike':strike,'type':typ}
    # Friendly forms: TICKER [YYYY-MM-DD|YYMMDD] STRIKE C/P
    m=re.match(r'^(SPXW|SPX|NDX|NDXP|[A-Z][A-Z0-9.\-]{0,7})\s+(?:(\d{4}-\d{2}-\d{2}|\d{6}|\d{1,2}/\d{1,2}(?:/\d{2,4})?)\s+)?([0-9]+(?:\.[0-9]+)?)\s*([CP])$',q)
    if not m: return None
    root=m.group(1); exp=m.group(2); strike=float(m.group(3)); typ='CALL' if m.group(4)=='C' else 'PUT'
    if exp:
        if re.fullmatch(r'\d{6}',exp): exp=f'20{exp[:2]}-{exp[2:4]}-{exp[4:6]}'
        elif '/' in exp:
            parts=exp.split('/'); year=int(parts[2]) if len(parts)==3 else datetime.now(timezone.utc).year; year=year+2000 if year<100 else year; exp=f'{year:04d}-{int(parts[0]):02d}-{int(parts[1]):02d}'
    under='SPX' if root=='SPXW' else ('NDX' if root in ('NDX','NDXP') else root)
    return {'contract':None,'root':root,'underlying':under,'expiration':exp,'strike':strike,'type':typ}


def analyze_underlying(symbol, session='OPEN', market_context=None):
    """On-demand analysis for any valid ticker, even if it is outside STOCK_SYMBOLS."""
    symbol=resolve_underlying(symbol)
    if market_context is None: market_context={'bias':'NEUTRAL','breadth':None,'vix':None}
    if symbol in ('SPXW','SPX'): proxy='SPY'; option_underlying='SPX'; prefix='SPXW' if symbol=='SPXW' else None
    elif symbol in ('NDX','NDXP'): proxy='QQQ'; option_underlying='NDX'; prefix=symbol if symbol!='NDX' else None
    else: proxy=symbol; option_underlying=symbol; prefix=None
    caches={}
    for key,(tf,days) in {'5m':('5Min',7),'15m':('15Min',20),'1h':('1Hour',45),'4h':('4Hour',180)}.items():
        try: caches[key]=fetch_bars_batch([proxy],tf,days)
        except Exception: caches[key]={}
    m=multi_tf(proxy,caches); _attach_gap_and_expected_move(m)
    qualified,diag=scan_underlying(proxy,session,contract_prefix=prefix,caches=caches,option_underlying=option_underlying,market_context=market_context)
    for x in qualified: x['symbol']=symbol; x['indicator_proxy']=proxy
    diag['requested_symbol']=symbol; diag['indicator_proxy']=proxy; diag['market']=market_context
    calls=[x for x in qualified if x.get('signal')=='CALL']; puts=[x for x in qualified if x.get('signal')=='PUT']
    call_score=max([float(x.get('score') or 0) for x in calls] or [0]); put_score=max([float(x.get('score') or 0) for x in puts] or [0])
    call_put_bias='CALL' if call_score-put_score>=5 else 'PUT' if put_score-call_score>=5 else 'NEUTRAL'
    return {'symbol':symbol,'indicator_proxy':proxy,'underlying_price':m['5m']['last'],'trend_4h':_trend_from_frame(m['4h']),'trend_1h':_trend_from_frame(m['1h']),'trend_15m':_trend_from_frame(m['15m']),'trend_5m':_trend_from_frame(m['5m']),'momentum':'BULLISH' if m['5m']['macd_delta']>0 and m['5m']['last']>m['vwap'] else 'BEARISH' if m['5m']['macd_delta']<0 and m['5m']['last']<m['vwap'] else 'NEUTRAL','support':m['recent_low'],'resistance':m['recent_high'],'volume_ratio':m['volume_ratio'],'regime':m['regime'],'gap_pct':m.get('gap_pct',0.0),'expected_move_pct':m.get('expected_move_pct'),'market':market_context,'call_put_bias':call_put_bias,'candidates':qualified,'diagnostics':diag}


def analyze_contract_query(query, session='OPEN', market_context=None):
    parsed=_parse_contract_query(query)
    if not parsed: return {'ok':False,'error':'Could not parse contract. Use OCC symbol or TICKER [EXPIRATION] STRIKE C/P.'}
    market_context=market_context or {'bias':'NEUTRAL','breadth':None,'vix':None}
    root=parsed['root']; under=parsed['underlying']; proxy='SPY' if under=='SPX' else 'QQQ' if under=='NDX' else under
    if under=='NDX':
        # Alpaca currently documents NDX/NQX index options as unsupported; still provide QQQ technical context.
        try: base=analyze_underlying(proxy,session,market_context)
        except Exception as e: base={'symbol':root,'indicator_proxy':proxy,'candidates':[],'error':str(e)}
        return {'ok':False,'unsupported':True,'symbol':root,'proxy':proxy,'error':'Alpaca does not currently provide NDX index options at this API offering; QQQ technical proxy is available.', 'analysis':base}
    try:
        strike_hint=parsed.get('strike'); exp_hint=parsed.get('expiration'); type_hint=parsed.get('type')
        # Need technical cache for the requested underlying.
        caches={}
        for key,(tf,days) in {'5m':('5Min',7),'15m':('15Min',20),'1h':('1Hour',45),'4h':('4Hour',180)}.items():
            caches[key]=fetch_bars_batch([proxy],tf,days)
        m=multi_tf(proxy,caches); _attach_gap_and_expected_move(m)
        is_index=root=='SPXW'
        chain=option_chain('SPX',root_symbol='SPXW' if is_index else None,strike_hint=strike_hint,expiration_hint=exp_hint,type_hint=type_hint)
        rows=chain.get('snapshots') or {}
        selected=[]; today=datetime.now(timezone.utc).date()
        for contract,snap in rows.items():
            n,reason=normalize_contract(snap,contract)
            if not n: continue
            if n['option_type']!=type_hint: continue
            if strike_hint is not None and abs(n['strike']-strike_hint)>max(.01,strike_hint*.003): continue
            if exp_hint and n['expiration']!=exp_hint: continue
            dte=(datetime.fromisoformat(n['expiration']).date()-today).days
            if dte<0 or dte>MAX_DTE: continue
            selected.append((contract,snap,n,dte))
        if not selected:
            # If no exact expiration was specified, search the chain and choose nearest strike/expiry.
            if exp_hint:
                return {'ok':False,'error':'Contract not found in the available Alpaca chain after normalization.','symbol':under,'requested':query,'chain_pages':chain.get('pages',0),'provider_error':chain.get('primary_error')}
            return {'ok':False,'error':'No matching strike/type found in the available chain.','symbol':under,'requested':query,'chain_pages':chain.get('pages',0),'provider_error':chain.get('primary_error')}
        # Reuse scan engine by exact strike/expiry/type, then choose exact result or nearest.
        qualified,diag=scan_underlying(proxy,session,contract_prefix='SPXW' if is_index else None,caches=caches,option_underlying='SPX' if is_index else proxy,market_context=market_context,strike_hint=parsed.get('strike'),expiration_hint=parsed.get('expiration'),type_hint=parsed.get('type'))
        best=min(qualified,key=lambda x:abs(float(x.get('strike') or 0)-float(parsed.get('strike') or 0))) if qualified else None
        if not best:
            # Build a neutral analysis row from the exact snapshot instead of returning not-found.
            contract,snap,n,dte=selected[0]; q=snap.get('latestQuote') or {}; tr=snap.get('latestTrade') or {}; g=snap.get('greeks') or {}
            bid=_optional_float(q.get('bp')); ask=_optional_float(q.get('ap')); last=_optional_float(tr.get('p')); prem=(bid+ask)/2 if bid and ask and ask>=bid else last
            iv=_optional_float(g.get('iv')); delta=_optional_float(g.get('delta')); theta=_optional_float(g.get('theta'))
            best={'symbol':under,'contract':contract,'signal':n['option_type'],'expiration':n['expiration'],'dte':dte,'strike':n['strike'],'premium':prem,'bid':bid,'ask':ask,'delta':delta,'theta':theta,'iv':iv,'volume':_optional_int((snap.get('dailyBar') or {}).get('v')) or 0,'open_interest':_optional_int(n['details'].get('open_interest')) or 0,'spread_pct':round((ask-bid)/max((ask+bid)/2,1e-9)*100,1) if bid and ask and ask>=bid else None,'score':45.0,'tier':None,'reasons':['Exact contract identified; technical/quality score below WATCH or data incomplete.']}
        best['market']=market_context; best['underlying_price']=m['5m']['last']; best['support']=m['recent_low']; best['resistance']=m['recent_high']; best['trend_4h']=_trend_from_frame(m['4h']); best['trend_1h']=_trend_from_frame(m['1h']); best['volume_ratio']=m['volume_ratio']; best['expected_move_pct']=m.get('expected_move_pct'); best['gap_pct']=m.get('gap_pct')
        return {'ok':True,'result':best}
    except Exception as e:
        return {'ok':False,'error':f'{type(e).__name__}: {e}','symbol':under,'requested':query}


def scan_all(session):
    if not headers():
        raise RuntimeError('Missing ALPACA_API_KEY / ALPACA_API_SECRET')

    results = []
    diagnostics = {}
    symbols = list(dict.fromkeys(STOCKS))
    periods = {'5m':('5Min',7),'15m':('15Min',20),'1h':('1Hour',45),'4h':('4Hour',180)}
    caches = {}
    progress('scan_start', phase=session, symbols=symbols)

    # Fetch technical bars concurrently, preserving per-timeframe failures.
    progress('fetching_bars', symbols_total=len(symbols), timeframes=len(periods), completed=0)
    def _fetch_period(item):
        key,(tf,days)=item
        progress('fetching_bars', timeframe=key, state='started', symbols_total=len(symbols))
        started=time.monotonic()
        try:
            data=fetch_bars_batch(symbols,tf,days)
            return key,data,None
        except Exception as e:
            return key,{},e
        finally:
            progress('fetching_bars', timeframe=key, state='finished',
                     elapsed=round(time.monotonic()-started,2))

    completed=0
    with ThreadPoolExecutor(max_workers=min(4,len(periods))) as ex:
        futures=[ex.submit(_fetch_period,item) for item in periods.items()]
        for fut in as_completed(futures):
            key,data,err=fut.result()
            caches[key]=data
            completed += 1
            if err is not None:
                diagnostics[f'__{key}']={'error':f'{type(err).__name__}: {err}','provider':'Alpaca',
                    'endpoint':getattr(err,'endpoint',''),'status_code':getattr(err,'status_code',None),
                    'retry_count':getattr(err,'retry_count',None)}
                progress('fetching_bars',timeframe=key,state='failed',symbols=len(data),completed=completed,error=str(err))
            else:
                progress('fetching_bars',timeframe=key,state='complete',symbols=len(data),completed=completed)

    # Repair sparse higher timeframes from lower timeframes rather than dropping symbols.
    repair_plan=[('15m','5m',15,7),('1h','15m',60,20),('4h','1h',240,45)]
    for target,source_tf,minutes,src_days in repair_plan:
        target_cache=caches.setdefault(target,{})
        source_cache=caches.setdefault(source_tf,{})
        for sym in symbols:
            good=len([b for b in (target_cache.get(sym) or []) if num(b.get('c'))>0])
            if good>=55: continue
            src=source_cache.get(sym) or []
            if len([b for b in src if num(b.get('c'))>0])<55:
                try:
                    fetched=fetch_bars(sym, {'5m':'5Min','15m':'15Min','1h':'1Hour'}[source_tf], src_days, limit=2000)
                    if fetched: source_cache[sym]=fetched; src=fetched
                except Exception as e:
                    progress('bars_repair_fetch_failed',symbol=sym,target=target,source=source_tf,error=str(e)[:300])
            agg=_aggregate_bars(src,minutes)
            if len([b for b in agg if num(b.get('c'))>0])>=55:
                target_cache[sym]=agg

    progress('fetching_bars_complete', symbols=len(symbols), completed=completed)
    market_ctx=market_context_from_caches(caches)
    progress('market_analysis', bias=market_ctx.get('bias'), breadth=market_ctx.get('breadth'), vix=market_ctx.get('vix'))
    progress('underlying_scan', symbols=symbols)

    def _scan_one(sym):
        try:
            r,d=scan_underlying(sym,session,caches=caches,market_context=market_ctx)
            return sym,r,d,None
        except Exception as e:
            return sym,[],{'error':f'{type(e).__name__}: {e}','provider':'Alpaca',
                'endpoint':getattr(e,'endpoint',''),'status_code':getattr(e,'status_code',None),
                'retry_count':getattr(e,'retry_count',None),'detail':repr(e)},e

    # Options chains are independent. One failed underlying must never stop the rest.
    with ThreadPoolExecutor(max_workers=MAX_CONCURRENCY) as ex:
        futs={ex.submit(_scan_one,s):s for s in symbols}
        for fut in as_completed(futs):
            sym,r,d,err=fut.result()
            diagnostics[sym]=d
            if err is None:
                results.extend(r)
            progress('scoring',symbol=sym,symbols_scanned=len([k for k in diagnostics if not k.startswith('__')]),
                     contracts_scanned=sum(int((v or {}).get('chain_items',0)) for v in diagnostics.values() if isinstance(v,dict)),
                     candidates=len(results))
            if err is not None:
                progress('symbol_error',symbol=sym,error=str(err)[:300])

    # Index options are scanned separately: use a liquid ETF for technical
    # context, but request option chains for the actual index underlying.
    # Provider entitlements still apply; failures are recorded per root.
    index_specs = {
        'SPXW': ('SPY', 'SPX'),
        'NDX': ('QQQ', 'NDX'),
        'NDXP': ('QQQ', 'NDX'),
    }
    for root in INDEX_ROOTS:
        if root not in index_specs:
            diagnostics[root] = {'error': 'Unsupported index root; configure SPXW, NDX, or NDXP', 'provider': 'Alpaca'}
            continue
        proxy, option_symbol = index_specs[root]
        try:
            progress('underlying_scan', symbol=root, state='started', proxy=proxy, option_underlying=option_symbol)
            proxy_caches = {k: dict(v or {}) for k, v in caches.items()}
            r, d = scan_underlying(proxy, session, contract_prefix=root if root=='SPXW' else None, caches=proxy_caches,
                                   option_underlying=option_symbol, market_context=market_ctx)
            for x in r:
                x['symbol'] = root
                x['indicator_proxy'] = proxy
                x['option_underlying'] = option_symbol
            results.extend(r)
            diagnostics[root] = d
        except Exception as e:
            diagnostics[root] = {'error': f'{type(e).__name__}: {e}', 'provider': 'Alpaca',
                'endpoint': getattr(e, 'endpoint', ''), 'status_code': getattr(e, 'status_code', None),
                'retry_count': getattr(e, 'retry_count', None), 'detail': repr(e)}
            progress('symbol_error', symbol=root, error=str(e)[:300])

    progress('scoring_complete', candidates=len(results))
    tier_rank={'HERO':4,'STRONG':3,'WATCH':2,'MOONSHOT':1}
    results.sort(key=lambda x:(tier_rank.get(x.get('tier'),0),float(x.get('score',0) or 0),
                               float(x.get('explosive_score',0) or 0),float(x.get('volume',0) or 0)),reverse=True)

    # Enforce diversification: at most 1-3 qualified contracts per underlying.
    max_per = max(1, int(os.getenv('MAX_CANDIDATES_PER_UNDERLYING','3')))
    grouped={}
    for x in results:
        grouped.setdefault(x.get('symbol','?'),[]).append(x)
    diversified=[]
    for sym,items in grouped.items():
        items.sort(key=lambda x:(tier_rank.get(x.get('tier'),0),float(x.get('score',0) or 0),
                                 float(x.get('explosive_score',0) or 0)),reverse=True)
        diversified.extend(items[:max_per])
    results=sorted(diversified,key=lambda x:(tier_rank.get(x.get('tier'),0),float(x.get('score',0) or 0),
                                             float(x.get('explosive_score',0) or 0),
                                             float(x.get('volume',0) or 0)),reverse=True)

    # Build top-candidate pool across ALL scored contracts, including below-WATCH.
    all_pool=[]
    for key,d in diagnostics.items():
        if isinstance(d,dict):
            all_pool.extend(d.get('top_candidates') or [])
    all_pool.sort(key=lambda x:(float(x.get('score',0) or 0),float(x.get('explosive_score',0) or 0),
                                float(x.get('volume',0) or 0),float(x.get('open_interest',0) or 0)),reverse=True)
    # Keep at most three from each underlying in the global pool.
    pool_grouped={}
    for x in all_pool:
        pool_grouped.setdefault(x.get('symbol','?'),[]).append(x)
    top_candidates=[]
    for sym,items in pool_grouped.items():
        top_candidates.extend(items[:max_per])
    top_candidates.sort(key=lambda x:(float(x.get('score',0) or 0),float(x.get('explosive_score',0) or 0)),reverse=True)
    top_candidates=top_candidates[:max(10,int(os.getenv('TOP_CANDIDATES_LIMIT','10')))]

    aggregate_rej={}
    counters={
        'contracts_received':0,'contracts_normalized':0,'contracts_valid':0,'contracts_scored':0,
        'contracts_with_quotes':0,'contracts_stale_quotes':0,'contracts_missing_bid':0,'contracts_quote_stage':0,
        'contracts_missing_ask':0,'contracts_missing_last':0,'contracts_missing_volume':0,
        'contracts_missing_oi':0,'rejected_price':0,'rejected_spread':0,
        'rejected_liquidity':0,'rejected_dte':0,'rejected_momentum':0,
        'rejected_trend':0,'rejected_score':0,'candidate_pool_size':len(top_candidates),'bad_contract_schema':0,'missing_symbol':0,'invalid_expiration':0,'expired':0,'invalid_strike':0,'missing_type':0,'invalid_type':0,'missing_quote':0,'invalid_bid':0,'invalid_ask':0,'invalid_last':0,'zero_volume':0,'zero_open_interest':0,'spread_too_wide':0,'premium_too_low':0,'premium_too_high':0
    }
    no_setup=[]
    for key,d in diagnostics.items():
        if not isinstance(d,dict): continue
        counters['contracts_received'] += int(d.get('chain_items',0) or 0)
        counters['contracts_normalized'] += int(d.get('normalized',0) or 0)
        # A contract is counted as VALID only after it reaches the quote stage
        # and has a usable quote (BID/ASK or valid LAST fallback). Quote-stage
        # entry remains tracked separately.
        qm=d.get('quote_metrics') or {}
        counters['contracts_valid'] += int(qm.get('with_data',0) or 0)
        counters['contracts_scored'] += int(d.get('scored',0) or 0)
        counters['contracts_quote_stage'] += int(d.get('quote_stage',0) or 0)
        counters['contracts_with_quotes'] += int(qm.get('with_data',0) or 0)
        counters['contracts_stale_quotes'] += int(qm.get('stale',0) or 0)
        r=d.get('rejections') or {}
        mapping={'rejected_price':'price','rejected_spread':'spread','rejected_liquidity':'liquidity',
                 'rejected_dte':'dte','rejected_score':'score','rejected_momentum':'direction',
                 'rejected_trend':'alignment'}
        for dst,src in mapping.items(): counters[dst]+=int(r.get(src,0) or 0)
        counters['contracts_missing_bid'] += int(r.get('no_bid',0) or 0)
        counters['contracts_missing_ask'] += int(r.get('no_ask',0) or 0)
        counters['contracts_missing_last'] += int(r.get('missing_last',0) or 0)
        counters['contracts_missing_volume'] += int(r.get('missing_volume',0) or 0)
        counters['contracts_missing_oi'] += int(r.get('missing_oi',0) or 0)
        for rk,rv in r.items():
            aggregate_rej[rk]=aggregate_rej.get(rk,0)+int(rv or 0)
            if rk in counters: counters[rk]+=int(rv or 0)
        no_setup.extend(d.get('no_setup_reasons') or [])

    # Recompute authoritative totals from the actual result rows. Do not rely
    # on intermediate counters when building the final scan state.
    heroes=sum(1 for x in results if str(x.get('tier') or '').upper()=='HERO')
    strongs=sum(1 for x in results if str(x.get('tier') or '').upper()=='STRONG')
    watchs=sum(1 for x in results if str(x.get('tier') or '').upper()=='WATCH')
    counters.update({'hero_count':heroes,'strong_count':strongs,'watch_count':watchs})
    # Defensive reconciliation: every per-symbol diagnostic is authoritative
    # for the pipeline counters if an intermediate aggregate was lost.
    if counters['contracts_received'] == 0:
        counters['contracts_received'] = sum(int((d or {}).get('chain_items',0) or 0) for d in diagnostics.values() if isinstance(d,dict))
    if counters['contracts_normalized'] == 0:
        counters['contracts_normalized'] = sum(int((d or {}).get('normalized',0) or 0) for d in diagnostics.values() if isinstance(d,dict))
    if counters['contracts_quote_stage'] == 0:
        counters['contracts_quote_stage'] = sum(int((d or {}).get('quote_stage',0) or 0) for d in diagnostics.values() if isinstance(d,dict))
    if counters['contracts_with_quotes'] == 0:
        counters['contracts_with_quotes'] = sum(int(((d or {}).get('quote_metrics') or {}).get('with_data',0) or 0) for d in diagnostics.values() if isinstance(d,dict))
    if counters['contracts_valid'] == 0:
        counters['contracts_valid'] = counters['contracts_with_quotes']
    if counters['contracts_scored'] == 0:
        counters['contracts_scored'] = sum(int((d or {}).get('scored',0) or 0) for d in diagnostics.values() if isinstance(d,dict))

    # Keep the authoritative scan summary inside diagnostics so the parent
    # process (/status, /top, /diagnostics and Telegram summary) never loses
    # the final counters. V14 previously built `meta` locally but returned it
    # only indirectly, causing a real scan with candidates to display zeros.
    meta={
        'scan_id':None,'symbols_scanned':len(symbols)+ (1 if 'SPXW' in diagnostics else 0),
        'contracts_scanned':counters['contracts_received'],'valid_contracts':counters['contracts_valid'],'contracts_normalized':counters['contracts_normalized'],'quote_stage':counters['contracts_quote_stage'],
        'contracts_scored':counters['contracts_scored'],'candidates':len(results),
        'heroes':heroes,'strong':strongs,'watch':watchs,'moonshots':sum(1 for x in results if x.get('moonshot')),'top_candidates':top_candidates,
        'candidate_pool_size':len(top_candidates),'rejections':aggregate_rej,
        'counters':counters,'no_setup_reasons':list(dict.fromkeys(no_setup))[:10],
        'provider':provider_status(),'market':market_ctx
    }
    if not results:
        meta['zero_candidate_diagnostics']={
            'contracts_scanned':counters['contracts_received'],
            'contracts_with_valid_underlying':len([k for k in diagnostics if not k.startswith('__') and not (diagnostics[k] or {}).get('error')]),
            'contracts_with_valid_quotes':counters['contracts_with_quotes'],
            'contracts_stale':counters['contracts_stale_quotes'],
            'contracts_scored':counters['contracts_scored'],
            'rejected_price':counters['rejected_price'],'rejected_spread':counters['rejected_spread'],'normalized':counters['contracts_normalized'],'quote_stage':counters['contracts_quote_stage'],'rejected_dte':counters['rejected_dte'],
            'rejected_liquidity':counters['rejected_liquidity'],'rejected_dte':counters['rejected_dte'],
            'rejected_momentum':counters['rejected_momentum'],'rejected_trend':counters['rejected_trend'],
            'rejected_score':counters['rejected_score']
        }
        top_reason=max(aggregate_rej.items(),key=lambda kv:kv[1])[0] if aggregate_rej else 'none'
        meta['zero_candidate_diagnostics']['top_rejection_reason']=top_reason

    diagnostics['__meta__'] = meta
    progress('final_ranking',candidates=len(results),heroes=heroes,strong=strongs,watch=watchs,moonshots=sum(1 for x in results if x.get('moonshot')),top=len(top_candidates),
             symbols_scanned=meta['symbols_scanned'],contracts_scanned=meta['contracts_scanned'],
             valid_contracts=meta['valid_contracts'],contracts_scored=meta['contracts_scored'])
    progress('scan_complete',symbols=meta['symbols_scanned'],contracts=meta['contracts_scanned'],
             candidates=len(results),heroes=heroes,strong=strongs,watch=watchs,moonshots=sum(1 for x in results if x.get('moonshot')),
             valid_contracts=meta['valid_contracts'],contracts_scored=meta['contracts_scored'])
    return results,diagnostics

def options_data_mode():
    feed=str(os.getenv('ALPACA_OPTIONS_FEED','opra')).strip().lower()
    return {'opra':'LIVE','live':'LIVE','delayed':'DELAYED','indicative':'INDICATIVE'}.get(feed,'UNKNOWN')

def provider_status():
    feed=os.getenv('ALPACA_OPTIONS_FEED','opra')
    mode=str(feed).lower()
    return {'name':'Alpaca','configured':bool(os.getenv('ALPACA_API_KEY') and os.getenv('ALPACA_API_SECRET')),
            'options_feed':feed,'effective_fallback':'indicative' if feed.lower()=='opra' else feed,
            'underlying_feed':os.getenv('ALPACA_UNDERLYING_FEED','iex'),'data_mode':options_data_mode(),
            'index_options':{'SPXW':'supported_under_SPX','NDX':'not_currently_supported_at_Alpaca_index_options_offering'},
            'note':'OPRA is attempted first; if the account/feed rejects it, the scanner falls back to indicative discovery. SPXW is requested under SPX; NDX is reported as provider-unsupported and uses QQQ only as a technical proxy.' if feed.lower()=='opra' else
                  ('Options data may be delayed/indicative; underlying feed is IEX.' if mode in ('indicative','delayed','snapshot')
                   else 'Options feed mode is configured explicitly; verify entitlement before treating it as real-time.') }
