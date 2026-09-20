import logging, threading, time
from datetime import datetime, time as dt_time
from zoneinfo import ZoneInfo
from config import cfg
from scanner import scan_symbols
from telegram_bot import send_telegram, telegram_configured
log=logging.getLogger(__name__); NY=ZoneInfo('America/New_York')
_seen={}; _lock=threading.Lock(); _thread=None
_last_scan=None; _last_error=None; _last_candidates=0; _last_alerts=0; _last_session='closed'

def market_session():
    now=datetime.now(NY); wd=now.weekday(); t=now.time()
    if wd>=5: return 'closed'
    # SPX/SPXW global session is 20:15-09:25 ET. Equity options do not trade premarket.
    if cfg.spxw_gth_enabled and (t>=dt_time(20,15) or t<dt_time(9,25)): return 'spxw-gth'
    if cfg.premarket_enabled and dt_time(4,0)<=t<dt_time(9,30): return 'pre-market'
    if cfg.regular_enabled and dt_time(9,30)<=t<=dt_time(16,15): return 'regular'
    if cfg.afterhours_enabled and dt_time(16,15)<t<=dt_time(20,0): return 'after-hours'
    return 'closed'

def market_is_open(): return market_session()!='closed' if cfg.market_only else True

def _cleanup(ts):
    cutoff=ts-cfg.alert_cooldown_minutes*60
    for k,v in list(_seen.items()):
        if v<cutoff: del _seen[k]

def _claim(key):
    ts=time.time()
    with _lock:
        _cleanup(ts)
        if key in _seen: return False
        _seen[key]=ts; return True

def format_alert(x,signal='AUTO ALERT',session=None):
    session=session or _last_session
    tradable='YES' if session in ('regular','spxw-gth') else 'NO — setup/watch only'
    return (f'🚨 OPTIONS OPPORTUNITY — {session.upper()}\n\n{x["symbol"]} {x["contract"]} | Exp {x["expiration"]}\n'
            f'Product: {x["product"]} | Tradable now: {tradable}\nUnderlying: ${x["underlying_price"]:.2f} | Premium: ${x["premium"]:.2f}\n'
            f'Score: {x["score"]:.0f}/100 | DTE: {x["dte"]}\nVol: {x["volume"]:,} | OI: {x["open_interest"]:,} | Spread: {x["spread_pct"]:.1f}%\n'
            f'Delta≈{x["delta"]:.2f} ({x["delta_source"]}) | IV: {x["iv"]:.1%}\n5D: {x["ret5"]:+.1f}% | 20D: {x["ret20"]:+.1f}% | Vol ratio: {x["volume_ratio"]:.1f}x\n\n'
            f'Entry: ${x["entry_low"]:.2f}–${x["entry_high"]:.2f}\nSL: ${x["stop_loss"]:.2f} | Underlying SL: ${x["stop_underlying"]:.2f}\n'
            f'TP1/TP2/TP3: ${x["tp1"]:.2f} / ${x["tp2"]:.2f} / ${x["tp3"]:.2f}\n'
            f'Expected: +{x["profit_pct_tp1"]:.0f}% / +{x["profit_pct_tp2"]:.0f}% / +{x["profit_pct_tp3"]:.0f}%\n'
            f'R:R: {x["rr_tp1"]:.1f}R / {x["rr_tp2"]:.1f}R / {x["rr_tp3"]:.1f}R\n'
            f'Risk/contract: ${x["risk_dollars_per_contract"]:.0f} | Budget: ${x["risk_budget"]:.0f} | Size: {x["suggested_contracts"]}\n'
            f'Max loss at suggested size: ${x["max_loss_position"]:.0f}\n\nReasons: {", ".join(x["reasons"])}\n\n'
            '⚠️ Alert/research only. No automatic orders. Prices/data may be delayed.')

def run_once(force=False):
    global _last_scan,_last_error,_last_candidates,_last_alerts,_last_session
    _last_scan=datetime.now(NY).isoformat(); _last_error=None; _last_session=market_session()
    if not force and not market_is_open(): log.info('Market closed; scan skipped'); return []
    try:
        results=scan_symbols(cfg.scan_symbols)
        candidates=[x for x in results if x['score']>=cfg.min_alert_score*10 and x['suggested_contracts']>0]
        # Pre-market: send setups, but only SPXW is considered currently tradable in GTH.
        if _last_session=='pre-market': candidates=[x for x in candidates if cfg.notify_premarket]
        if _last_session=='regular' and not cfg.notify_regular: candidates=[]
        sent=0
        for x in candidates[:cfg.top_n]:
            key=f'{x["symbol"]}|{x["contract"]}|{x["expiration"]}|{_last_session}'
            if _claim(key) and send_telegram(format_alert(x,session=_last_session)): sent+=1
        _last_candidates=len(candidates); _last_alerts=sent
        log.info('Scan complete: session=%s candidates=%s alerts=%s',_last_session,len(candidates),sent)
        return candidates
    except Exception as exc:
        _last_error=str(exc); log.exception('Scanner error'); return []

def status():
    return {'running':_thread is not None and _thread.is_alive(),'last_scan':_last_scan,'last_error':_last_error,'last_candidates':_last_candidates,'last_alerts':_last_alerts,'last_session':_last_session,'telegram_configured':telegram_configured(),'spxw_enabled':cfg.scan_spxw}

def _loop():
    log.info('V7.1 scanner started: every %ss; premarket=%s regular=%s SPXW-GTH=%s symbols=%s',cfg.scan_interval_seconds,cfg.premarket_enabled,cfg.regular_enabled,cfg.spxw_gth_enabled,','.join(cfg.scan_symbols))
    log.info('Telegram configured: %s','YES' if telegram_configured() else 'NO')
    while True:
        try: run_once()
        except Exception: log.exception('Unhandled scanner loop error')
        time.sleep(max(30,cfg.scan_interval_seconds))

def start():
    global _thread
    if not cfg.run_scanner: log.info('RUN_SCANNER=false; auto scanner disabled'); return
    if _thread and _thread.is_alive(): return
    _thread=threading.Thread(target=_loop,name='options-scanner',daemon=True); _thread.start()
