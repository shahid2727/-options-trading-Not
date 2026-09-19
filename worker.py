import logging
import threading
import time
from datetime import datetime, time as dt_time
from zoneinfo import ZoneInfo
from config import cfg
from scanner import scan_symbols
from telegram_bot import send_telegram, telegram_configured

log = logging.getLogger(__name__)
NY = ZoneInfo('America/New_York')
_seen = {}
_lock = threading.Lock()
_thread = None
_last_scan = None
_last_error = None
_last_candidates = 0
_last_alerts = 0


def market_is_open():
    if not cfg.market_only:
        return True
    now = datetime.now(NY)
    return now.weekday() < 5 and dt_time(9, 30) <= now.time() <= dt_time(16, 0)


def _cleanup(now_ts):
    cutoff = now_ts - cfg.alert_cooldown_minutes * 60
    for k, ts in list(_seen.items()):
        if ts < cutoff:
            del _seen[k]


def _claim(key):
    now_ts = time.time()
    with _lock:
        _cleanup(now_ts)
        if key in _seen:
            return False
        _seen[key] = now_ts
        return True


def format_alert(x, signal='AUTO ALERT'):
    return (f'🚨 {signal}\n\n{x["symbol"]} {x["contract"]} | Exp {x["expiration"]}\n'
            f'Underlying: ${x["underlying_price"]:.2f} | Premium: ${x["premium"]:.2f}\n'
            f'Score: {x["score"]:.0f}/100 | DTE: {x["dte"]}\n'
            f'Vol: {x["volume"]:,} | OI: {x["open_interest"]:,} | Spread: {x["spread_pct"]:.1f}%\n'
            f'5D: {x["ret5"]:+.1f}% | 20D: {x["ret20"]:+.1f}% | Vol ratio: {x["volume_ratio"]:.1f}x\n\n'
            f'Entry: ${x["entry_low"]:.2f}–${x["entry_high"]:.2f}\n'
            f'SL: ${x["stop_loss"]:.2f} | Underlying SL: ${x["stop_underlying"]:.2f}\n'
            f'TP1/TP2/TP3: ${x["tp1"]:.2f} / ${x["tp2"]:.2f} / ${x["tp3"]:.2f}\n'
            f'Expected: +{x["profit_pct_tp1"]:.0f}% / +{x["profit_pct_tp2"]:.0f}% / +{x["profit_pct_tp3"]:.0f}%\n'
            f'R:R: {x["rr_tp1"]:.1f}R / {x["rr_tp2"]:.1f}R / {x["rr_tp3"]:.1f}R\n'
            f'Risk/contract: ${x["risk_dollars_per_contract"]:.0f} | Size: {x["suggested_contracts"]}\n\n'
            f'Reasons: {", ".join(x["reasons"])}\n'
            f'Plan: TP1 partial → breakeven; TP2 partial → trail; TP3 close remainder.\n'
            f'⚠️ Research/alert only; no automatic orders.')


def run_once(force=False):
    global _last_scan, _last_error, _last_candidates, _last_alerts
    _last_scan = datetime.now(NY).isoformat()
    _last_error = None
    if not force and not market_is_open():
        log.info('Market closed; scan skipped')
        return []
    try:
        results = scan_symbols(cfg.scan_symbols)
        candidates = [x for x in results if x['score'] >= cfg.min_alert_score * 10]
        sent = 0
        for x in candidates[:cfg.max_alerts_per_scan]:
            key = f'{x["symbol"]}|{x["contract"]}|{x["expiration"]}'
            if _claim(key) and send_telegram(format_alert(x)):
                sent += 1
        _last_candidates, _last_alerts = len(candidates), sent
        log.info('Scan complete: %s candidates, %s alerts sent', len(candidates), sent)
        return candidates
    except Exception as exc:
        _last_error = str(exc)
        log.exception('Scanner error')
        return []


def status():
    return {'running': _thread is not None and _thread.is_alive(), 'last_scan': _last_scan,
            'last_error': _last_error, 'last_candidates': _last_candidates,
            'last_alerts': _last_alerts, 'telegram_configured': telegram_configured()}


def _loop():
    log.info('V6 auto scanner started: every %ss; symbols=%s', cfg.scan_interval_seconds, ','.join(cfg.scan_symbols))
    log.info('Telegram configured: %s', 'YES' if telegram_configured() else 'NO')
    while True:
        try:
            run_once()
        except Exception:
            log.exception('Unhandled scanner loop error')
        time.sleep(max(30, cfg.scan_interval_seconds))


def start():
    global _thread
    if not cfg.run_scanner:
        log.info('RUN_SCANNER=false; auto scanner disabled')
        return
    if _thread and _thread.is_alive():
        return
    _thread = threading.Thread(target=_loop, name='options-scanner', daemon=True)
    _thread.start()
