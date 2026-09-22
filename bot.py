import os, threading, time, uuid, multiprocessing
from datetime import datetime, time as dtime
from zoneinfo import ZoneInfo
from flask import Flask, jsonify, request
from scanner import scan_all, provider_status, set_progress_callback
from telegram_bot import send_message, get_updates, diagnostics as telegram_diagnostics, mark_polling_running

app = Flask(__name__)
TZ = ZoneInfo('America/New_York')
lock = threading.RLock()
state = {
    'last_scan': None, 'last_candidates': 0, 'last_alerts': 0, 'last_error': None,
    'last_session': 'CLOSED', 'running': True, 'scan_id': None,
    'scan_running': False, 'scan_started': None, 'scan_finished': None,
    'scan_duration': None, 'scan_stage': 'idle', 'symbols_scanned': 0, 'contracts_scanned': 0,
    'last_top': [], 'diagnostics': {}, 'last_alert_keys': {}, 'market_warning_sent': None,
    'provider_errors': [], 'scan_process_pid': None, 'fetching_timeframe': None, 'fetching_completed': 0, 'fetching_total': 4, 'fetching_state': None, 'alert_diagnostics': {'candidates': []}, 'hero_sent_date': None
}


def phase():
    n = datetime.now(TZ); t = n.time()
    if n.weekday() >= 5: return 'CLOSED'
    if dtime(4,0) <= t < dtime(9,30): return 'PRE_MARKET'
    if dtime(9,30) <= t < dtime(16,0): return 'REGULAR'
    if dtime(16,0) <= t < dtime(20,0): return 'AFTER_HOURS'
    return 'CLOSED'


def phase_label(p):
    return {'PRE_MARKET':'🌅 PRE-MARKET','REGULAR':'🟢 REGULAR','AFTER_HOURS':'🌙 AFTER-HOURS','CLOSED':'⚪ CLOSED'}.get(p,p)


def allowed(p):
    env = {'PRE_MARKET':'PREMARKET_ENABLED','REGULAR':'REGULAR_ENABLED','AFTER_HOURS':'AFTERHOURS_ENABLED'}.get(p)
    return bool(env and os.getenv(env, 'false').lower() == 'true')


def secret_ok():
    secret = os.getenv('SCAN_SECRET')
    return not secret or request.args.get('secret') == secret


def pct_change(target, entry):
    try: return (target / entry - 1.0) * 100.0 if entry else 0.0
    except Exception: return 0.0


def points_text(target, entry):
    pts = target - entry; sign = '+' if pts >= 0 else ''
    return f"{sign}{pts:.2f} pts"


def format_alert(x):
    tier=x.get('tier','STRONG')
    badge=('⚠️ RELAXED SETUP' if x.get('relaxed_mode') else {'HERO':'🏆 HERO SETUP','STRONG':'🟢 STRONG SETUP','WATCH':'🟡 WATCH'}.get(tier,'⚠️ RELAXED SETUP'))
    entry_low=x.get('entry_low'); entry_high=x.get('entry_high')
    premium=x.get('premium'); bid=x.get('bid'); ask=x.get('ask')
    stop=x.get('stop_loss'); tp1=x.get('tp1'); tp2=x.get('tp2'); tp3=x.get('tp3')
    data_mode=x.get('data_mode','INDICATIVE').upper()
    session=x.get('session',phase())
    def money(v):
        return f"${float(v):.2f}" if isinstance(v,(int,float)) else "Target unavailable"
    def pct(v,base):
        try: return f"{(float(v)/float(base)-1)*100:+.0f}%"
        except Exception: return "n/a"
    level_line=lambda label,v: f"{label}: {money(v)}"
    reasons=', '.join(x.get('reasons',[])) or '—'
    setup_text=reasons.replace(', ', '\n• ')
    risk=[]
    if x.get('spread_pct',0)>25: risk.append('Wide spread vs preferred threshold')
    if x.get('volume',0)<5 and x.get('open_interest',0)<10: risk.append('Low option liquidity')
    if x.get('trend_4h')=='NEUTRAL': risk.append('4H neutral')
    if x.get('market_regime') in ('SIDEWAYS','CHOPPY'): risk.append(f"{x.get('market_regime')} regime")
    if not risk: risk.append('No major model risk flag')
    target_note='Model levels are derived from the current quote and available ATR; they are not guaranteed fills.'
    return (
        f"{badge}\n\n"
        f"Symbol: {x.get('symbol','')}\n"
        f"Direction: {x.get('signal','')}\n"
        f"Contract: {x.get('contract','')}\n"
        f"Expiry/DTE: {x.get('dte','—')} days\n"
        f"Strike: {money(x.get('strike'))}\n\n"
        f"CURRENT QUOTE\n"
        f"Bid: {money(bid)} | Ask: {money(ask)} | Mid: {money(premium)}\n"
        f"Entry: {money(entry_low)} – {money(entry_high)}\n\n"
        f"RISK / TARGETS\n"
        f"{level_line('STOP',stop)}\n"
        f"{level_line('TP1',tp1)}\n"
        f"{level_line('TP2',tp2)}\n"
        f"{level_line('TP3',tp3)}\n"
        f"Risk/contract: ${float(x.get('risk_dollars_per_contract',0) or 0):.2f}\n"
        f"Suggested contracts: {int(x.get('suggested_contracts',0) or 0)}\n\n"
        f"UNDERLYING\n"
        f"Entry: {x.get('underlying_entry') if x.get('underlying_entry') is not None else 'Target unavailable'}\n"
        f"Stop: {x.get('underlying_stop_loss') if x.get('underlying_stop_loss') is not None else 'Target unavailable'}\n"
        f"TP1/TP2/TP3: {x.get('underlying_tp1') if x.get('underlying_tp1') is not None else 'Target unavailable'} / "
        f"{x.get('underlying_tp2') if x.get('underlying_tp2') is not None else 'Target unavailable'} / "
        f"{x.get('underlying_tp3') if x.get('underlying_tp3') is not None else 'Target unavailable'}\n\n"
        f"SCORE: {float(x.get('score',0) or 0):.0f}/100 | {tier}\n"
        f"Explosive score: {float(x.get('explosive_score',0) or 0):.0f}/100\n"
        f"Direction evidence: {x.get('direction_evidence',0)} | Opposite: {x.get('opposite_evidence',0)}\n"
        f"15M: {x.get('trend_15m','NEUTRAL')} | 4H: {x.get('trend_4h','NEUTRAL')}\n"
        f"VWAP: {x.get('vwap_state','—')} | Breakout: {x.get('breakout','NO')}\n"
        f"Volume: {x.get('volume',0)} | RV: {x.get('volume_ratio',0)} | OI: {x.get('open_interest',0)}\n"
        f"Spread: {x.get('spread_pct',0):.1f}% | Delta: {x.get('delta',0):.2f}\n"
        f"Regime: {x.get('market_regime','—')} | Data: {data_mode}\n\n"
        f"SETUP\n• {setup_text}\n\n"
        f"RISK\n• " + "\n• ".join(risk) + "\n\n"
        f"{target_note}\n"
        f"Model upside scenario: {('+'+str(x.get('projected_upside_pct'))+'%') if x.get('projected_upside_pct') is not None else 'Target unavailable'}\n"
        f"⚠️ Analysis + alerts only. No brokerage execution. No guaranteed profit."
    )

def market_warning(diagnostics):
    regs=[d.get('market_regime') for d in diagnostics.values() if isinstance(d,dict) and d.get('market_regime')]
    if not regs:return None
    side=regs.count('SIDEWAYS'); chop=regs.count('CHOPPY')
    if side+chop < max(1, len(regs)//2):return None
    regime='SIDEWAYS' if side>=chop else 'CHOPPY'
    return (f"⚠️ MARKET WARNING — {regime}\n\n"
            f"أغلب الرموز التي تم فحصها تظهر سوقًا {regime.lower()}.\n"
            "الاتجاه ضعيف/متذبذب، واحتمال الكسر الكاذب أعلى.\n"
            "⛔ البوت لن يرسل صفقات منخفضة الجودة في هذه الحالة.")


def _apply_progress(msg):
    with lock:
        if msg.get('scan_id') and msg.get('scan_id') != state.get('scan_id'): return
        state['scan_stage'] = msg.get('stage', state['scan_stage'])
        for key in ('symbols_scanned','contracts_scanned','last_candidates'):
            if key in msg: state[key] = msg[key]
        if msg.get('stage') in ('fetching_bars','fetching_bars_page','fetching_bars_complete'):
            if 'timeframe' in msg: state['fetching_timeframe'] = msg.get('timeframe')
            if 'completed' in msg: state['fetching_completed'] = msg.get('completed', state.get('fetching_completed',0))
            if 'timeframes' in msg: state['fetching_total'] = msg.get('timeframes', state.get('fetching_total',4))
            if 'state' in msg: state['fetching_state'] = msg.get('state')
        if msg.get('stage') == 'provider_retry':
            err = f"Alpaca {msg.get('status_code')} {msg.get('endpoint')} retry {msg.get('retry')}"
            state['provider_errors'] = (state.get('provider_errors') or [])[-9:] + [err]


def _scan_process_worker(session_name, scan_id, conn):
    """Runs the V13.8 scanner in a killable child process; Telegram stays in the parent."""
    try:
        def cb(stage, **fields):
            try: conn.send({'type':'progress','scan_id':scan_id,'stage':stage,**fields})
            except Exception: pass
        set_progress_callback(cb)
        results, diagnostics = scan_all(session_name)
        conn.send({'type':'result','scan_id':scan_id,'results':results,'diagnostics':diagnostics})
    except Exception as e:
        try:
            conn.send({'type':'error','scan_id':scan_id,'error':f'{type(e).__name__}: {e}','provider':getattr(e,'provider',None),'endpoint':getattr(e,'endpoint',''),'status_code':getattr(e,'status_code',None),'retry_count':getattr(e,'retry_count',None)})
        except Exception: pass
    finally:
        try: conn.close()
        except Exception: pass


def _tier_rank(x):
    return {'HERO':3,'STRONG':2,'WATCH':1}.get(x.get('tier'),0)

def _sorted_setups(results):
    return sorted(
        list(results or []),
        key=lambda x:(
            _tier_rank(x),
            float(x.get('score',0) or 0),
            float(x.get('explosive_score',0) or 0),
            float(x.get('confidence',0) or 0)
        ),
        reverse=True
    )


def _finalize_scan(p, scan_id, results, diagnostics, started_at, error=None):
    now=datetime.now(TZ)
    duration=max(0.0,(now-datetime.fromisoformat(started_at)).total_seconds())
    max_alerts=max(1,int(os.getenv('MAX_ALERTS_PER_SCAN', os.getenv('MAX_ALERTS','5')) or 5))
    cooldown=max(300,int(os.getenv('ALERT_COOLDOWN_SECONDS','300') or 900))
    ordered=_sorted_setups(results)
    alert_diag={
        'max_alerts':max_alerts,'cooldown_seconds':cooldown,'candidates':[],
        'eligible':0,'cooldown_rejected':0,'max_alerts_rejected':0,
        'other_rejected':0,'send_attempted':0,'send_success':0,'send_failed':0
    }
    alerts=0

    if error is None:
        for x in ordered:
            symbol=str(x.get('symbol',''))
            direction=str(x.get('signal',''))
            contract=str(x.get('contract',''))
            # Duplicate protection is explicitly symbol + direction + contract.
            key=f"{symbol}:{direction}:{contract}"
            item={
                'symbol':symbol,'direction':direction,'contract':contract,
                'tier':x.get('tier'),'score':x.get('score'),
                'explosive_score':x.get('explosive_score'),'premium':x.get('premium'),
                'bid':x.get('bid'),'ask':x.get('ask'),'status':'pending','reason':None
            }
            last=state.get('last_alert_keys',{}).get(key)
            cooldown_active=False
            if last and cooldown>0:
                try: cooldown_active=(now-datetime.fromisoformat(last)).total_seconds()<cooldown
                except Exception: cooldown_active=False
            if cooldown_active:
                item.update(status='rejected',reason=f'cooldown active for {symbol} {direction} {contract}')
                alert_diag['cooldown_rejected']+=1
                alert_diag['candidates'].append(item)
                continue
            if alerts>=max_alerts:
                item.update(status='rejected',reason=f'max alerts per scan reached ({max_alerts})')
                alert_diag['max_alerts_rejected']+=1
                alert_diag['candidates'].append(item)
                continue

            item['status']='eligible'
            item['reason']='qualified setup selected by tier + score'
            alert_diag['eligible']+=1
            alert_diag['send_attempted']+=1
            item['send_attempted']=True
            try:
                sent=bool(send_message(format_alert(x)))
                if sent:
                    alerts+=1
                    alert_diag['send_success']+=1
                    item['status']='sent'; item['send_success']=True
                    with lock: state.setdefault('last_alert_keys',{})[key]=now.isoformat()
                else:
                    alert_diag['send_failed']+=1
                    item['status']='send_failed'
                    item['reason']='Telegram send_message returned False'
                    item['send_success']=False
            except Exception as e:
                alert_diag['send_failed']+=1
                item['status']='send_failed'
                item['reason']=f'Telegram {type(e).__name__}: {e}'
                item['send_success']=False
            alert_diag['candidates'].append(item)

        warning=market_warning(diagnostics)
        if warning and state.get('market_warning_sent') is None:
            if send_message(warning):
                state['market_warning_sent']=now.isoformat()
        elif not warning:
            state['market_warning_sent']=None

        # One compact post-scan summary keeps the ranking visible without turning
        # every WATCH contract into a separate alert.
        if os.getenv('SCAN_SUMMARY_ENABLED','true').lower() in ('1','true','yes','on'):
            meta=diagnostics.get('__meta__',{}) if isinstance(diagnostics,dict) else {}
            summary=[f"🟢 MARKET SCAN COMPLETE",
                     f"Symbols: {meta.get('symbols_scanned',0)}",
                     f"Contracts: {meta.get('contracts_scanned',0)}",
                     f"🏆 HERO: {meta.get('heroes',0)}",
                     f"🟢 STRONG: {meta.get('strong',0)}",
                     f"🟡 WATCH: {meta.get('watch',0)}"]
            if ordered:
                summary.append("")
                summary.append("TOP SETUPS")
                for i,x in enumerate(ordered[:5],1):
                    summary.append(f"{i}. {x.get('symbol')} {x.get('signal')} — {x.get('tier')} — {float(x.get('score',0)):.0f} | {x.get('contract')}")
                    summary.append(f"   Entry {x.get('entry_low')}–{x.get('entry_high')} | SL {x.get('stop_loss')} | TP {x.get('tp1')}/{x.get('tp2')}/{x.get('tp3')}")
            else:
                summary.append("")
                summary.append("NO QUALIFIED SETUPS")
                for reason in (meta.get('no_setup_reasons') or ['No qualified setup'])[:5]:
                    summary.append(f"• {reason}")
            send_message("\n".join(summary))

        # End-of-day: send up to three HERO setups, including SPXW, without
        # choosing a contract merely because its premium is lower.
        hero_enabled=os.getenv('MARKET_CLOSE_HERO_ENABLED','true').lower() in ('1','true','yes','on')
        if hero_enabled and p=='AFTER_HOURS':
            today_key=now.date().isoformat()
            with lock: hero_already_sent=state.get('hero_sent_date')==today_key
            heroes=[x for x in ordered if x.get('tier')=='HERO'][:3]
            if heroes and not hero_already_sent:
                chunks=["🌙 END OF DAY — TOP HERO SETUPS",""]
                for i,x in enumerate(heroes,1):
                    chunks.append(
                        f"{i}. {x.get('symbol')} {x.get('signal')} — HERO — Score {float(x.get('score',0)):.0f}\n"
                        f"Contract: {x.get('contract')}\n"
                        f"Entry: {x.get('entry_low')} – {x.get('entry_high')}\n"
                        f"Stop: {x.get('stop_loss')}\n"
                        f"TP1/TP2/TP3: {x.get('tp1')} / {x.get('tp2')} / {x.get('tp3')}\n"
                    )
                chunks.append("Analysis + alerts only; levels are model-derived and not guaranteed fills.")
                if send_message("\n".join(chunks)):
                    with lock: state['hero_sent_date']=today_key

    meta=diagnostics.get('__meta__',{}) if isinstance(diagnostics,dict) else {}
    provider_errors=[]
    for k,v in (diagnostics or {}).items():
        if isinstance(v,dict) and v.get('error'):
            provider_errors.append({
                'source':k,'error':v.get('error'),'endpoint':v.get('endpoint'),
                'status_code':v.get('status_code'),'retry_count':v.get('retry_count')
            })

    with lock:
        state.update(
            last_scan=now.isoformat(), last_candidates=len(results), last_alerts=alerts,
            last_error=error, last_session=p, scan_id=scan_id, scan_running=False,
            scan_finished=now.isoformat(), scan_duration=round(duration,2),
            scan_stage='complete' if error is None else 'failed',
            last_top=ordered[:max_alerts], diagnostics=diagnostics or {},
            alert_diagnostics=alert_diag,
            symbols_scanned=meta.get('symbols_scanned',state.get('symbols_scanned',0)),
            contracts_scanned=meta.get('contracts_scanned',state.get('contracts_scanned',0)),
            provider_errors=provider_errors, scan_process_pid=None
        )

def _watch_scan(proc, conn, p, scan_id, started_at):
    timeout=max(30,int(os.getenv('SCAN_TIMEOUT_SECONDS','180')))
    result=None; fatal_error=None
    try:
        while True:
            while conn.poll(0.1):
                msg=conn.recv()
                if msg.get('type')=='progress':
                    _apply_progress(msg)
                elif msg.get('type')=='result': result=msg; break
                elif msg.get('type')=='error': fatal_error=msg; break
            if result or fatal_error: break
            if not proc.is_alive(): break
            elapsed=(datetime.now(TZ)-datetime.fromisoformat(started_at)).total_seconds()
            if elapsed >= timeout:
                try: proc.terminate()
                except Exception: pass
                proc.join(timeout=5)
                _finalize_scan(p,scan_id,[],{},started_at,error=f'Scanner timeout after {timeout}s at stage {state.get("scan_stage")}')
                return
        proc.join(timeout=2)
        if result:
            _apply_progress({'scan_id':scan_id,'stage':'alerts'})
            _finalize_scan(p,scan_id,result.get('results') or [],result.get('diagnostics') or {},started_at)
        elif fatal_error:
            err=fatal_error.get('error') or 'scanner worker failed'
            detail=err
            if fatal_error.get('status_code'): detail += f" | HTTP {fatal_error['status_code']} | {fatal_error.get('endpoint','')} | retry {fatal_error.get('retry_count',0)}"
            _finalize_scan(p,scan_id,[],{},started_at,error=detail)
        else:
            _finalize_scan(p,scan_id,[],{},started_at,error='Scanner worker exited without result')
    except Exception as e:
        try: proc.terminate()
        except Exception: pass
        proc.join(timeout=2)
        _finalize_scan(p,scan_id,[],{},started_at,error=f'Watcher {type(e).__name__}: {e}')
    finally:
        try: conn.close()
        except Exception: pass


def start_scan(p):
    if p == 'CLOSED': return None, False
    with lock:
        if state['scan_running']: return None, False
        scan_id=uuid.uuid4().hex[:10]; started=datetime.now(TZ).isoformat()
        state.update(scan_running=True, scan_id=scan_id, scan_started=started, scan_finished=None,
                     scan_duration=None, scan_stage='starting', last_error=None, last_session=p,
                     symbols_scanned=0, contracts_scanned=0, provider_errors=[], fetching_timeframe=None,
                     fetching_completed=0, fetching_total=4, fetching_state=None)
    ctx=multiprocessing.get_context('fork' if 'fork' in multiprocessing.get_all_start_methods() else 'spawn')
    parent_conn, child_conn=ctx.Pipe(duplex=False)
    proc=ctx.Process(target=_scan_process_worker,args=(p,scan_id,child_conn),daemon=True)
    try:
        proc.start(); child_conn.close()
    except Exception as e:
        try: parent_conn.close()
        except Exception: pass
        with lock: state.update(scan_running=False,scan_stage='failed',last_error=f'Scanner start {type(e).__name__}: {e}')
        return None, False
    with lock: state['scan_process_pid']=proc.pid
    threading.Thread(target=_watch_scan,args=(proc,parent_conn,p,scan_id,started),daemon=True).start()
    return scan_id, True


def _status_text():
    _max_alerts=max(1,int(os.getenv('MAX_ALERTS_PER_SCAN',os.getenv('MAX_ALERTS','5')) or 5))
    _cooldown=max(300,int(os.getenv('ALERT_COOLDOWN_SECONDS','300') or 900))
    with lock: s=dict(state); ad=dict(s.get('alert_diagnostics') or {})
    td=telegram_diagnostics(); ps=provider_status()
    lines=[
        "🟢 Bot status",
        f"Phase: {phase_label(phase())}",
        f"Running: {s['running']}",
        f"Scan running: {s['scan_running']}",
        f"Scan stage: {s.get('scan_stage')}",
        f"Bars: {s.get('fetching_completed',0)}/{s.get('fetching_total',4)} {s.get('fetching_timeframe') or ''} {s.get('fetching_state') or ''}",
        f"Scan ID: {s.get('scan_id') or '—'}",
        f"Symbols scanned: {s.get('symbols_scanned',0)}",
        f"Contracts scanned: {s.get('contracts_scanned',0)}",
        f"Last candidates: {s['last_candidates']}",
        f"Last alerts: {s['last_alerts']}",
        f"Last scan: {s['last_scan'] or '—'}",
        f"Scan duration: {s.get('scan_duration') if s.get('scan_duration') is not None else '—'} s",
        f"Last error: {s['last_error'] or 'None'}",
        f"Telegram polling: {td.get('telegram_running')}",
        f"Telegram last update: {td.get('telegram_last_update') or '—'}",
        f"Telegram last error: {td.get('telegram_last_error') or 'None'}",
        f"Provider: {ps.get('name')} | Options feed: {ps.get('options_feed')} | Underlying feed: {ps.get('underlying_feed')}",
        f"Data mode: {ps.get('data_mode')}",
        f"Provider errors: {len(s.get('provider_errors') or [])}",
        "",
        "🔔 Alert diagnostics",
        f"Max alerts/scan: {ad.get('max_alerts',_max_alerts)} | Cooldown: {ad.get('cooldown_seconds',_cooldown)}s",
        f"Candidates: {len(ad.get('candidates') or [])} | Eligible: {ad.get('eligible',0)} | Cooldown rejected: {ad.get('cooldown_rejected',0)} | Max-alerts rejected: {ad.get('max_alerts_rejected',0)}",
        f"Send attempted: {ad.get('send_attempted',0)} | Success: {ad.get('send_success',0)} | Failed: {ad.get('send_failed',0)}",
    ]
    di=s.get('diagnostics') or {}
    meta=di.get('__meta__') if isinstance(di,dict) else {}
    if isinstance(meta,dict):
        lines += [
            "",
            "📊 MARKET SCAN SUMMARY",
            f"Potential setups: {meta.get('candidates',0)}",
            f"🏆 HERO: {meta.get('heroes',0)} | 🟢 STRONG: {meta.get('strong',0)} | 🟡 WATCH: {meta.get('watch',0)}"
        ]
        if not meta.get('candidates'):
            reasons=meta.get('no_setup_reasons') or ['No qualified setup']
            lines.append("NO QUALIFIED SETUPS")
            lines.extend(f"• {r}" for r in reasons[:6])

    # Every symbol gets its own diagnostics, not only SPXW.
    for key,d in di.items() if isinstance(di,dict) else []:
        if key.startswith('__') or not isinstance(d,dict): continue
        r=d.get('rejections') or {}
        lines.append("")
        lines.append(
            f"{key}: {'OK' if not d.get('error') else 'ERROR'} | Chain: {d.get('chain_items',0)} | "
            f"HERO: {d.get('hero',0)} | STRONG: {d.get('strong',0)} | WATCH: {d.get('watch',0)}"
        )
        lines.append(
            f"Rejected: Prefix={r.get('prefix',0)} | BadContract={r.get('bad_contract',0)} | "
            f"DTE={r.get('dte',0)} | Premium soft={r.get('premium',0)} | HardPremium={r.get('hard_premium',0)} | Spread={r.get('spread',0)} | "
            f"Liquidity={r.get('liquidity',0)} | Score={r.get('score',0)} | 4H={r.get('alignment',0)} | "
            f"Regime={r.get('regime',0)} | Stale={r.get('quote_stale',0)} | NoBid={r.get('no_bid',0)} | "
            f"NoAsk={r.get('no_ask',0)} | InvalidQuote={r.get('invalid_quote',0)}"
        )
        if d.get('no_setup_reasons'):
            lines.append("Reason: " + " | ".join(d.get('no_setup_reasons')[:4]))
        if d.get('error'):
            lines.append(f"Provider detail: {str(d.get('error'))[:300]}")

    provider_errors=s.get('provider_errors') or []
    if provider_errors:
        lines.append("")
        lines.append("⚠️ Provider error details")
        for pe in provider_errors[:8]:
            if isinstance(pe,dict):
                lines.append(f"• {pe.get('source','?')}: {pe.get('error','?')} | HTTP={pe.get('status_code','—')} | {pe.get('endpoint','')}")
    for c in (ad.get('candidates') or [])[:50]:
        lines.append(
            f"• {c.get('symbol','—')} {c.get('direction','—')} | {c.get('contract','—')} | "
            f"{c.get('tier','—')} | score={c.get('score','—')} | premium=${c.get('premium','—')} | "
            f"status={c.get('status','—')} | {c.get('reason','—')}"
        )
    return "\n".join(lines)

def telegram_command_loop():
    offset=None; allowed_chat=str(os.getenv('TELEGRAM_CHAT_ID','')).strip(); mark_polling_running(True)
    try:
        while True:
            try:
                updates=get_updates(offset=offset,timeout=20)
                for u in updates:
                    offset=int(u.get('update_id',0))+1
                    msg=u.get('message') or {}; chat=msg.get('chat') or {}; chat_id=str(chat.get('id','')).strip()
                    if not chat_id: continue
                    text=(msg.get('text') or '').strip()
                    if not text: continue
                    cmd=text.split()[0].split('@')[0].lower()
                    if cmd in ('/start','/help'):
                        send_message('🤖 Options Opportunity Bot V13.8\n\nAlert-only options scanner.\n/start — start\n/help — help\n/status — diagnostics\n/scan — manual scan\n/top — top setups\n/heroes — HERO setups\n/watchlist — WATCH setups\n/diagnostics — rejection diagnostics',chat_id); continue
                    if cmd=='/privacy':
                        send_message('🔐 البوت Alert-only ولا ينفذ صفقات عبر وسيط.',chat_id); continue
                    if not allowed_chat or chat_id != allowed_chat:
                        continue
                    if cmd=='/status':
                        send_message(_status_text(),chat_id)
                    elif cmd=='/scan':
                        p=phase(); sid,started=start_scan(p)
                        send_message(f'🔎 Scan started\nPhase: {phase_label(p)}\nScan ID: {sid}' if started else f'⚠️ Scan already running\nScan ID: {state.get("scan_id")}',chat_id)
                    elif cmd in ('/top','/heroes','/watchlist'):
                        with lock: top=list(state.get('last_top') or [])
                        if cmd=='/heroes':
                            top=[x for x in top if x.get('tier')=='HERO']
                        elif cmd=='/watchlist':
                            top=[x for x in top if x.get('tier')=='WATCH']
                        if not top:
                            send_message('ℹ️ لا توجد setups من هذا النوع في آخر scan.',chat_id)
                        else:
                            for x in top[:10]: send_message(format_alert(x),chat_id)
                    elif cmd=='/diagnostics':
                        send_message(_status_text(),chat_id)
                    else:
                        send_message('الأوامر: /start /help /status /scan /top /heroes /watchlist /diagnostics',chat_id)
            except Exception as e:
                # Keep polling alive; the error is captured by telegram_bot diagnostics.
                time.sleep(2)
    finally:
        mark_polling_running(False)


def loop():
    interval=max(60,int(os.getenv('SCAN_INTERVAL_SECONDS','300')))
    while True:
        try:
            p=phase()
            if allowed(p): start_scan(p)
            else:
                with lock: state['last_session']=p
        except Exception as e:
            with lock: state['last_error']=f'Scanner loop {type(e).__name__}: {e}'
        time.sleep(interval)

@app.get('/')
def root(): return jsonify({'service':'options-opportunity-bot','version':'13.8.0','status':'ok','docs':'/health','scan':'/scan','scan_status':'/scan/status'})

@app.get('/health')
def health():
    with lock: s=dict(state)
    s['telegram_configured']=bool(os.getenv('TELEGRAM_BOT_TOKEN') and os.getenv('TELEGRAM_CHAT_ID')); s['scan_secret_configured']=bool(os.getenv('SCAN_SECRET')); s['provider']=provider_status(); s['telegram']=telegram_diagnostics()
    return jsonify({'service':'options-opportunity-bot','version':'13.8.0','status':'ok','phase':phase(),'scanner':s})

@app.get('/status')
def status(): return health()

@app.route('/scan',methods=['GET','POST'])
def scan():
    if not secret_ok(): return jsonify({'ok':False,'error':'unauthorized'}),401
    p=phase(); sid,started=start_scan(p)
    if not started:
        with lock: current=state.get('scan_id')
        return jsonify({'ok':True,'accepted':False,'message':'scan already running','scan_id':current,'phase':p}),202
    return jsonify({'ok':True,'accepted':True,'message':'scan started in background','scan_id':sid,'phase':p,'status_url':'/scan/status'}),202

@app.get('/scan/status')
def scan_status():
    if not secret_ok(): return jsonify({'ok':False,'error':'unauthorized'}),401
    with lock: s=dict(state)
    s['telegram']=telegram_diagnostics(); return jsonify({'ok':True,**s,'phase':phase()})

@app.get('/telegram-test')
def telegram_test():
    secret=os.getenv('TELEGRAM_TEST_SECRET')
    if secret and request.args.get('secret')!=secret: return jsonify({'ok':False,'error':'unauthorized'}),401
    ok=send_message('✅ Options bot Telegram test: connection OK')
    return jsonify({'ok':ok,'configured':bool(os.getenv('TELEGRAM_BOT_TOKEN') and os.getenv('TELEGRAM_CHAT_ID'))})

@app.post('/webhook')
def webhook():
    hook_secret=os.getenv('WEBHOOK_SECRET')
    if hook_secret and request.args.get('secret')!=hook_secret: return jsonify({'ok':False,'error':'unauthorized'}),401
    payload=request.get_json(silent=True); text=payload if payload is not None else request.get_data(as_text=True)
    if isinstance(text,dict): text=' | '.join(f'{k}={v}' for k,v in text.items())
    sent=send_message(f'📡 TradingView alert\n{text}')
    return jsonify({'ok':True,'telegram_sent':sent})

if __name__=='__main__':
    threading.Thread(target=loop,daemon=True,name='scanner-loop').start()
    if os.getenv('TELEGRAM_COMMANDS_ENABLED','true').lower()=='true': threading.Thread(target=telegram_command_loop,daemon=True,name='telegram-polling').start()
    app.run(host='0.0.0.0',port=int(os.getenv('PORT','10000')),threaded=True)
