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
    'scan_duration': None, 'scan_stage': 'idle', 'symbols_scanned': 0, 'contracts_scanned': 0, 'valid_contracts': 0, 'contracts_scored': 0,
    'last_top': [], 'diagnostics': {}, 'last_alert_keys': {}, 'weak_alert_keys': {}, 'alert_snapshots': {}, 'market_warning_sent': None,
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
    badge={'HERO':'🏆 HERO SETUP','STRONG':'🟢 STRONG SETUP','WATCH':'🟡 WATCH','MOONSHOT':'🚀 MOONSHOT OPPORTUNITY'}.get(tier,'🔎 TOP CANDIDATE')
    if x.get('moonshot'): badge += ' 🚀 MOONSHOT'
    def money(v):
        return f"${float(v):.2f}" if isinstance(v,(int,float)) else "—"
    reasons=x.get('reasons') or []
    spread=x.get('spread_pct')
    spread_text=f"{float(spread):.1f}%" if spread is not None else "—"
    risk=[]
    if spread is not None and float(spread)>25: risk.append('Wide spread vs preferred threshold')
    if x.get('volume',0)<5 and x.get('open_interest',0)<10: risk.append('Low option liquidity')
    if x.get('trend_4h')=='NEUTRAL': risk.append('4H neutral')
    if x.get('market_regime') in ('SIDEWAYS','CHOPPY'): risk.append(f"{x.get('market_regime')} regime")
    if x.get('quote_is_stale'): risk.append('Quote is stale')
    if not risk: risk.append('No major model risk flag')
    return (
        f"{badge}\n\n"
        f"Underlying: {x.get('symbol','—')}\n"
        f"Contract: {x.get('contract','—')}\n"
        f"Direction: {x.get('signal','—')}\n"
        f"Strike: {money(x.get('strike'))}\n"
        f"Expiration/DTE: {x.get('dte','—')}\n\n"
        f"Entry: {money(x.get('entry'))}\n"
        f"Current: {money(x.get('premium'))}\n"
        f"Bid: {money(x.get('bid'))} | Ask: {money(x.get('ask'))}\n"
        f"Spread: {spread_text}\n"
        f"Volume: {x.get('volume',0)} | OI: {x.get('open_interest',0)} | DTE: {x.get('dte','—')}\n\n"
        f"Score: {float(x.get('score',0) or 0):.0f}/100 | Setup: {tier or 'BELOW_WATCH'}\n"
        f"Liquidity: {x.get('score_components',{}).get('volume_oi','—')}\n"
        f"Momentum: {x.get('score_components',{}).get('momentum','—')}\n"
        f"Trend: {x.get('score_components',{}).get('trend','—')}\n"
        f"Technical: {x.get('score_components',{}).get('price_action','—')}\n"
        f"Options: {x.get('score_components',{}).get('options_activity','—')}\n\n"
        f"🎯 TARGETS\nTP1: {money(x.get('tp1'))}\nTP2: {money(x.get('tp2'))}\nTP3: {money(x.get('tp3'))}\n\n"
        f"🛑 STOP: {money(x.get('stop_loss'))}\n"
        f"Risk/Reward: {x.get('risk_reward','—')}\n"
        f"Expected Profit %: {x.get('expected_profit_pct','—')}\n"
        f"Expected Loss %: {x.get('expected_loss_pct','—')}\n"
        f"📈 Expected Move: {x.get('projected_upside_pct','—')}%\n\n"
        f"🔥 Reasons:\n• " + "\n• ".join(reasons[:8] or ['—']) + "\n\n"
        f"🚀 MOONSHOT MODEL: {'YES' if x.get('moonshot') else 'NO'}\n"
        f"Moonshot flags: {', '.join(x.get('moonshot_flags',[])[:6]) or '—'}\n\n"
        f"Risk flags:\n• " + "\n• ".join(risk) + "\n\n"
        f"Quote source: {x.get('quote_source','—')}\n"
        f"Quote age: {x.get('quote_age_sec','—')} sec\n"
        f"Data mode: {x.get('data_mode','—')}\n"
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
        # Scanner progress uses the canonical `candidates` field; the public
        # bot state uses `last_candidates`. Keep both paths synchronized.
        if 'candidates' in msg:
            state['last_candidates'] = int(msg.get('candidates') or 0)
        if 'heroes' in msg:
            state['hero_count'] = int(msg.get('heroes') or 0)
        if 'strong' in msg:
            state['strong_count'] = int(msg.get('strong') or 0)
        if 'watch' in msg:
            state['watch_count'] = int(msg.get('watch') or 0)
        if 'moonshots' in msg:
            state['moonshot_count'] = int(msg.get('moonshots') or 0)
        if 'valid_contracts' in msg: state['valid_contracts'] = msg['valid_contracts']
        if 'contracts_scored' in msg: state['contracts_scored'] = msg['contracts_scored']
        if msg.get('stage') in ('fetching_bars','fetching_bars_page','fetching_bars_complete'):
            if 'timeframe' in msg: state['fetching_timeframe'] = msg.get('timeframe')
            if 'completed' in msg: state['fetching_completed'] = msg.get('completed', state.get('fetching_completed',0))
            if 'timeframes' in msg: state['fetching_total'] = msg.get('timeframes', state.get('fetching_total',4))
            if 'state' in msg: state['fetching_state'] = msg.get('state')
        if msg.get('stage') == 'provider_retry':
            err = f"Alpaca {msg.get('status_code')} {msg.get('endpoint')} retry {msg.get('retry')}"
            state['provider_errors'] = (state.get('provider_errors') or [])[-9:] + [err]


def _scan_process_worker(session_name, scan_id, conn):
    """Runs the V14.4 scanner in a killable child process; Telegram stays in the parent."""
    try:
        def cb(stage, **fields):
            # Progress is telemetry. Build one payload dictionary first so
            # duplicate keys can never break the worker or scan process.
            try:
                payload = {'type':'progress', 'scan_id':scan_id, 'stage':stage}
                payload.update(dict(fields or {}))
                payload['stage'] = stage
                payload['scan_id'] = scan_id
                conn.send(payload)
            except Exception:
                pass
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
    return {'HERO':4,'STRONG':3,'WATCH':2,'MOONSHOT':1}.get(x.get('tier'),0)

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



def _weak_alert_text(x, previous):
    return (
        f"⚠️ CONTRACT WEAKNESS ALERT\n\n"
        f"Contract: {x.get('contract','—')}\n"
        f"Underlying: {x.get('symbol','—')} | {x.get('signal','—')}\n"
        f"Previous score: {float(previous.get('score',0) or 0):.0f} → Current: {float(x.get('score',0) or 0):.0f}\n"
        f"Previous premium: ${float(previous.get('premium',0) or 0):.2f} → Current: ${float(x.get('premium',0) or 0):.2f}\n"
        f"Stop Loss: ${float(x.get('stop_loss',0) or 0):.2f}\n"
        f"Tier: {x.get('tier') or 'BELOW_WATCH'}\n\n"
        f"Reason: {'premium at/below model stop' if float(x.get('premium',0) or 0) <= float(x.get('stop_loss',0) or 0) else 'model score deterioration >=20 points'}\n"
        f"⚠️ Model warning only — recheck the live quote and thesis."
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
                    state.setdefault('alert_snapshots',{})[key]={
                        'score':float(x.get('score',0) or 0),'premium':float(x.get('premium',0) or 0),
                        'stop_loss':float(x.get('stop_loss',0) or 0),'tier':x.get('tier'),
                        'symbol':symbol,'contract':contract,'signal':direction}
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

        # Monitor previously alerted contracts for deterioration on every scan.
        weak_cooldown=max(300,int(os.getenv('WEAK_ALERT_COOLDOWN_SECONDS','900') or 900))
        weak_max=max(0,int(os.getenv('MAX_WEAK_ALERTS_PER_SCAN','3') or 3))
        weak_sent=0
        for x in ordered:
            key=f"{x.get('symbol','')}:{x.get('signal','')}:{x.get('contract','')}"
            previous=state.get('alert_snapshots',{}).get(key)
            if not previous or weak_sent>=weak_max:
                continue
            current=float(x.get('premium',0) or 0)
            stop=float(x.get('stop_loss',0) or 0)
            prev_score=float(previous.get('score',0) or 0)
            cur_score=float(x.get('score',0) or 0)
            weak = (stop>0 and current>0 and current<=stop) or (prev_score-cur_score>=20)
            if not weak:
                continue
            last_weak=state.get('weak_alert_keys',{}).get(key)
            if last_weak:
                try:
                    if (now-datetime.fromisoformat(last_weak)).total_seconds()<weak_cooldown:
                        continue
                except Exception:
                    pass
            if send_message(_weak_alert_text(x,previous)):
                weak_sent += 1
                with lock:
                    state.setdefault('weak_alert_keys',{})[key]=now.isoformat()

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
                     f"🟡 WATCH: {meta.get('watch',0)}",
                     f"🚀 MOONSHOT: {meta.get('moonshots',0)}"]
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
    if isinstance(meta, dict):
        meta['scan_id'] = scan_id
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
            last_top=(meta.get('top_candidates') or ordered[:10]), diagnostics=diagnostics or {},
            alert_diagnostics=alert_diag,
            symbols_scanned=meta.get('symbols_scanned',state.get('symbols_scanned',0)),
            contracts_scanned=meta.get('contracts_scanned',state.get('contracts_scanned',0)),
            valid_contracts=meta.get('valid_contracts',state.get('valid_contracts',0)),
            contracts_scored=meta.get('contracts_scored',state.get('contracts_scored',0)),
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
                     symbols_scanned=0, contracts_scanned=0, valid_contracts=0, contracts_scored=0, provider_errors=[], fetching_timeframe=None,
                     fetching_completed=0, fetching_total=4, fetching_state=None, last_candidates=0, hero_count=0, strong_count=0, watch_count=0, moonshot_count=0)
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
    with lock:
        s=dict(state)
        ad=dict(s.get('alert_diagnostics') or {})
        di=dict(s.get('diagnostics') or {})
    td=telegram_diagnostics() or {}
    try:
        ps=provider_status() or {}
    except Exception as e:
        ps={'name':'Alpaca','options_feed':os.getenv('ALPACA_OPTIONS_FEED','—'),
            'underlying_feed':'iex','data_mode':'UNKNOWN','error':f'{type(e).__name__}: {e}'}
    meta=di.get('__meta__') if isinstance(di,dict) else {}
    meta=meta if isinstance(meta,dict) else {}
    c=meta.get('counters') or {}
    lines=[
        "🟢 BOT STATUS",
        f"Phase: {phase_label(phase())}",
        f"ET clock: {datetime.now(TZ).strftime('%Y-%m-%d %H:%M:%S %Z')}",
        f"Running: {s.get('running',False)}",
        f"Scan running: {s.get('scan_running',False)}",
        f"Scan stage: {s.get('scan_stage') or '—'}",
        f"Scan ID: {s.get('scan_id') or '—'}",
        f"Symbols scanned: {meta.get('symbols_scanned',s.get('symbols_scanned',0)) or 0}",
        f"Contracts scanned: {meta.get('contracts_scanned',s.get('contracts_scanned',0)) or 0}",
        f"Valid contracts: {meta.get('valid_contracts',s.get('valid_contracts',c.get('contracts_valid',0))) or 0}",
        f"Normalized: {c.get('contracts_normalized',0) or 0}",
        f"Quote stage: {c.get('contracts_quote_stage',0) or 0}",
        f"Candidates: {meta.get('candidates',s.get('last_candidates',0)) or 0}",
        f"HERO: {meta.get('heroes',c.get('hero_count',0)) or 0}",
        f"STRONG: {meta.get('strong',c.get('strong_count',0)) or 0}",
        f"WATCH: {meta.get('watch',c.get('watch_count',0)) or 0}",
        f"🚀 MOONSHOT: {meta.get('moonshots',c.get('moonshot_count',0)) or 0}",
        f"Last scan: {s.get('last_scan') or '—'}",
        f"Scan duration: {s.get('scan_duration') if s.get('scan_duration') is not None else '—'} s",
        f"Weak alerts tracked: {len(s.get('weak_alert_keys') or {})}",
        f"Last error: {s.get('last_error') or '—'}",
        f"Provider: {ps.get('name','Alpaca')}{' / DEGRADED' if ps.get('error') else ''}",
        f"Options feed: {ps.get('options_feed') or '—'}",
        f"Telegram polling: {bool(td.get('telegram_running'))}",
        f"Telegram last update: {td.get('telegram_last_update') if td.get('telegram_last_update') is not None else '—'}",
        f"Telegram last error: {td.get('telegram_last_error') or '—'}",
    ]
    if meta.get('zero_candidate_diagnostics'):
        z=meta['zero_candidate_diagnostics']
        lines += [
            "",
            "🔍 ZERO-CANDIDATE DIAGNOSTICS",
            f"Contracts scanned: {z.get('contracts_scanned',0)}",
            f"Contracts with valid quotes: {z.get('contracts_with_valid_quotes',0)}",
            f"Contracts stale: {z.get('contracts_stale',0)}",
            f"Contracts scored: {z.get('contracts_scored',0)}",
            f"Rejected price: {z.get('rejected_price',0)}",
            f"Rejected spread: {z.get('rejected_spread',0)}",
            f"Rejected liquidity: {z.get('rejected_liquidity',0)}",
            f"Rejected DTE: {z.get('rejected_dte',0)}",
            f"Rejected momentum: {z.get('rejected_momentum',0)}",
            f"Rejected trend: {z.get('rejected_trend',0)}",
            f"Rejected score: {z.get('rejected_score',0)}",
            f"Top rejection reason: {z.get('top_rejection_reason','—')}",
        ]
    if isinstance(c,dict):
        lines += [
            "",
            "📊 SCAN COUNTERS",
            f"Received: {c.get('contracts_received',0)} | Valid: {c.get('contracts_valid',0)} | Scored: {c.get('contracts_scored',0)}",
            f"Quotes: {c.get('contracts_with_quotes',0)} | Stale: {c.get('contracts_stale_quotes',0)}",
            f"Missing bid/ask/last: {c.get('contracts_missing_bid',0)}/{c.get('contracts_missing_ask',0)}/{c.get('contracts_missing_last',0)}",
            f"Missing volume/OI: {c.get('contracts_missing_volume',0)}/{c.get('contracts_missing_oi',0)}",
            f"Pool: {c.get('candidate_pool_size',0)}",
            f"Zero-candidate mode: {'DATA / VALIDATION FAILURE' if not meta.get('contracts_scored',0) and not meta.get('candidates',0) and c.get('contracts_received',0) else '—'}",
        ]
    lines += ["", "🔎 REJECTION BREAKDOWN",
               f"Received: {c.get('contracts_received',0)}",
               f"Normalized: {c.get('contracts_normalized',0)}",
               f"Expired: {c.get('expired',0)}",
               f"DTE rejected: {c.get('rejected_dte',0)}",
               f"Quote rejected: {c.get('missing_quote',0) + c.get('invalid_quote',0)}",
               f"Liquidity rejected: {c.get('rejected_liquidity',0)}",
               f"Premium rejected: {c.get('premium_too_low',0) + c.get('premium_too_high',0) + c.get('hard_premium',0)}",
               f"Technical rejected: {c.get('rejected_momentum',0) + c.get('rejected_trend',0) + c.get('rejected_direction',0) if 'rejected_direction' in c else c.get('rejected_momentum',0) + c.get('rejected_trend',0)}",
               f"Scored: {c.get('contracts_scored',0)}",
               f"Bad schema: {c.get('bad_contract_schema',0)} | Missing symbol: {c.get('missing_symbol',0)} | Strike: {c.get('invalid_strike',0)}",
               f"Missing bid/ask/last: {c.get('contracts_missing_bid',0)}/{c.get('contracts_missing_ask',0)}/{c.get('contracts_missing_last',0)}",
               f"Zero volume/OI: {c.get('zero_volume',0)}/{c.get('zero_open_interest',0)}",
               "", "🔔 ALERT DIAGNOSTICS",
               f"Max alerts/scan: {ad.get('max_alerts',os.getenv('MAX_ALERTS_PER_SCAN','5'))} | Cooldown: {ad.get('cooldown_seconds',os.getenv('ALERT_COOLDOWN_SECONDS','300'))}s",
               f"Eligible: {ad.get('eligible',0)} | Cooldown rejected: {ad.get('cooldown_rejected',0)} | Max-alerts rejected: {ad.get('max_alerts_rejected',0)}"]
    # Compact per-symbol health so one provider failure is visible without breaking status.
    for key,d in sorted(di.items()):
        if key.startswith('__') or not isinstance(d,dict): continue
        if d.get('error'):
            lines.append(f"⚠️ {key}: ERROR — {str(d.get('error'))[:180]}")
        else:
            r=d.get('rejections') or {}
            lines.append(f"✅ {key}: scanned | Chain {d.get('chain_items',0)} | Scored {d.get('scored',0)} | H/S/W {d.get('hero',0)}/{d.get('strong',0)}/{d.get('watch',0)}")
            if DEBUG_STATUS := (os.getenv('DEBUG_SCANNER','false').lower() in ('1','true','yes','on')):
                lines.append(f"   Rejects: price={r.get('price',0)} spread={r.get('spread',0)} dte={r.get('dte',0)} score={r.get('score',0)}")
    return "\n".join(lines)

def _top_text(limit=10):
    with lock:
        di=dict(state.get('diagnostics') or {})
        top=list(state.get('last_top') or [])
    meta=di.get('__meta__') if isinstance(di,dict) else {}
    if not top and isinstance(meta,dict): top=list(meta.get('top_candidates') or [])
    if not top:
        return "🔎 TOP CANDIDATES\nNo scored candidates are available from the last scan."
    lines=["🔎 TOP CANDIDATES"]
    for i,x in enumerate(top[:limit],1):
        lines += [
            f"{i}️⃣ {x.get('symbol','—')} {x.get('contract','—')}",
            f"Score: {float(x.get('score',0) or 0):.1f} | Setup: {x.get('tier') or 'BELOW_WATCH'}",
            f"Underlying: {x.get('underlying','—')} | {x.get('signal','—')} | Strike: {x.get('strike','—')} | DTE: {x.get('dte','—')}",
            f"Premium: ${x.get('premium','—')} | Bid: {x.get('bid','—')} | Ask: {x.get('ask','—')} | Spread: {x.get('spread_pct','—')}%",
            f"Volume: {x.get('volume',0)} | OI: {x.get('open_interest',0)} | Quote: {x.get('quote_source','—')} age={x.get('quote_age_sec','—')}",
            f"Reason: {', '.join(x.get('reasons',[])[:4]) or '—'}",
            ""
        ]
    return "\n".join(lines).strip()


def _diagnostics_text():
    with lock:
        di=dict(state.get('diagnostics') or {})
    meta=di.get('__meta__') if isinstance(di,dict) else {}
    c=(meta or {}).get('counters') or {}
    z=(meta or {}).get('zero_candidate_diagnostics') or {}
    lines=[
        "🔍 SCAN DIAGNOSTICS",
        f"Scan ID: {(meta or {}).get('scan_id') or '—'}",
        f"Phase: {phase_label(phase())}",
        f"Symbols: {(meta or {}).get('symbols_scanned',0)}",
        f"Contracts: {(meta or {}).get('contracts_scanned',0)}",
        f"Received: {c.get('contracts_received',0)}",
        f"Normalized: {c.get('contracts_normalized',0)}",
        f"Quote stage: {c.get('contracts_quote_stage',0)}",
        f"Valid: {(meta or {}).get('valid_contracts',0)}",
        f"Scored: {(meta or {}).get('contracts_scored',0)}",
        f"Candidates: {(meta or {}).get('candidates',0)}",
        "",
        f"Quotes: {c.get('contracts_with_quotes',0)}",
        f"Fresh: {max(0,c.get('contracts_with_quotes',0)-c.get('contracts_stale_quotes',0))}",
        f"Stale: {c.get('contracts_stale_quotes',0)}",
        "",
        f"Rejected Price: {c.get('rejected_price',0)}",
        f"Rejected Spread: {c.get('rejected_spread',0)}",
        f"Rejected Liquidity: {c.get('rejected_liquidity',0)}",
        f"Rejected DTE: {c.get('rejected_dte',0)}",
        f"Rejected Momentum: {c.get('rejected_momentum',0)}",
        f"Rejected Trend: {c.get('rejected_trend',0)}",
        f"Rejected Score: {c.get('rejected_score',0)}",
        "",
        f"HERO: {(meta or {}).get('heroes',0)}",
        f"STRONG: {(meta or {}).get('strong',0)}",
        f"WATCH: {(meta or {}).get('watch',0)}",
    ]
    if z:
        lines += ["",f"Top rejection reason: {z.get('top_rejection_reason','—')}"]
    return "\n".join(lines)


def _find_setup(query):
    q=str(query or '').strip().upper()
    with lock:
        top=list(state.get('last_top') or [])
        di=dict(state.get('diagnostics') or {})
    pool=list(top)
    meta=di.get('__meta__') if isinstance(di,dict) else {}
    pool += list((meta or {}).get('top_candidates') or [])
    seen=set()
    for x in pool:
        key=str(x.get('contract','')).upper()
        if key in seen: continue
        seen.add(key)
        if q in (key, str(x.get('symbol','')).upper()) or q==str(x.get('contract','')).upper():
            return x
    return None

def _analysis_text(x):
    if not x: return "🔎 CONTRACT ANALYSIS\nContract not found in the latest scan. Use /top first or provide a contract that appeared in the latest scan."
    entry=float(x.get('entry') or x.get('premium') or 0)
    current=float(x.get('premium') or 0)
    sl=float(x.get('stop_loss') or 0)
    score=float(x.get('score') or 0)
    explosive=float(x.get('explosive_score') or 0)
    weak = bool(sl and current and current <= sl) or score < 50
    change=((current/entry)-1)*100 if entry else 0
    status='🔴 WEAK' if weak else ('🟡 WATCH' if score < 70 else '🟢 HEALTHY MODEL')
    return (
        f"🔎 CONTRACT ANALYSIS\n\n"
        f"Contract: {x.get('contract','—')}\n"
        f"Underlying: {x.get('symbol','—')} | {x.get('signal','—')}\n"
        f"Status: {status}\n\n"
        f"Entry: ${entry:.2f} | Current: ${current:.2f}\n"
        f"Change from entry: {change:+.1f}%\n"
        f"Stop Loss: ${sl:.2f}\n"
        f"Score: {score:.0f}/100\n"
        f"Explosive score: {explosive:.0f}/100\n"
        f"Tier: {x.get('tier') or 'BELOW_WATCH'}\n"
        f"Moonshot: {'🚀 YES' if x.get('moonshot') else 'NO'}\n\n"
        f"4H/1H/15M/5M: {x.get('trend_4h','—')} / {x.get('trend_1h','—')} / {x.get('trend_15m','—')} / {x.get('trend_5m','—')}\n"
        f"Volume ratio: {x.get('volume_ratio','—')} | Volume: {x.get('volume',0)} | OI: {x.get('open_interest',0)}\n"
        f"Spread: {x.get('spread_pct','—')}%\n\n"
        f"Reasons:\n• " + "\n• ".join((x.get('reasons') or [])[:8] or ['—']) +
        "\n\n⚠️ Model analysis only; current quote must be rechecked before trading."
    )

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
                        send_message('🤖 Options Opportunity Bot V14\n\nAlert-only options scanner.\n/start — start\n/help — help\n/status — diagnostics\n/scan — manual scan\n/top — top setups\n/heroes — HERO setups\n/watchlist — WATCH setups\n/diagnostics — rejection diagnostics\n/analyze CONTRACT — detailed contract analysis\n/analysis CONTRACT — same as /analyze',chat_id); continue
                    if cmd=='/privacy':
                        send_message('🔐 البوت Alert-only ولا ينفذ صفقات عبر وسيط.',chat_id); continue
                    if not allowed_chat or chat_id != allowed_chat:
                        continue
                    if cmd=='/status':
                        send_message(_status_text(),chat_id)
                    elif cmd=='/scan':
                        p=phase(); sid,started=start_scan(p)
                        send_message(f'🔎 Scan started\nPhase: {phase_label(p)}\nScan ID: {sid}' if started else f'⚠️ Scan already running\nScan ID: {state.get("scan_id")}',chat_id)
                    elif cmd in ('/top','/heroes','/watchlist','/moonshots'):
                        with lock: top=list(state.get('last_top') or [])
                        if cmd=='/heroes':
                            top=[x for x in top if x.get('tier')=='HERO']
                        elif cmd=='/watchlist':
                            top=[x for x in top if x.get('tier')=='WATCH']
                        elif cmd=='/moonshots':
                            top=[x for x in top if x.get('moonshot')]
                        if cmd=='/top':
                            send_message(_top_text(10),chat_id)
                        elif not top:
                            send_message('ℹ️ لا توجد setups من هذا النوع في آخر scan.',chat_id)
                        else:
                            for x in top[:10]: send_message(format_alert(x),chat_id)
                    elif cmd=='/diagnostics':
                        send_message(_diagnostics_text(),chat_id)
                    elif cmd in ('/analyze','/analysis'):
                        arg=text.split(maxsplit=1)[1].strip() if len(text.split(maxsplit=1))>1 else ''
                        x=_find_setup(arg) if arg else None
                        if not arg:
                            send_message('استخدم: /analyze CONTRACT\nمثال: /analyze QQQ260923C00745000',chat_id)
                        else:
                            send_message(_analysis_text(x),chat_id)
                    else:
                        send_message('الأوامر: /start /help /status /scan /top /heroes /watchlist /analyze CONTRACT /diagnostics',chat_id)
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
def root(): return jsonify({'service':'options-opportunity-bot','version':'14.4.0','status':'ok','docs':'/health','scan':'/scan','scan_status':'/scan/status'})

@app.get('/health')
def health():
    with lock: s=dict(state)
    s['telegram_configured']=bool(os.getenv('TELEGRAM_BOT_TOKEN') and os.getenv('TELEGRAM_CHAT_ID')); s['scan_secret_configured']=bool(os.getenv('SCAN_SECRET')); s['provider']=provider_status(); s['telegram']=telegram_diagnostics()
    return jsonify({'service':'options-opportunity-bot','version':'14.4.0','status':'ok','phase':phase(),'scanner':s})

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

@app.get('/top')
def top():
    return jsonify({'ok':True,'top':(lambda: None)()}) if False else jsonify({'ok':True,'text':_top_text(10)})

@app.get('/diagnostics')
def diagnostics_route():
    return jsonify({'ok':True,'text':_diagnostics_text()})

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
