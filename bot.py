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
    'provider_errors': [], 'scan_process_pid': None,
    'alert_diagnostics': {'candidates': 0, 'eligible': 0, 'cooldown_rejected': 0, 'max_alerts_rejected': 0, 'send_attempted': 0, 'send_success': 0, 'send_failed': 0, 'send_errors': []}
}


def phase():
    """Return the configured bot session using America/New_York.

    Session windows:
      00:00-04:00 ET -> OVERNIGHT (Mon-Fri session continuation; Sunday is closed)
      04:00-09:30 ET -> PRE_MARKET (Mon-Fri)
      09:30-16:00 ET -> REGULAR (Mon-Fri)
      16:00-20:00 ET -> AFTER_HOURS (Mon-Fri)
      20:00-24:00 ET -> OVERNIGHT (Sun-Thu)

    This is a scheduling/data-mode label only. It does not claim that every
    US option contract has live quotes or executable trading overnight.
    """
    n = datetime.now(TZ)
    t = n.time()
    wd = n.weekday()  # Mon=0 ... Sun=6

    # 00:00-04:00 ET belongs to the overnight session that started the prior
    # evening. Monday 00:00-04:00 is the Sunday-night continuation.
    if dtime(0, 0) <= t < dtime(4, 0):
        return 'OVERNIGHT' if wd in (0, 1, 2, 3, 4) else 'CLOSED'

    if dtime(4, 0) <= t < dtime(9, 30):
        return 'PRE_MARKET' if wd < 5 else 'CLOSED'
    if dtime(9, 30) <= t < dtime(16, 0):
        return 'REGULAR' if wd < 5 else 'CLOSED'
    if dtime(16, 0) <= t < dtime(20, 0):
        return 'AFTER_HOURS' if wd < 5 else 'CLOSED'

    # 20:00-24:00 ET: Sunday through Thursday nights.
    return 'OVERNIGHT' if wd in (0, 1, 2, 3, 6) else 'CLOSED'


def session_clock():
    """Return ET/UTC clocks for diagnostics without exposing secrets."""
    n = datetime.now(TZ)
    return n.isoformat(), n.strftime('%A %Y-%m-%d %H:%M:%S %Z')


def phase_label(p):
    return {'PRE_MARKET':'🌅 PRE-MARKET','REGULAR':'🟢 REGULAR','AFTER_HOURS':'🌙 AFTER-HOURS','OVERNIGHT':'🌙 OVERNIGHT','CLOSED':'⚪ CLOSED'}.get(p,p)


def allowed(p):
    env = {'PRE_MARKET':'PREMARKET_ENABLED','REGULAR':'REGULAR_ENABLED','AFTER_HOURS':'AFTERHOURS_ENABLED','OVERNIGHT':'OVERNIGHT_ENABLED'}.get(p)
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
    badge = '🚨 1000%+ ANALYZED UPSIDE 🚨' if x.get('extreme_upside') else '🔥 STRONG SETUP'
    contract_entry = x.get('entry_high', x.get('premium', 0.0))
    data_mode = x.get('data_mode','INDICATIVE').upper()
    session = x.get('session', phase())
    return (
        f"{badge}\n\n"
        f"🎯 SYMBOL: {x.get('symbol','')}\n"
        f"📄 CONTRACT: {x.get('contract','')}\n"
        f"📈 {x.get('signal','')}\n\n"
        f"Entry: ${contract_entry:.2f}\n"
        f"Stop Loss: ${x['stop_loss']:.2f} → {pct_change(x['stop_loss'], contract_entry):+.0f}%\n"
        f"TP1: ${x['tp1']:.2f} → {pct_change(x['tp1'], contract_entry):+.0f}%\n"
        f"TP2: ${x['tp2']:.2f} → {pct_change(x['tp2'], contract_entry):+.0f}%\n"
        f"TP3: ${x['tp3']:.2f} → {pct_change(x['tp3'], contract_entry):+.0f}%\n\n"
        f"Expected Profit: TP1 ${x['expected_profit_tp1']:.0f} | TP2 ${x['expected_profit_tp2']:.0f} | TP3 ${x['expected_profit_tp3']:.0f}\n"
        f"Score: {x['score']:.0f}/100\n"
        f"Confidence: {x['confidence']:.0f}/100\n"
        f"Volume: {x['volume']}\n"
        f"Open Interest: {x['open_interest']}\n"
        f"Spread: {x['spread_pct']:.1f}%\n"
        f"4H Trend: {x.get('trend_4h','NEUTRAL')}\n"
        f"Session: {phase_label(session)}\n"
        f"Data Mode: {data_mode}\n\n"
        f"📦 DTE: {x['dte']} | Delta: {x['delta']:.2f}\n"
        f"📊 1H: {x.get('trend_1h','NEUTRAL')} | 15M: {x.get('trend_15m','NEUTRAL')} | 5M: {x.get('trend_5m','NEUTRAL')}\n"
        f"📍 Underlying Entry: {x['underlying_entry']:.2f}\n"
        f"Underlying SL: {x['underlying_stop_loss']:.2f}\n"
        f"Underlying TP1/TP2/TP3: {x['underlying_tp1']:.2f} / {x['underlying_tp2']:.2f} / {x['underlying_tp3']:.2f}\n"
        f"📈 Analyzed upside: +{x['projected_upside_pct']:.0f}% | Target premium: ${x['projected_premium']:.2f}\n"
        f"Why: {', '.join(x['reasons'])}\n\n"
        f"⚠️ Confidence/upside are model estimates, not guarantees.\n"
        f"⚠️ Options data mode is {data_mode}; underlying feed is IEX."
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
        if msg.get('stage') == 'provider_retry':
            err = f"Alpaca {msg.get('status_code')} {msg.get('endpoint')} retry {msg.get('retry')}"
            state['provider_errors'] = (state.get('provider_errors') or [])[-9:] + [err]


def _scan_process_worker(session_name, scan_id, conn):
    """Runs the V9.9 scanner in a killable child process; Telegram stays in the parent."""
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


def _finalize_scan(p, scan_id, results, diagnostics, started_at, error=None):
    now=datetime.now(TZ); duration=max(0.0,(now-datetime.fromisoformat(started_at)).total_seconds())
    alerts=0; max_alerts=max(1,int(os.getenv('MAX_ALERTS','5'))); cooldown=max(300,int(os.getenv('ALERT_COOLDOWN_SECONDS','900')))
    alert_diag={'candidates':len(results),'eligible':0,'cooldown_rejected':0,'max_alerts_rejected':max(0,len(results)-max_alerts),
                'send_attempted':0,'send_success':0,'send_failed':0,'send_errors':[]}
    if error is None:
        for idx, x in enumerate(results):
            if idx >= max_alerts:
                break
            key=f"{x['contract']}:{x['signal']}"; last=state.get('last_alert_keys',{}).get(key)
            if last:
                try:
                    if (now-datetime.fromisoformat(last)).total_seconds()<cooldown:
                        alert_diag['cooldown_rejected'] += 1
                        continue
                except Exception: pass
            alert_diag['eligible'] += 1
            alert_diag['send_attempted'] += 1
            try:
                sent=send_message(format_alert(x))
                if sent:
                    alerts+=1
                    alert_diag['send_success'] += 1
                    with lock: state.setdefault('last_alert_keys',{})[key]=now.isoformat()
                else:
                    alert_diag['send_failed'] += 1
                    alert_diag['send_errors'].append(f"send_message returned False for {x.get('contract','?')}")
            except Exception as e:
                alert_diag['send_failed'] += 1
                alert_diag['send_errors'].append(f"{type(e).__name__}: {e}")
                with lock: state['last_error']=f'Telegram alert {type(e).__name__}: {e}'
        warning=market_warning(diagnostics)
        if warning and state.get('market_warning_sent') is None:
            if send_message(warning): state['market_warning_sent']=now.isoformat()
        elif not warning: state['market_warning_sent']=None
    meta=diagnostics.get('__meta__',{}) if isinstance(diagnostics,dict) else {}
    provider_errors=[]
    for k,v in (diagnostics or {}).items():
        if isinstance(v,dict) and v.get('error'):
            err=str(v.get('error'))
            provider_errors.append({'source':k,'error':err,'endpoint':v.get('endpoint'),'status_code':_normalize_provider_status(v.get('status_code'),err),'retry_count':v.get('retry_count')})
    with lock:
        state.update(last_scan=now.isoformat(), last_candidates=len(results), last_alerts=alerts,
                     last_error=error, last_session=p, scan_id=scan_id, scan_running=False,
                     scan_finished=now.isoformat(), scan_duration=round(duration,2), scan_stage='complete' if error is None else 'failed',
                     last_top=results[:max_alerts], diagnostics=diagnostics or {}, alert_diagnostics=alert_diag,
                     symbols_scanned=meta.get('symbols_scanned',state.get('symbols_scanned',0)),
                     contracts_scanned=meta.get('contracts_scanned',state.get('contracts_scanned',0)), provider_errors=provider_errors,
                     scan_process_pid=None)


def _watch_scan(proc, conn, p, scan_id, started_at):
    timeout=max(1,int(os.getenv('SCAN_TIMEOUT_SECONDS','120')))
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
    if p == 'CLOSED': return None, 'closed'
    if not allowed(p): return None, 'disabled'
    with lock:
        if state['scan_running']: return None, 'running'
        scan_id=uuid.uuid4().hex[:10]; started=datetime.now(TZ).isoformat()
        state.update(scan_running=True, scan_id=scan_id, scan_started=started, scan_finished=None,
                     scan_duration=None, scan_stage='starting', last_error=None, last_session=p,
                     symbols_scanned=0, contracts_scanned=0, provider_errors=[])
    ctx=multiprocessing.get_context('fork' if 'fork' in multiprocessing.get_all_start_methods() else 'spawn')
    parent_conn, child_conn=ctx.Pipe(duplex=False)
    proc=ctx.Process(target=_scan_process_worker,args=(p,scan_id,child_conn),daemon=True)
    try:
        proc.start(); child_conn.close()
    except Exception as e:
        try: parent_conn.close()
        except Exception: pass
        with lock: state.update(scan_running=False,scan_stage='failed',last_error=f'Scanner start {type(e).__name__}: {e}')
        return None, 'start_failed'
    with lock: state['scan_process_pid']=proc.pid
    threading.Thread(target=_watch_scan,args=(proc,parent_conn,p,scan_id,started),daemon=True).start()
    return scan_id, True


def _normalize_provider_status(value, error=''):
    if isinstance(value, int):
        return value
    if isinstance(value, str):
        import re
        m=re.search(r'\bHTTP\s+(\d{3})\b', value)
        if m: return int(m.group(1))
        if value.isdigit(): return int(value)
    import re
    m=re.search(r'\bHTTP\s+(\d{3})\b', str(error or ''))
    return int(m.group(1)) if m else None


def _diagnostic_summary(diagnostics):
    totals={}
    score_evaluated=0; final_candidates=0; chain_items=0
    score_values=[]; score_at_threshold=0
    for d in (diagnostics or {}).values():
        if not isinstance(d,dict): continue
        chain_items += int(d.get('chain_items',0) or 0)
        score_evaluated += int(d.get('scored',0) or 0)
        final_candidates += int(d.get('final_candidates',0) or 0)
        if d.get('score_min') is not None: score_values.append((d.get('score_min'),d.get('score_max'),d.get('score_avg')))
        score_at_threshold += int(d.get('score_at_or_above_threshold',0) or 0)
        r=d.get('rejections')
        if isinstance(r,dict):
            for k,v in r.items(): totals[k]=totals.get(k,0)+int(v or 0)
    mins=[x[0] for x in score_values if x[0] is not None]; maxs=[x[1] for x in score_values if x[1] is not None]
    avgs=[x[2] for x in score_values if x[2] is not None]
    score_diag={'min':min(mins) if mins else None,'max':max(maxs) if maxs else None,'avg':round(sum(avgs)/len(avgs),2) if avgs else None,'at_or_above_threshold':score_at_threshold}
    return totals, score_evaluated, final_candidates, chain_items, score_diag


def _status_text():
    with lock: s=dict(state)
    td=telegram_diagnostics(); ps=provider_status(); et_iso, et_text = session_clock()
    rejection_totals, score_evaluated, final_candidates, chain_items, score_diag = _diagnostic_summary(s.get('diagnostics') or {})
    rejection_order=['invalid_contract','dte','premium','spread','volume','open_interest','score','trend_alignment','regime']
    rejection_text=', '.join(f'{k}={rejection_totals.get(k,0)}' for k in rejection_order if rejection_totals.get(k,0)) or 'None'
    provider_details=s.get('provider_errors') or []
    spxw_diag=(s.get('diagnostics') or {}).get('SPXW') or {}
    provider_text=''
    if provider_details:
        lines=[]
        for e in provider_details[-8:]:
            status=_normalize_provider_status(e.get('status_code'),e.get('error'))
            err=e.get('error') or 'unknown error'
            lines.append(f"{e.get('source','?')} HTTP {status if status is not None else 'UNKNOWN'} retry {e.get('retry_count') or 0} | {e.get('endpoint') or 'endpoint unknown'} | {err}")
        provider_text='\nProvider error details: '+ ' || '.join(lines)
    return (f"🟢 Bot status\nPhase: {phase_label(phase())}\nET clock: {et_text}\nRunning: {s['running']}\n"
            f"Scan running: {s['scan_running']}\nScan stage: {s.get('scan_stage')}\nScan ID: {s.get('scan_id') or '—'}\n"
            f"Symbols scanned: {s.get('symbols_scanned',0)}\nContracts scanned: {s.get('contracts_scanned',0)}\n"
            f"Last candidates: {s['last_candidates']}\nLast alerts: {s['last_alerts']}\nLast scan: {s['last_scan'] or '—'}\n"
            f"Scan duration: {s.get('scan_duration') if s.get('scan_duration') is not None else '—'} s\n"
            f"Last error: {s['last_error'] or 'None'}\n"
            f"Diagnostics: chain={chain_items} | score_evaluated={score_evaluated} | final_candidates={final_candidates}\n"
            f"Score stats: min={score_diag.get('min','—')} max={score_diag.get('max','—')} avg={score_diag.get('avg','—')} >=threshold={score_diag.get('at_or_above_threshold',0)}\n"
            f"Rejections: {rejection_text}\n"
            f"Alert diagnostics: candidates={s.get('alert_diagnostics',{}).get('candidates',0)} | eligible={s.get('alert_diagnostics',{}).get('eligible',0)} | cooldown_rejected={s.get('alert_diagnostics',{}).get('cooldown_rejected',0)} | max_alerts_rejected={s.get('alert_diagnostics',{}).get('max_alerts_rejected',0)} | send_attempted={s.get('alert_diagnostics',{}).get('send_attempted',0)} | send_success={s.get('alert_diagnostics',{}).get('send_success',0)} | send_failed={s.get('alert_diagnostics',{}).get('send_failed',0)}\n"
            f"Alert send errors: {' || '.join(s.get('alert_diagnostics',{}).get('send_errors',[])[:5]) or 'None'}\n"
            f"Telegram polling: {td.get('telegram_running')}\n"
            f"Telegram last update: {td.get('telegram_last_update') or '—'}\nTelegram last error: {td.get('telegram_last_error') or 'None'}\n"
            f"Provider: {ps.get('name')} | Options feed: {ps.get('options_feed')} | Underlying feed: {ps.get('underlying_feed')}\n"
            f"Data mode: {ps.get('data_mode')}\nProvider errors: {len(provider_details)}{provider_text}\n"
            f"SPXW: {spxw_diag.get('status','—')}" + (f" | {spxw_diag.get('reason')}" if spxw_diag.get('reason') else ""))


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
                        send_message('🤖 Options Opportunity Bot V10.0\n\nAlert-only options scanner.\n/start — start\n/help — help\n/status — diagnostics\n/scan — manual scan\n/top — latest candidates',chat_id); continue
                    if cmd=='/privacy':
                        send_message('🔐 البوت Alert-only ولا ينفذ صفقات عبر وسيط.',chat_id); continue
                    if not allowed_chat or chat_id != allowed_chat:
                        continue
                    if cmd=='/status': send_message(_status_text(),chat_id)
                    elif cmd=='/scan':
                        p=phase(); sid,started=start_scan(p)
                        if started is True:
                            send_message(f'🔎 Scan started\nPhase: {phase_label(p)}\nScan ID: {sid}', chat_id)
                        elif started == 'closed':
                            send_message('⚪ Market is CLOSED\n\nNo scan started.\nScanning is available during the configured market/extended sessions.', chat_id)
                        elif started == 'disabled':
                            send_message(f'⏸️ {phase_label(p)} is disabled\n\nEnable the corresponding session environment variable to allow scanning.', chat_id)
                        else:
                            send_message(f'⚠️ Scan already running\nScan ID: {state.get("scan_id") or "—"}', chat_id)
                    elif cmd=='/top':
                        with lock: top=list(state.get('last_top') or [])
                        if not top: send_message('ℹ️ لا توجد candidates من آخر scan.',chat_id)
                        else:
                            for x in top: send_message(format_alert(x),chat_id)
                    else: send_message('الأوامر: /start /help /status /scan /top',chat_id)
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
def root(): return jsonify({'service':'options-opportunity-bot','version':'10.0.0','status':'ok','docs':'/health','scan':'/scan','scan_status':'/scan/status'})

@app.get('/health')
def health():
    with lock: s=dict(state)
    s['telegram_configured']=bool(os.getenv('TELEGRAM_BOT_TOKEN') and os.getenv('TELEGRAM_CHAT_ID')); s['scan_secret_configured']=bool(os.getenv('SCAN_SECRET')); s['provider']=provider_status(); s['telegram']=telegram_diagnostics()
    return jsonify({'service':'options-opportunity-bot','version':'10.0.0','status':'ok','phase':phase(),'scanner':s})

@app.get('/status')
def status(): return health()

@app.route('/scan',methods=['GET','POST'])
def scan():
    if not secret_ok(): return jsonify({'ok':False,'error':'unauthorized'}),401
    p=phase(); sid,started=start_scan(p)
    if started == 'closed':
        return jsonify({'ok':True,'accepted':False,'message':'market closed','scan_id':None,'phase':p}),202
    if started == 'disabled':
        return jsonify({'ok':True,'accepted':False,'message':'session disabled','scan_id':None,'phase':p}),202
    if started != True:
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
