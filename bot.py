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
    'provider_errors': [], 'scan_process_pid': None, 'alert_diagnostics': {'candidates': []}
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
    badge = '🚨 1000%+ ANALYZED UPSIDE 🚨' if x.get('extreme_upside') else '🔥 STRONG SETUP'
    entry_low = float(x.get('entry_low', x.get('premium', 0.0)) or 0.0)
    entry_high = float(x.get('entry_high', x.get('premium', 0.0)) or 0.0)
    premium = float(x.get('premium', 0.0) or 0.0)
    bid = float(x.get('bid', 0.0) or 0.0)
    ask = float(x.get('ask', 0.0) or 0.0)
    contracts = int(x.get('suggested_contracts', 0) or 0)
    risk_per_contract = float(x.get('risk_dollars_per_contract', 0.0) or 0.0)
    data_mode = x.get('data_mode','INDICATIVE').upper()
    session = x.get('session', phase())

    # The scanner already calculates these levels. We only expose them in the alert;
    # no scoring/filtering logic is changed here.
    tp1 = float(x.get('tp1', 0.0) or 0.0)
    tp2 = float(x.get('tp2', 0.0) or 0.0)
    tp3 = float(x.get('tp3', 0.0) or 0.0)
    stop = float(x.get('stop_loss', 0.0) or 0.0)
    profit1_per_contract = max(0.0, tp1 - entry_high) * 100
    profit2_per_contract = max(0.0, tp2 - entry_high) * 100
    profit3_per_contract = max(0.0, tp3 - entry_high) * 100

    return (
        f"{badge}\n\n"
        f"🎯 SYMBOL: {x.get('symbol','')}\n"
        f"📄 CONTRACT: {x.get('contract','')}\n"
        f"📈 {x.get('signal','')} | Score {x.get('score',0):.0f}/100\n\n"
        f"💰 CURRENT QUOTE\n"
        f"Bid: ${bid:.2f} | Ask: ${ask:.2f} | Mid: ${premium:.2f}\n\n"
        f"🟢 ENTRY FROM LOW\n"
        f"Ideal / Low Entry: ${entry_low:.2f}\n"
        f"Entry Zone: ${entry_low:.2f} – ${entry_high:.2f}\n"
        f"⚠️ Max Entry (model): ${entry_high:.2f}\n\n"
        f"🔴 EXIT / RISK\n"
        f"Stop Loss: ${stop:.2f} ({pct_change(stop, entry_low):+.0f}% from low entry)\n"
        f"Risk / Contract: ${risk_per_contract:.2f}\n\n"
        f"🎯 CONTRACT TARGETS\n"
        f"TP1 / Partial Exit: ${tp1:.2f} → {pct_change(tp1, entry_low):+.0f}% | ${profit1_per_contract:.0f}/contract\n"
        f"TP2 / Partial Exit: ${tp2:.2f} → {pct_change(tp2, entry_low):+.0f}% | ${profit2_per_contract:.0f}/contract\n"
        f"TP3 / Final Target: ${tp3:.2f} → {pct_change(tp3, entry_low):+.0f}% | ${profit3_per_contract:.0f}/contract\n"
        f"Suggested Contracts: {contracts}\n"
        f"Max Planned Risk: ${x.get('max_loss',0):.0f}\n"
        f"Expected Profit (all suggested contracts): TP1 ${x.get('expected_profit_tp1',0):.0f} | TP2 ${x.get('expected_profit_tp2',0):.0f} | TP3 ${x.get('expected_profit_tp3',0):.0f}\n\n"
        f"📍 UNDERLYING LEVELS\n"
        f"Entry: {x.get('underlying_entry',0):.2f}\n"
        f"SL: {x.get('underlying_stop_loss',0):.2f}\n"
        f"TP1 / TP2 / TP3: {x.get('underlying_tp1',0):.2f} / {x.get('underlying_tp2',0):.2f} / {x.get('underlying_tp3',0):.2f}\n\n"
        f"📊 SETUP\n"
        f"Confidence: {x.get('confidence',0):.0f}/100\n"
        f"Volume: {x.get('volume',0)} | OI: {x.get('open_interest',0)} | Spread: {x.get('spread_pct',0):.1f}%\n"
        f"Delta: {x.get('delta',0):.2f} | DTE: {x.get('dte',0)}\n"
        f"4H: {x.get('trend_4h','NEUTRAL')} | 1H: {x.get('trend_1h','NEUTRAL')} | 15M: {x.get('trend_15m','NEUTRAL')} | 5M: {x.get('trend_5m','NEUTRAL')}\n"
        f"Session: {phase_label(session)}\n"
        f"Data Mode: {data_mode}\n\n"
        f"📈 Analyzed upside: +{x.get('projected_upside_pct',0):.0f}% | Target premium: ${x.get('projected_premium',0):.2f}\n"
        f"Why: {', '.join(x.get('reasons',[]))}\n\n"
        f"⚠️ Entry/targets are model levels derived from the scanner; they are not guaranteed fills or support/resistance.\n"
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
    max_alerts=max(1,int(os.getenv('MAX_ALERTS_PER_SCAN', os.getenv('MAX_ALERTS','5'))))
    cooldown=max(0,int(os.getenv('ALERT_COOLDOWN_SECONDS','900')))
    alert_diag={'max_alerts':max_alerts,'cooldown_seconds':cooldown,'candidates':[],
                'eligible':0,'cooldown_rejected':0,'max_alerts_rejected':0,
                'other_rejected':0,'send_attempted':0,'send_success':0,'send_failed':0}
    alerts=0
    if error is None:
        # Keep scanner/scoring order untouched, but prioritize high-score candidates for alerts.
        alert_candidates=sorted(list(results), key=lambda x:(x.get('score',0), x.get('confidence',0)), reverse=True)
        for x in alert_candidates:
            symbol=str(x.get('symbol',''))
            contract=str(x.get('contract',''))
            key=f"{symbol}:{contract}"
            item={'symbol':symbol,'contract':contract,'score':x.get('score'),
                  'premium':x.get('premium'),'bid':x.get('bid'),'ask':x.get('ask'),
                  'status':'pending','reason':None}
            last=state.get('last_alert_keys',{}).get(key)
            cooldown_active=False
            if last and cooldown>0:
                try: cooldown_active=(now-datetime.fromisoformat(last)).total_seconds()<cooldown
                except Exception: cooldown_active=False
            if cooldown_active:
                item.update(status='rejected',reason=f'cooldown active for {symbol} + {contract}')
                alert_diag['cooldown_rejected']+=1; alert_diag['candidates'].append(item); continue
            if alerts>=max_alerts:
                item.update(status='rejected',reason=f'max alerts per scan reached ({max_alerts})')
                alert_diag['max_alerts_rejected']+=1; alert_diag['candidates'].append(item); continue
            item['status']='eligible'; item['reason']='candidate qualified for alert delivery'
            alert_diag['eligible']+=1
            alert_diag['send_attempted']+=1; item['send_attempted']=True
            try:
                sent=bool(send_message(format_alert(x)))
                if sent:
                    alerts+=1; alert_diag['send_success']+=1; item['status']='sent'; item['send_success']=True
                    with lock: state.setdefault('last_alert_keys',{})[key]=now.isoformat()
                else:
                    alert_diag['send_failed']+=1; item['status']='send_failed'; item['reason']='Telegram send_message returned False'; item['send_success']=False
            except Exception as e:
                alert_diag['send_failed']+=1; item['status']='send_failed'; item['reason']=f'Telegram {type(e).__name__}: {e}'; item['send_success']=False
                with lock: state['last_error']=item['reason']
            alert_diag['candidates'].append(item)
        warning=market_warning(diagnostics)
        if warning and state.get('market_warning_sent') is None:
            if send_message(warning): state['market_warning_sent']=now.isoformat()
        elif not warning: state['market_warning_sent']=None
    meta=diagnostics.get('__meta__',{}) if isinstance(diagnostics,dict) else {}
    provider_errors=[]
    for k,v in (diagnostics or {}).items():
        if isinstance(v,dict) and v.get('error'): provider_errors.append({'source':k,'error':v.get('error'),'endpoint':v.get('endpoint'),'status_code':v.get('status_code'),'retry_count':v.get('retry_count')})
    with lock:
        state.update(last_scan=now.isoformat(), last_candidates=len(results), last_alerts=alerts,
                     last_error=error, last_session=p, scan_id=scan_id, scan_running=False,
                     scan_finished=now.isoformat(), scan_duration=round(duration,2), scan_stage='complete' if error is None else 'failed',
                     last_top=sorted(list(results), key=lambda x:(x.get('score',0),x.get('confidence',0)), reverse=True)[:max_alerts],
                     diagnostics=diagnostics or {}, alert_diagnostics=alert_diag,
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
    if p == 'CLOSED': return None, False
    with lock:
        if state['scan_running']: return None, False
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
        return None, False
    with lock: state['scan_process_pid']=proc.pid
    threading.Thread(target=_watch_scan,args=(proc,parent_conn,p,scan_id,started),daemon=True).start()
    return scan_id, True


def _status_text():
    with lock: s=dict(state); ad=dict(s.get('alert_diagnostics') or {})
    td=telegram_diagnostics(); ps=provider_status()
    lines=[f"🟢 Bot status",f"Phase: {phase_label(phase())}",f"Running: {s['running']}",
           f"Scan running: {s['scan_running']}",f"Scan stage: {s.get('scan_stage')}",f"Scan ID: {s.get('scan_id') or '—'}",
           f"Symbols scanned: {s.get('symbols_scanned',0)}",f"Contracts scanned: {s.get('contracts_scanned',0)}",
           f"Last candidates: {s['last_candidates']}",f"Last alerts: {s['last_alerts']}",f"Last scan: {s['last_scan'] or '—'}",
           f"Scan duration: {s.get('scan_duration') if s.get('scan_duration') is not None else '—'} s",
           f"Last error: {s['last_error'] or 'None'}",
           f"Telegram polling: {td.get('telegram_running')}",f"Telegram last update: {td.get('telegram_last_update') or '—'}",
           f"Telegram last error: {td.get('telegram_last_error') or 'None'}",
           f"Provider: {ps.get('name')} | Options feed: {ps.get('options_feed')} | Underlying feed: {ps.get('underlying_feed')}",
           f"Data mode: {ps.get('data_mode')}",f"Provider errors: {len(s.get('provider_errors') or [])}","",
           "🔔 Alert diagnostics",
           f"Max alerts/scan: {ad.get('max_alerts',0)} | Cooldown: {ad.get('cooldown_seconds',0)}s",
           f"Candidates: {len(ad.get('candidates') or [])} | Eligible: {ad.get('eligible',0)} | Cooldown rejected: {ad.get('cooldown_rejected',0)} | Max-alerts rejected: {ad.get('max_alerts_rejected',0)}",
           f"Send attempted: {ad.get('send_attempted',0)} | Success: {ad.get('send_success',0)} | Failed: {ad.get('send_failed',0)}"]
    di=s.get('diagnostics') or {}
    for key in ('SPXW',):
        d=di.get(key) if isinstance(di,dict) else None
        if isinstance(d,dict):
            r=d.get('rejections') or {}
            lines.append(f"{key}: {'OK' if not d.get('error') else 'ERROR'} | Chain: {d.get('chain_items',0)} | Candidates: {d.get('scored',0)}")
            lines.append(f"{key} rejections: Prefix={r.get('prefix',0)} | BadContract={r.get('bad_contract',0)} | DTE={r.get('dte',0)} | Premium={r.get('premium',0)} | Spread/Liquidity={r.get('spread',0)+r.get('liquidity',0)} | Score={r.get('score',0)} | 4H={r.get('alignment',0)} | Regime={r.get('regime',0)} | Relaxed={r.get('relaxed_candidates',0)}")
    for c in (ad.get('candidates') or [])[:50]:
        lines.append(f"• {c.get('symbol','—')} | {c.get('contract','—')} | score={c.get('score','—')} | premium=${c.get('premium','—')} | bid={c.get('bid','—')} | ask={c.get('ask','—')} | {c.get('status','—')} | {c.get('reason','—')}")
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
                        send_message('🤖 Options Opportunity Bot V10.3\n\nAlert-only options scanner.\n/start — start\n/help — help\n/status — diagnostics\n/scan — manual scan\n/top — latest candidates',chat_id); continue
                    if cmd=='/privacy':
                        send_message('🔐 البوت Alert-only ولا ينفذ صفقات عبر وسيط.',chat_id); continue
                    if not allowed_chat or chat_id != allowed_chat:
                        continue
                    if cmd=='/status': send_message(_status_text(),chat_id)
                    elif cmd=='/scan':
                        p=phase(); sid,started=start_scan(p)
                        send_message(f'🔎 Scan started\nPhase: {phase_label(p)}\nScan ID: {sid}' if started else f'⚠️ Scan already running\nScan ID: {state.get("scan_id")}',chat_id)
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
def root(): return jsonify({'service':'options-opportunity-bot','version':'10.1.0','status':'ok','docs':'/health','scan':'/scan','scan_status':'/scan/status'})

@app.get('/health')
def health():
    with lock: s=dict(state)
    s['telegram_configured']=bool(os.getenv('TELEGRAM_BOT_TOKEN') and os.getenv('TELEGRAM_CHAT_ID')); s['scan_secret_configured']=bool(os.getenv('SCAN_SECRET')); s['provider']=provider_status(); s['telegram']=telegram_diagnostics()
    return jsonify({'service':'options-opportunity-bot','version':'10.1.0','status':'ok','phase':phase(),'scanner':s})

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
