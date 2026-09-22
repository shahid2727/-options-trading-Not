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
    'provider_errors': [], 'scan_process_pid': None
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
    if error is None:
        for x in results[:max_alerts]:
            key=f"{x['contract']}:{x['signal']}"; last=state.get('last_alert_keys',{}).get(key)
            if last:
                try:
                    if (now-datetime.fromisoformat(last)).total_seconds()<cooldown: continue
                except Exception: pass
            try:
                if send_message(format_alert(x)):
                    alerts+=1
                    with lock: state.setdefault('last_alert_keys',{})[key]=now.isoformat()
            except Exception as e:
                with lock: state['last_error']=f'Telegram alert {type(e).__name__}: {e}'
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
                     last_top=results[:max_alerts], diagnostics=diagnostics or {},
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
    with lock: s=dict(state)
    td=telegram_diagnostics(); ps=provider_status()
    return (f"🟢 Bot status\nPhase: {phase_label(phase())}\nRunning: {s['running']}\n"
            f"Scan running: {s['scan_running']}\nScan stage: {s.get('scan_stage')}\nScan ID: {s.get('scan_id') or '—'}\n"
            f"Symbols scanned: {s.get('symbols_scanned',0)}\nContracts scanned: {s.get('contracts_scanned',0)}\n"
            f"Last candidates: {s['last_candidates']}\nLast alerts: {s['last_alerts']}\nLast scan: {s['last_scan'] or '—'}\n"
            f"Scan duration: {s.get('scan_duration') if s.get('scan_duration') is not None else '—'} s\n"
            f"Last error: {s['last_error'] or 'None'}\nTelegram polling: {td.get('telegram_running')}\n"
            f"Telegram last update: {td.get('telegram_last_update') or '—'}\nTelegram last error: {td.get('telegram_last_error') or 'None'}\n"
            f"Provider: {ps.get('name')} | Options feed: {ps.get('options_feed')} | Underlying feed: {ps.get('underlying_feed')}\n"
            f"Data mode: {ps.get('data_mode')}\nProvider errors: {len(s.get('provider_errors') or [])}")


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
def root(): return jsonify({'service':'options-opportunity-bot','version':'14.0.0','status':'ok','docs':'/health','scan':'/scan','scan_status':'/scan/status'})

@app.get('/health')
def health():
    with lock: s=dict(state)
    s['telegram_configured']=bool(os.getenv('TELEGRAM_BOT_TOKEN') and os.getenv('TELEGRAM_CHAT_ID')); s['scan_secret_configured']=bool(os.getenv('SCAN_SECRET')); s['provider']=provider_status(); s['telegram']=telegram_diagnostics()
    return jsonify({'service':'options-opportunity-bot','version':'14.0.0','status':'ok','phase':phase(),'scanner':s})

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



# ================= V14 BOT CONTROL / DIRECT ANALYSIS / MONITORING =================
from scanner import direct_analyze

state.update({
    'valid_contracts':0,'normalized_contracts':0,'quote_stage':0,
    'hero':0,'strong':0,'watch':0,'moonshot':0,'debug_counters':{},
    'watchlist':{},'last_end_of_market_date':None,'scan_started_mono':None,
})
watch_lock=threading.RLock()


def _fmt(v, digits=2):
    if v is None: return 'N/A'
    try: return f'{float(v):.{digits}f}'
    except Exception: return 'N/A'


def _n(v):
    return 'N/A' if v is None else str(v)


def _apply_progress(msg):
    with lock:
        if msg.get('scan_id') and msg.get('scan_id') != state.get('scan_id'): return
        stage=msg.get('stage',state.get('scan_stage'))
        state['scan_stage']=stage
        mapping={'fetching_options':'options_feed','options_chain_fallback':'options_feed_retry','fetching_bars':'symbols','fetching_bars_complete':'symbols','normalization':'normalization','quotes':'quotes','liquidity':'liquidity','technical':'technical','scoring':'scoring','sorting':'ranking','ranking':'ranking','alerts':'alerts','complete':'complete'}
        state['scan_stage']=mapping.get(stage,stage)
        for key in ('symbols_scanned','contracts_scanned','last_candidates','raw_contracts','parsed_contracts','normalized_contracts','quote_valid','quote_stale','liquid_contracts','technical_valid','scored_contracts','candidates','hero','strong','watch','moonshot'):
            if key in msg: state[key]=msg[key]
        if 'quote_valid' in msg: state['quote_stage']=msg['quote_valid']
        if msg.get('stage')=='provider_retry':
            err=f"Alpaca {msg.get('status_code')} {msg.get('endpoint')} retry {msg.get('retry')}"
            state['provider_errors']=(state.get('provider_errors') or [])[-19:]+[err]


def _ranked(results, tier=None):
    xs=[x for x in results if not tier or x.get('tier')==tier]
    return sorted(xs,key=lambda x:(x.get('score',0),x.get('liquidity_score',0)),reverse=True)


def _format_setup(x, prefix=''):
    return (f"{prefix}{'🏆 ' if x.get('tier')=='HERO' else '🟢 ' if x.get('tier')=='STRONG' else '🟡 ' if x.get('tier')=='WATCH' else ''}{x.get('symbol','')} {x.get('signal','')}\n"
            f"Contract: {x.get('contract','N/A')}\n"
            f"Score: {_fmt(x.get('score'),0)}/100 | Risk: {x.get('risk_class','NORMAL')} | R:R 1:{_fmt(x.get('rr'),1)}\n"
            f"Current: ${_fmt(x.get('mid'))} | Entry: ${_fmt(x.get('entry_low'))}–${_fmt(x.get('entry_high'))}\n"
            f"SL: ${_fmt(x.get('stop_loss'))} | TP1: ${_fmt(x.get('tp1'))} | TP2: ${_fmt(x.get('tp2'))} | TP3: ${_fmt(x.get('tp3'))}\n"
            f"Bid/Ask: ${_fmt(x.get('bid'))}/${_fmt(x.get('ask'))} | Spread: {_fmt(x.get('spread_pct'),1)}%\n"
            f"Vol/OI: {_n(x.get('volume'))}/{_n(x.get('open_interest'))} | DTE: {_n(x.get('dte'))}\n"
            f"Why: {', '.join(x.get('reasons') or []) or 'N/A'}\n")


def format_alert(x):
    return _format_setup(x, prefix='') + ("\n⚠️ Quote is stale; it is not treated as live." if x.get('quote_state')=='STALE' else '')


def _format_scan_complete(results, diagnostics):
    meta=diagnostics.get('__meta__',{}) if isinstance(diagnostics,dict) else {}
    c=meta.get('counters',{}) or {}
    lines=["🟢 MARKET SCAN COMPLETE",'',f"Symbols: {meta.get('symbols_scanned',0)}",f"Contracts: {meta.get('contracts_scanned',0)}",f"Valid: {c.get('quote_valid',0)}",f"Normalized: {c.get('normalized_contracts',0)}",f"Quotes: {c.get('quote_valid',0)}",'',f"🏆 HERO: {c.get('hero',0)}",f"🟢 STRONG: {c.get('strong',0)}",f"🟡 WATCH: {c.get('watch',0)}",f"🚀 MOONSHOT: {c.get('moonshot',0)}",'',"TOP SETUPS"]
    top=results[:max(1,int(os.getenv('MAX_ALERTS','5')))]
    if not top:
        reasons=[]
        for k,v in (c.get('rejections') or {}).items(): reasons.append(f'{k}: {v}')
        lines.append('NO QUALIFIED SETUPS' if not reasons else 'NO QUALIFIED SETUPS\nReason: '+', '.join(reasons[:8]))
    else:
        for i,x in enumerate(top,1): lines.append(f"\n{i}. "+_format_setup(x))
    return '\n'.join(lines)


def _end_of_market(results):
    if not results: return "🏆 END OF MARKET HEROES\n\nNo valid setup was available from the final scan. No contract was invented."
    calls=_ranked([x for x in results if x.get('signal')=='CALL'])
    puts=_ranked([x for x in results if x.get('signal')=='PUT'])
    spx=_ranked([x for x in results if x.get('symbol')=='SPXW'])
    overall=_ranked(results)
    parts=["🏆 END OF MARKET HEROES"]
    for label,arr in [('BEST CALL',calls),('BEST PUT',puts),('BEST SPXW',spx),('BEST OVERALL SETUP',overall)]:
        parts.append('\n'+label+'\n'+(_format_setup(arr[0]) if arr else 'No valid setup.'))
    return '\n'.join(parts)


def _finalize_scan(p, scan_id, results, diagnostics, started_at, error=None):
    now=datetime.now(TZ)
    try: duration=max(0.0,(now-datetime.fromisoformat(started_at)).total_seconds())
    except Exception: duration=0.0
    meta=diagnostics.get('__meta__',{}) if isinstance(diagnostics,dict) else {}; c=meta.get('counters',{}) or {}
    alerts=0; max_alerts=max(1,int(os.getenv('MAX_ALERTS','5'))); cooldown=max(60,int(os.getenv('ALERT_COOLDOWN_SECONDS','300')))
    if error is None:
        # Alert by state/contract cooldown, not by a global max that blocks the next good opportunity.
        for x in results[:max_alerts]:
            key=f"{x.get('contract')}:{x.get('signal')}"
            last=state.get('last_alert_keys',{}).get(key)
            if last:
                try:
                    if (now-datetime.fromisoformat(last)).total_seconds()<cooldown: continue
                except Exception: pass
            if send_message(format_alert(x)):
                alerts+=1; state.setdefault('last_alert_keys',{})[key]=now.isoformat()
        send_message(_format_scan_complete(results,diagnostics))
        # End-of-market report once per NY trading day after 15:55 ET.
        if p in ('REGULAR','AFTER_HOURS') and now.hour>=15 and state.get('last_end_of_market_date')!=now.date().isoformat():
            if now.hour>=15 and now.minute>=55:
                if send_message(_end_of_market(results)): state['last_end_of_market_date']=now.date().isoformat()
    provider_errors=[]
    for k,v in (diagnostics or {}).items():
        if isinstance(v,dict) and v.get('error'): provider_errors.append({'source':k,'error':v.get('error'),'endpoint':v.get('endpoint'),'status_code':v.get('status_code'),'retry_count':v.get('retry_count')})
    with lock:
        state.update(last_scan=now.isoformat(),last_candidates=len(results),last_alerts=alerts,last_error=error,last_session=p,scan_id=scan_id,scan_running=False,scan_finished=now.isoformat(),scan_duration=round(duration,2),scan_stage='complete' if error is None else 'error',last_top=results[:max_alerts],diagnostics=diagnostics or {},symbols_scanned=meta.get('symbols_scanned',state.get('symbols_scanned',0)),contracts_scanned=meta.get('contracts_scanned',state.get('contracts_scanned',0)),provider_errors=provider_errors,valid_contracts=c.get('quote_valid',0)+c.get('quote_stale',0),normalized_contracts=c.get('normalized_contracts',0),quote_stage=c.get('quote_valid',0),hero=c.get('hero',0),strong=c.get('strong',0),watch=c.get('watch',0),moonshot=c.get('moonshot',0),debug_counters=c,scan_process_pid=None)


def _status_text():
    with lock: s=dict(state)
    td=telegram_diagnostics(); ps=provider_status()
    return (f"🟢 BOT STATUS\n\nPhase: {phase_label(phase())}\nET clock: {datetime.now(TZ).strftime('%Y-%m-%d %H:%M:%S %Z')}\n\n"
            f"Running: {s.get('running')}\nScan running: {s.get('scan_running')}\nScan stage: {s.get('scan_stage')}\nScan ID: {s.get('scan_id') or '—'}\n"
            f"Symbols scanned: {s.get('symbols_scanned',0)}\nContracts scanned: {s.get('contracts_scanned',0)}\nValid contracts: {s.get('valid_contracts',0)}\nNormalized: {s.get('normalized_contracts',0)}\nQuote stage: {s.get('quote_stage',0)}\nCandidates: {s.get('last_candidates',0)}\n"
            f"🏆 HERO: {s.get('hero',0)}\n🟢 STRONG: {s.get('strong',0)}\n🟡 WATCH: {s.get('watch',0)}\n🚀 MOONSHOT: {s.get('moonshot',0)}\n\n"
            f"Last scan: {s.get('last_scan') or '—'}\nScan duration: {s.get('scan_duration') if s.get('scan_duration') is not None else '—'} s\nLast error: {s.get('last_error') or 'None'}\n\n"
            f"Telegram polling: {td.get('telegram_running')}\nTelegram last update: {td.get('telegram_last_update') or '—'}\nTelegram last error: {td.get('telegram_last_error') or 'None'}\n"
            f"Provider: {ps.get('name')} | Options feed: {ps.get('options_feed')} | Underlying feed: {ps.get('underlying_feed')}\nData mode: {ps.get('data_mode')}\nProvider errors: {len(s.get('provider_errors') or [])}")


def _format_direct(x):
    if not x.get('ok',True):
        return f"❌ {x.get('reason','CONTRACT NOT FOUND')}\n\n{x.get('detail','')}"
    if x.get('multiple'):
        lines=["🔎 MULTIPLE MATCHES",""]
        for i,m in enumerate(x.get('matches',[]),1):
            lines.append(f"{i}. {m.get('contract_symbol')} | {m.get('expiration')} | {m.get('option_type')} | Strike {m.get('strike')}")
        return '\n'.join(lines)
    ua=x.get('underlying_analysis') or {}; frames=ua.get('frames',{}); f5=frames.get('5m') or {}; f1=frames.get('1h') or {}
    def gv(k): return _n(x.get(k))
    reasons='\n'.join(f"- {r}" for r in (x.get('reasons') or ['N/A']))
    vals={
        'tp1_profit': ((x.get('tp1')-x.get('entry_high'))*100 if x.get('tp1') is not None and x.get('entry_high') is not None else None),
        'tp2_profit': ((x.get('tp2')-x.get('entry_high'))*100 if x.get('tp2') is not None and x.get('entry_high') is not None else None),
        'tp3_profit': ((x.get('tp3')-x.get('entry_high'))*100 if x.get('tp3') is not None and x.get('entry_high') is not None else None),
    }
    return (f"🔎 CONTRACT ANALYSIS\n\n"
            f"Contract: {x.get('contract')}\nUnderlying: {x.get('underlying') or x.get('indicator_symbol')}\nDirection: {x.get('option_type')}\n"
            f"Strike: {gv('strike')}\nExpiry: {gv('expiration')}\nDTE: {gv('dte')}\n\n"
            f"Current: ${gv('mid')}\nBid: ${gv('bid')}\nAsk: ${gv('ask')}\nLast: ${gv('last')}\nSpread: {gv('spread_pct')}%\n"
            f"Quote: {x.get('quote_state','N/A')} | Age: {gv('quote_age_seconds')}s\nVolume: {gv('volume')}\nOI: {gv('open_interest')}\n"
            f"IV: {gv('iv')}\nDelta: {gv('delta')}\nGamma: {gv('gamma')}\nTheta: {gv('theta')}\nVega: {gv('vega')}\n\n"
            f"UNDERLYING\nTrend: {('BULLISH' if f1 and f1.get('last',0)>f1.get('ema20',0) else 'BEARISH' if f1 else 'N/A')}\n"
            f"Momentum: {('STRONG' if f5 and abs(f5.get('slope',0))>0 else 'N/A')}\nRelative Volume: {_n(ua.get('volume_ratio'))}\n"
            f"Support: {_n(ua.get('recent_low'))}\nResistance: {_n(ua.get('recent_high'))}\nBreakout: {_n(ua.get('breakout'))}\n\n"
            f"DECISION\n{x.get('decision')}\nScore: {_fmt(x.get('score'),0)}/100\n"
            f"Entry: ${_fmt(x.get('entry_low'))}–${_fmt(x.get('entry_high'))}\nSL: ${_fmt(x.get('stop_loss'))}\n"
            f"TP1: ${_fmt(x.get('tp1'))}\nTP2: ${_fmt(x.get('tp2'))}\nTP3: ${_fmt(x.get('tp3'))}\n"
            f"Risk/contract: ${_fmt(x.get('risk_dollars_per_contract'))}\n"
            f"Potential TP1: ${_fmt(vals['tp1_profit'])}\nPotential TP2: ${_fmt(vals['tp2_profit'])}\nPotential TP3: ${_fmt(vals['tp3_profit'])}\n"
            f"R:R: 1:{_fmt(x.get('rr'),1)}\n\nReasons:\n{reasons}\n\n⚠️ Missing provider fields remain N/A; no values were invented.")

def _direct_worker(contract,chat_id):
    try:
        result=direct_analyze(contract,phase())
        if result.get('ok') and not result.get('multiple'):
            with watch_lock:
                state['watchlist'][result.get('contract')]= {'last':result.get('score'),'state':'STRONG' if result.get('score',0)>=80 else 'WATCH','last_alert':None,'tp_hit':set(),'added':datetime.now(TZ).isoformat()}
        send_message(_format_direct(result),chat_id)
    except Exception as e:
        send_message(f"❌ DIRECT ANALYSIS ERROR\n{type(e).__name__}: {e}",chat_id)


def _watch_status_label(score, prev):
    if score>=80:return 'STRONG'
    if score>=70:return 'VALID ENTRY'
    if prev and score>=prev:return 'RECOVERY'
    if score>=60:return 'WEAKENING'
    if score>=45:return 'WEAK'
    return 'EXIT / INVALIDATED'


def monitor_loop():
    interval=max(30,int(os.getenv('MONITOR_INTERVAL_SECONDS','60')))
    while True:
        try:
            with watch_lock: items=list(state['watchlist'].items())
            for contract,meta in items:
                try:
                    x=direct_analyze(contract,'MONITOR')
                    if not x.get('ok') or x.get('multiple'): continue
                    score=x.get('score',0); prev=meta.get('last'); old=meta.get('state'); new=_watch_status_label(score,prev)
                    now=datetime.now(TZ)
                    if x.get('mid') is not None:
                        for key in ('tp1','tp2','tp3'):
                            if x.get(key) is not None and x['mid']>=x[key] and key not in meta.get('tp_hit',set()):
                                send_message(f"🎯 {key.upper()} HIT\nContract: {contract}\nPrice: ${_fmt(x.get('mid'))}\nProfit/contract: ${_fmt((x['mid']-x.get('entry_high',x['mid']))*100)}")
                                meta.setdefault('tp_hit',set()).add(key)
                    change=(prev is not None and abs(score-prev)>=10)
                    invalid=score<70 and x.get('decision')=='NO ENTRY'
                    recovery=prev is not None and prev<70 and score>=80
                    if invalid and old!='EXIT / INVALIDATED': send_message(f"🔴 CONTRACT INVALIDATED\n\nContract: {contract}\nScore: {prev} → {score}\nReasons:\n- "+'\n- '.join(x.get('reasons') or ['setup conditions lost']))
                    elif recovery: send_message(f"🟢 CONTRACT RECOVERY\n\nContract: {contract}\nScore: {prev} → {score}\nReasons:\n- "+'\n- '.join(x.get('reasons') or ['momentum recovered']))
                    elif change and score<prev:
                        title='🟠 CONTRACT WEAK' if prev-score>=20 else '🟡 CONTRACT WEAKENING'
                        send_message(f"{title}\n\nContract: {contract}\nScore: {prev} → {score}\nReasons:\n- "+'\n- '.join(x.get('reasons') or ['score deterioration'])+"\n\nAction: Monitor closely / protect profit / reassess entry")
                    with watch_lock:
                        meta['last']=score; meta['state']=new; meta['last_data']=x; state['watchlist'][contract]=meta
                except Exception as e:
                    continue
        except Exception:
            pass
        time.sleep(interval)


def _watchlist_text():
    with watch_lock: items=list(state['watchlist'].items())
    if not items:return '👀 WATCHLIST\n\nNo monitored contracts.'
    out=['👀 WATCHLIST']
    for c,m in items: out.append(f"\n{c}\nState: {m.get('state')}\nScore: {m.get('last','N/A')}")
    return '\n'.join(out)


def telegram_command_loop():
    offset=None; allowed_chat=str(os.getenv('TELEGRAM_CHAT_ID','')).strip(); mark_polling_running(True)
    try:
        while True:
            try:
                updates=get_updates(offset=offset,timeout=20)
                for u in updates:
                    offset=int(u.get('update_id',0))+1; msg=u.get('message') or {}; chat=u.get('chat') or msg.get('chat') or {}; chat_id=str(chat.get('id','')).strip(); text=(msg.get('text') or '').strip()
                    if not chat_id or not text: continue
                    parts=text.split(); cmd=parts[0].split('@')[0].lower(); arg=' '.join(parts[1:]).strip()
                    if cmd in ('/start','/help'):
                        send_message('🤖 Options Scanner V14\n\n/status /scan /hero /moonshot\n/analyze CONTRACT\n/contract CONTRACT\n/watchlist /unwatch CONTRACT /clearwatch',chat_id); continue
                    if cmd=='/privacy': send_message('🔐 Alert-only: no brokerage execution.',chat_id); continue
                    if not allowed_chat or chat_id!=allowed_chat: continue
                    if cmd=='/status': send_message(_status_text(),chat_id)
                    elif cmd=='/scan':
                        p=phase(); sid,started=start_scan(p); send_message(f"🔎 Scan {'started' if started else 'already running'}\nPhase: {phase_label(p)}\nScan ID: {sid or state.get('scan_id')}",chat_id)
                    elif cmd in ('/hero','/top'):
                        with lock: top=list(state.get('last_top') or [])
                        heroes=_ranked(top,'HERO')
                        send_message('\n'.join(["🏆 HERO"]+[ _format_setup(x) for x in heroes]) if heroes else 'No HERO in latest scan. Showing strongest valid setups:\n\n'+'\n'.join(_format_setup(x) for x in _ranked(top)[:5]),chat_id)
                    elif cmd=='/moonshot':
                        with lock: top=list(state.get('last_top') or [])
                        ms=sorted([x for x in top if x.get('explosive_setup')],key=lambda x:x.get('explosive_score',0),reverse=True)
                        if not ms: send_message('🚀 MOONSHOT SCANNER\n\nNo qualifying explosive candidate in the latest scan.',chat_id)
                        else:
                            send_message('🚀 MOONSHOT SCANNER\n\n'+'\n'.join(f"{x.get('contract')} | {x.get('signal')} | Premium ${_fmt(x.get('mid'))} | Explosive {x.get('explosive_score')}/100 | Entry ${_fmt(x.get('entry_low'))}-${_fmt(x.get('entry_high'))} | SL ${_fmt(x.get('stop_loss'))} | TP1 ${_fmt(x.get('moonshot_100'))} | +500% ${_fmt(x.get('moonshot_500'))} | +1000% ${_fmt(x.get('moonshot_1000'))}" for x in ms[:10])+"\n\n⚠️ +500%/+1000% are extreme scenarios, not predictions or guarantees.",chat_id)
                    elif cmd in ('/analyze','/contract'):
                        if not arg: send_message('Usage: /analyze CONTRACT',chat_id); continue
                        send_message('🔎 Analyzing contract…',chat_id); threading.Thread(target=_direct_worker,args=(arg,chat_id),daemon=True).start()
                    elif cmd=='/watchlist': send_message(_watchlist_text(),chat_id)
                    elif cmd=='/unwatch':
                        with watch_lock: removed=state['watchlist'].pop(arg.upper(),None)
                        send_message('✅ Removed from watchlist.' if removed else 'ℹ️ Contract not in watchlist.',chat_id)
                    elif cmd=='/clearwatch':
                        with watch_lock: state['watchlist'].clear()
                        send_message('✅ Watchlist cleared.',chat_id)
                    elif re.match(r'^[A-Z.]{1,8}\d{6}[CP]\d{8}$',text.upper()):
                        send_message('🔎 Analyzing contract…',chat_id); threading.Thread(target=_direct_worker,args=(text.upper(),chat_id),daemon=True).start()
                    else: send_message('Commands: /start /help /status /scan /hero /moonshot /analyze CONTRACT /contract CONTRACT /watchlist /unwatch CONTRACT /clearwatch',chat_id)
            except Exception:
                time.sleep(2)
    finally: mark_polling_running(False)


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

# Keep existing /health, /status, /scan, /scan/status, /telegram-test, /webhook routes.

if __name__=='__main__':
    threading.Thread(target=loop,daemon=True,name='scanner-loop').start()
    threading.Thread(target=monitor_loop,daemon=True,name='monitor-loop').start()
    if os.getenv('TELEGRAM_COMMANDS_ENABLED','true').lower()=='true': threading.Thread(target=telegram_command_loop,daemon=True,name='telegram-polling').start()
    app.run(host='0.0.0.0',port=int(os.getenv('PORT','10000')),threaded=True)
