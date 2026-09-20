import os, threading, time, uuid
from datetime import datetime, time as dtime
from zoneinfo import ZoneInfo
from flask import Flask, jsonify, request
from scanner import scan_all, provider_status
from telegram_bot import send_message, get_updates

app = Flask(__name__)
TZ = ZoneInfo('America/New_York')
lock = threading.Lock()
state = {
    'last_scan': None, 'last_candidates': 0, 'last_alerts': 0, 'last_error': None,
    'last_session': 'CLOSED', 'running': True, 'scan_id': None,
    'scan_running': False, 'scan_started': None, 'scan_finished': None,
    'last_top': [], 'diagnostics': {}, 'last_alert_keys': {}, 'market_warning_sent': None
}

def phase():
    n = datetime.now(TZ); t = n.time()
    # Avoid scanning weekends; US equity/options sessions are Monday-Friday.
    if n.weekday() >= 5: return 'CLOSED'
    if dtime(4,0) <= t < dtime(9,30): return 'PRE_MARKET'
    if dtime(9,30) <= t < dtime(16,0): return 'REGULAR'
    if dtime(16,0) <= t < dtime(20,0): return 'AFTER_HOURS'
    return 'CLOSED'

def allowed(p):
    return {'PRE_MARKET':'PREMARKET_ENABLED','REGULAR':'REGULAR_ENABLED','AFTER_HOURS':'AFTERHOURS_ENABLED'}.get(p) and os.getenv({'PRE_MARKET':'PREMARKET_ENABLED','REGULAR':'REGULAR_ENABLED','AFTER_HOURS':'AFTERHOURS_ENABLED'}[p], 'false').lower() == 'true'

def secret_ok():
    secret = os.getenv('SCAN_SECRET')
    return not secret or request.args.get('secret') == secret

def pct_change(target, entry):
    try:
        return (target / entry - 1.0) * 100.0 if entry else 0.0
    except Exception:
        return 0.0

def points_text(target, entry, positive_label=True):
    pts = target - entry
    sign = '+' if pts >= 0 else ''
    return f"{sign}{pts:.2f} pts"

def format_alert(x):
    badge = '🚨 1000%+ ANALYZED UPSIDE 🚨' if x.get('extreme_upside') else '🔥 STRONG SETUP'
    contract_entry = x.get('entry_high', x.get('premium', 0.0))
    ctp1 = pct_change(x['tp1'], contract_entry)
    ctp2 = pct_change(x['tp2'], contract_entry)
    ctp3 = pct_change(x['tp3'], contract_entry)
    underlying = x.get('symbol','')
    direction = x.get('signal','')
    market = x.get('market_regime','MIXED')
    data_mode = x.get('data_mode','INDICATIVE').upper()
    trend4 = x.get('trend_4h','NEUTRAL')
    trend1 = x.get('trend_1h','NEUTRAL')
    trend15 = x.get('trend_15m','NEUTRAL')
    trend5 = x.get('trend_5m','NEUTRAL')
    upside_line = (f"🚀 Analyzed upside scenario: +{x['projected_upside_pct']:.0f}% | Target premium: ${x['projected_premium']:.2f} | Underlying target: ${x['projected_underlying_target']:.2f}\n"
                   if x.get('extreme_upside') else f"📈 Analyzed upside scenario: +{x['projected_upside_pct']:.0f}% | Target premium: ${x['projected_premium']:.2f}\n")
    return (
        f"{badge}\n\n"
        f"{underlying} {direction} — STRONG SETUP\n\n"
        f"🎯 Contract\n"
        f"• Entry: ${contract_entry:.2f}\n"
        f"• SL: ${x['stop_loss']:.2f} → {pct_change(x['stop_loss'], contract_entry):+.0f}%\n"
        f"• TP1: ${x['tp1']:.2f} → {ctp1:+.0f}%\n"
        f"• TP2: ${x['tp2']:.2f} → {ctp2:+.0f}%\n"
        f"• TP3: ${x['tp3']:.2f} → {ctp3:+.0f}%\n\n"
        f"📍 Underlying / {underlying}\n"
        f"• Entry: {x['underlying_entry']:.2f}\n"
        f"• SL: {x['underlying_stop_loss']:.2f} → {points_text(x['underlying_stop_loss'], x['underlying_entry'])}\n"
        f"• TP1: {x['underlying_tp1']:.2f} → {points_text(x['underlying_tp1'], x['underlying_entry'])}\n"
        f"• TP2: {x['underlying_tp2']:.2f} → {points_text(x['underlying_tp2'], x['underlying_entry'])}\n"
        f"• TP3: {x['underlying_tp3']:.2f} → {points_text(x['underlying_tp3'], x['underlying_entry'])}\n\n"
        f"📊 Analysis\n"
        f"• 4H: {trend4.title()}\n"
        f"• 1H: {trend1.title()}\n"
        f"• 15M: {trend15.title()}\n"
        f"• 5M: {trend5.title()}\n"
        f"• Market: {market.title()}\n"
        f"• Score: {x['score']:.0f}/100\n"
        f"• Confidence: {x['confidence']:.0f}/100\n"
        f"• R:R: 1:{max(0.1, (x['tp1']-contract_entry)/max(contract_entry-x['stop_loss'],0.01)):.1f}\n\n"
        f"💰 Risk\n"
        f"• Risk/contract: ${x['risk_dollars_per_contract']:.0f}\n"
        f"• Suggested contracts: {x['suggested_contracts']}\n"
        f"• Max loss: ${x['max_loss']:.0f}\n\n"
        f"{upside_line}"
        f"📦 Contract: {x['contract']} | DTE: {x['dte']} | Delta: {x['delta']:.2f}\n"
        f"📊 Volume: {x['volume']} | OI: {x['open_interest']} | Spread: {x['spread_pct']:.1f}%\n"
        f"💵 Expected profit: TP1 ${x['expected_profit_tp1']:.0f} | TP2 ${x['expected_profit_tp2']:.0f} | TP3 ${x['expected_profit_tp3']:.0f}\n"
        f"📡 Data: {data_mode} options / IEX underlying\n"
        f"Why: {', '.join(x['reasons'])}\n\n"
        f"⚠️ Confidence and upside are model estimates, not guarantees.\n"
        f"⚠️ Free Alpaca options data may be delayed/indicative; it is not OPRA real-time."
    )

def market_warning(diagnostics):
    regs=[d.get('market_regime') for d in diagnostics.values() if isinstance(d,dict) and d.get('market_regime')]
    if not regs:return None
    side=regs.count('SIDEWAYS'); chop=regs.count('CHOPPY')
    if side+chop < max(1, len(regs)//2):return None
    state='SIDEWAYS' if side>=chop else 'CHOPPY'
    return (f"⚠️ MARKET WARNING — {state}\n\n"
            f"أغلب الرموز التي تم فحصها تظهر سوقًا {state.lower()}.\n"
            "الاتجاه ضعيف/متذبذب، واحتمال الكسر الكاذب أعلى.\n"
            "⛔ البوت لن يرسل صفقات منخفضة الجودة في هذه الحالة.\n"
            "انتظر اتجاه 4H/1H أو Breakout مدعوم بالحجم قبل الدخول.")

def run_scan_job(p, scan_id):
    try:
        results, diagnostics = scan_all(p)
        alerts=0; max_alerts=max(1,int(os.getenv('MAX_ALERTS','5'))); now=datetime.now(TZ)
        cooldown=max(300,int(os.getenv('ALERT_COOLDOWN_SECONDS','900')))
        # Deduplicate the same contract so a 5-minute scan does not spam Telegram.
        for x in results[:max_alerts]:
            key=f"{x['contract']}:{x['signal']}"; last=state.get('last_alert_keys',{}).get(key)
            if last:
                try:
                    if (now-datetime.fromisoformat(last)).total_seconds()<cooldown: continue
                except Exception: pass
            try:
                if send_message(format_alert(x)):
                    alerts+=1; state.setdefault('last_alert_keys',{})[key]=now.isoformat()
            except Exception: pass
        warning=market_warning(diagnostics)
        if warning and state.get('market_warning_sent') is None:
            if send_message(warning): state['market_warning_sent']=now.isoformat()
        elif not warning:
            state['market_warning_sent']=None
        with lock:
            state.update(last_scan=now.isoformat(),last_candidates=len(results),last_alerts=alerts,last_error=None,last_session=p,scan_id=scan_id,scan_running=False,scan_finished=now.isoformat(),last_top=results[:max_alerts],diagnostics=diagnostics)
    except Exception as e:
        with lock: state.update(last_scan=datetime.now(TZ).isoformat(),last_candidates=0,last_alerts=0,last_error=f'{type(e).__name__}: {e}',last_session=p,scan_id=scan_id,scan_running=False,scan_finished=datetime.now(TZ).isoformat())

def start_scan(p):
    with lock:
        if state['scan_running']: return None, False
        scan_id = uuid.uuid4().hex[:10]
        state.update(scan_running=True, scan_id=scan_id, scan_started=datetime.now(TZ).isoformat(), last_error=None, last_session=p)
    threading.Thread(target=run_scan_job, args=(p, scan_id), daemon=True).start()
    return scan_id, True



def telegram_command_loop():
    """Long-poll Telegram and handle lightweight control commands."""
    offset = None
    allowed_chat = str(os.getenv('TELEGRAM_CHAT_ID', '')).strip()
    while True:
        try:
            updates = get_updates(offset=offset, timeout=20)
            for u in updates:
                offset = int(u.get('update_id', 0)) + 1
                msg = u.get('message') or {}
                chat = msg.get('chat') or {}
                chat_id = str(chat.get('id', '')).strip()
                if not chat_id:
                    continue
                text = (msg.get('text') or '').strip()
                if not text:
                    continue
                cmd = text.split()[0].split('@')[0].lower()
                # /start and /help are public. Control/data commands stay private.
                if cmd in ('/start', '/help'):
                    send_message(
                        '🤖 Options Opportunity Bot\n\n'
                        'أهلًا بك 👋\n'
                        'هذا البوت لمراقبة فرص عقود الخيارات وإرسال التنبيهات.\n\n'
                        'الأوامر العامة:\n'
                        '/start — بدء البوت\n'
                        '/help — عرض المساعدة\n'
                        '/privacy — معلومات الخصوصية\n\n'
                        'ملاحظة: قد يتم تسجيل رسائل المستخدمين وإرسالها إلى مشرف البوت لأغراض الدعم والإدارة.\n'
                        'أوامر الفحص والبيانات متاحة للمستخدم المصرّح له فقط.', chat_id)
                    continue
                if cmd == '/privacy':
                    send_message(
                        '🔐 الخصوصية\n\n'
                        'هذا البوت قد يرسل للمشرف رسائل المستخدمين واسم المستخدم/المعرّف لغرض الدعم والإدارة.\n'
                        'لا يملك البوت صلاحية تنفيذ صفقات تلقائيًا.', chat_id)
                    continue
                if not allowed_chat or chat_id != allowed_chat:
                    # Public users cannot access private control/data commands.
                    # Their messages may be logged transparently to the configured admin.
                    admin = allowed_chat
                    if admin and text and not text.startswith('/'):
                        username = (msg.get('from') or {}).get('username') or '—'
                        first_name = (msg.get('from') or {}).get('first_name') or '—'
                        send_message(
                            f'📩 رسالة مستخدم جديدة\n'
                            f'الاسم: {first_name}\n'
                            f'Username: @{username}\n'
                            f'Chat ID: {chat_id}\n'
                            f'الرسالة: {text}', admin)
                    continue
                elif cmd == '/status':
                    with lock: s = dict(state)
                    send_message(
                        f"🟢 Bot status\\nPhase: {phase()}\\n"
                        f"Running: {s['running']}\\nScan running: {s['scan_running']}\\n"
                        f"Last candidates: {s['last_candidates']}\\nLast alerts: {s['last_alerts']}\\n"
                        f"Last scan: {s['last_scan'] or '—'}\\n"
                        f"Last error: {s['last_error'] or 'None'}\n4H/market diagnostics: {len(s.get('diagnostics') or {})}", chat_id)
                elif cmd == '/scan':
                    p = phase(); scan_id, started = start_scan(p)
                    if started:
                        send_message(f'🔎 Scan started\\nPhase: {p}\\nScan ID: {scan_id}', chat_id)
                    else:
                        with lock: current = state['scan_id']
                        send_message(f'⏳ A scan is already running.\\nScan ID: {current}', chat_id)
                elif cmd == '/top':
                    with lock: top = list(state.get('last_top') or [])
                    if not top:
                        send_message('ℹ️ لا توجد فرص في آخر فحص حتى الآن.', chat_id)
                    else:
                        for x in top:
                            send_message(format_alert(x), chat_id)
                else:
                    send_message('الأوامر المتاحة: /status /scan /top /help\n\n/scan و /scan/status عبر الويب محميان بـ SCAN_SECRET، أما من Telegram فلا تحتاج Secret.', chat_id)
        except Exception:
            time.sleep(3)

def loop():
    interval = max(60, int(os.getenv('SCAN_INTERVAL_SECONDS','300')))
    while True:
        try:
            p = phase()
            if allowed(p): start_scan(p)
            else:
                with lock: state['last_session'] = p
        except Exception as e:
            with lock: state['last_error'] = f'{type(e).__name__}: {e}'
        time.sleep(interval)

@app.get('/')
def root():
    return jsonify({'service':'options-opportunity-bot','status':'ok','docs':'/health','scan':'/scan','scan_status':'/scan/status','webhook':'/webhook'})

@app.get('/health')
def health():
    with lock: s = dict(state)
    s['telegram_configured'] = bool(os.getenv('TELEGRAM_BOT_TOKEN') and os.getenv('TELEGRAM_CHAT_ID'))
    s['scan_secret_configured'] = bool(os.getenv('SCAN_SECRET'))
    s['provider'] = provider_status()
    return jsonify({'service':'options-opportunity-bot','status':'ok','phase':phase(),'scanner':s})

@app.get('/status')
def status(): return health()

@app.route('/scan', methods=['GET','POST'])
def scan():
    if not secret_ok(): return jsonify({'ok':False,'error':'unauthorized'}), 401
    p = phase(); scan_id, started = start_scan(p)
    if not started:
        return jsonify({'ok':True,'accepted':False,'message':'scan already running','scan_id':state['scan_id'],'phase':p}), 202
    return jsonify({'ok':True,'accepted':True,'message':'scan started in background','scan_id':scan_id,'phase':p,'status_url':'/scan/status'}), 202

@app.get('/scan/status')
def scan_status():
    if not secret_ok(): return jsonify({'ok':False,'error':'unauthorized'}), 401
    with lock: s = dict(state)
    return jsonify({'ok':True,**s,'phase':phase()})

@app.get('/telegram-test')
def telegram_test():
    secret = os.getenv('TELEGRAM_TEST_SECRET')
    if secret and request.args.get('secret') != secret: return jsonify({'ok':False,'error':'unauthorized'}), 401
    ok = send_message('✅ Options bot Telegram test: connection OK')
    return jsonify({'ok':ok,'configured':bool(os.getenv('TELEGRAM_BOT_TOKEN') and os.getenv('TELEGRAM_CHAT_ID')),'message':'Telegram test message sent successfully' if ok else 'Telegram send failed'})

@app.post('/webhook')
def webhook():
    # TradingView can POST JSON/text here. This endpoint never executes trades.
    hook_secret = os.getenv('WEBHOOK_SECRET')
    if hook_secret and request.args.get('secret') != hook_secret: return jsonify({'ok':False,'error':'unauthorized'}), 401
    payload = request.get_json(silent=True)
    text = payload if payload is not None else request.get_data(as_text=True)
    if isinstance(text, dict):
        text = ' | '.join(f'{k}={v}' for k,v in text.items())
    msg = f"📡 TradingView alert\n{text}"
    sent = send_message(msg)
    return jsonify({'ok':True,'telegram_sent':sent})

if __name__ == '__main__':
    threading.Thread(target=loop, daemon=True).start()
    if os.getenv('TELEGRAM_COMMANDS_ENABLED','true').lower() == 'true':
        threading.Thread(target=telegram_command_loop, daemon=True).start()
    app.run(host='0.0.0.0', port=int(os.getenv('PORT','10000')))
