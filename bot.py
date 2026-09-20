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
    'last_top': [], 'diagnostics': {}
}

def phase():
    n = datetime.now(TZ); t = n.time()
    if dtime(4,0) <= t < dtime(9,30): return 'PRE_MARKET'
    if dtime(9,30) <= t < dtime(16,0): return 'REGULAR'
    if dtime(16,0) <= t < dtime(20,0): return 'AFTER_HOURS'
    return 'CLOSED'

def allowed(p):
    return {'PRE_MARKET':'PREMARKET_ENABLED','REGULAR':'REGULAR_ENABLED','AFTER_HOURS':'AFTERHOURS_ENABLED'}.get(p) and os.getenv({'PRE_MARKET':'PREMARKET_ENABLED','REGULAR':'REGULAR_ENABLED','AFTER_HOURS':'AFTERHOURS_ENABLED'}[p], 'false').lower() == 'true'

def secret_ok():
    secret = os.getenv('SCAN_SECRET')
    return not secret or request.args.get('secret') == secret

def format_alert(x):
    return (f"🚨 {x['signal']} | {x['symbol']} {x['contract']}\n"
            f"Session: {x['session']} | DTE: {x['dte']} | Data: {x['data_mode']}\n"
            f"Premium: ${x['premium']:.2f} | Score: {x['score']:.0f}/100\n"
            f"Entry: ${x['entry_low']:.2f}-${x['entry_high']:.2f}\n"
            f"SL: ${x['stop_loss']:.2f} | TP1: ${x['tp1']:.2f} | TP2: ${x['tp2']:.2f} | TP3: ${x['tp3']:.2f}\n"
            f"Underlying: ${x['underlying']:.2f} | VWAP: {x['vwap_state']} | EMA9/21: {x['ema_state']}\n"
            f"RSI: {x['rsi']:.1f} | MACD: {x['macd_state']} | Vol: {x['volume_ratio']:.1f}x | Breakout: {x['breakout']}\n"
            f"Option Vol: {x['volume']} | OI: {x['open_interest']} | Spread: {x['spread_pct']:.1f}% | Delta: {x['delta']:.2f}\n"
            f"Suggested contracts: {x['suggested_contracts']} | Risk: ${x['risk_dollars_per_contract']:.0f}/contract\n"
            f"Reasons: {', '.join(x['reasons'])}")

def run_scan_job(p, scan_id):
    try:
        results, diagnostics = scan_all(p)
        alerts = 0
        max_alerts = max(1, int(os.getenv('MAX_ALERTS','5')))
        for x in results[:max_alerts]:
            try:
                if send_message(format_alert(x)): alerts += 1
            except Exception:
                pass
        with lock:
            state.update(last_scan=datetime.now(TZ).isoformat(), last_candidates=len(results), last_alerts=alerts,
                         last_error=None, last_session=p, scan_id=scan_id, scan_running=False,
                         scan_finished=datetime.now(TZ).isoformat(), last_top=results[:max_alerts], diagnostics=diagnostics)
    except Exception as e:
        with lock:
            state.update(last_scan=datetime.now(TZ).isoformat(), last_candidates=0, last_alerts=0,
                         last_error=f'{type(e).__name__}: {e}', last_session=p, scan_id=scan_id,
                         scan_running=False, scan_finished=datetime.now(TZ).isoformat())

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
                        f"Last error: {s['last_error'] or 'None'}", chat_id)
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
                        lines = ['🏆 Top opportunities from last scan']
                        for i, x in enumerate(top, 1):
                            lines.append(f"{i}. {x['signal']} {x['symbol']} {x['contract']} | Score {x['score']:.0f} | Premium ${x['premium']:.2f} | Entry ${x['entry_low']:.2f}-${x['entry_high']:.2f} | TP1 ${x['tp1']:.2f}")
                        send_message('\\n'.join(lines), chat_id)
                else:
                    send_message('الأوامر المتاحة: /status /scan /top /help', chat_id)
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
