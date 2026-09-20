import os, threading, time, uuid
from datetime import datetime, time as dtime
from zoneinfo import ZoneInfo
from flask import Flask, jsonify, request
from scanner import scan_all
from telegram_bot import send_message

app=Flask(__name__)
TZ=ZoneInfo('America/New_York')
lock=threading.Lock()
state={'last_scan':None,'last_candidates':0,'last_alerts':0,'last_error':None,'last_session':'closed','running':True,'scan_id':None,'scan_running':False,'scan_started':None,'scan_finished':None}

def phase():
    n=datetime.now(TZ); t=n.time()
    if dtime(4,0)<=t<dtime(9,30): return 'PRE_MARKET'
    if dtime(9,30)<=t<dtime(16,0): return 'REGULAR'
    if dtime(16,0)<=t<dtime(20,0): return 'AFTER_HOURS'
    return 'CLOSED'

def allowed(p):
    return ((p=='PRE_MARKET' and os.getenv('PREMARKET_ENABLED','true').lower()=='true') or
            (p=='REGULAR' and os.getenv('REGULAR_ENABLED','true').lower()=='true') or
            (p=='AFTER_HOURS' and os.getenv('AFTERHOURS_ENABLED','false').lower()=='true'))

def format_alert(x,p):
    return (f"🚨 {x['market']} {p}\n{x['symbol']} {x['contract']} | DTE {x['dte']}\n"
            f"Score: {x['score']:.0f}/100 | Premium: ${x['premium']:.2f}\n"
            f"Entry: ${x['entry_low']:.2f}-${x['entry_high']:.2f}\n"
            f"SL: ${x['stop_loss']:.2f}\nTP1: ${x['tp1']:.2f} | TP2: ${x['tp2']:.2f} | TP3: ${x['tp3']:.2f}\n"
            f"Vol: {x['volume']} | OI: {x['open_interest']} | Spread: {x['spread_pct']:.1f}%\n"
            f"Momentum 5d: {x['ret5']:.1f}% | 20d: {x['ret20']:.1f}% | Delta: {x['delta']:.2f}\n"
            f"Suggested contracts: {x['suggested_contracts']} | Risk: ${x['risk_dollars_per_contract']:.0f}/contract\n"
            f"Reasons: {', '.join(x['reasons'])}")

def run_scan_job(p, scan_id):
    try:
        results=scan_all(p)
        alerts=0
        max_alerts=int(os.getenv('MAX_ALERTS','5'))
        for x in results[:max_alerts]:
            try:
                if send_message(format_alert(x,p)): alerts+=1
            except Exception:
                pass
        with lock:
            state.update(last_scan=datetime.now(TZ).isoformat(),last_candidates=len(results),last_alerts=alerts,last_error=None,last_session=p,scan_id=scan_id,scan_running=False,scan_finished=datetime.now(TZ).isoformat())
    except Exception as e:
        with lock:
            state.update(last_scan=datetime.now(TZ).isoformat(),last_candidates=0,last_alerts=0,last_error=f'{type(e).__name__}: {e}',last_session=p,scan_id=scan_id,scan_running=False,scan_finished=datetime.now(TZ).isoformat())

def start_scan(p):
    with lock:
        if state['scan_running']:
            return None, False
        scan_id=uuid.uuid4().hex[:10]
        state.update(scan_running=True,scan_id=scan_id,scan_started=datetime.now(TZ).isoformat(),last_error=None,last_session=p)
    threading.Thread(target=run_scan_job,args=(p,scan_id),daemon=True).start()
    return scan_id, True

def loop():
    interval=max(30,int(os.getenv('SCAN_INTERVAL_SECONDS','300')))
    while True:
        try:
            p=phase()
            if allowed(p): start_scan(p)
            else:
                with lock: state['last_session']=p
        except Exception as e:
            with lock: state['last_error']=f'{type(e).__name__}: {e}'
        time.sleep(interval)

@app.get('/health')
def health():
    with lock: s=dict(state)
    s['telegram_configured']=bool(os.getenv('TELEGRAM_BOT_TOKEN') and os.getenv('TELEGRAM_CHAT_ID'))
    s['scan_secret_configured']=bool(os.getenv('SCAN_SECRET'))
    s['telegram_test_secret_configured']=bool(os.getenv('TELEGRAM_TEST_SECRET'))
    return jsonify({'service':'options-opportunity-bot','status':'ok','phase':phase(),'scanner':s})

@app.get('/status')
def status(): return health()

def check_secret():
    # SCAN_SECRET is dedicated to scanner endpoints. TELEGRAM_TEST_SECRET is
    # retained only for the Telegram test endpoint.
    secret=os.getenv('SCAN_SECRET')
    if not secret:
        return True
    return request.args.get('secret') == secret

@app.route('/scan',methods=['GET','POST'])
def scan():
    if not check_secret(): return jsonify({'ok':False,'error':'unauthorized'}),401
    p=phase()
    scan_id,started=start_scan(p)
    if not started:
        return jsonify({'ok':True,'accepted':False,'message':'scan already running','scan_id':state['scan_id'],'phase':p}),202
    return jsonify({'ok':True,'accepted':True,'message':'scan started in background','scan_id':scan_id,'phase':p,'status_url':'/scan/status'}),202

@app.get('/scan/status')
def scan_status():
    if not check_secret(): return jsonify({'ok':False,'error':'unauthorized'}),401
    with lock: s=dict(state)
    return jsonify({'ok':True,**s,'phase':phase()})


@app.get('/telegram-test')
def telegram_test():
    secret=os.getenv('TELEGRAM_TEST_SECRET')
    if secret and request.args.get('secret') != secret:
        return jsonify({'ok':False,'error':'unauthorized'}),401
    if not os.getenv('TELEGRAM_BOT_TOKEN') or not os.getenv('TELEGRAM_CHAT_ID'):
        return jsonify({'ok':False,'configured':False,'error':'Telegram is not configured'}),200
    ok=send_message('✅ Options Bot V7.3 Telegram test successful')
    return jsonify({'ok':ok,'configured':True,'message':'Telegram test message sent successfully' if ok else 'Telegram send failed'}),200

if __name__=='__main__':
    threading.Thread(target=loop,daemon=True).start()
    app.run(host='0.0.0.0',port=int(os.getenv('PORT','10000')))
