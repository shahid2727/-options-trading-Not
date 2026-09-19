import os, math, time, logging, threading
from datetime import datetime, timezone
from typing import Any, Dict, List
import requests
import yfinance as yf
from flask import Flask, request, jsonify
from dotenv import load_dotenv

load_dotenv()
logging.basicConfig(level=os.getenv('LOG_LEVEL','INFO'), format='%(asctime)s | %(levelname)s | %(message)s')
log=logging.getLogger('options-bot')

app=Flask(__name__)

# ---------- Config ----------
WATCHLIST=[x.strip().upper() for x in os.getenv('WATCHLIST','SPY,QQQ,NVDA,AMD,TSLA,AAPL,AMZN,META,MSFT,GOOGL,MU,PLTR').split(',') if x.strip()]
MIN_PREMIUM=float(os.getenv('MIN_PREMIUM','50000'))
MIN_VOLUME=int(os.getenv('MIN_VOLUME','100'))
MIN_OI=int(os.getenv('MIN_OI','50'))
MIN_VOL_OI=float(os.getenv('MIN_VOL_OI','1.5'))
MAX_DTE=int(os.getenv('MAX_DTE','14'))
MIN_DTE=int(os.getenv('MIN_DTE','0'))
MIN_CONTRACT=float(os.getenv('MIN_CONTRACT','0.50'))
MAX_CONTRACT=float(os.getenv('MAX_CONTRACT','1.50'))
ALERT_SCORE=float(os.getenv('ALERT_SCORE','7.0'))
POLL_SECONDS=int(os.getenv('POLL_SECONDS','120'))
PORT=int(os.getenv('PORT','8080'))
TELEGRAM_TOKEN=os.getenv('TELEGRAM_BOT_TOKEN','')
TELEGRAM_CHAT_ID=os.getenv('TELEGRAM_CHAT_ID','')
WEBHOOK_SECRET=os.getenv('WEBHOOK_SECRET','change-me')

# Optional providers. They are intentionally isolated because vendor APIs change.
UW_API_KEY=os.getenv('UNUSUAL_WHALES_API_KEY','')
UW_BASE=os.getenv('UNUSUAL_WHALES_BASE_URL','https://api.unusualwhales.com')

seen=set()

# ---------- Utilities ----------
def safe_float(x, default=0.0):
    try: return float(x)
    except (TypeError,ValueError): return default

def telegram(text):
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        log.info('TELEGRAM not configured:\n%s', text)
        return False
    try:
        r=requests.post(f'https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage',json={'chat_id':TELEGRAM_CHAT_ID,'text':text},timeout=15)
        r.raise_for_status(); return True
    except Exception as e:
        log.warning('Telegram error: %s',e); return False

def score_contract(c:Dict[str,Any])->float:
    score=0.0
    vol,oi,pre=c['volume'],c['oi'],c['premium']
    ratio=vol/max(oi,1)
    # Transparent score; not a prediction.
    if pre>=500_000: score+=2.0
    elif pre>=100_000: score+=1.5
    elif pre>=50_000: score+=1.0
    if ratio>=5: score+=2.0
    elif ratio>=3: score+=1.5
    elif ratio>=1.5: score+=1.0
    if vol>=5000: score+=1.5
    elif vol>=1000: score+=1.0
    elif vol>=MIN_VOLUME: score+=0.5
    if c['mid']>=MIN_CONTRACT and c['mid']<=MAX_CONTRACT: score+=1.0
    if MIN_DTE<=c['dte']<=MAX_DTE: score+=1.0
    if c['spread_pct']<=10: score+=1.0
    elif c['spread_pct']<=20: score+=0.5
    if c['side'] in ('CALL','PUT'): score+=0.5
    return round(min(score,10),1)

def fmt_money(x):
    if x>=1_000_000:return f'${x/1_000_000:.2f}M'
    if x>=1_000:return f'${x/1_000:.1f}K'
    return f'${x:,.0f}'

def make_alert(c):
    emoji='🟢' if c['side']=='CALL' else '🔴'
    return (f"{emoji} OPTIONS OPPORTUNITY\n\n"
            f"{c['ticker']} {c['side']} | {c['expiry']} | {c['strike']:.2f}\n"
            f"Contract mid: ${c['mid']:.2f}\n"
            f"Premium: {fmt_money(c['premium'])}\n"
            f"Volume/OI: {c['volume']:,}/{c['oi']:,} ({c['vol_oi']:.1f}x)\n"
            f"Spread: {c['spread_pct']:.1f}% | DTE: {c['dte']}\n\n"
            f"SCORE: {c['score']}/10\n"
            f"Reason: unusual volume/premium + liquidity filters\n"
            f"Risk: HIGH — options can expire worthless.\n"
            f"Mode: ALERT ONLY / NO AUTO-TRADE")

# ---------- Yahoo Finance chain fallback ----------
def yf_candidates(ticker:str)->List[Dict[str,Any]]:
    out=[]
    try:
        tk=yf.Ticker(ticker)
        expiries=tk.options or []
        now=datetime.now(timezone.utc).date()
        for exp in expiries:
            dte=(datetime.strptime(exp,'%Y-%m-%d').date()-now).days
            if dte<MIN_DTE or dte>MAX_DTE: continue
            for kind, chain in [('CALL',tk.option_chain(exp).calls),('PUT',tk.option_chain(exp).puts)]:
                if chain is None or chain.empty: continue
                for _,r in chain.iterrows():
                    bid=safe_float(r.get('bid')); ask=safe_float(r.get('ask')); last=safe_float(r.get('lastPrice'))
                    mid=(bid+ask)/2 if bid>0 and ask>0 else last
                    vol=int(safe_float(r.get('volume'))); oi=int(safe_float(r.get('openInterest')))
                    if mid<=0 or vol<MIN_VOLUME or oi<MIN_OI: continue
                    spread=(ask-bid)/mid*100 if mid>0 and ask>=bid else 999
                    premium=vol*mid*100
                    if not (MIN_CONTRACT<=mid<=MAX_CONTRACT): continue
                    if premium<MIN_PREMIUM: continue
                    c={'ticker':ticker,'side':kind,'expiry':exp,'strike':safe_float(r.get('strike')),'mid':mid,
                       'bid':bid,'ask':ask,'volume':vol,'oi':oi,'vol_oi':vol/max(oi,1),'premium':premium,
                       'spread_pct':spread,'dte':dte}
                    c['score']=score_contract(c)
                    if c['score']>=ALERT_SCORE: out.append(c)
    except Exception as e: log.warning('%s chain error: %s',ticker,e)
    return sorted(out,key=lambda x:(x['score'],x['premium']),reverse=True)

# ---------- Unusual Whales adapter ----------
def uw_headers(): return {'Authorization':f'Bearer {UW_API_KEY}','Accept':'application/json'}

def uw_flow()->List[Dict[str,Any]]:
    if not UW_API_KEY: return []
    # Endpoint paths vary by plan/version; override UW_FLOW_PATH in .env.
    path=os.getenv('UW_FLOW_PATH','/api/option-trades/flow-alerts')
    try:
        r=requests.get(UW_BASE.rstrip('/')+path,headers=uw_headers(),params={'limit':100},timeout=20)
        r.raise_for_status(); data=r.json()
        rows=data.get('data',data if isinstance(data,list) else [])
        out=[]
        for row in rows:
            ticker=str(row.get('ticker') or row.get('symbol') or '').upper()
            if ticker not in WATCHLIST: continue
            side=str(row.get('type') or row.get('side') or '').upper()
            if side not in ('CALL','PUT'): continue
            mid=safe_float(row.get('price') or row.get('mid'))
            vol=int(safe_float(row.get('volume'))); oi=int(safe_float(row.get('open_interest') or row.get('oi')))
            premium=safe_float(row.get('premium') or row.get('total_premium'))
            if mid<=0: continue
            c={'ticker':ticker,'side':side,'expiry':str(row.get('expiry') or row.get('expiration') or ''),
               'strike':safe_float(row.get('strike')),'mid':mid,'bid':safe_float(row.get('bid')),'ask':safe_float(row.get('ask')),
               'volume':vol,'oi':oi,'vol_oi':vol/max(oi,1),'premium':premium or vol*mid*100,'spread_pct':0,'dte':0}
            c['score']=score_contract(c)
            if c['score']>=ALERT_SCORE: out.append(c)
        return sorted(out,key=lambda x:(x['score'],x['premium']),reverse=True)
    except Exception as e:
        log.warning('Unusual Whales error: %s',e); return []

# ---------- Scanner ----------
def scan_once():
    candidates=[]
    # Provider flow first; Yahoo fallback ensures the bot remains useful without UW.
    candidates.extend(uw_flow())
    for t in WATCHLIST:
        candidates.extend(yf_candidates(t))
    candidates=sorted(candidates,key=lambda x:(x['score'],x['premium']),reverse=True)
    unique=[]
    for c in candidates:
        key=(c['ticker'],c['side'],c['expiry'],c['strike'])
        if key in seen: continue
        seen.add(key); unique.append(c)
    for c in unique[:5]: telegram(make_alert(c))
    return unique[:5]

# ---------- TradingView webhook ----------
@app.post('/webhook')
def webhook():
    if request.headers.get('X-Webhook-Secret')!=WEBHOOK_SECRET:
        return jsonify({'error':'unauthorized'}),401
    data=request.get_json(silent=True) or {}
    text=(f"📡 TRADINGVIEW ALERT\n{data.get('ticker','?')}\n"
          f"Signal: {data.get('signal',data.get('message','unknown'))}\n"
          f"Price: {data.get('price','?')}\n\n"
          f"This is a technical trigger; options confirmation required.")
    telegram(text)
    return jsonify({'ok':True})

@app.get('/health')
def health(): return jsonify({'ok':True,'watchlist':WATCHLIST,'time':datetime.now(timezone.utc).isoformat()})

# ---------- Background scanner ----------
def loop():
    while True:
        try: scan_once()
        except Exception as e: log.exception('scanner error: %s',e)
        time.sleep(POLL_SECONDS)

if __name__=='__main__':
    if os.getenv('RUN_SCANNER','true').lower()=='true':
        threading.Thread(target=loop,daemon=True).start()
    app.run(host='0.0.0.0',port=PORT)
