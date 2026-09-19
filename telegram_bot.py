import os,requests
def send_telegram(text):
    token=os.getenv('TELEGRAM_BOT_TOKEN',''); chat_id=os.getenv('TELEGRAM_CHAT_ID','')
    if not token or not chat_id: return False
    try:
        r=requests.post(f'https://api.telegram.org/bot{token}/sendMessage',json={'chat_id':chat_id,'text':text},timeout=15); r.raise_for_status(); return True
    except requests.RequestException: return False
