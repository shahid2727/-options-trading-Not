import os, requests

API = 'https://api.telegram.org/bot{}/{}'

def _token():
    return os.getenv('TELEGRAM_BOT_TOKEN')

def send_message(text, chat_id=None):
    token = _token(); chat = chat_id or os.getenv('TELEGRAM_CHAT_ID')
    if not token or not chat: return False
    try:
        r = requests.post(API.format(token, 'sendMessage'), json={'chat_id': chat, 'text': text}, timeout=15)
        return r.ok
    except Exception:
        return False

def get_updates(offset=None, timeout=20):
    token = _token()
    if not token: return []
    try:
        params = {'timeout': timeout, 'allowed_updates': ['message']}
        if offset is not None: params['offset'] = offset
        r = requests.get(API.format(token, 'getUpdates'), params=params, timeout=timeout + 5)
        r.raise_for_status()
        data = r.json()
        return data.get('result', []) if data.get('ok') else []
    except Exception:
        return []
