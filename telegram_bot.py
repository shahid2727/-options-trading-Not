import os
import threading
import requests
import time

API = 'https://api.telegram.org/bot{}/{}'
_local = threading.local()
_diag_lock = threading.Lock()
_diag = {
    'telegram_running': False,
    'telegram_last_update': None,
    'telegram_last_error': None,
    'telegram_last_poll': None,
}

def _token():
    return os.getenv('TELEGRAM_BOT_TOKEN')

def _client():
    client = getattr(_local, 'client', None)
    if client is None:
        client = requests.Session()
        _local.client = client
    return client

def _set_diag(**values):
    with _diag_lock:
        _diag.update(values)

def diagnostics():
    with _diag_lock:
        return dict(_diag)

def mark_polling_running(running=True):
    _set_diag(telegram_running=bool(running))

def send_message(text, chat_id=None):
    token = _token(); chat = chat_id or os.getenv('TELEGRAM_CHAT_ID')
    if not token or not chat:
        _set_diag(telegram_last_error='Telegram not configured')
        return False
    retries=max(0,int(os.getenv('TELEGRAM_RETRIES','2'))); timeout=max(3,float(os.getenv('TELEGRAM_HTTP_TIMEOUT','10')))
    for attempt in range(retries+1):
        try:
            r=_client().post(API.format(token,'sendMessage'),json={'chat_id':chat,'text':text},timeout=timeout)
            if r.ok:
                _set_diag(telegram_last_error=None); return True
            if r.status_code not in (429,500,502,503,504) or attempt>=retries:
                _set_diag(telegram_last_error=f'HTTP {r.status_code}: sendMessage'); return False
            time.sleep(min(3.0,0.5*(attempt+1)))
        except Exception as e:
            if attempt>=retries:
                _set_diag(telegram_last_error=f'{type(e).__name__}: {e}'); return False
            time.sleep(min(3.0,0.5*(attempt+1)))
    return False

def get_updates(offset=None, timeout=20):
    token = _token()
    if not token:
        _set_diag(telegram_last_error='Telegram bot token missing', telegram_last_poll=None)
        return []
    try:
        params = {'timeout': timeout, 'allowed_updates': ['message']}
        if offset is not None: params['offset'] = offset
        r = _client().get(API.format(token, 'getUpdates'), params=params, timeout=timeout + 5)
        _set_diag(telegram_last_poll=True)
        r.raise_for_status()
        data = r.json()
        if not data.get('ok'):
            _set_diag(telegram_last_error='Telegram API returned ok=false')
            return []
        updates = data.get('result', []) or []
        if updates:
            _set_diag(telegram_last_update=updates[-1].get('update_id'), telegram_last_error=None)
        else:
            _set_diag(telegram_last_error=None)
        return updates
    except Exception as e:
        _set_diag(telegram_last_error=f'{type(e).__name__}: {e}')
        return []
