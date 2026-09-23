import os
import threading
import requests

API = 'https://api.telegram.org/bot{}/{}'
_local = threading.local()
_diag_lock = threading.Lock()
_diag = {
    'telegram_running': False,
    'telegram_last_update': None,
    'telegram_last_error': None,
    'telegram_last_poll': None,
    'telegram_polling': False,
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
    _set_diag(telegram_running=bool(running), telegram_polling=bool(running))

def _split_telegram_text(text, limit=3800):
    """Split long Telegram messages safely below Telegram's 4096-char limit."""
    text = str(text or '')
    if len(text) <= limit:
        return [text]
    parts = []
    while len(text) > limit:
        cut = text.rfind("\n", 0, limit)
        if cut < 500:
            cut = limit
        parts.append(text[:cut])
        text = text[cut:].lstrip("\n")
    if text:
        parts.append(text)
    return parts

def send_message(text, chat_id=None):
    token = _token(); chat = chat_id or os.getenv('TELEGRAM_CHAT_ID')
    if not token or not chat:
        _set_diag(telegram_last_error='Telegram not configured')
        return False
    try:
        ok = True
        parts = _split_telegram_text(text)
        for part in parts:
            r = _client().post(API.format(token, 'sendMessage'), json={'chat_id': chat, 'text': part}, timeout=10)
            if not r.ok:
                _set_diag(telegram_last_error=f'HTTP {r.status_code}: sendMessage')
                ok = False
                break
        if ok:
            _set_diag(telegram_last_error=None)
        return ok
    except Exception as e:
        _set_diag(telegram_last_error=f'{type(e).__name__}: {e}')
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
        _set_diag(telegram_last_poll=__import__('datetime').datetime.utcnow().isoformat()+'Z')
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
