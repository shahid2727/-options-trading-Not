import os
import logging
import requests

log = logging.getLogger(__name__)


def _env(name):
    return (os.getenv(name) or '').strip()


def telegram_config():
    return _env('TELEGRAM_BOT_TOKEN'), _env('TELEGRAM_CHAT_ID')


def telegram_configured():
    token, chat_id = telegram_config()
    return bool(token and chat_id)


def send_telegram(text, return_error=False):
    token, chat_id = telegram_config()
    if not token or not chat_id:
        msg = 'missing TELEGRAM_BOT_TOKEN or TELEGRAM_CHAT_ID'
        log.warning('TELEGRAM not configured: %s', msg)
        return (False, msg) if return_error else False
    try:
        r = requests.post(
            f'https://api.telegram.org/bot{token}/sendMessage',
            json={'chat_id': chat_id, 'text': text},
            timeout=15,
        )
        try:
            data = r.json()
        except ValueError:
            data = {}
        if not r.ok or not data.get('ok'):
            desc = str(data.get('description') or f'HTTP {r.status_code}')
            log.error('Telegram API error: status=%s description=%s', r.status_code, desc)
            return (False, desc) if return_error else False
        return (True, '') if return_error else True
    except requests.RequestException as exc:
        msg = str(exc)
        log.error('Telegram request failed: %s', msg)
        return (False, msg) if return_error else False


def send_test_message():
    return send_telegram(
        '✅ Telegram connection test successful\n\n'
        'Options Opportunity Bot V6 is connected and ready.\n'
        'Alert/research mode only — no brokerage orders are executed.'
    )
