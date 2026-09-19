import os
import logging
import requests

log = logging.getLogger(__name__)

def _env(name):
    return (os.getenv(name) or "").strip()

def telegram_config():
    token = _env("TELEGRAM_BOT_TOKEN")
    chat_id = _env("TELEGRAM_CHAT_ID")
    return token, chat_id

def telegram_configured():
    token, chat_id = telegram_config()
    return bool(token and chat_id)

def send_telegram(text):
    token, chat_id = telegram_config()
    if not token or not chat_id:
        log.warning("TELEGRAM not configured: missing TELEGRAM_BOT_TOKEN or TELEGRAM_CHAT_ID")
        return False
    try:
        r = requests.post(
            f"https://api.telegram.org/bot{token}/sendMessage",
            json={"chat_id": chat_id, "text": text},
            timeout=15,
        )
        r.raise_for_status()
        data = r.json()
        if not data.get("ok"):
            log.error("Telegram API returned ok=false: %s", data.get("description", "unknown error"))
            return False
        return True
    except requests.RequestException as exc:
        log.error("Telegram send failed: %s", exc)
        return False


def send_test_message():
    """Send a harmless one-time connectivity test message."""
    return send_telegram(
        "✅ Telegram connection test successful\n\n"
        "Options Opportunity Bot is connected and ready.\n"
        "Alert mode only — no brokerage orders are executed."
    )
