import os, logging
from flask import Flask, request, jsonify
from dotenv import load_dotenv
from scanner import scan_symbols
from telegram_bot import send_telegram, telegram_configured, telegram_config
from config import cfg
from worker import start as start_scanner, status as scanner_status, run_once, format_alert

load_dotenv()
logging.basicConfig(level=logging.INFO, format='%(asctime)s | %(levelname)s | %(message)s')
app = Flask(__name__)

@app.get('/')
def home():
    return '<h1>Options Opportunity Bot</h1><p>Alert/research mode. No brokerage orders are executed.</p><p>GET /health</p><p>POST /webhook</p>'

@app.get('/health')
def health():
    return jsonify(status='ok', service='options-opportunity-bot', scanner=scanner_status())

@app.get('/status')
def status():
    return jsonify(scanner=scanner_status(), symbols=cfg.scan_symbols, interval_seconds=cfg.scan_interval_seconds, telegram_configured=telegram_configured())

@app.get('/scan')
def manual_scan():
    configured_secret = os.getenv('TELEGRAM_TEST_SECRET', '').strip() or cfg.webhook_secret
    supplied = request.headers.get('X-Webhook-Secret', '').strip() or request.args.get('secret', '').strip()
    if configured_secret and supplied != configured_secret:
        return jsonify(ok=False, error='unauthorized'), 401
    results = run_once(force=True)
    return jsonify(ok=True, count=len(results), results=results[:cfg.max_alerts_per_scan])

@app.get('/telegram-test')
def telegram_test():
    """Send a Telegram test message. Requires TRADINGVIEW_WEBHOOK_SECRET or TELEGRAM_TEST_SECRET."""
    configured_secret = os.getenv('TELEGRAM_TEST_SECRET', '').strip() or cfg.webhook_secret
    if not configured_secret:
        return jsonify(ok=False, error='test secret is not configured'), 503

    supplied = request.headers.get('X-Webhook-Secret', '').strip() or request.args.get('secret', '').strip()
    if supplied != configured_secret:
        return jsonify(ok=False, error='unauthorized'), 401

    token, chat_id = telegram_config()
    if not token or not chat_id:
        return jsonify(ok=False, configured=False, error='missing Telegram environment variables'), 503

    ok, err = send_telegram('✅ Telegram connection test successful\n\nOptions Opportunity Bot V6 is connected and ready.\nAlert/research mode only — no brokerage orders are executed.', return_error=True)
    if ok:
        logging.getLogger(__name__).info('Telegram manual test sent successfully')
        return jsonify(ok=True, configured=True, message='Telegram test message sent successfully')
    logging.getLogger(__name__).error('Telegram manual test failed: %s', err)
    return jsonify(ok=False, configured=True, error=err), 502

def format_alert(x, signal='ALERT'):
    return (f"🚨 OPTIONS OPPORTUNITY\n\n{x['symbol']} {x['contract']}\nSignal: {signal}\n"
            f"Underlying: ${x['underlying_price']:.2f}\nPremium: ${x['premium']:.2f}\n"
            f"Volume: {x['volume']:,} | OI: {x['open_interest']:,}\nSpread: {x['spread_pct']:.1f}%\n"
            f"DTE: {x['dte']} | Delta: {x['delta']:.2f} | IV: {x['iv']:.1%}\nScore: {x['score']:.1f}/10\n\n"
            f"Entry zone: ${x['entry_low']:.2f}–${x['entry_high']:.2f}\n"
            f"Stop Loss: ${x['stop_loss']:.2f} | Underlying SL: ${x['stop_underlying']:.2f}\n"
            f"Targets: ${x['tp1']:.2f} / ${x['tp2']:.2f} / ${x['tp3']:.2f}\n"
            f"Underlying targets: ${x['tp1_underlying']:.2f} / ${x['tp2_underlying']:.2f} / ${x['tp3_underlying']:.2f}\n\n"
            f"Expected profit/contract: TP1 +${x['reward_tp1']*100:.0f} ({x['profit_pct_tp1']:.1f}%) | TP2 +${x['reward_tp2']*100:.0f} ({x['profit_pct_tp2']:.1f}%) | TP3 +${x['reward_tp3']*100:.0f} ({x['profit_pct_tp3']:.1f}%)\n"
            f"Risk/contract: ${x['risk_dollars_per_contract']:.0f} | R:R {x['rr_tp1']:.1f}R / {x['rr_tp2']:.1f}R / {x['rr_tp3']:.1f}R\n"
            f"Risk budget: ${x['risk_budget']:.0f} | Suggested size: {x['suggested_contracts']} contracts\n"
            f"Position max loss: ${x['max_loss_position']:.0f}\n"
            f"Position profit: TP1 +${x['tp1_profit_position']:.0f} | TP2 +${x['tp2_profit_position']:.0f} | TP3 +${x['tp3_profit_position']:.0f}\n\n"
            f"Exit plan: TP1 partial → stop toward breakeven; TP2 partial → trail; TP3 close remainder; stop hit → exit.\n"
            f"Reasons: {', '.join(x['reasons'])}")

@app.post('/webhook')
def webhook():
    secret = request.headers.get('X-Webhook-Secret', '')
    if cfg.webhook_secret and secret != cfg.webhook_secret:
        return jsonify(error='unauthorized'), 401
    data = request.get_json(silent=True) or {}
    ticker = str(data.get('ticker') or data.get('symbol') or '').upper().strip()
    signal = str(data.get('signal') or 'ALERT').upper()
    if not ticker:
        return jsonify(error='ticker is required'), 400
    results = scan_symbols([ticker])
    if results:
        send_telegram(format_alert(results[0], signal))
    return jsonify({'ticker': ticker, 'signal': signal, 'results': results})

start_scanner()


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=int(os.getenv('PORT', '10000')))
