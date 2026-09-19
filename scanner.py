from datetime import datetime, timezone
import math
import pandas as pd
import yfinance as yf
from config import cfg
from risk import build_trade_plan, position_size


def num(x, default=0.0):
    try:
        if x is None or pd.isna(x):
            return default
        value = float(x)
        return default if not math.isfinite(value) else value
    except Exception:
        return default


def market_snapshot(t):
    """Return ATR/support/resistance and simple momentum/breakout metrics."""
    try:
        hist = t.history(period='90d', interval='1d', auto_adjust=False)
        if hist.empty:
            return 0, None, None, 0, 0, 0, 0
        h = hist['High'].astype(float)
        l = hist['Low'].astype(float)
        c = hist['Close'].astype(float)
        v = hist['Volume'].astype(float) if 'Volume' in hist else pd.Series(index=hist.index, dtype=float)
        prev_c = c.shift(1)
        tr = pd.concat([(h-l), (h-prev_c).abs(), (l-prev_c).abs()], axis=1).max(axis=1)
        atr = num(tr.rolling(14).mean().iloc[-1], 0)
        support = num(l.tail(20).min(), None)
        resistance = num(h.tail(20).max(), None)
        ret5 = num((c.iloc[-1] / c.iloc[-6] - 1) * 100, 0) if len(c) >= 6 else 0
        ret20 = num((c.iloc[-1] / c.iloc[-21] - 1) * 100, 0) if len(c) >= 21 else 0
        avg_vol20 = num(v.tail(20).mean(), 0)
        vol_ratio = num(v.iloc[-1] / avg_vol20, 0) if avg_vol20 > 0 else 0
        prior20_high = num(h.iloc[-21:-1].max(), 0) if len(h) >= 22 else 0
        prior20_low = num(l.iloc[-21:-1].min(), 0) if len(l) >= 22 else 0
        last = num(c.iloc[-1], 0)
        breakout_up = 1 if prior20_high and last >= prior20_high * 0.995 else 0
        breakout_down = 1 if prior20_low and last <= prior20_low * 1.005 else 0
        return atr, support, resistance, ret5, ret20, vol_ratio, breakout_up, breakout_down
    except Exception:
        return 0, None, None, 0, 0, 0, 0, 0


def option_score(row, side, momentum):
    """Score option quality + underlying momentum on a 0-100 scale."""
    vol = int(num(row.get('volume')))
    oi = int(num(row.get('openInterest')))
    bid = num(row.get('bid')); ask = num(row.get('ask')); last = num(row.get('lastPrice'))
    premium = last or ((bid + ask) / 2 if bid and ask else 0)
    spread = ((ask-bid)/premium*100) if premium > 0 and ask >= bid else 100
    delta = abs(num(row.get('delta')))
    iv = num(row.get('impliedVolatility'))

    score = 0.0
    reasons = []
    if vol >= cfg.min_volume: score += 15; reasons.append('volume')
    elif vol >= cfg.min_volume * 0.5: score += 7
    if oi >= cfg.min_oi: score += 10; reasons.append('OI')
    elif oi >= cfg.min_oi * 0.5: score += 5
    if oi and vol / oi >= 1: score += 12; reasons.append('volume/OI')
    elif oi and vol / oi >= 0.5: score += 6
    if spread <= cfg.max_spread_pct: score += 15; reasons.append('tight spread')
    elif spread <= cfg.max_spread_pct * 1.5: score += 6
    if cfg.min_premium <= premium <= cfg.max_premium: score += 10; reasons.append('premium range')
    if delta >= 0.20: score += 8; reasons.append('usable delta')
    if iv > 0: score += 3

    direction_return = momentum['ret5'] if side == 'C' else -momentum['ret5']
    direction_return20 = momentum['ret20'] if side == 'C' else -momentum['ret20']
    if direction_return >= 3: score += 8; reasons.append('momentum')
    elif direction_return >= 1: score += 4
    if direction_return20 >= 5: score += 5; reasons.append('trend')
    elif direction_return20 >= 2: score += 2
    if momentum['vol_ratio'] >= 1.5: score += 6; reasons.append('volume surge')
    elif momentum['vol_ratio'] >= 1.2: score += 3
    if (side == 'C' and momentum['breakout_up']) or (side == 'P' and momentum['breakout_down']):
        score += 8; reasons.append('breakout')

    return min(score, 100), premium, spread, reasons


def scan_symbols(symbols):
    out = []
    for symbol in symbols:
        try:
            t = yf.Ticker(symbol)
            price = num(getattr(t.fast_info, 'last_price', 0))
            if price <= 0:
                continue
            atr, support, resistance, ret5, ret20, vol_ratio, breakout_up, breakout_down = market_snapshot(t)
            momentum = {'ret5': ret5, 'ret20': ret20, 'vol_ratio': vol_ratio,
                        'breakout_up': breakout_up, 'breakout_down': breakout_down}
            for exp in t.options:
                try:
                    exp_dt = datetime.fromisoformat(str(exp)).replace(tzinfo=timezone.utc)
                    dte = (exp_dt - datetime.now(timezone.utc)).days
                except Exception:
                    continue
                if not cfg.min_dte <= dte <= cfg.max_dte:
                    continue
                try:
                    chain = t.option_chain(exp)
                except Exception:
                    continue
                for side, df in [('C', chain.calls), ('P', chain.puts)]:
                    for _, row in df.iterrows():
                        try:
                            r = row.to_dict()
                            strike = num(r.get('strike'), None)
                            if strike is None or strike <= 0:
                                continue
                            score, premium, spread, reasons = option_score(r, side, momentum)
                            vol = int(num(r.get('volume'))); oi = int(num(r.get('openInterest')))
                            if not (premium > 0 and vol >= cfg.min_volume and oi >= cfg.min_oi and
                                    cfg.min_premium <= premium <= cfg.max_premium and
                                    spread <= cfg.max_spread_pct and score >= cfg.min_score * 10):
                                continue
                            delta = num(r.get('delta'))
                            plan = build_trade_plan(premium, price, side, delta, atr, support, resistance)
                            size = position_size(cfg.risk_budget, plan.risk_per_contract)
                            out.append({
                                'symbol': symbol, 'contract': f"{strike:g}{side}", 'expiration': exp, 'dte': dte,
                                'underlying_price': price, 'premium': premium, 'volume': vol, 'open_interest': oi,
                                'spread_pct': spread, 'delta': delta, 'iv': num(r.get('impliedVolatility')),
                                'atr': atr, 'support': support, 'resistance': resistance,
                                'ret5': ret5, 'ret20': ret20, 'volume_ratio': vol_ratio,
                                'breakout': bool(breakout_up if side == 'C' else breakout_down),
                                'score': score, 'entry_low': plan.entry_low, 'entry_high': plan.entry_high,
                                'stop_loss': plan.stop_loss, 'tp1': plan.tp1, 'tp2': plan.tp2, 'tp3': plan.tp3,
                                'stop_underlying': plan.stop_underlying, 'tp1_underlying': plan.tp1_underlying,
                                'tp2_underlying': plan.tp2_underlying, 'tp3_underlying': plan.tp3_underlying,
                                'risk_per_contract': plan.risk_per_contract,
                                'risk_dollars_per_contract': round(plan.risk_per_contract * 100, 2),
                                'reward_tp1': plan.reward_tp1, 'reward_tp2': plan.reward_tp2, 'reward_tp3': plan.reward_tp3,
                                'profit_pct_tp1': round(plan.reward_tp1 / plan.entry_high * 100, 1),
                                'profit_pct_tp2': round(plan.reward_tp2 / plan.entry_high * 100, 1),
                                'profit_pct_tp3': round(plan.reward_tp3 / plan.entry_high * 100, 1),
                                'rr_tp1': plan.rr_tp1, 'rr_tp2': plan.rr_tp2, 'rr_tp3': plan.rr_tp3,
                                'risk_budget': cfg.risk_budget, 'suggested_contracts': size,
                                'max_loss_position': round(size * plan.risk_per_contract * 100, 2),
                                'tp1_profit_position': round(size * plan.reward_tp1 * 100, 2),
                                'tp2_profit_position': round(size * plan.reward_tp2 * 100, 2),
                                'tp3_profit_position': round(size * plan.reward_tp3 * 100, 2),
                                'plan_method': plan.method,
                                'reasons': reasons,
                            })
                        except Exception:
                            continue
        except Exception:
            continue
    return sorted(out, key=lambda x: x['score'], reverse=True)
