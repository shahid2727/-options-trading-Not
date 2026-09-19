from dataclasses import dataclass
import math

@dataclass
class TradePlan:
    entry_low: float
    entry_high: float
    stop_loss: float
    tp1: float
    tp2: float
    tp3: float
    risk_per_contract: float
    reward_tp1: float
    reward_tp2: float
    reward_tp3: float
    rr_tp1: float
    rr_tp2: float
    rr_tp3: float
    stop_underlying: float
    tp1_underlying: float
    tp2_underlying: float
    tp3_underlying: float
    method: str


def build_trade_plan(premium, underlying_price, side, delta=0.0, atr=0.0, support=None, resistance=None):
    """Reference option plan. Uses ATR/SR and a delta approximation; not a price prediction."""
    p = max(float(premium), 0.01)
    u = max(float(underlying_price), 0.01)
    d = abs(float(delta or 0.0))
    entry_low = max(0.01, round(p * 0.97, 2))
    entry_high = max(entry_low, round(p * 1.03, 2))
    atr = max(float(atr or 0.0), u * 0.005)

    if side == 'C':
        sr_stop = float(support) if support and support < u else u - atr
        stop_u = min(u - 0.5 * atr, sr_stop)
        direction = 1.0
    else:
        sr_stop = float(resistance) if resistance and resistance > u else u + atr
        stop_u = max(u + 0.5 * atr, sr_stop)
        direction = -1.0

    # yfinance commonly does not supply Greeks. If delta is absent, use a conservative proxy.
    if d < 0.05:
        d = 0.25

    stop = p + d * (stop_u - u) * direction
    # Keep the option stop below entry for both calls and puts; this is a reference level.
    stop = min(p * 0.95, stop)
    stop = max(0.01, stop)

    risk = max(entry_high - stop, p * 0.10)
    tp1 = entry_high + risk
    tp2 = entry_high + 2 * risk
    tp3 = entry_high + 3 * risk

    tp1_u = u + direction * ((tp1 - p) / d)
    tp2_u = u + direction * ((tp2 - p) / d)
    tp3_u = u + direction * ((tp3 - p) / d)

    r1, r2, r3 = tp1-entry_high, tp2-entry_high, tp3-entry_high
    return TradePlan(
        entry_low, entry_high, round(stop,2), round(tp1,2), round(tp2,2), round(tp3,2),
        round(risk,2), round(r1,2), round(r2,2), round(r3,2),
        round(r1/max(risk,0.01),2), round(r2/max(risk,0.01),2), round(r3/max(risk,0.01),2),
        round(stop_u,2), round(tp1_u,2), round(tp2_u,2), round(tp3_u,2),
        'ATR + support/resistance + delta approximation'
    )


def position_size(risk_budget, risk_per_contract):
    """Return whole-contract size based on a user-defined max dollar risk."""
    try:
        rb = float(risk_budget)
        rpc = float(risk_per_contract) * 100.0  # options multiplier
        if rb <= 0 or rpc <= 0:
            return 0
        return max(0, math.floor(rb / rpc))
    except Exception:
        return 0
