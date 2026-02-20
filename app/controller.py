"""
controller.py — Main bot control loop.

Polls indicators every POLL_SEC seconds, decides on vol-spike signals,
and inserts actions into signals_action_v3.
"""
import os
import time
import json
from datetime import datetime, timezone
from sqlalchemy import create_engine, text

try:
    from db_config import SQLALCHEMY_URL
    DATABASE_URL = SQLALCHEMY_URL
except Exception:
    DATABASE_URL = os.environ.get('DATABASE_URL', '')
SYMBOL = os.environ.get('SYMBOL', 'BTC/USDT:USDT')
TF = os.environ.get('TF', '1m')
DRY_RUN = os.environ.get('DRY_RUN', '1') == '1'
BOT_STATE = os.environ.get('BOT_STATE', 'RUNNING')
POLL_SEC = float(os.environ.get('CONTROLLER_POLL_SEC', '2'))
MIN_SIGNAL_INTERVAL_SEC = int(os.environ.get('CONTROLLER_MIN_SIGNAL_INTERVAL_SEC', '60'))
VOL_SPIKE_LOOKBACK = int(os.environ.get('VOL_SPIKE_LOOKBACK', '60'))
ACTION_TBL = 'signals_action_v3'
engine = create_engine(DATABASE_URL, future=True, connect_args={
    'connect_timeout': 10,
    'options': '-c statement_timeout=30000'})


def ts_to_text(ts_val=None):
    if ts_val is not None:
        return str(ts_val)
    return datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S+00')


def fetch_indicator_for_decision():
    sql = '''
        SELECT ts, vol_spike
        FROM indicators
        WHERE symbol=:s AND tf=:t
        ORDER BY ts DESC
        LIMIT 1
    '''
    with engine.connect() as conn:
        row = conn.execute(text(sql), {'s': SYMBOL, 't': TF}).fetchone()
        if row:
            return {'ts': row[0], 'vol_spike': row[1]}
    return None


def action_exists(ts_text=None, signal_txt=None, action_txt=None):
    sql = f'''
        SELECT 1
        FROM {ACTION_TBL}
        WHERE symbol=:s AND tf=:tf AND ts=:ts AND signal=:sig AND action=:act
        LIMIT 1
    '''
    with engine.connect() as conn:
        row = conn.execute(text(sql), {
            's': SYMBOL, 'tf': TF, 'ts': ts_text,
            'sig': signal_txt, 'act': action_txt}).fetchone()
        return row is not None


def insert_action(ts_text, signal_txt=None, action_txt=None, price=None, meta=None):
    now_unix = int(time.time())
    strategy_name = 'VOL_SPIKE_TEST'
    sql = f'''
        INSERT INTO {ACTION_TBL} (
            ts, symbol, tf, strategy, signal, action, price, meta, created_at_unix, processed
        )
        VALUES (
            :ts, :s, :tf, :stg, :sig, :act, :p, CAST(:m AS jsonb), :cu, false
        )
    '''
    with engine.connect() as conn:
        conn.execute(text(sql), {
            'ts': ts_text, 's': SYMBOL, 'tf': TF, 'stg': strategy_name,
            'sig': signal_txt, 'act': action_txt, 'p': price,
            'm': json.dumps(meta or {}, default=str), 'cu': now_unix})
        conn.commit()


def check_trade_switch():
    '''trade_switch 테이블에서 현재 ON/OFF 확인. 실패 시 False(안전).'''
    try:
        with engine.connect() as conn:
            row = conn.execute(text(
                'SELECT enabled FROM trade_switch ORDER BY id DESC LIMIT 1'
            )).fetchone()
            if row:
                return bool(row[0])
    except Exception:
        pass
    return False


def check_executor_state():
    """executor_state 테이블에서 현재 mode 확인. 실패 시 'STOPPED'(안전)."""
    try:
        with engine.connect() as conn:
            row = conn.execute(text(
                'SELECT mode FROM executor_state ORDER BY id DESC LIMIT 1'
            )).fetchone()
            if row:
                return str(row[0])
    except Exception:
        pass
    return 'STOPPED'


def decide(ind=None):
    if not ind:
        return None
    if not ind.get('vol_spike'):
        return None
    if not check_trade_switch():
        return {
            'signal': 'VOL_SPIKE',
            'action': 'STOPPED',
            'price': None,
            'meta': {
                'reason': 'trade_switch_off',
                'dry_run': DRY_RUN}}
    exec_mode = check_executor_state()
    if exec_mode in ('PAUSED', 'STOPPED'):
        return {
            'signal': 'VOL_SPIKE',
            'action': 'STOPPED',
            'price': None,
            'meta': {
                'reason': f'executor_{exec_mode.lower()}',
                'dry_run': DRY_RUN}}
    tenkan = ind.get('ich_tenkan')
    kijun = ind.get('ich_kijun')
    direction = 'LONG'
    if tenkan is not None and kijun is not None:
        if tenkan < kijun:
            direction = 'SHORT'
    return {
        'signal': 'VOL_SPIKE',
        'action': direction,
        'price': None,
        'meta': {
            'direction': direction,
            'dry_run': DRY_RUN}}


def main():
    print('=== CONTROLLER STARTED ===', flush=True)
    print(f'symbol={SYMBOL!s} tf={TF!s} poll={POLL_SEC!s}s min_interval={MIN_SIGNAL_INTERVAL_SEC!s}s lookback={VOL_SPIKE_LOOKBACK!s} dry_run={DRY_RUN!s} bot_state={BOT_STATE!s}', flush=True)
    last_emit = 0
    while True:
        time.sleep(POLL_SEC)
        try:
            bot_state = os.environ.get('BOT_STATE', 'RUNNING')
            if bot_state != 'RUNNING':
                continue
            ind = fetch_indicator_for_decision()
            if not ind:
                continue
            decision = decide(ind)
            if not decision:
                continue
            now = time.time()
            if now - last_emit < MIN_SIGNAL_INTERVAL_SEC:
                continue
            ts_text = ts_to_text(ind['ts'])
            sig = decision['signal']
            act = decision['action']
            if action_exists(ts_text, sig, act):
                print(f'[controller] SKIP duplicate ts={ts_text!s} signal={sig!s} action={act!s}', flush=True)
                last_emit = now
                continue
            print(f'[controller] INSERT action ts={ts_text!s} signal={sig!s} action={act!s}', flush=True)
            insert_action(ts_text, sig, act, decision['price'], decision['meta'])
            last_emit = now
        except KeyboardInterrupt:
            print('[controller] interrupted, shutting down', flush=True)
            break
        except Exception as e:
            print(f'[controller] loop error: {e}', flush=True)
            import traceback
            traceback.print_exc()
            time.sleep(POLL_SEC)


if __name__ == '__main__':
    main()
