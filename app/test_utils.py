# Source Generated with Decompyle++
# File: test_utils.cpython-312.pyc (Python 3.12)

'''
test_utils.py — Shared test lifecycle helpers.

Pure utility functions, no side effects.
All modules use these to determine test state consistently.
'''
import json
from datetime import datetime, timezone, timedelta
TEST_MODE_PATH = '/root/trading-bot/app/test_mode.json'
KST = timezone(timedelta(hours=9))

def load_test_mode():
    '''Read test_mode.json. Returns {"enabled": False} on any error.'''
    try:
        with open(TEST_MODE_PATH, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception:
        return {'enabled': False}


def get_end_utc(test=None):
    '''Return test end datetime (UTC).

    Priority: end_utc field > start_utc + duration_hours.
    Returns None if nothing parseable (callers must handle).
    '''
    if not test:
        return None
    # Try end_utc directly
    end_str = test.get('end_utc')
    if end_str:
        try:
            dt = datetime.fromisoformat(end_str.replace('Z', '+00:00'))
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt
        except Exception:
            pass
    # Try end_kst
    end_kst_str = test.get('end_kst')
    if end_kst_str:
        try:
            dt = datetime.fromisoformat(end_kst_str.replace('Z', '+00:00'))
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=KST)
            return dt.astimezone(timezone.utc)
        except Exception:
            pass
    # Try start_utc + duration_hours
    start_str = test.get('start_utc')
    hours = test.get('duration_hours')
    if start_str and hours:
        try:
            dt = datetime.fromisoformat(start_str.replace('Z', '+00:00'))
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt + timedelta(hours=float(hours))
        except Exception:
            pass
    return None


def is_test_active(test=None):
    '''True if test enabled AND current time is within test window.'''
    if not test or not test.get('enabled'):
        return False
    end = get_end_utc(test)
    if not end:
        return False
    now = datetime.now(timezone.utc)
    return now < end


def is_test_frozen(test=None):
    '''True if test enabled AND within 30 minutes of end.

    During freeze: OPEN/ADD blocked, only CLOSE/REDUCE/STOPLOSS allowed.
    Fails safe: returns True if end time is unparseable (blocks new orders).
    '''
    if not test or not test.get('enabled'):
        return False
    end = get_end_utc(test)
    if not end:
        return True  # Fail safe
    now = datetime.now(timezone.utc)
    remaining = (end - now).total_seconds()
    return 0 < remaining <= 1800  # 30 minutes


def is_test_ended(test=None):
    '''True if test enabled AND past end time.'''
    if not test or not test.get('enabled'):
        return False
    end = get_end_utc(test)
    if not end:
        return False
    now = datetime.now(timezone.utc)
    return now >= end


def time_remaining(test=None):
    '''Time remaining until test end. Negative if past end. None if unparseable.'''
    if not test:
        return None
    end = get_end_utc(test)
    if not end:
        return None
    now = datetime.now(timezone.utc)
    return end - now


def test_status_text(test=None):
    '''Formatted test info for Telegram.'''
    if not test or not test.get('enabled'):
        return '테스트 모드: 비활성'
    end = get_end_utc(test)
    if not end:
        return '테스트 모드: 활성 (종료시간 파싱 불가)'
    now = datetime.now(timezone.utc)
    rem = end - now
    total_sec = rem.total_seconds()
    if total_sec <= 0:
        return '테스트 모드: 종료됨'
    hours = int(total_sec // 3600)
    minutes = int((total_sec % 3600) // 60)
    end_kst = end.astimezone(KST)
    frozen = is_test_frozen(test)
    lines = [
        f'테스트 모드: 활성',
        f'종료: {end_kst.strftime("%m/%d %H:%M")} KST',
        f'남은 시간: {hours}h {minutes}m',
    ]
    if frozen:
        lines.append('🧊 Freeze 상태 (신규 진입 차단)')
    return '\n'.join(lines)

if __name__ == '__main__':
    test = load_test_mode()
    end = get_end_utc(test)
    rem = time_remaining(test)
    print(f"test_mode: enabled={test.get('enabled')}")
    print(f'end_utc:   {end.isoformat() if end else "None"}')
    print(f'active:    {is_test_active(test)}')
    print(f'frozen:    {is_test_frozen(test)}')
    print(f'ended:     {is_test_ended(test)}')
    print(f'remaining: {rem}')
    print()
    print(test_status_text(test))
