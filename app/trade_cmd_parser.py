# Source Generated with Decompyle++
# File: trade_cmd_parser.cpython-312.pyc (Python 3.12)

'''
trade_cmd_parser.py — Natural language trade command parser (Korean + English).

Detects and parses commands like:
  "지금 10프로 롱 들어가"
  "8시에 15% 숏 진입"
  "10프로 들어가"          (side=None → auto direction)
  "500 usdt 롱 들어가"    (capital_limit)
  "예약 취소 #7"
'''
import re
_SIDE_LONG = {
    'ㄹ',
    '롱',
    '매수',
    'long'}
_SIDE_SHORT = {
    'ㅅ',
    '숏',
    '매도',
    'short'}
_TRIGGER_WORDS = {
    '가자',
    '넣어',
    '열어',
    '오픈',
    '잡아',
    '진입',
    '들어가',
    '매매해',
    'open',
    'enter'}
_SIZE_RE = re.compile('(\\d{1,3})\\s*(프로|%|퍼센트|퍼)')
_CAPITAL_RE = re.compile('(\\d+)\\s*usdt', re.IGNORECASE)
_TIME_SCHEDULED_RE = re.compile('(\\d{1,2})\\s*시')
_TIME_IMMEDIATE = {
    '당장',
    '바로',
    '즉시',
    '지금',
    'now'}
_CANCEL_RE = re.compile('예약\\s*취소\\s*#?\\s*(\\d+)', re.IGNORECASE)
MAX_SIZE_PERCENT = 30
MIN_SIZE_PERCENT = 1
DEFAULT_TEST_CAPITAL = 900

def is_trade_command(text = None):
    '''Fast keyword check: does the text look like a trade command?
    Accepts: side + (trigger or size), OR trigger + size (no side = auto).'''
    pass
# WARNING: Decompyle incomplete


def parse_trade_command(text = None):
    '''Parse a trade command into structured dict.

    Returns:
        {
            "side": "LONG" | "SHORT" | None,  # None = auto direction
            "size_percent": int (1-30),
            "scheduled_hour": int | None,
            "immediate": bool,
            "capital_limit": float | None,   # USDT cap if specified
            "raw_text": str,
        }
        or None if parsing fails (missing size).
    '''
    pass
# WARNING: Decompyle incomplete


def is_cancel_command(text = None):
    """Detect '예약 취소 #7' style cancel commands.

    Returns (is_cancel, order_id) or (False, None).
    """
    if not text:
        text
    t = ''.strip()
    m = _CANCEL_RE.search(t)
    if m:
        return (True, int(m.group(1)))

