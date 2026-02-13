# Source Generated with Decompyle++
# File: vol_profile.cpython-312.pyc (Python 3.12)

import json
import time
import os
import psycopg2
from dotenv import load_dotenv
load_dotenv()
DB_DSN = dict(host = os.getenv('DB_HOST', 'localhost'), port = int(os.getenv('DB_PORT', '5432')), dbname = os.getenv('DB_NAME', 'trading'), user = os.getenv('DB_USER', 'bot'), password = os.getenv('DB_PASS', 'botpass'), connect_timeout = 10, options = '-c statement_timeout=30000')
db = None
symbol = os.getenv('SYMBOL', 'BTC/USDT:USDT')
tf = '1m'
lookback = 240
bin_size = 50

def bucket(px = None):
    return float(int(px // bin_size) * bin_size)


def _get_db():
    pass
# WARNING: Decompyle incomplete

print('=== VOLUME PROFILE STARTED ===')
db = _get_db()
# WARNING: Decompyle incomplete
