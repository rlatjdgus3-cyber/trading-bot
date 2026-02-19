"""
db_config_bench.py — DB connections for benchmark service.

Three connection types:
  - get_bench_conn()   → trading_benchmark (read/write)
  - get_main_conn_ro() → trading (read-only, autocommit)
  - get_main_conn_rw() → trading (write, ONLY for /apply_confirm)
"""
import os
import psycopg2

_ENV_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), '.env')


def _load_env():
    if os.path.isfile(_ENV_PATH):
        with open(_ENV_PATH, 'r') as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith('#') or '=' not in line:
                    continue
                k, v = line.split('=', 1)
                os.environ.setdefault(k.strip(), v.strip())


_load_env()


def get_bench_conn(autocommit=False):
    """Connect to trading_benchmark DB (read/write)."""
    conn = psycopg2.connect(
        host=os.getenv('BENCH_DB_HOST', 'localhost'),
        port=int(os.getenv('BENCH_DB_PORT', '5432')),
        dbname=os.getenv('BENCH_DB_NAME', 'trading_benchmark'),
        user=os.getenv('BENCH_DB_USER', 'bot'),
        password=os.getenv('BENCH_DB_PASS', 'botpass'),
        connect_timeout=10,
        options='-c statement_timeout=30000',
    )
    conn.autocommit = autocommit
    return conn


def get_main_conn_ro(autocommit=True):
    """Connect to trading DB (read-only, autocommit)."""
    conn = psycopg2.connect(
        host=os.getenv('MAIN_DB_HOST', 'localhost'),
        port=int(os.getenv('MAIN_DB_PORT', '5432')),
        dbname=os.getenv('MAIN_DB_NAME', 'trading'),
        user=os.getenv('MAIN_DB_USER', 'bot'),
        password=os.getenv('MAIN_DB_PASS', 'botpass'),
        connect_timeout=10,
        options='-c statement_timeout=30000 -c default_transaction_read_only=on',
    )
    conn.autocommit = autocommit
    return conn


def get_main_conn_rw(autocommit=False):
    """Connect to trading DB (read/write). ONLY for /apply_confirm."""
    conn = psycopg2.connect(
        host=os.getenv('MAIN_DB_HOST', 'localhost'),
        port=int(os.getenv('MAIN_DB_PORT', '5432')),
        dbname=os.getenv('MAIN_DB_NAME', 'trading'),
        user=os.getenv('MAIN_DB_USER', 'bot'),
        password=os.getenv('MAIN_DB_PASS', 'botpass'),
        connect_timeout=10,
        options='-c statement_timeout=30000',
    )
    conn.autocommit = autocommit
    return conn
