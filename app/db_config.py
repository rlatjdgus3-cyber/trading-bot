"""
db_config.py — Centralized database configuration.

All modules should import from here instead of hardcoding credentials.

Usage:
    from db_config import get_conn
    conn = get_conn()

    # or for dict config:
    from db_config import DB_CONFIG
"""
import os
from dotenv import load_dotenv

load_dotenv('/root/trading-bot/app/.env')

DB_HOST = os.getenv('DB_HOST', 'localhost')
DB_PORT = int(os.getenv('DB_PORT', '5432'))
DB_NAME = os.getenv('DB_NAME', 'trading')
DB_USER = os.getenv('DB_USER', 'bot')
DB_PASS = os.getenv('DB_PASS', 'botpass')

DB_CONFIG = dict(
    host=DB_HOST,
    port=DB_PORT,
    dbname=DB_NAME,
    user=DB_USER,
    password=DB_PASS,
    connect_timeout=10,
    options='-c statement_timeout=30000',
)

# SQLAlchemy URL (for executor.py and others using SQLAlchemy)
SQLALCHEMY_URL = (
    f"postgresql+psycopg2://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
)


def get_conn(autocommit=False):
    """Create a new psycopg2 connection using centralized config."""
    import psycopg2
    conn = psycopg2.connect(**DB_CONFIG)
    conn.autocommit = autocommit
    return conn
