"""
ttl_cleanup.py — Nightly JSONB/summary cleanup.

Cleans up large text/JSONB fields past their retention period:
  - news.summary = NULL for rows > 90 days old
  - score_history.context = '{}' for rows > 30 days old
  - claude_analyses.input_packet = '{}' for rows > 60 days old
  - emergency_analysis_log.context_packet = '{}' for rows > 60 days old

Run via systemd timer at 01:00 KST (16:00 UTC) daily.

Usage: python3 ttl_cleanup.py
"""
import os
import sys

sys.path.insert(0, '/root/trading-bot/app')
import psycopg2
from dotenv import load_dotenv

load_dotenv('/root/trading-bot/app/.env')

LOG_PREFIX = '[ttl_cleanup]'


def _log(msg):
    print(f"{LOG_PREFIX} {msg}", flush=True)


def _db_conn():
    return psycopg2.connect(
        host=os.getenv('DB_HOST', 'localhost'),
        port=int(os.getenv('DB_PORT', '5432')),
        dbname=os.getenv('DB_NAME', 'trading'),
        user=os.getenv('DB_USER', 'bot'),
        password=os.getenv('DB_PASS', 'botpass'),
        connect_timeout=10,
        options='-c statement_timeout=120000',
    )


def main():
    _log('=== TTL CLEANUP START ===')
    conn = _db_conn()
    conn.autocommit = True
    total = 0

    try:
        with conn.cursor() as cur:
            # 1) news.summary = NULL for rows > 90 days old
            cur.execute("""
                UPDATE news SET summary = NULL
                WHERE summary IS NOT NULL
                  AND ts < now() - interval '90 days';
            """)
            count = cur.rowcount
            total += count
            _log(f"news.summary cleared: {count} rows")

            # 2) score_history.context = '{}' for rows > 30 days old
            cur.execute("""
                UPDATE score_history SET context = '{}'::jsonb
                WHERE context != '{}'::jsonb
                  AND ts < now() - interval '30 days';
            """)
            count = cur.rowcount
            total += count
            _log(f"score_history.context cleared: {count} rows")

            # 3) claude_analyses.input_packet = '{}' for rows > 60 days old
            cur.execute("""
                UPDATE claude_analyses SET input_packet = '{}'::jsonb
                WHERE input_packet != '{}'::jsonb
                  AND ts < now() - interval '60 days';
            """)
            count = cur.rowcount
            total += count
            _log(f"claude_analyses.input_packet cleared: {count} rows")

            # 4) emergency_analysis_log.context_packet = '{}' for rows > 60 days old
            cur.execute("""
                UPDATE emergency_analysis_log SET context_packet = '{}'::jsonb
                WHERE context_packet != '{}'::jsonb
                  AND ts < now() - interval '60 days';
            """)
            count = cur.rowcount
            total += count
            _log(f"emergency_analysis_log.context_packet cleared: {count} rows")

        _log(f"=== TTL CLEANUP DONE. Total rows cleaned: {total} ===")
    finally:
        conn.close()


if __name__ == '__main__':
    main()
