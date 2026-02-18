"""
backfill_macro_trace.py — 과거 뉴스 macro_trace 일괄 계산.

macro_trace_computer의 compute_trace_for_news를 재사용하여
과거 뉴스에 대한 BTC 반응 데이터를 일괄 계산.

기존 macro_trace_computer가 최근 6시간만 처리하는 제한을 해제하고
전체 과거 뉴스에 대해 trace를 생성.

Usage:
    python backfill_macro_trace.py                    # 전체 백필
    python backfill_macro_trace.py --resume           # 재개
    python backfill_macro_trace.py --batch-size 200   # 배치 크기
"""
import sys
import argparse
import traceback

sys.path.insert(0, '/root/trading-bot/app')
from db_config import get_conn
from backfill_utils import start_job, get_last_cursor, update_progress, finish_job, check_stop, check_pause

LOG_PREFIX = '[backfill_macro_trace]'
JOB_NAME = 'backfill_macro_trace'
BATCH_SIZE = 100


def _log(msg):
    print(f'{LOG_PREFIX} {msg}', flush=True)


def main():
    parser = argparse.ArgumentParser(description='Backfill macro_trace for historical news')
    parser.add_argument('--resume', action='store_true')
    parser.add_argument('--batch-size', type=int, default=BATCH_SIZE)
    parser.add_argument('--min-impact', type=int, default=0,
                        help='Minimum impact_score to process (default=0, all news)')
    args = parser.parse_args()

    # Import compute function from macro_trace_computer
    from macro_trace_computer import compute_trace_for_news

    conn = get_conn()
    conn.autocommit = True

    last_id = 0
    if args.resume:
        cursor = get_last_cursor(conn, JOB_NAME)
        if cursor and 'last_news_id' in cursor:
            last_id = cursor['last_news_id']
            _log(f'Resuming from news_id > {last_id}')

    job_id = start_job(conn, JOB_NAME, metadata={
        'last_id': last_id, 'min_impact': args.min_impact
    })

    total_computed = 0
    total_skipped = 0
    total_failed = 0
    batch_num = 0

    try:
        while True:
            if check_stop():
                _log('STOP signal received')
                update_progress(conn, job_id, {'last_news_id': last_id})
                finish_job(conn, job_id, status='PARTIAL', error='stopped_by_user')
                break
            if not check_pause():
                _log('STOP during pause')
                update_progress(conn, job_id, {'last_news_id': last_id})
                finish_job(conn, job_id, status='PARTIAL', error='stopped_during_pause')
                break

            with conn.cursor() as cur:
                # Find news items needing trace (not yet in macro_trace or incomplete)
                cur.execute("""
                    SELECT n.id, n.ts FROM news n
                    LEFT JOIN macro_trace mt ON mt.news_id = n.id
                    WHERE n.id > %s
                      AND n.impact_score >= %s
                      AND n.ts <= now() - interval '30 minutes'
                      AND (mt.id IS NULL
                           OR (mt.btc_ret_2h IS NULL AND n.ts <= now() - interval '2 hours')
                           OR (mt.btc_ret_24h IS NULL AND n.ts <= now() - interval '24 hours'))
                    ORDER BY n.id ASC LIMIT %s;
                """, (last_id, args.min_impact, args.batch_size))
                rows = cur.fetchall()

            if not rows:
                _log('No more news to process')
                break

            batch_num += 1
            batch_ok = 0

            for news_id, ts_news in rows:
                try:
                    with conn.cursor() as cur:
                        result = compute_trace_for_news(cur, news_id, ts_news)
                    if result:
                        batch_ok += 1
                        total_computed += 1
                    else:
                        total_skipped += 1
                except Exception as e:
                    _log(f'Error news_id={news_id}: {e}')
                    total_failed += 1

                last_id = news_id

            update_progress(conn, job_id, {'last_news_id': last_id},
                            inserted=batch_ok, failed=total_failed)

            if batch_num % 10 == 0:
                _log(f'Batch {batch_num}: computed={total_computed}, '
                     f'skipped={total_skipped}, failed={total_failed}, last_id={last_id}')

        finish_job(conn, job_id, status='COMPLETED')
        _log(f'DONE: {total_computed} computed, {total_skipped} skipped, {total_failed} failed')

    except KeyboardInterrupt:
        _log('Interrupted')
        update_progress(conn, job_id, {'last_news_id': last_id})
        finish_job(conn, job_id, status='PARTIAL', error='KeyboardInterrupt')
    except Exception as e:
        _log(f'FATAL: {e}')
        traceback.print_exc()
        finish_job(conn, job_id, status='FAILED', error=str(e)[:500])
    finally:
        conn.close()


if __name__ == '__main__':
    main()
