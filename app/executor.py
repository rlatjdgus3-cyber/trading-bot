#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import time
import json
import logging
from datetime import timezone

from sqlalchemy import create_engine, text

# ==============================
# 기본 설정
# ==============================
DATABASE_URL = os.getenv(
    "DATABASE_URL",
    f"postgresql+psycopg2://{os.getenv('DB_USER', 'bot')}:{os.getenv('DB_PASS', 'botpass')}@{os.getenv('DB_HOST', 'localhost')}:{os.getenv('DB_PORT', '5432')}/{os.getenv('DB_NAME', 'trading')}"
)

DRY_RUN = os.getenv("DRY_RUN", "1").lower() in ("1", "true", "yes")
REAL_ENABLE_ENV = os.getenv("REAL_ENABLE_ENV", "NO").lower() in ("1", "true", "yes")

KILL_SWITCH_PATH = "/root/trading-bot/app/KILL_SWITCH"

ACTION_TBL = "signals_action_v3"
STATE_TBL = "executor_state"

engine = create_engine(DATABASE_URL, future=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [EXECUTOR] %(levelname)s: %(message)s",
)

UTC = timezone.utc


# ==============================
# 상태 머신 헬퍼
# ==============================
def load_state(conn):
    """
    최신 row '무조건'이 아니라,
    mode가 비어있는(무효) row는 스킵하고,
    '유효한 상태' 기준으로 최신 row를 사용한다.

    이유:
    executor_state에 외부/다른 프로세스가 빈 row를 계속 INSERT하는 구조가 있어
    최신 row만 잡으면 fix_attempts 누적이 깨짐.
    """
    row = conn.execute(
        text(f"""
            SELECT *
            FROM {STATE_TBL}
            WHERE mode IS NOT NULL AND mode <> ''
            ORDER BY id DESC
            LIMIT 1
        """)
    ).mappings().first()
    return row


def record_error(conn, state_id, reason, fatal=False):
    logging.error(reason)
    conn.execute(
        text(f"""
            UPDATE {STATE_TBL}
            SET
                error_count = error_count + 1,
                last_error_at = now(),
                auto_stop_reason = :reason,
                mode = CASE
                    WHEN :fatal = true AND fix_attempts < 1 THEN 'AUTO_FIX_TESTING'
                    WHEN :fatal = true THEN 'NEED_USER_DECISION'
                    ELSE mode
                END,
                fix_attempts = CASE
                    WHEN :fatal = true THEN fix_attempts + 1
                    ELSE fix_attempts
                END
            WHERE id = :id
        """),
        {
            "id": state_id,
            "reason": reason,
            "fatal": fatal,
        }
    )


# ==============================
# OPEN meta 검증 로직
# ==============================
def validate_open_meta(meta_raw):
    if meta_raw is None:
        return False, "OPEN meta is required"

    if isinstance(meta_raw, str):
        try:
            meta = json.loads(meta_raw)
        except Exception:
            return False, "OPEN meta is not valid JSON"
    elif isinstance(meta_raw, dict):
        meta = meta_raw
    else:
        return False, "OPEN meta has invalid type"

    if "qty" not in meta:
        return False, "meta.qty is required"

    try:
        qty = float(meta["qty"])
    except Exception:
        return False, "meta.qty must be a number"

    if qty <= 0:
        return False, "meta.qty must be > 0"

    leverage = meta.get("leverage", 1)
    try:
        leverage = int(leverage)
    except Exception:
        return False, "meta.leverage must be an integer"

    if leverage < 1 or leverage > 5:
        return False, "meta.leverage must be between 1 and 5"

    return True, {"qty": qty, "leverage": leverage}


# ==============================
# 메인 루프
# ==============================
def main():
    logging.info("executor started")

    while True:
        if os.path.exists(KILL_SWITCH_PATH):
            logging.warning("KILL_SWITCH detected, exiting")
            return

        need_user_wait = False

        with engine.begin() as conn:
            state = load_state(conn)
            if not state:
                # 유효한 상태 row가 없으면 그냥 대기
                time.sleep(5)
                continue

            state_id = state["id"]
            mode = (state.get("mode") or "").strip()

            # =========================================================
            # [핵심] NEED_USER_DECISION이면: 처리 루프 멈추고 대기 상태로
            # - 사용자 개입 전까지 OPEN/CLOSE 처리 금지
            # - 로그에 명확히 출력
            # =========================================================
            if mode == "NEED_USER_DECISION":
                logging.warning("[EXECUTOR] NEED_USER_DECISION - waiting for user action")
                need_user_wait = True
            else:
                action = conn.execute(
                    text(f"""
                        SELECT *
                        FROM {ACTION_TBL}
                        WHERE processed = false
                        ORDER BY id ASC
                        LIMIT 1
                    """)
                ).mappings().first()

                if not action:
                    time.sleep(2)
                    continue

                action_id = action["id"]
                symbol = action.get("symbol")
                action_type = action.get("action")  # OPEN/CLOSE
                sig = action.get("signal")

                if action_type not in ("OPEN", "CLOSE"):
                    logging.warning(
                        f"Unknown action type: action={action_type} signal={sig} id={action_id}. Marking processed."
                    )
                    conn.execute(
                        text(f"UPDATE {ACTION_TBL} SET processed = true WHERE id = :id"),
                        {"id": action_id},
                    )
                    continue

                if action_type == "OPEN":
                    ok, result = validate_open_meta(action.get("meta"))
                    if not ok:
                        record_error(
                            conn,
                            state_id,
                            f"OPEN meta validation failed: {result} (id={action_id}, symbol={symbol}, signal={sig})",
                            fatal=True,
                        )
                        conn.execute(
                            text(f"UPDATE {ACTION_TBL} SET processed = true WHERE id = :id"),
                            {"id": action_id},
                        )
                        continue

                    qty = result["qty"]
                    leverage = result["leverage"]

                    logging.info(
                        f"OPEN validated id={action_id} symbol={symbol} qty={qty} leverage={leverage} "
                        f"DRY_RUN={DRY_RUN} REAL_ENABLE_ENV={REAL_ENABLE_ENV}"
                    )
                    # 실주문/포지션 테이블 조작은 아직 미부착: 여기서는 '검증 통과'까지만

                elif action_type == "CLOSE":
                    logging.info(
                        f"CLOSE received id={action_id} symbol={symbol} signal={sig} (no-op close for now)"
                    )

                conn.execute(
                    text(f"UPDATE {ACTION_TBL} SET processed = true WHERE id = :id"),
                    {"id": action_id},
                )

        if need_user_wait:
            time.sleep(5)
            continue

        time.sleep(1)


if __name__ == "__main__":
    main()
