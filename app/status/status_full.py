#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Status Full Report — stdout + Telegram 통합 리포트.
Executed by systemd: status_full_report.service
"""

import os
import sys
import urllib.parse
import urllib.request
import psycopg2
import ccxt
from datetime import datetime, timezone, timedelta
from dotenv import load_dotenv

load_dotenv("/root/trading-bot/app/.env")

SYMBOL = os.getenv("SYMBOL", "BTC/USDT:USDT")

DB = dict(
    host=os.getenv("DB_HOST", "localhost"),
    port=int(os.getenv("DB_PORT", "5432")),
    dbname=os.getenv("DB_NAME", "trading"),
    user=os.getenv("DB_USER", "bot"),
    password=os.getenv("DB_PASS", "botpass"),
    connect_timeout=10,
    options="-c statement_timeout=30000",
)

TG_ENV_PATH = "/root/trading-bot/app/telegram_cmd.env"


# ── helpers ──────────────────────────────────────────────

def _db():
    conn = psycopg2.connect(**DB)
    conn.autocommit = True
    return conn


def _q1(sql, params=None):
    conn = _db()
    try:
        with conn.cursor() as cur:
            cur.execute(sql, params or ())
            return cur.fetchone()
    finally:
        conn.close()


def _load_tg_env():
    token, chat_id = "", ""
    try:
        with open(TG_ENV_PATH, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                k, v = line.split("=", 1)
                k, v = k.strip(), v.strip()
                if k == "TELEGRAM_BOT_TOKEN":
                    token = v
                elif k == "TELEGRAM_ALLOWED_CHAT_ID":
                    chat_id = v
    except Exception:
        pass
    return token, chat_id


def _send_telegram(text):
    token, chat_id = _load_tg_env()
    if not token or not chat_id:
        print("[status_full] SKIP telegram: env missing", flush=True)
        return
    s = text or ""
    chunks = []
    while len(s) > 3800:
        chunks.append(s[:3800])
        s = s[3800:]
    chunks.append(s)
    for c in chunks:
        url = f"https://api.telegram.org/bot{token}/sendMessage"
        data = urllib.parse.urlencode({
            "chat_id": chat_id,
            "text": c,
            "disable_web_page_preview": "true",
        }).encode("utf-8")
        req = urllib.request.Request(url, data=data, method="POST")
        urllib.request.urlopen(req, timeout=20)


def _safe_float(x):
    try:
        return float(x)
    except Exception:
        return None


def _exchange():
    return ccxt.bybit({
        "apiKey": os.getenv("BYBIT_API_KEY"),
        "secret": os.getenv("BYBIT_SECRET"),
        "enableRateLimit": True,
        "options": {"defaultType": "swap"},
    })


# ── sections ─────────────────────────────────────────────

def _section_system():
    lines = []
    live = os.getenv("LIVE_TRADING", "") == "YES_I_UNDERSTAND"
    lines.append(f"- LIVE_TRADING: {'ON' if live else 'OFF'}")

    try:
        sw = _q1("SELECT enabled FROM trade_switch ORDER BY id DESC LIMIT 1;")
        sw_on = bool(sw and sw[0])
        lines.append(f"- trade_switch: {'ON' if sw_on else 'OFF'}")
    except Exception as e:
        lines.append(f"- trade_switch: error ({e})")

    try:
        import subprocess
        r = subprocess.run(
            ["systemctl", "is-active", "autopilot"],
            capture_output=True, text=True, timeout=5,
        )
        lines.append(f"- autopilot: {r.stdout.strip()}")
    except Exception:
        lines.append("- autopilot: unknown")

    return "\U0001f527 시스템\n" + "\n".join(lines)


def _section_positions():
    lines = []
    try:
        ex = _exchange()
        positions = ex.fetch_positions([SYMBOL])
        found = False
        for p in positions:
            if p.get("symbol") != SYMBOL:
                continue
            side = p.get("side")
            contracts = _safe_float(p.get("contracts") or 0)
            if not side or not contracts or contracts == 0:
                continue
            entry = _safe_float(p.get("entryPrice") or p.get("entry_price"))
            upl = _safe_float(p.get("unrealizedPnl") or p.get("unrealized_pnl"))
            entry_s = f"${entry:,.0f}" if entry else "n/a"
            upl_s = f"${upl:+.2f}" if upl is not None else "n/a"
            lines.append(f"- {SYMBOL}: {side.upper()} qty={contracts} entry={entry_s} uPnL={upl_s}")
            found = True
        if not found:
            lines.append("- 포지션 없음")
    except Exception as e:
        lines.append(f"- error: {type(e).__name__}: {e}")
    return "\U0001f4ca 포지션 (Bybit)\n" + "\n".join(lines)


def _section_daily_performance():
    lines = []
    try:
        kst = timezone(timedelta(hours=9))
        today_kst = datetime.now(kst).strftime("%Y-%m-%d")
        conn = _db()
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT COUNT(*),
                           COUNT(*) FILTER (WHERE realized_pnl > 0),
                           COUNT(*) FILTER (WHERE realized_pnl < 0),
                           COALESCE(SUM(realized_pnl), 0)
                    FROM execution_log
                    WHERE order_type IN ('CLOSE','REDUCE','REVERSE_CLOSE',
                                         'EXIT','EMERGENCY_CLOSE','STOP_LOSS')
                      AND status IN ('FILLED','VERIFIED')
                      AND (ts AT TIME ZONE 'Asia/Seoul')::date = %s::date;
                """, (today_kst,))
                row = cur.fetchone()
        finally:
            conn.close()

        if row and row[0] > 0:
            total, wins, losses = row[0], row[1], row[2]
            pnl = float(row[3])
            wr = (wins / total * 100) if total > 0 else 0
            lines.append(f"- 거래: {total}건 (W:{wins} / L:{losses}) 승률: {wr:.1f}%")
            lines.append(f"- 총 PnL: {pnl:+.2f} USDT")
        else:
            lines.append("- 오늘 거래 없음")
    except Exception as e:
        lines.append(f"- error: {e}")
    return "\U0001f4b0 일일 성능\n" + "\n".join(lines)


def _section_claude_gate():
    try:
        sys.path.insert(0, "/root/trading-bot/app")
        from claude_gate import get_daily_cost_report
        report = get_daily_cost_report()
        # indent each line
        indented = "\n".join(f"- {l}" for l in report.strip().splitlines() if l.strip())
        return "\U0001f916 Claude Gate\n" + indented
    except Exception as e:
        return f"\U0001f916 Claude Gate\n- error: {e}"


def _section_events():
    lines = []
    try:
        kst = timezone(timedelta(hours=9))
        today_kst = datetime.now(kst).strftime("%Y-%m-%d")
        conn = _db()
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT mode, COUNT(*)
                    FROM event_trigger_log
                    WHERE (ts AT TIME ZONE 'Asia/Seoul')::date = %s::date
                    GROUP BY mode;
                """, (today_kst,))
                rows = cur.fetchall()
        finally:
            conn.close()

        counts = {r[0]: r[1] for r in rows} if rows else {}
        emerg = counts.get("EMERGENCY", 0)
        event = counts.get("EVENT", 0)
        lines.append(f"- EMERGENCY: {emerg}건 | EVENT: {event}건")
        # include others if present
        for mode, cnt in sorted(counts.items()):
            if mode not in ("EMERGENCY", "EVENT"):
                lines.append(f"- {mode}: {cnt}건")
    except Exception as e:
        lines.append(f"- error: {e}")
    return "\u26a1 이벤트 (오늘)\n" + "\n".join(lines)


# ── main ─────────────────────────────────────────────────

def main():
    kst = timezone(timedelta(hours=9))
    now_kst = datetime.now(kst)
    header = f"\U0001f4cc STATUS FULL ({now_kst.strftime('%m/%d %H:%M')} KST)"
    sep = "━━━━━━━━━━━━━━━━━━━━━━"

    sections = []
    sections.append(_section_system())
    sections.append(_section_positions())
    sections.append(_section_daily_performance())
    sections.append(_section_claude_gate())
    sections.append(_section_events())

    msg = header + "\n" + sep + "\n" + "\n\n".join(sections)

    print(msg, flush=True)

    try:
        _send_telegram(msg)
        print("[status_full] telegram sent", flush=True)
    except Exception as e:
        print(f"[status_full] telegram error: {e}", flush=True)

    return 0


if __name__ == "__main__":
    sys.exit(main())
