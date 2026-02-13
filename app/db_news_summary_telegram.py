import os, sys, urllib.request, urllib.parse, subprocess

ENV_PATH = "/root/trading-bot/app/telegram_cmd.env"

def load_env(path: str) -> dict:
    env = {}
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            k, v = line.split("=", 1)
            env[k.strip()] = v.strip()
    return env

def tg_send(token: str, chat_id: int, text: str) -> None:
    text = (text or "").strip()
    if len(text) > 3500:
        text = text[:3500] + "\n…(truncated)"

    url = f"https://api.telegram.org/bot{token}/sendMessage"
    data = urllib.parse.urlencode({
        "chat_id": str(chat_id),
        "text": text,
        "disable_web_page_preview": "true",
    }).encode("utf-8")

    req = urllib.request.Request(url, data=data, method="POST")
    with urllib.request.urlopen(req, timeout=20) as resp:
        body = resp.read().decode("utf-8", errors="ignore")
        if resp.status != 200:
            raise RuntimeError(f"telegram status={resp.status} body={body}")

def build_summary(minutes: int, limit: int) -> str:
    env = os.environ.copy()
    env["NEWS_SUMMARY_MINUTES"] = str(minutes)
    env["NEWS_SUMMARY_LIMIT"] = str(limit)

    p = subprocess.run(
        ["/usr/bin/python3", "/root/trading-bot/app/news_bot.py", "--summary"],
        capture_output=True, text=True, env=env
    )
    if p.returncode != 0:
        raise RuntimeError(f"news_bot.py --summary failed rc={p.returncode} err={(p.stderr or '')[:500]}")
    return (p.stdout or "").strip() or "📰 DB News\n• (none)"

def main():
    # 사용법:
    #   python3 db_news_summary_telegram.py           -> 기본: 24시간(1440m), 20개
    #   python3 db_news_summary_telegram.py 60 15     -> 최근 60분, 15개
    minutes = 1440
    limit = 20
    if len(sys.argv) >= 2:
        minutes = int(sys.argv[1])
    if len(sys.argv) >= 3:
        limit = int(sys.argv[2])

    env = load_env(ENV_PATH)
    token = (env.get("TELEGRAM_BOT_TOKEN", "") or "").strip()
    chat_id = int(env.get("TELEGRAM_ALLOWED_CHAT_ID", "0") or "0")
    if not token or chat_id == 0:
        raise SystemExit("ENV missing: TELEGRAM_BOT_TOKEN / TELEGRAM_ALLOWED_CHAT_ID")

    text = build_summary(minutes, limit)
    tg_send(token, chat_id, text)
    print("[OK] sent DB news summary to telegram")

if __name__ == "__main__":
    main()
