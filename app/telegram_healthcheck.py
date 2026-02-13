#!/usr/bin/env python3
"""
Telegram service healthcheck.
Runs periodically via systemd timer. Checks if telegram_cmd_poller
is consistently failing, and sends an alert + attempts recovery.
"""
import os
import sys
import json
import time
import subprocess
import urllib.parse
import urllib.request

ENV_PATH = "/root/trading-bot/app/telegram_cmd.env"
COOLDOWN_FILE = "/tmp/tg_healthcheck_last_alert.json"
COOLDOWN_SEC = 300  # 5 min between alerts

def load_env(path):
    env = {}
    if not os.path.isfile(path):
        return env
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            k, v = line.split("=", 1)
            env[k.strip()] = v.strip()
    return env

def send_tg(token, chat_id, text):
    try:
        url = f"https://api.telegram.org/bot{token}/sendMessage"
        data = urllib.parse.urlencode({
            "chat_id": str(chat_id),
            "text": text,
            "disable_web_page_preview": "true",
        }).encode("utf-8")
        req = urllib.request.Request(url, data=data, method="POST")
        urllib.request.urlopen(req, timeout=10)
    except Exception:
        pass

def in_cooldown():
    try:
        with open(COOLDOWN_FILE, "r") as f:
            data = json.load(f)
        return (time.time() - data.get("ts", 0)) < COOLDOWN_SEC
    except Exception:
        return False

def set_cooldown():
    try:
        with open(COOLDOWN_FILE, "w") as f:
            json.dump({"ts": time.time()}, f)
    except Exception:
        pass

def main():
    # Check if env file exists
    if not os.path.isfile(ENV_PATH):
        print(f"[healthcheck] WARN: {ENV_PATH} missing!")
        # Try to restore from backup
        backups = [
            "/root/trading-bot/app/.backup_20260211/telegram_cmd.env",
        ]
        for b in backups:
            if os.path.isfile(b):
                subprocess.run(["cp", b, ENV_PATH], check=True)
                print(f"[healthcheck] Restored env from {b}")
                break

    # Check service status via journal (last 3 runs)
    result = subprocess.run(
        ["journalctl", "-u", "telegram_cmd_poller.service",
         "-n", "30", "--no-pager", "-o", "short"],
        capture_output=True, text=True, timeout=10
    )
    output = result.stdout
    recent_failures = output.count("status=1/FAILURE")

    if recent_failures >= 3:
        print(f"[healthcheck] ALERT: {recent_failures} recent failures detected")

        env = load_env(ENV_PATH)
        token = env.get("TELEGRAM_BOT_TOKEN", "")
        chat_id = env.get("TELEGRAM_ALLOWED_CHAT_ID", "0")

        if token and chat_id != "0" and not in_cooldown():
            send_tg(token, int(chat_id),
                     f"⚠️ telegram_cmd_poller: {recent_failures} failures detected.\n"
                     f"Healthcheck auto-recovering...")
            set_cooldown()

        # Force restart timer
        subprocess.run(["systemctl", "restart", "telegram_cmd_poller.timer"],
                       capture_output=True)
        print("[healthcheck] Restarted telegram_cmd_poller.timer")
    else:
        print("[healthcheck] OK")

if __name__ == "__main__":
    main()
