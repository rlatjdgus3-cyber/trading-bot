"""systemd watchdog helper — Type=notify 서비스에서 사용."""
import threading


def init_watchdog(interval_sec=10):
    """READY=1 보내고 백그라운드 스레드로 WATCHDOG=1 주기 전송."""
    try:
        import sdnotify
        n = sdnotify.SystemdNotifier()
        n.notify('READY=1')

        def _ping():
            import time
            while True:
                n.notify('WATCHDOG=1')
                time.sleep(interval_sec)

        t = threading.Thread(target=_ping, daemon=True)
        t.start()
        return n
    except ImportError:
        return None  # sdnotify 없으면 무시 (기존 동작 유지)
