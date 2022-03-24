from runners import Runner
from status_daemon.status_daemon.daemon import StatusRoutingDaemon


class RoutingRunner(Runner):
    """Запуск маршрутизатора статусов (status-daemon)"""

    @staticmethod
    def _run():
        daemon = StatusRoutingDaemon.connect()
        try:
            daemon.run()
        finally:
            daemon.disconnect()
