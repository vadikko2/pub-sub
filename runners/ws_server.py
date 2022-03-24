from runners import Runner
from status_daemon.servers.status.daemon import WebSocketServerDaemon


class WSServerRunner(Runner):
    """Запуск сервера статусов (ws-server)"""

    @staticmethod
    def _run():
        WebSocketServerDaemon.run(
            app=WebSocketServerDaemon.get_app()
        )
