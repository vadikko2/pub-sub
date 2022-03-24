from runners import Runner
from status_daemon.servers.synchronize.sender.daemon import SyncStatusSenderDaemon


class SenderSyncRunner(Runner):
    """Запуск клиента синхронизации статусов (status-ssync)"""

    @staticmethod
    def _run():
        daemon = SyncStatusSenderDaemon.connect()
        try:
            daemon.run()
        finally:
            daemon.disconnect()
