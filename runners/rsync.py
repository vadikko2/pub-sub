from runners import Runner
from status_daemon.servers.synchronize.receiver.daemon import SyncStatusReceiverDaemon


class ReceiverSyncRunner(Runner):
    """Запуск сервера синхронизации статусов (status-rsync)"""

    @staticmethod
    def _run():
        SyncStatusReceiverDaemon.run(
            app=SyncStatusReceiverDaemon.get_app()
        )
