import logging
from typing import Any

from aiohttp import web

from config import settings
from status_daemon import BlockingRunnableWebServer
from status_daemon.constants import LAST_PATTERN
from status_daemon.redis.redis import RedisController
from status_daemon.servers.synchronize.receiver.handlers import sync_handler
from status_daemon.servers.utils import flush_and_publish_statuses


class SyncStatusReceiverDaemon(BlockingRunnableWebServer):
    """Приемник статусов со встречных серверов"""

    @staticmethod
    async def flush_statuses(_):
        """Очищает все статусы, приходящие с прослушиваемого сервера"""
        redis = await RedisController.connect()
        await flush_and_publish_statuses(
            redis=redis.pool, last_pattern=LAST_PATTERN,
            except_filter='LAST@local@'
        )
        await redis.disconnect()

    @staticmethod
    def get_app() -> web.Application:
        app = web.Application()
        app.on_startup.append(SyncStatusReceiverDaemon.flush_statuses)
        app.on_shutdown.append(SyncStatusReceiverDaemon.flush_statuses)
        app.router.add_route('GET', '/sync', sync_handler)
        return app

    @staticmethod
    def run(
            app: web.Application,
            host: Any = settings.SYNCHRONIZE.host,
            port: Any = settings.SYNCHRONIZE.port
    ):
        """Запуск"""
        logging.info('Запущен WebSocket сервер синхронизации статусов.')
        web.run_app(app=app, host=host, port=port)
