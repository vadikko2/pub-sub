import logging
from typing import Any

from aiohttp import web
from cachetools import TTLCache

from config import settings
from status_daemon import BlockingRunnableWebServer
from status_daemon.constants import WISH_PATTERN, LAST_PATTERN
from status_daemon.redis.redis import RedisController
from status_daemon.servers.status.handlers import pubsub_handler


class WebSocketServerDaemon(BlockingRunnableWebServer):
    """WebSocket сервер статусов"""

    @staticmethod
    async def flush_last_statuses(_):
        """Очищает кэш последних статусов (при старте status-ws-server)"""
        logging.info('Запущена очистка кэша последних статусов...')
        try:
            redis = await RedisController.connect()
            await redis.flush_keys_by_pattern(pattern=LAST_PATTERN)
            await redis.disconnect()
        except Exception as e:
            raise ValueError(
                'Ошибка при попытке очистки кэша последних статусов: %s' % e
            )
        else:
            logging.info('Кэш последних статусов успешно очищен.')

    @staticmethod
    async def flush_wish_lists(_):
        """Очищает кэш WISH-листов"""
        logging.info('Запущена очистка WISH-листов...')
        try:
            redis = await RedisController.connect()
            await redis.flush_keys_by_pattern(pattern=WISH_PATTERN)
            await redis.disconnect()
        except Exception as e:
            raise ValueError('Ошибка при попытке очистки кэша WISH-листов: %s' % e)
        else:
            logging.info('Кэш WISH-листов успешно очищен.')

    @staticmethod
    async def init_cache(app: web.Application):
        """Инициализация кэша в памяти"""
        app['privileges_cache'] = TTLCache(maxsize=2500, ttl=3600)
        app['is_local_cache'] = TTLCache(maxsize=2500, ttl=3600)

    @staticmethod
    def get_app() -> web.Application:
        app = web.Application()
        app.router.add_route('GET', '/pubsub', pubsub_handler)

        app.on_startup.append(WebSocketServerDaemon.flush_wish_lists)
        app.on_startup.append(WebSocketServerDaemon.flush_last_statuses)
        app.on_startup.append(WebSocketServerDaemon.init_cache)

        app.on_shutdown.append(WebSocketServerDaemon.flush_last_statuses)
        app.on_shutdown.append(WebSocketServerDaemon.flush_wish_lists)

        return app

    @staticmethod
    def run(
            app: web.Application,
            host: Any = settings.SERVICE.host,
            port: Any = settings.SERVICE.port
    ):
        """Запуск"""
        logging.info('Запущен WebSocket сервер статусов.')
        web.run_app(app, host=host, port=port)
