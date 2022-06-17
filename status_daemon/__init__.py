import logging
import logging.config
import os
from asyncio import get_event_loop
from concurrent.futures.thread import ThreadPoolExecutor
from typing import Any

from aiohttp import web

from config import settings


class LoggerMeta(type):

    def __new__(cls, *args, **kwargs):
        cls._executor = ThreadPoolExecutor(max_workers=os.cpu_count())
        logging.config.dictConfig(settings.LOGGING_BASE_CONFIG)
        logging.getLogger('aiohttp').setLevel(logging.WARNING)
        logging.getLogger('aioredis').setLevel(logging.WARNING)

        cls._logger = logging.getLogger('default')
        return super(LoggerMeta, cls).__new__(cls, *args, **kwargs)

    @property
    def logger(cls) -> logging.Logger:
        return cls._logger

    @property
    def executor(cls):
        return cls._executor


class Logger(metaclass=LoggerMeta):

    @staticmethod
    def info(msg, *args) -> None:
        return Logger.log(logging.INFO, msg, *args)

    @staticmethod
    def debug(msg, *args) -> None:
        return Logger.log(logging.DEBUG, msg, *args)

    @staticmethod
    def warning(msg, *args) -> None:
        return Logger.log(logging.WARNING, msg, *args)

    @staticmethod
    def error(msg, *args) -> None:
        return Logger.log(logging.ERROR, msg, *args)

    @staticmethod
    def log(lvl: int, msg: str, *args) -> None:
        loop = get_event_loop()

        if loop.is_running():
            loop.run_in_executor(Logger.executor, Logger.logger.log, lvl, msg, *args)
        else:
            Logger.logger.log(lvl, msg, *args)


class ConnectableMixin:
    """Примесь, предоставляющая абстрактный интерфейс connect/disconnect"""
    pass


class AsyncConnectableMixin(ConnectableMixin):

    @classmethod
    async def connect(cls, *args, **kwargs):
        """Возвращает объекты класса, подключенный к чему-либо"""
        raise NotImplementedError

    async def disconnect(self, *args, **kwargs):
        """Отключает объект от того, к чему осуществлялось подключение в методе connect"""
        raise NotImplementedError


class BlockingConnectableMixin(ConnectableMixin):

    @classmethod
    def connect(cls, *args, **kwargs):
        """Возвращает объекты класса, подключенный к чему-либо"""
        raise NotImplementedError

    def disconnect(self, *args, **kwargs):
        """Отключает объект от того, к чему осуществлялось подключение в методе connect"""
        raise NotImplementedError


class RunnableMixin:
    """Примесь, предоставляющая абстрактный интерфейс run"""
    pass


class AsyncRunnableMixin(RunnableMixin):

    async def run(self, *args, **kwargs):
        """Запуск"""
        raise NotImplementedError


class BlockingRunnableMixin(RunnableMixin):

    def run(self, *args, **kwargs):
        """Запуск"""
        raise NotImplementedError


class BlockingRunnableWebServer(BlockingRunnableMixin):

    @staticmethod
    def get_app(*args, **kwargs) -> web.Application:
        raise NotImplementedError

    @staticmethod
    def run(app: web.Application, host: Any, port: Any):
        """Запуск"""
        raise NotImplementedError
