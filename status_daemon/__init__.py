from abc import ABC, abstractmethod
from typing import Any

from aiohttp import web


class ConnectableMixin(ABC):
    """Примесь, предоставляющая абстрактный интерфейс connect/disconnect"""
    pass


class AsyncConnectableMixin(ConnectableMixin, ABC):

    @classmethod
    @abstractmethod
    async def connect(cls, *args, **kwargs):
        """Возвращает объекты класса, подключенный к чему-либо"""
        pass

    @abstractmethod
    async def disconnect(self, *args, **kwargs):
        """Отключает объект от того, к чему осуществлялось подключение в методе connect"""
        pass


class BlockingConnectableMixin(ConnectableMixin, ABC):

    @classmethod
    @abstractmethod
    def connect(cls, *args, **kwargs):
        """Возвращает объекты класса, подключенный к чему-либо"""
        pass

    @abstractmethod
    def disconnect(self, *args, **kwargs):
        """Отключает объект от того, к чему осуществлялось подключение в методе connect"""
        pass


class RunnableMixin(ABC):
    """Примесь, предоставляющая абстрактный интерфейс run"""
    pass


class AsyncRunnableMixin(RunnableMixin, ABC):

    @abstractmethod
    async def run(self, *args, **kwargs):
        """Запуск"""
        pass


class BlockingRunnableMixin(RunnableMixin, ABC):

    @abstractmethod
    def run(self, *args, **kwargs):
        """Запуск"""
        pass


class BlockingRunnableWebServer(BlockingRunnableMixin, ABC):

    @staticmethod
    @abstractmethod
    def get_app(*args, **kwargs) -> web.Application:
        pass

    @staticmethod
    @abstractmethod
    def run(app: web.Application, host: Any, port: Any):
        """Запуск"""
        pass
