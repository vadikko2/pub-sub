from abc import ABC, abstractmethod
from typing import Any

from aiohttp import web


class Authenticator(ABC):
    """Аутентификатор"""

    @staticmethod
    @abstractmethod
    async def aauthenticate(request: web.Request) -> Any:
        """Асинхронный метод аутентификации"""
        pass

    @staticmethod
    @abstractmethod
    def authenticate(request: web.Request) -> Any:
        """Блокирующий метод аутентификации"""
        pass
