import logging
from abc import ABC, abstractmethod
from typing import Any

from aiohttp import web


def raise_auth_error(err, request: web.Request):
    """raise ошибки аутентификации"""
    reason = "Ошибка аутентификации запроса от %s : %s." % (request.remote, err)
    logging.error(reason)
    return web.HTTPUnauthorized(
        headers={"WWW-Authenticate": reason}, reason="Authentication required"
    )


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
