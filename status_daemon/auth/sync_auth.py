from typing import Any

from aiohttp import web

from status_daemon.auth import Authenticator
from status_daemon.auth.exceptions import SyncUnauthorizedException


class SyncAuthenticator(Authenticator):
    """Аутентификация при синхронизации серверов статусов"""

    @staticmethod
    async def aauthenticate(request: web.Request) -> Any:
        raise NotImplemented

    @staticmethod
    def authenticate(request: web.Request) -> str:
        suv_name = request.headers.get('SUV')
        if suv_name is None:
            raise SyncUnauthorizedException('Отсутствует поле SUV в заголовке.')

        return suv_name
