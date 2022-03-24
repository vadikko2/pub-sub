from typing import Optional

from aiohttp import web

from status_daemon.auth.gss_auth import KerberosAuthenticator
from status_daemon.privileges.privileges import Privileges


class User:
    """Несет в себе всю информацию о пользователе, умеет получать UID через GSS аутентификацию."""

    def __init__(self, uid: str, name: Optional[str] = None, request: Optional[web.Request] = None):
        self._uid = uid
        self._name = name
        self._request = request

    def __repr__(self):
        remote = None
        if self._request is not None:
            remote = self._request.remote
        return '(addr=%s, uid=%s, name=%s)' % (remote, self._uid, self._name)

    def __hash__(self):
        return hash(self.uid)

    @property
    def uid(self):
        return self._uid

    @property
    def request(self):
        return self._request

    @classmethod
    async def create_with_auth(cls, request: web.Request, privileges: Privileges):
        """
        Создает новый объект, проверя пользователя через GSS аутентификацию.
        Из Redis сервиса привилегий получает UID абонента по его имени.
        request: web.Request - контекст запроса
        privileges: Privileges - инстанс привилегий, подключенный к Redis сервиса привилегий
        """
        # проводим аутентификацию
        user, _ = await KerberosAuthenticator.aauthenticate(request)
        # получаем uid пользователя
        uid = await privileges.get_uid_by_name(user)
        return cls(name=user, uid=uid, request=request)
