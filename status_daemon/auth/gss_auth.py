"""GSS аутентификация"""

import os
import socket
from typing import Tuple, Any

from aiohttp import web
from kerberos import (
    authGSSServerInit, AUTH_GSS_COMPLETE,
    authGSSServerStep, authGSSServerResponse,
    authGSSServerUserName, GSSError, KrbError,
    authGSSServerClean
)

from config import settings
from status_daemon.auth import Authenticator
from status_daemon.auth.exceptions import GSSUnauthorizedException

SERVICE_NAME = 'HTTP@{}'.format(socket.getfqdn())

KEYTAB = settings.AUTH.keytab


class KerberosAuthenticator(Authenticator):
    """GSS аутентификация через kerberos"""

    @staticmethod
    def authenticate(request: web.Request) -> Any:
        raise NotImplemented

    @staticmethod
    async def setup(_):
        """Добавляет keytab в environ"""
        os.environ["KRB5_KTNAME"] = KEYTAB

    @staticmethod
    async def aauthenticate(request: web.Request) -> Tuple[str, Any]:
        """GSS аутентификация"""
        auth_header = request.headers.get("Authorization")
        if not auth_header:
            err = 'Отсутствует Authorization в заголовке %s запроса %s' % \
                  (request.headers, await request.text())
            raise GSSUnauthorizedException(err)

        auth_type, auth_key = auth_header.split(" ", 1)
        if auth_type != "Negotiate":
            err = 'Отсутствует Negotiate в поле Authorization заголовка %s' % request.headers
            raise GSSUnauthorizedException(err)

        gss_context = None
        try:
            # Initialize kerberos context
            rc_, gss_context = authGSSServerInit(SERVICE_NAME)

            if rc_ != AUTH_GSS_COMPLETE:
                raise GSSUnauthorizedException(
                    "GSS server init failed, return code = %r" % rc_, request
                )

            # Challenge step
            rc_ = authGSSServerStep(gss_context, auth_key)
            if rc_ != AUTH_GSS_COMPLETE:
                raise GSSUnauthorizedException(
                    "GSS server step failed, return code = %r" % rc_, request
                )
            gss_key = authGSSServerResponse(gss_context)

            # Retrieve user name
            fulluser = authGSSServerUserName(gss_context)
            user = fulluser.split("@", 1)[0]
        except (GSSError, KrbError, Exception) as err:
            raise GSSUnauthorizedException(err)
        finally:
            if gss_context is not None:
                authGSSServerClean(gss_context)

        return user, gss_key
