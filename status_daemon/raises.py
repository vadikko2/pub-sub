import logging

from aiohttp import web

from status_daemon.servers.status.users import User


def raise_auth_error(err, request: web.Request):
    """raise ошибки аутентификации"""
    reason = "Ошибка аутентификации запроса от %s : %s." % (request.remote, err)
    logging.error(reason)
    return web.HTTPUnauthorized(
        headers={"WWW-Authenticate": reason}, reason="Authentication required"
    )


def raise_available_error(user: User):
    reason = "Ошибка доступа абонента %s к сервису статусов" % user
    logging.error(reason)
    return web.HTTPNotAcceptable(
        reason=reason
    )
