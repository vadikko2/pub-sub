from asyncio import CancelledError
from typing import Optional

from aiohttp import web

from status_daemon import Logger
from status_daemon.auth.exceptions import UnauthorizedException
from status_daemon.auth.sync_auth import SyncAuthenticator
from status_daemon.raises import raise_auth_error
from status_daemon.redis.redis import RedisController
from status_daemon.servers.synchronize.receiver.receiver import SyncReceiver


async def sync_handler(request: web.Request):
    """endpoint синхронизации"""
    redis = None  # type: Optional[RedisController]
    try:
        suv_name = SyncAuthenticator.authenticate(request=request)
        redis = await RedisController.connect()
        async with SyncReceiver(request=request, redis=redis, suv_name=suv_name) as receiver:
            await receiver.listen_statuses()
    except UnauthorizedException as err:
        # ошибка аутентификации при синхронизации серверов
        return raise_auth_error(err=err, request=request)
    except CancelledError:
        Logger.info('Закрыто соединение с %s', request.remote)
    finally:
        if isinstance(redis, RedisController):
            await redis.disconnect()
