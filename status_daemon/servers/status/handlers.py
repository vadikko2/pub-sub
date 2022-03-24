"""Handler сервиса статусов"""

import logging
from asyncio import gather
from typing import Optional

from aiohttp import web

from status_daemon.auth import raise_auth_error
from status_daemon.auth.exceptions import UnauthorizedException
from status_daemon.privileges.privileges import Privileges
from status_daemon.redis.redis import RedisController
from status_daemon.servers.status.publihser import Publisher
from status_daemon.servers.status.subscriber import Subscriber
from status_daemon.servers.status.users import User


async def pubsub_handler(request: web.Request):
    """WebSocket для публикации и получения статусов"""

    redis = None  # type: Optional[RedisController]
    privileges = None  # type: Optional[Privileges]
    try:

        privileges = await Privileges.connect(
            pr_cache=request.get('privileges_cache'),
            is_local_cache=request.get('is_local_cache')
        )

        user = await User.create_with_auth(request=request, privileges=privileges)  # type: User
        # если получилось получить UID абонента - подключаемся к Redis
        redis = await RedisController.connect()
        async with Publisher(
                user=user,
                redis=redis,
                privileges=privileges,
        ) as publisher:
            async with Subscriber(
                    publisher=publisher,
                    privileges=privileges,
            ) as subscriber:
                tasks = [
                    request.loop.create_task(publisher.run()),  # publisher task
                    request.loop.create_task(subscriber.run())  # subscriber task
                ]

                try:
                    await gather(*tasks)
                finally:
                    for task in tasks:
                        if not task.cancelled():
                            task.cancel()

    except UnauthorizedException as err:
        # ошибка аутентификации
        return raise_auth_error(err=err, request=request)
    except Exception as e:
        logging.info('Завершение соединения с абонентом %s по причине: %s', request.remote, e)
    finally:
        if isinstance(redis, RedisController):
            await redis.disconnect()
        if isinstance(privileges, Privileges):
            await privileges.disconnect()
