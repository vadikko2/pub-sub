import logging.config
import socket
from asyncio import gather, get_event_loop, sleep
from typing import List, Iterable

import kerberos
from aiohttp import ClientSession, WSMsgType
from ujson import dumps

from config import settings
from status_daemon import Logger
from status_daemon.messages import Message
from status_daemon.privileges.privileges import Privileges
from status_daemon.redis.redis import RedisController
from status_daemon.status_daemon.constants import Status, MessageAttributes, Operation
from status_daemon.utils import pattern_to_key

logging.config.dictConfig(settings.LOGGING_BASE_CONFIG)
logging.getLogger('aiohttp').setLevel(logging.WARNING)
logging.getLogger('aioredis').setLevel(logging.WARNING)
logging.getLogger('asyncio').setLevel(logging.WARNING)

service = 'HTTP@{}'.format(socket.getfqdn())


async def request_uids_by_names(remote_names: Iterable[str], pr_redis_pool: RedisController) -> Iterable[str]:
    uids = set(await pr_redis_pool.pool.mget(
        *[pattern_to_key(name, pattern=Privileges.DNAME_PATTERN) for name in remote_names]
    ) or [])
    return uids


def get_auth_header():
    __, ctx = kerberos.authGSSClientInit(service)
    kerberos.authGSSClientStep(ctx, "")
    token = kerberos.authGSSClientResponse(ctx)
    return {"Authorization": "Negotiate " + token}


async def get_ws(session: ClientSession, host: str, port: int):
    return await session.ws_connect(url='http://{}:{}/pubsub'.format(host, port))


async def listen_statuses(ws, remote_names: List[str]):
    pr_redis = await RedisController.connect(**settings.PRIVILEGE_REDIS)  # type: RedisController
    try:
        remote_uids = await request_uids_by_names(remote_names=remote_names, pr_redis_pool=pr_redis)

        subscribe_msg = dumps({
            MessageAttributes.events.value: {
                remote_uid: Operation.SUBSCRIBE.value for remote_uid in remote_uids
            }
        })

        await ws.send_str(subscribe_msg)
        Logger.debug('Отправлен запрос на подписку на пользователей {}'.format(remote_names))

        while True:
            msg = await ws.receive()  # type: WSMsgType

            if msg.type == WSMsgType.text:
                if msg.data == 'close':
                    await ws.close()
                    break
                else:
                    Logger.debug("Получено сообщение: %s" % msg.data)
            elif msg.tp == WSMsgType.closed:
                break
            elif msg.tp == WSMsgType.error:
                break
    finally:
        await pr_redis.disconnect()


async def publish_status(ws, status_list: List[int], sleep_: int = 5):
    while True:
        for item in status_list:
            try:
                status = Status(item)
            except Exception:
                Logger.error('Неизвестный статус %s' % item)
                continue
            msg = Message.create_message(Message(status=status))
            await sleep(sleep_)  # ждать перед отправкой
            await ws.send_str(msg)
            Logger.info('Опубликовано сообщение: %s' % msg)


async def run_client(host: str, port: int, remote_names: List[str], status_list: List[int], sleep_: int = 5):
    async with ClientSession(headers=get_auth_header()) as session:
        ws = await get_ws(session, host=host, port=port)
        await gather(
            listen_statuses(ws, remote_names=remote_names),
            publish_status(ws, status_list=status_list, sleep_=sleep_)
        )


def run(host: str, port: int, remote_names: List[str], status_list: List[int], sleep_: int = 5):
    loop = get_event_loop()
    loop.run_until_complete(run_client(host, port, remote_names, status_list, sleep_))
