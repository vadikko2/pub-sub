import logging.config
from asyncio import get_event_loop, sleep
from datetime import datetime, timedelta
from random import sample, choice
from typing import List

from async_generator import async_generator, yield_

from config import settings
from status_daemon import Logger
from status_daemon.messages import Message
from status_daemon.servers.status.constants import BATCH_SIZE
from status_daemon.servers.status.utils import divide_to_equal_batches
from status_daemon.servers.synchronize.sender.observer import RemoteServerObserver
from status_daemon.status_daemon.constants import Status

logging.config.dictConfig(settings.LOGGING_BASE_CONFIG)
logging.getLogger('aiohttp').setLevel(logging.WARNING)
logging.getLogger('aioredis').setLevel(logging.WARNING)
logging.getLogger('asyncio').setLevel(logging.WARNING)


async def exception_handler(_, error):
    Logger.error('Ошибка перенаправления сообщений на сервер: %s', error)


async def send_messages(
        observer: RemoteServerObserver, seconds: int,
        max_status_number: int, uids: List[str], sleep_time: int
):
    start = datetime.now()
    end = start + timedelta(seconds=seconds)
    loop = get_event_loop()
    loop.set_exception_handler(exception_handler)
    while datetime.now() < end:
        for uid in sample(uids, max_status_number):
            loop.create_task(observer.put(
                message=Message(
                    status=choice([status for status in Status]),
                    uid=uid
                )
            ))
        await sleep(sleep_time)
    raise KeyboardInterrupt


@async_generator
async def reconnect_callback(uids: List[str]):
    statuses = [Message(status=choice([status for status in Status]), uid=uid) for uid in uids]
    try:
        batches = divide_to_equal_batches(statuses, BATCH_SIZE)
        for batch in batches:
            try:
                await yield_(batch)
            except Exception as e:
                Logger.error('Ошибка при попытке формирования сообщения из батча {}: {}'.format(batch, e))
                continue
    except Exception as e:
        raise ValueError(
            'Ошибка при попытке разделения последних статусов на батчи размером {}: {}'.format(BATCH_SIZE, e)
        )


async def run_client(
        host: str, port: int, name: str,
        uids: List[str], seconds: int,
        max_status_number: int, sleep_time: int
) -> None:
    observer = await RemoteServerObserver.connect(
        host=host, port=port, server_name=name,
        name=host
    )
    loop = get_event_loop()
    observer_task = loop.create_task(observer.run(reconnect_callback, **{'uids': uids}))
    loop.create_task(send_messages(
        observer=observer, seconds=seconds,
        max_status_number=max_status_number,
        uids=uids, sleep_time=sleep_time
    )
    )

    try:
        await observer_task
    finally:
        await observer.disconnect()


def read_uuids_from_file(filename: str) -> List[str]:
    with open(filename) as uidsf:
        return uidsf.read().splitlines()


def run(
        host: str, port: int, name: str,
        uids_filename: str, seconds: int,
        max_status_number: int = 128, sleep_time: int = 10
) -> None:
    uids = read_uuids_from_file(uids_filename)
    loop = get_event_loop()
    loop.run_until_complete(run_client(
        host=host, port=port, name=name,
        uids=uids, seconds=seconds,
        max_status_number=max_status_number,
        sleep_time=sleep_time
    ))
