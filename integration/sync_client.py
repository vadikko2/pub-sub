import logging
import logging.config
from asyncio import get_event_loop, gather, sleep
from datetime import datetime, timedelta
from random import sample, choice
from typing import List, Iterable

from config import settings
from status_daemon.messages import Message
from status_daemon.servers.status.constants import BATCH_SIZE
from status_daemon.servers.status.utils import divide_to_equal_batches
from status_daemon.servers.synchronize.sender.observer import RemoteServerObserver
from status_daemon.status_daemon.constants import Status

logging.config.dictConfig(settings.LOGGING_BASE_CONFIG)
logging.getLogger('aiohttp').setLevel(logging.WARNING)
logging.getLogger('aioredis').setLevel(logging.WARNING)
logging.getLogger('asyncio').setLevel(logging.WARNING)


async def send_messages(
        observer: RemoteServerObserver, seconds: int,
        max_status_number: int, uids: List[str], sleep_time: int
):
    start = datetime.now()
    end = start + timedelta(seconds=seconds)

    while datetime.now() < end:
        status = choice([status for status in Status])
        tasks = gather(*(
            observer._loop.create_task(observer.put(
                message='{"status": %s, "publisher": "%s"}' % (status.value, uid)
            ))
            for uid in sample(uids, max_status_number)
        ), return_exceptions=True)
        try:
            await tasks
        except Exception as e:
            logging.error('Ошибка перенаправления сообщений на сервер: %s', e)
        finally:
            for task in tasks:
                if not task.cancelled():
                    task.cancel()
            await sleep(sleep_time)
    raise KeyboardInterrupt


async def reconnect_callback(uids: List[str]) -> Iterable[str]:
    statuses = [(choice([status for status in Status]), None, uid) for uid in uids]
    try:
        result = []
        batches = divide_to_equal_batches(statuses, BATCH_SIZE)
        for batch in batches:
            try:
                result.append(Message.create_package(messages=batch))  # type: str
            except Exception as e:
                logging.error('Ошибка при попытке формирования сообщения из батча {}: {}'.format(batch, e))
                continue
        return result
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
    tasks = gather(*(
        observer.run(reconnect_callback, reconnect_callback_kwargs={'uids': uids}),
        send_messages(
            observer=observer, seconds=seconds,
            max_status_number=max_status_number,
            uids=uids, sleep_time=sleep_time
        )
    ))

    try:
        await tasks
    finally:
        for task in tasks:
            if not task.cancelled():
                task.cancel()
        await observer.disconnect()


def read_uuids_from_file(filename: str) -> List[str]:
    with open(filename) as uidsf:
        return uidsf.read().splitlines()


def run(
        host: str, port: int, name: str,
        uids_filename: str, seconds: int,
        max_status_number: int = 998, sleep_time: int = 10
) -> None:
    uids = read_uuids_from_file(uids_filename)
    loop = get_event_loop()
    loop.run_until_complete(run_client(
        host=host, port=port, name=name,
        uids=uids, seconds=seconds,
        max_status_number=max_status_number,
        sleep_time=sleep_time
    ))
