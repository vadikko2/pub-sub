from asyncio import Queue, AbstractEventLoop
from typing import Set, Dict, List, Callable, Any

from aiohttp import ClientSession
from async_generator import async_generator, yield_


async def default_callback(*args, **kwargs):
    raise NotImplementedError('Reconnect Callback Does Not Implemented')


async def gql_request(url: str, request_body: str, callback: Callable = lambda x: x) -> Any:
    """Запрашивает список встречных серверов из сервиса int-configs"""
    async with ClientSession() as client:
        r_ = await client.post(url=url, json={'query': request_body})
        res = await r_.json()
        if r_.status != 200:
            raise ValueError('Ошибка запроса {} на адрес {}: {}'.format(request_body, url, res))
    return callback(res)


def extract_host_name(gql_response: Dict) -> str:
    """Извлекает имя сервера из ответа сервиса int-configs"""
    try:
        return gql_response['data']['getServerName']['host']
    except Exception as e:
        raise ValueError('Ошибка извлечения имени сервера из '
                         'ответа {} сервиса int-configs: {}'.format(gql_response, e))


def extract_hosts(gql_response: Dict) -> Set[str]:
    """Извлекает список имен встречных серверов из ответа сервиса int-configs"""
    try:
        hosts = gql_response['data']['getTrustedServers']  # type: List[Dict]
        result = {host['host'] for host in hosts}  # type: Set[str]
    except Exception as e:
        raise ValueError('Ошибка извлечения списка встречных '
                         'серверов из ответа {}: {}'.format(gql_response, e))

    return result


@async_generator
def merge_async_iters(*aiters, loop: AbstractEventLoop):
    queue = Queue(1)
    run_count = len(aiters)
    cancelling = False

    async def drain(aiter):
        nonlocal run_count
        try:
            async for item in aiter:
                await queue.put((False, item))
        except Exception as e:
            if not cancelling:
                await queue.put((True, e))
            else:
                raise
        finally:
            run_count -= 1

    async def merged():
        try:
            while run_count:
                raised, next_item = await queue.get()
                if raised:
                    cancel_tasks()
                    raise next_item
                await yield_(next_item)
        finally:
            cancel_tasks()

    def cancel_tasks():
        nonlocal cancelling
        cancelling = True
        for t in tasks:
            t.cancel()

    tasks = [loop.create_task(drain(aiter)) for aiter in aiters]
    return merged()
