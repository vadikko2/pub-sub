from typing import Set, Dict, List, Callable, Any

from aiohttp import ClientSession


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
