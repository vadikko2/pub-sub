import logging
from asyncio import sleep
from functools import wraps
from typing import (
    Tuple
)

from status_daemon.exceptions import MessageDecodeException
from status_daemon.servers.status.constants import RETRY


def parse_keyspace(keyspace: str) -> str:
    """Парсит __keyspace___"""
    if isinstance(keyspace, bytes):
        keyspace = keyspace.decode()
    key_name = keyspace.split(':')[1]
    return key_name


def pattern_to_key(*values, pattern: str) -> str:
    """Создает ключ из паттерна"""
    return pattern.replace('*', '{}').format(*values)


def extract_info_from_key(key: str) -> Tuple[str, str]:
    """Извлекает имя сервера и отправителя из ключа"""
    try:
        if isinstance(key, bytes):
            key = key.decode()
        parts = key.split('@')
        if not len(parts) >= 3:
            raise ValueError('Слишком мало частей ключа (должно быть >= 3)')
        suv_name = parts[1]
        publisher = parts[2]
    except Exception as e:
        raise MessageDecodeException(
            'Ошибка извлечения имени сервера из %s: %s' % (key, e)
        )

    return publisher, suv_name


def extract_uid_from_key(key: str, index=1) -> str:
    """Извлекает UID из ключа"""
    try:
        if isinstance(key, bytes):
            key = key.decode()
        uid = key.split('@')[index]  # type: str
    except Exception as e:
        raise ValueError(
            'Ошибка извлечения UID из %s : %s ' % (key, e)
        )
    return uid


def async_retry(
        retry: int = RETRY,
        exceptions: Tuple = (Exception,)
):
    """Декоратор, перезапускающий асинхронный метод"""

    def decorator(func):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            for _ in range(3):  # сперва пробуем 3 раза подключиться без sleep-ов
                try:
                    return await func(*args, **kwargs)
                except (KeyboardInterrupt, RuntimeError) as e:
                    logging.error('Завершение работы %s с ошибкой %s', func.__name__, e)
                    return None
                except exceptions as e:
                    logging.error(e)
            while True:
                try:
                    return await func(*args, **kwargs)
                except (KeyboardInterrupt, RuntimeError) as e:
                    logging.error('Завершение работы %s с ошибкой %s', func.__name__, e)
                    break
                except exceptions as e:
                    logging.error(e)
                await sleep(retry)

        return wrapper

    return decorator
