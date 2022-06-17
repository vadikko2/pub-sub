from asyncio import CancelledError
from typing import Optional

from aioredis import Redis

from status_daemon import Logger
from status_daemon.messages import Message
from status_daemon.redis.redis import RedisController
from status_daemon.status_daemon.constants import Status
from status_daemon.utils import pattern_to_key, extract_uid_from_key


async def has_subscribers(redis: Redis, uid: str) -> bool:
    """Проверяет подписан ли кто-нибудь вообще"""
    try:
        key = pattern_to_key(uid, pattern=RedisController.WISH_PATTERN)
        subscriber = await redis.srandmember(key)
        return subscriber is not None
    except Exception as e:
        raise ValueError(
            'Ошибка проверки наличия подписчиков у абонента %s: %r' % (uid, e)
        )


async def flush_and_publish_statuses(
        redis: Redis, last_pattern: str,
        except_filter: Optional[str] = None
):
    """Очищает LAST статусы и уведомляет в очереди PUB"""
    Logger.debug('Производится очистка последних статусов встречных абонентов.')
    try:
        cursor = b'0'
        while cursor:
            # получаем все последние статусы абонентов с сервера
            cursor, keys = await redis.scan(cursor=cursor, match=last_pattern)
            if not keys:
                continue
            keys = set(filter(lambda k: (except_filter is None) or not (except_filter in k), keys))
            for key in keys:
                try:
                    uid = extract_uid_from_key(key, index=2)
                    suv_name = extract_uid_from_key(key, index=1)
                    # если на него кто-то был подписан - публикуем в очередь
                    has_subs = await has_subscribers(redis=redis, uid=uid)  # type: bool
                    if has_subs:
                        await redis.publish(
                            channel=pattern_to_key(suv_name, uid, pattern=RedisController.PUB_PATTERN),
                            message=Message.create_message(Message(status=Status.UNKNOWN))
                        )
                except Exception as e:
                    Logger.error(
                        'Ошибка при попытке зануления последнего статуса абонента %s: %s',
                        key, e
                    )
            # очищаем значения в базе
            if keys:
                await redis.delete(*keys)
    except CancelledError:
        pass
    except Exception as e:
        raise ValueError(
            'Ошибка при очистке последних статусов пользователей: %s' % e
        )
    else:
        Logger.debug(
            'Успешная очистка статусов по паттерну %s с исключением %s',
            last_pattern, except_filter
        )
