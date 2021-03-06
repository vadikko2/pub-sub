from asyncio import get_event_loop
from typing import Optional

import aioredis
from aioredis import Redis

from config import settings
from status_daemon import AsyncConnectableMixin, Logger


class RedisController(AsyncConnectableMixin):
    DAEMON_KEYSPACE = '__keyspace@*__:*'
    WISH_PATTERN = "WISH@*@"  # таблица с "желаемыми"
    # для получения статусов uuid-ов абонентов (ключ - отправитель статуса,
    # значение - список получателей)
    LAST_PATTERN = "LAST@*@*@"  # Значение последнего статуса
    LOCAL_LAST_PATTERN = "LAST@local@*@"  # Значение последнего статуса локального абонента
    PUB_PATTERN = "PUB@*@*@"  # очереди для публикации статусов
    LOCAL_PUB_PATTERN = "PUB@local@*@"  # очереди для публикации статусов локального абонента
    SUB_PATTERN = "SUB@*@"  # очереди для получения статусов

    def __init__(self, redis_pool: Redis):
        self._loop = get_event_loop()
        self._redis = redis_pool

    @property
    def pool(self):
        return self._redis

    def __repr__(self):
        return str(self._redis.address)

    @staticmethod
    async def get_redis_pool(
            host=settings.BASE_REDIS.host,
            port=settings.BASE_REDIS.port,
            db=settings.BASE_REDIS.db
    ) -> Redis:
        """Возвращает подключенный инстанс Redis"""
        try:
            pool_string = 'redis://{}:{}'.format(host, port)
            redis = await aioredis.create_redis_pool(pool_string, db=db, encoding='utf-8')  # type: Redis
            Logger.debug('Соединение с %s установлено', redis.address)
        except Exception as e:
            raise ConnectionRefusedError(
                'Ошибка при установке соединения с %s: %s' % ((host, port), e)
            )
        return redis

    @staticmethod
    async def close_redis_pool(redis: Optional[Redis]):
        """Отключает инстанс Redis"""
        if isinstance(redis, Redis) and not redis.closed:
            try:
                redis.close()
                await redis.wait_closed()
            except Exception as e:
                raise ValueError(
                    'Ошибка при попытке закрытия соединения с %s: %s' %
                    (redis.address, e)
                )
            else:
                Logger.debug('Соединение с %s закрыто', redis.address)

    @classmethod
    async def connect(
            cls,
            host=settings.BASE_REDIS.host,
            port=settings.BASE_REDIS.port,
            db=settings.BASE_REDIS.db
    ):
        pool = await RedisController.get_redis_pool(host=host, port=port, db=db)
        return cls(redis_pool=pool)

    async def disconnect(self):
        await RedisController.close_redis_pool(redis=self.pool)

    async def flush_keys_by_pattern(self, pattern: str = '*'):
        """Очищает ключи по паттерну"""
        cursor = b'0'
        while cursor:
            cursor, keys = await self.pool.scan(cursor=cursor, match=pattern)
            if keys:
                try:
                    await self.pool.delete(*keys)
                except Exception as e:
                    Logger.error(
                        'Ошибка при попытке очистить значения по ключами %s: %s',
                        keys, e
                    )
