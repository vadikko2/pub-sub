from typing import Iterable, Optional

from aiohttp import ClientSession
from cachetools import TTLCache

from config import settings
from status_daemon import AsyncConnectableMixin
from status_daemon.constants import STATE
from status_daemon.redis.redis import RedisController
from status_daemon.utils import pattern_to_key


class Privileges(AsyncConnectableMixin):
    """Реализует всю работу с привилегиями"""
    IN_STS = 1
    OUT_STS = 6

    USERSACL_PATTERN = "USERSACL@*@"  # паттерн пользовательских ACL в
    # Redis хранилище сервиса привилегий
    DNAME_PATTERN = "DNAME@*@"  # паттерн соответствия доменного имени UID-у
    LOCAL_USERS_PATTERN = "LOCAL_USERS"  # паттерн набора локальных пользователей
    PRIVILEGE_TTL = int(settings.PRIVILEGE_CACHE_TTL or 60)

    def __init__(self, redis: RedisController, pr_cache: TTLCache, is_local_cache: TTLCache):
        self._pr_cache = pr_cache
        self._is_local_cache = is_local_cache
        self._redis = redis  # type: RedisController

    @classmethod
    async def connect(
            cls,
            redis_host: str = settings.PRIVILEGE_REDIS.host,
            redis_port: int = settings.PRIVILEGE_REDIS.port,
            db: int = settings.PRIVILEGE_REDIS.db,
            pr_cache: Optional[TTLCache] = None,
            is_local_cache: Optional[TTLCache] = None
    ):
        """Возвращает инстанс Privileges, подключенный к Redis сервиса привилегий"""
        redis_controller = await RedisController.connect(host=redis_host, port=redis_port, db=db)
        pr_cache = pr_cache if isinstance(pr_cache, TTLCache) else TTLCache(maxsize=2500, ttl=3600)
        is_local_cache = is_local_cache if isinstance(is_local_cache, TTLCache) else TTLCache(maxsize=2500, ttl=3600)
        return cls(redis=redis_controller, pr_cache=pr_cache, is_local_cache=is_local_cache)

    async def disconnect(self):
        """Отключает инстанс Privileges от Redis"""
        await self._redis.disconnect()

    @staticmethod
    def _check_bit(privilege: int, bit_number: int) -> bool:
        """Проверяет разрешение в конкретном соответствующем бите event привилегии privilege"""
        return privilege & 1 << bit_number != 0

    async def get_privilege(self, publisher_uid: str, subscriber_uid: str) -> bool:
        """Возвращает привилегию между 2 абонентами"""
        if publisher_uid not in self._pr_cache:
            self._pr_cache[publisher_uid] = TTLCache(maxsize=500, ttl=self.PRIVILEGE_TTL)

        if subscriber_uid not in self._pr_cache[publisher_uid]:
            publisher_key = self.USERSACL_PATTERN.replace('*', publisher_uid)
            publisher_out_pr = await self._redis.pool.hget(key=publisher_key, field=subscriber_uid) or 0
            out_bit = self._check_bit(int(publisher_out_pr), self.OUT_STS)  # type: bool
            if not out_bit:  # отправлять нельзя - дальше можно не проверять
                self._pr_cache[publisher_uid][subscriber_uid] = out_bit
            else:  # проверяем что там на входящих у получателя
                subscriber_key = self.USERSACL_PATTERN.replace('*', subscriber_uid)
                subscriber_in_pr = await self._redis.pool.hget(key=subscriber_key, field=publisher_uid) or 0
                in_bit = self._check_bit(int(subscriber_in_pr), self.IN_STS)
                self._pr_cache[publisher_uid][subscriber_uid] = out_bit and in_bit
        return self._pr_cache[publisher_uid][subscriber_uid]

    async def get_uid_by_name(self, user: str):
        """Возвращает uid по имени"""
        key = pattern_to_key(user, pattern=self.DNAME_PATTERN)
        uid = await self._redis.pool.get(key=key)
        if not uid:
            raise ValueError('В хранилище привилегий отсутствует ключ {}'.format(uid))
        return uid

    async def is_local(self, uid: str) -> bool:
        """Проверяет пользователя на локальность"""
        if uid not in self._is_local_cache:
            is_local = await self._redis.pool.sismember(
                key=self.LOCAL_USERS_PATTERN, member=uid
            )  # type: bool
            self._is_local_cache[uid] = (is_local == 1)
        return self._is_local_cache[uid]

    async def get_local(self) -> Iterable[str]:
        """Возвращает набор всех локальных абонентов"""
        local_users = await self._redis.pool.smembers(key=self.LOCAL_USERS_PATTERN) or set()
        for user in local_users:
            self._is_local_cache[user] = True
        return local_users

    @staticmethod
    async def check_cache_ready(
            host=settings.SERVICE_CACHE_UPDATER_STATE.host,
            port=settings.SERVICE_CACHE_UPDATER_STATE.port
    ) -> STATE:
        """Проверяет готовность кэша (обращается к сервису int-service-cache-updater)"""
        try:
            async with ClientSession() as session:
                async with session.get(
                        'http://{host}:{port}/state'.format(host=host, port=port)
                ) as resp:
                    return STATE(int(await resp.text()))
        except Exception:
            return STATE.not_ready
