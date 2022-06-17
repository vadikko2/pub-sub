from enum import Enum
from typing import Optional

from aiohttp import ClientSession
from async_generator import async_generator, yield_
from cachetools import TTLCache

from config import settings
from status_daemon import AsyncConnectableMixin
from status_daemon.exceptions import AvailableException
from status_daemon.redis.redis import RedisController
from status_daemon.servers.status.constants import BATCH_SIZE
from status_daemon.utils import pattern_to_key


class Privileges(AsyncConnectableMixin):
    """Реализует всю работу с привилегиями"""
    IN_STS = 1
    OUT_STS = 6

    USERSACL_PATTERN = "USERSACL@*@"  # паттерн пользовательских ACL в
    # Redis хранилище сервиса привилегий
    DNAME_PATTERN = "DNAME@*@"  # паттерн соответствия доменного имени UID-у
    LOCAL_USERS_PATTERN = "LOCAL_USERS"  # паттерн набора локальных пользователей
    STS_AVAILABLE_PATTERN = "STS@*@"  # паттерн доступа к сервису статусов
    AVAILABLE = 'TRUE'

    PRIVILEGE_TTL = int(settings.PRIVILEGE_CACHE_TTL or 60)

    class STATE(Enum):
        """Готовность привилегий"""
        ready = 1
        not_ready = 0

    def __init__(self, redis: RedisController, pr_cache: TTLCache, is_local_cache: TTLCache):
        self._pr_cache = pr_cache
        self._is_local_cache = is_local_cache
        self._redis = redis  # type: RedisController

    @property
    def redis(self):
        return self._redis.pool

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
            publisher_out_pr = await self.redis.hget(key=publisher_key, field=subscriber_uid) or 0
            out_bit = self._check_bit(int(publisher_out_pr), self.OUT_STS)  # type: bool
            if not out_bit:  # отправлять нельзя - дальше можно не проверять
                self._pr_cache[publisher_uid][subscriber_uid] = out_bit
            else:  # проверяем что там на входящих у получателя
                subscriber_key = self.USERSACL_PATTERN.replace('*', subscriber_uid)
                subscriber_in_pr = await self.redis.hget(key=subscriber_key, field=publisher_uid) or 0
                in_bit = self._check_bit(int(subscriber_in_pr), self.IN_STS)
                self._pr_cache[publisher_uid][subscriber_uid] = out_bit and in_bit
        return self._pr_cache[publisher_uid][subscriber_uid]

    async def get_uid_by_name(self, user: str):
        """Возвращает uid по имени"""
        key = pattern_to_key(user, pattern=self.DNAME_PATTERN)
        uid = await self.redis.get(key=key)
        if not uid:
            raise ValueError('В хранилище привилегий отсутствует информация об абоненте {}'.format(user))
        return uid

    async def is_local(self, uid: str) -> bool:
        """Проверяет пользователя на локальность"""
        if uid not in self._is_local_cache:
            is_local = await self.redis.sismember(
                key=self.LOCAL_USERS_PATTERN, member=uid
            )  # type: int
            self._is_local_cache[uid] = (is_local == 1)
        return self._is_local_cache[uid]

    @async_generator
    async def get_local(self):
        """Возвращает набор всех локальных абонентов"""
        cursor = b'0'
        while cursor:
            cursor, members = await self.redis.sscan(
                cursor=cursor, key=self.LOCAL_USERS_PATTERN, match='*', count=BATCH_SIZE
            )
            if not members: continue
            for user in members:
                self._is_local_cache[user] = True
            await yield_(members)

    @staticmethod
    async def check_cache_ready(
            host=settings.SERVICE_CACHE_UPDATER_STATE.host,
            port=settings.SERVICE_CACHE_UPDATER_STATE.port
    ) -> bool:
        """Проверяет готовность кэша (обращается к сервису int-service-cache-updater)"""
        try:
            async with ClientSession() as session:
                async with session.get(
                        'http://{host}:{port}/state'.format(host=host, port=port)
                ) as resp:
                    return Privileges.STATE(int(await resp.text())) == Privileges.STATE.ready
        except Exception:
            return False

    async def check_available(self, uid: str) -> None:
        """Проверяет доступ абонента к адресной книге"""
        key = pattern_to_key(uid, pattern=Privileges.STS_AVAILABLE_PATTERN)
        value = await self.redis.get(key)
        if value != Privileges.AVAILABLE:
            raise AvailableException('Абонент %s не имеет доступа к сервису статусов' % uid)
