import asyncio
import logging
from asyncio import gather, get_event_loop, sleep
from time import sleep as block_sleep
from typing import Iterable, Set

from aioredis import Channel, Redis
from async_generator import async_generator, yield_, asynccontextmanager
from async_timeout import timeout

from config import settings
from status_daemon import BlockingConnectableMixin, BlockingRunnableMixin
from status_daemon.constants import (
    LOCAL_LAST_PATTERN, DAEMON_KEYSPACE, STATE
)
from status_daemon.exceptions import base_exception_handler
from status_daemon.messages import Message
from status_daemon.privileges.privileges import Privileges
from status_daemon.redis.redis import RedisController
from status_daemon.servers.synchronize.sender.constants import GET_HOSTS_GQL_QUERY, GET_SERVER_NAME
from status_daemon.servers.synchronize.sender.observer import RemoteServerObserver
from status_daemon.servers.synchronize.sender.utils import (
    gql_request, extract_hosts, extract_host_name, merge_async_iters
)
from status_daemon.status_daemon.constants import Status
from status_daemon.utils import extract_uid_from_key, pattern_to_key, parse_keyspace


class SyncStatusSenderDaemon(BlockingConnectableMixin, BlockingRunnableMixin):
    """Отправитель статусов локальных абонентов на встречные сервера"""

    def __init__(
            self, redis: RedisController, privileges: Privileges,
            server_name: str, *trusted_host: str
    ):
        self._redis = redis  # type: RedisController
        self._loop = get_event_loop()
        self._loop.set_exception_handler(base_exception_handler)
        self._servers = ()  # type: Iterable[RemoteServerObserver]
        self._server_name = server_name  # type: str
        self._privileges = privileges
        self._hosts = set(trusted_host)
        self._servers = set()  # type: Set[RemoteServerObserver]

    @property
    def loop(self):
        return self._loop

    @property
    def redis(self):
        return self._redis.pool

    async def send_to_servers(self, message: Message):
        """Прослушивает свою очередь """
        for server in self._servers:
            self.loop.create_task(server.put(message=message))

    @staticmethod
    @async_generator
    async def reconnect_callback(redis: Redis, privileges: Privileges):
        """Callback запрашивает все последние статусы локальных
        пользователей и отдает их пакетами"""

        cache_status = await privileges.check_cache_ready()  # type: STATE
        while not cache_status == STATE.ready:
            logging.info('Кэш привилегий не готов. Ожидание ...')
            await sleep(5)
            cache_status = await privileges.check_cache_ready()  # type: STATE

        # получаем список локальных абонентов
        local_users_generator = privileges.get_local()

        async for batch in local_users_generator:
            try:
                keys = [pattern_to_key(uid, pattern=LOCAL_LAST_PATTERN) for uid in batch]
                values = await redis.mget(*keys)
                statuses = set()
                for status, uid in zip(values, batch):
                    try:
                        if status is None:
                            # если статус еще не был опубликован
                            status = Status.NOT_REGISTERED
                        else:
                            status = Status(int(status))
                        statuses.add(Message(status=status, uid=uid))
                    except Exception as e:
                        logging.error('Ошибка обработки статуса %s пользователя %s', status, e)
                        statuses.add(Message(status=Status.UNKNOWN, uid=uid))
            except Exception as e:
                raise ValueError(
                    'Ошибка при попытке получения значений последних статусов %s' % e
                )

            await yield_(statuses)

    async def _run_observer(self, server: RemoteServerObserver):
        """Запускает обсервера встречного сервера"""
        try:
            await server.run(
                reconnect_callback=SyncStatusSenderDaemon.reconnect_callback,
                **dict(redis=self.redis, privileges=self._privileges)
            )
        except Exception as e:
            raise ValueError(
                'Observer сервера %s завершил работу с ошибкой %s. '
                'Повторный запуск...' % (server.name, e)
            )

    async def _run_sending(self):
        if self._servers:
            tasks = gather(*(
                self.loop.create_task(self._run_observer(server=server)) for server in self._servers
            ))

            try:
                await tasks
            finally:
                for task in tasks:
                    if not task.cancelled:
                        task.cancel()
                raise ValueError('Нет активных задач по перенаправлению статусов')

    @staticmethod
    @asynccontextmanager
    @async_generator
    async def get_channel(redis: Redis, pattern: str):
        channel, = await redis.psubscribe(
            pattern=pattern
        )  # type: Channel
        try:
            await yield_(channel)
        finally:
            if not redis.closed:
                await redis.punsubscribe(pattern=pattern)
            logging.info('Прослушивание %s остановлено.', channel.name.decode(encoding='utf-8'))

    @async_generator
    async def last_status_updates(self):
        """Прослушивает изменение локальных статусов"""
        pattern = pattern_to_key(self.redis.db, LOCAL_LAST_PATTERN, pattern=DAEMON_KEYSPACE)
        async with self.get_channel(redis=self.redis, pattern=pattern) as channel:
            while True:
                try:
                    async with timeout(1):
                        keyspace, _ = await channel.get(encoding='utf-8')

                    key = parse_keyspace(keyspace=keyspace)
                    value = int(await self.redis.get(key) or 8)
                    uid = extract_uid_from_key(key, index=2)
                    try:
                        message = Message(
                            uid=uid,
                            status=Status(value)
                        )
                    except Exception as e:
                        logging.error('Ошибка при попытке разбора значения %s по ключу %s: %s', value, key, e)
                        message = Message(uid=uid, status=Status.UNKNOWN)
                except asyncio.TimeoutError:
                    pass
                else:
                    await yield_(message)

    @async_generator
    async def local_users_updates(self):
        """Прослушивает появление новых локальных абонентов"""
        pattern = pattern_to_key(
            self._privileges.redis.db, self._privileges.LOCAL_USERS_PATTERN,
            pattern=DAEMON_KEYSPACE
        )
        local_uids = set()
        async for members in self._privileges.get_local():
            local_uids.update(members)

        async with self.get_channel(redis=self._privileges.redis, pattern=pattern) as channel:  # type: Channel
            while True:
                try:
                    async with timeout(1):
                        _, operation = await channel.get(encoding='utf-8')

                    if operation == 'sadd':
                        async for members in self._privileges.get_local():
                            new_uids = set(members) - local_uids
                            for uid in new_uids:
                                try:
                                    local_uids.add(uid)
                                    await yield_(Message(uid=uid, status=Status.NOT_REGISTERED))
                                except Exception as e:
                                    logging.error(
                                        'Ошибка при формировании сообщения для нового абонента %s: %s', uid, e
                                    )

                except asyncio.TimeoutError:
                    pass
                except Exception as e:
                    raise ValueError(
                        'Ошибка при прослушивании изменений списка локальных пользователей %s' % e
                    )

    async def _listen_statuses(self):
        logging.info('Запущено прослушивание изменения статусов локальных абонентов.')

        try:
            async for message in merge_async_iters(
                    self.local_users_updates(), self.last_status_updates(), loop=self.loop
            ):
                self.loop.create_task(self.send_to_servers(message))
        except Exception as e:
            raise ValueError('Ошибка при прослушивании статусов %s', e)

    @classmethod
    def connect(cls):
        """Возвращает инстанс SyncStatusSenderDaemon,
        подключенный к Redis (локальному и сервиса привилегий)"""
        loop = get_event_loop()
        redis = loop.run_until_complete(RedisController.connect())
        configs_url = 'http://{host}:{port}/{url}'.format(
            **settings.SYNCHRONIZE.configs
        )

        retry = int(settings.RETRY or 10)
        while True:
            trusted_host_set = loop.run_until_complete(gql_request(
                url=configs_url,
                request_body=GET_HOSTS_GQL_QUERY,
                callback=extract_hosts
            ))  # type: Set[str]
            if not len(trusted_host_set):
                logging.warning(
                    'Не получено ни одного встречного сервера от сервиса конфигов. '
                    'Повторная попытка через %s секунд...',
                    retry
                )
                block_sleep(retry)
            else:
                break

        server_name = loop.run_until_complete(gql_request(
            url=configs_url,
            request_body=GET_SERVER_NAME,
            callback=extract_host_name
        ))  # type: str

        privileges = loop.run_until_complete(Privileges.connect())
        return cls(redis, privileges, server_name, *trusted_host_set)

    def disconnect(self):
        """Отключает от Redis (всех)"""
        self.loop.run_until_complete(self._redis.disconnect())
        self.loop.run_until_complete(self._privileges.disconnect())
        for server in self._servers:
            self.loop.run_until_complete(server.disconnect())

    def run(self):
        """Запуск"""

        self._servers = [
            self.loop.run_until_complete(
                RemoteServerObserver.connect(
                    server_name=self._server_name,
                    host=host,
                    port=settings.SYNCHRONIZE.port,
                    name=host
                )
            ) for host in self._hosts
        ]

        tasks = gather(*(
            self._listen_statuses(),
            self._run_sending()
        ))
        self.loop.run_until_complete(tasks)
