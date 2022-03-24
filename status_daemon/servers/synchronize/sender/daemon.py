import asyncio
import logging
from asyncio import gather, get_event_loop, sleep
from typing import Iterable, Set

from aioredis import Channel
from async_generator import async_generator, yield_
from async_timeout import timeout

from config import settings
from status_daemon import BlockingConnectableMixin, BlockingRunnableMixin
from status_daemon.constants import (
    LOCAL_LAST_PATTERN, LOCAL_PUB_PATTERN,
    DAEMON_KEYSPACE, STATE
)
from status_daemon.messages import Message
from status_daemon.privileges.privileges import Privileges
from status_daemon.redis.redis import RedisController
from status_daemon.servers.status.constants import BATCH_SIZE
from status_daemon.servers.status.utils import divide_to_equal_batches
from status_daemon.servers.synchronize.sender.constants import GET_HOSTS_GQL_QUERY, GET_SERVER_NAME
from status_daemon.servers.synchronize.sender.observer import RemoteServerObserver
from status_daemon.servers.synchronize.sender.utils import gql_request, extract_hosts, extract_host_name
from status_daemon.status_daemon.constants import Status
from status_daemon.utils import extract_uid_from_key, pattern_to_key, parse_keyspace


class SyncStatusSenderDaemon(BlockingConnectableMixin, BlockingRunnableMixin):
    """Отправитель статусов локальных абонентов на встречные сервера"""

    def __init__(
            self, redis: RedisController, privileges: Privileges,
            server_name: str, *trusted_host: str
    ):
        self._redis = redis  # type: RedisController
        self.loop = get_event_loop()
        self._servers = ()  # type: Iterable[RemoteServerObserver]
        self._server_name = server_name  # type: str
        self._privileges = privileges
        self._hosts = set(trusted_host)
        self._servers = set()  # type: Set[RemoteServerObserver]

    async def send_to_servers(self, message: Message):
        """Прослушивает свою очередь """
        for server in self._servers:
            await server.put(message=message)

    @staticmethod
    @async_generator
    async def reconnect_callback(redis: RedisController, privileges: Privileges):
        """Callback запрашивает все последние статусы локальных
        пользователей и отдает их пакетами"""

        cache_status = await privileges.check_cache_ready()  # type: STATE
        while not cache_status == STATE.ready:
            logging.info('Кэш привилегий не готов. Ожидание ...')
            await sleep(5)
            cache_status = await privileges.check_cache_ready()  # type: STATE

        # получаем список локальных абонентов
        local_users = await privileges.get_local()  # type: Iterable[str]
        batches = divide_to_equal_batches(local_users, BATCH_SIZE)

        for batch in batches:
            try:
                keys = [pattern_to_key(uid, pattern=LOCAL_LAST_PATTERN) for uid in batch]
                values = await redis.pool.mget(*keys)
                statuses = set()
                for status, uid in zip(values, batch):
                    if status is None:
                        # если статус еще не был опубликован
                        status = Status.NOT_REGISTERED
                    else:
                        status = Status(int(status))
                    statuses.add(Message(status=status, uid=uid))
            except Exception as e:
                raise ValueError(
                    'Ошибка при попытке получения значений последних статусов %s' % e
                )

            await yield_(statuses)

    async def _run_observer(self, server: RemoteServerObserver):
        """Запускает обсервера встречного сервера"""
        try:
            await  server.run(
                reconnect_callback=SyncStatusSenderDaemon.reconnect_callback,
                **dict(redis=self._redis, privileges=self._privileges)
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

    async def _listen_statuses(self):
        logging.info('Запущено прослушивание изменения статусов локальных абонентов.')
        channel, = await self._redis.pool.psubscribe(
            pattern=pattern_to_key(self._redis.pool.db, LOCAL_LAST_PATTERN, pattern=DAEMON_KEYSPACE)
        )  # type: Channel

        try:
            while True:
                try:
                    async with timeout(1):
                        keyspace, _ = await channel.get(encoding='utf-8')
                    key = parse_keyspace(keyspace=keyspace)
                    value = int(await self._redis.pool.get(key) or 8)

                    message = Message(
                        uid=extract_uid_from_key(key, index=2),
                        status=Status(value)
                    )
                    await self.send_to_servers(message)
                except asyncio.TimeoutError:
                    pass
                except Exception as e:
                    raise ValueError(e)
        except Exception as e:
            logging.error('Ошибка при прослушивании статусов %s', e)
        finally:
            if not self._redis.pool.closed:
                await self._redis.pool.punsubscribe(pattern=LOCAL_PUB_PATTERN)
        logging.info('Прослушивание %s остановлено.', channel.name.decode(encoding='utf-8'))

    @classmethod
    def connect(cls):
        """Возвращает инстанс SyncStatusSenderDaemon,
        подключенный к Redis (локальному и сервиса привилегий)"""
        loop = get_event_loop()
        redis = loop.run_until_complete(RedisController.connect())
        configs_url = 'http://{host}:{port}/{url}'.format(
            **settings.SYNCHRONIZE.configs
        )
        trusted_host_set = loop.run_until_complete(gql_request(
            url=configs_url,
            request_body=GET_HOSTS_GQL_QUERY,
            callback=extract_hosts
        ))  # type: Set[str]

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
