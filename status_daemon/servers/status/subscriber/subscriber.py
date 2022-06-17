import asyncio
from typing import Optional

from aioredis import Channel, Redis
from async_timeout import timeout

from status_daemon import AsyncRunnableMixin, Logger
from status_daemon.exceptions import MessageDecodeException
from status_daemon.messages import MessageParser, Message
from status_daemon.privileges.privileges import Privileges
from status_daemon.redis.redis import RedisController
from status_daemon.servers.status.publisher import Publisher
from status_daemon.status_daemon.constants import Status
from status_daemon.utils import pattern_to_key


class Subscriber(AsyncRunnableMixin):
    """Подписчик на получение статусов"""

    def __init__(
            self,
            publisher: Publisher,
            privileges: Privileges,
            redis: Optional[Redis] = None
    ):
        self._publisher = publisher
        self._privileges = privileges

        if isinstance(redis, Redis) and not redis.closed:
            self._redis = redis
        elif isinstance(self._publisher.redis, Redis) and not self._publisher.redis.closed:
            self._redis = self._publisher.redis
        else:
            raise ValueError('Can`t connect to Redis %s' % redis)

        self._channel_name = pattern_to_key(self._publisher.user.uid, pattern=RedisController.SUB_PATTERN)
        self._channel = None  # type: Optional[Channel]

    async def __aenter__(self):
        self._channel, = await self.redis.subscribe(self._channel_name)  # type: Channel
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if not self.redis.closed:
            await self.redis.unsubscribe(self._channel_name)

    @property
    def redis(self):
        return self._redis

    async def run(self):
        """Прослушивает приходящие статусы, которые необходимо отправить пользователю по WS"""
        Logger.info('Запущено прослушивание статусов абонентом %r', self._publisher.user)
        while True:
            try:
                async with timeout(1):
                    msg = await self._channel.get(encoding='utf-8')
                try:
                    message = MessageParser(message=msg).single_status_data
                except MessageDecodeException as e:
                    Logger.error('Ошибка парсинга сообщения %s: %s', msg, e)
                    continue

                if isinstance(message.status, Status):
                    pr_ = await self._privileges.get_privilege(
                        publisher_uid=message.uid,
                        subscriber_uid=self._publisher.user.uid
                    )
                    if not pr_:
                        message.status = Status.UNKNOWN

                    msg_ = Message.create_message(message=message)
                    self._publisher.loop.create_task(
                        self._publisher.send_message(message=msg_)
                    )
                else:
                    Logger.warning('В сообщении %s отсутствует статус', message)

            except asyncio.TimeoutError:
                pass
            except Exception as e:
                raise ValueError(
                    'Ошибка при прослушивании сообщений пользователем %r: %s' % (
                        self._publisher.user, e
                    )
                )
