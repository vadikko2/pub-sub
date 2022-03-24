import asyncio
import logging
from typing import Optional

from aioredis import Channel
from async_timeout import timeout

from status_daemon import AsyncRunnableMixin
from status_daemon.constants import SUB_PATTERN
from status_daemon.exceptions import MessageDecodeException
from status_daemon.messages import MessageParser, Message
from status_daemon.privileges.privileges import Privileges
from status_daemon.redis.redis import RedisController
from status_daemon.servers.status.publihser import Publisher
from status_daemon.status_daemon.constants import Status
from status_daemon.utils import pattern_to_key


class Subscriber(AsyncRunnableMixin):
    """Подписчик на получение статусов"""

    def __init__(
            self,
            publisher: Publisher,
            privileges: Privileges,
            redis: Optional[RedisController] = None
    ):
        self._publisher = publisher
        self._privileges = privileges

        if isinstance(redis, RedisController) and not redis.pool.closed:
            self._redis = redis
        elif isinstance(self._publisher.redis, RedisController) and not self._publisher.redis.pool.closed:
            self._redis = self._publisher.redis
        else:
            raise ValueError('Can`t connect to Redis %s' % redis)

        self._channel_name = pattern_to_key(self._publisher.user.uid, pattern=SUB_PATTERN)
        self._channel = None  # type: Optional[Channel]

    async def __aenter__(self):
        self._channel, = await self._redis.pool.subscribe(self._channel_name)  # type: Channel
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if not self.redis.pool.closed:
            await self.redis.pool.unsubscribe(self._channel_name)

    @property
    def redis(self):
        return self._redis

    async def run(self):
        """Прослушивает приходящие статусы, которые необходимо отправить пользователю по WS"""
        logging.info('Запущено прослушивание статусов абонентом %r', self._publisher.user)
        while True:
            try:
                async with timeout(1):
                    msg = await self._channel.get(encoding='utf-8')
                try:
                    message = MessageParser(message=msg).single_status_data
                except MessageDecodeException as e:
                    logging.error('Ошибка парсинга сообщения %s: %s', msg, e)
                    continue

                if isinstance(message.status, Status):
                    pr_ = await self._privileges.get_privilege(
                        publisher_uid=message.uid,
                        subscriber_uid=self._publisher.user.uid
                    )
                    if not pr_:
                        message.status = Status.UNKNOWN

                    msg_ = Message.create_message(message=message)
                    await self._publisher.send_message(message=msg_)
                else:
                    logging.warning('В сообщении %s отсутствует статус', message)

            except asyncio.TimeoutError:
                pass
            except Exception as e:
                logging.error(
                    'Ошибка при прослушивании сообщений пользователем %r: %s' % (
                        self._publisher.user, e
                    )
                )
                raise e
