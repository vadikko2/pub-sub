import asyncio
import logging
from asyncio import get_event_loop
from typing import Set, Any

from aioredis import Channel
from async_timeout import timeout

from status_daemon import BlockingRunnableMixin
from status_daemon.constants import (
    WISH_PATTERN, PUB_PATTERN, SUB_PATTERN
)
from status_daemon.exceptions import MessageDecodeException
from status_daemon.messages import MessageParser, Message
from status_daemon.redis.redis import RedisController
from status_daemon.utils import pattern_to_key, extract_info_from_key


class StatusRoutingDaemon(BlockingRunnableMixin):
    """Маршрутизатор статусов"""

    def __init__(self, redis: RedisController):
        self.loop = get_event_loop()
        self._redis = redis  # type: RedisController

    async def _run_daemon(self):
        """
        Перенаправляет сообщения от отправителя ко всем получателя, перечисленным в self.wishlist
        """
        logging.info('Запущено перенаправление статусов')
        channel, = await self._redis.pool.psubscribe(
            pattern=PUB_PATTERN
        )  # type: Channel

        try:
            while True:
                try:
                    async with timeout(1):
                        key, message = await channel.get(encoding='utf-8')
                    try:
                        sender, _ = extract_info_from_key(key=key)  # type: str
                    except MessageDecodeException as e:
                        logging.error('Ошибка при разборе имени очереди %s: %r', key, e)
                        continue

                    try:
                        msg = MessageParser(message=message).single_status_data
                        if msg.status is None:
                            continue
                        if msg.uid is None:
                            msg.uid = sender
                            message = Message.create_message(msg)

                    except Exception as e:
                        logging.error('Ошибка при разборе сообщения %s из очереди %s: %r', message, key, e)
                        continue

                    await self.send_status(
                        sender=sender,
                        message=message
                    )

                except asyncio.TimeoutError:
                    pass
        except Exception as e:
            raise ValueError(
                'Ошибка при маршрутизации %r: %r' % (
                    self._redis, e
                )
            )
        finally:
            if not self._redis.pool.closed:
                await self._redis.pool.punsubscribe(pattern=PUB_PATTERN)
            raise ValueError(
                'Прослушивание %s остановлено.' % channel.name.decode(encoding='utf-8')
            )

    async def send_status(self, sender: str, message: Any):
        """Рассылает статусы"""

        wishlist = set(await self.load_wishlists(sender=sender) or [])

        for receiver in wishlist:
            pattern = pattern_to_key(
                receiver,
                pattern=SUB_PATTERN
            )

            try:
                await self._redis.pool.publish(pattern, message)
                logging.debug('Сообщение %s отправлено в очередь %s', message, pattern)
            except Exception as e:
                logging.error(
                    'Ошибка при публикации сообщения %s в очередь %s: %s',
                    message, pattern, e
                )

    async def load_wishlists(self, sender: str) -> Set[str]:
        """Выгружает wishlist из Redis"""
        key = pattern_to_key(sender, pattern=WISH_PATTERN)
        wishlist = await self._redis.pool.smembers(key)
        result = set()
        if wishlist:
            result.update(wishlist)
        return result

    @classmethod
    def connect(cls):
        """Возвращает инстанс StatusRoutingDaemon, подключенный к Redis
        (базовому и сервиса привилегий)"""
        loop = get_event_loop()
        redis_controller = loop.run_until_complete(RedisController.connect())
        return cls(redis=redis_controller)

    def disconnect(self):
        """Отключает от Redis"""
        self.loop.run_until_complete(self._redis.disconnect())

    def run(self):
        """Запуск"""
        try:
            task = self.loop.create_task(self._run_daemon())
            self.loop.run_until_complete(task)
        except KeyboardInterrupt:
            logging.info('Демон остановлен')
        except Exception as e:
            logging.error('Демон завершил свою работу с ошибкой %r', e)
