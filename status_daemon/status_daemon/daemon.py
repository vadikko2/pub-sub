import asyncio
from asyncio import get_event_loop
from typing import Any

from aioredis import Channel
from async_generator import async_generator, yield_
from async_timeout import timeout

from status_daemon import BlockingRunnableMixin, Logger
from status_daemon.exceptions import MessageDecodeException, base_exception_handler, call_exception_handler
from status_daemon.messages import MessageParser, Message
from status_daemon.redis.redis import RedisController
from status_daemon.utils import pattern_to_key, extract_info_from_key


class StatusRoutingDaemon(BlockingRunnableMixin):
    """Маршрутизатор статусов"""

    def __init__(self, redis: RedisController):
        self.loop = get_event_loop()
        self.loop.set_exception_handler(base_exception_handler)
        self._redis = redis  # type: RedisController

    @property
    def redis(self):
        return self._redis.pool

    async def _run_daemon(self):
        """
        Перенаправляет сообщения от отправителя ко всем получателя, перечисленным в self.wishlist
        """
        Logger.info('Запущено перенаправление статусов')
        channel, = await self.redis.psubscribe(
            pattern=RedisController.PUB_PATTERN
        )  # type: Channel

        try:
            while True:
                try:
                    async with timeout(1):
                        key, message = await channel.get(encoding='utf-8')
                    try:
                        sender, _ = extract_info_from_key(key=key)  # type: str
                    except MessageDecodeException as e:
                        Logger.error('Ошибка при разборе имени очереди %s: %r', key, e)
                        continue

                    try:
                        msg = MessageParser(message=message).single_status_data
                        if msg.status is None:
                            continue
                        if msg.uid is None:
                            msg.uid = sender
                            message = Message.create_message(msg)

                    except Exception as e:
                        Logger.error('Ошибка при разборе сообщения %s из очереди %s: %r', message, key, e)
                        continue

                    self.loop.create_task(self.send_status(
                        sender=sender,
                        message=message
                    ))

                except asyncio.TimeoutError:
                    pass
        except Exception as e:
            call_exception_handler(
                loop=self.loop,
                message='Ошибка при маршрутизации %r: %r' % (
                    self._redis, e
                )
            )
            return
        finally:
            if not self.redis.closed:
                await self.redis.punsubscribe(pattern=RedisController.PUB_PATTERN)
            call_exception_handler(
                loop=self.loop,
                message='Прослушивание %s остановлено.' % channel.name.decode(encoding='utf-8')
            )

    async def send_status(self, sender: str, message: Any):
        """Рассылает статусы"""

        wishlist_generator = self.load_wishlists(sender=sender)

        async for receiver in wishlist_generator:
            pattern = pattern_to_key(
                receiver,
                pattern=RedisController.SUB_PATTERN
            )

            self.loop.create_task(self.send_to_receiver(pattern=pattern, message=message))

    async def send_to_receiver(self, pattern: str, message: Any):
        try:
            await self.redis.publish(pattern, message)
        except Exception as e:
            call_exception_handler(
                loop=self.loop,
                message='Ошибка при публикации сообщения %s в очередь %s: %s' % (message, pattern, e)
            )
        else:
            Logger.debug('Сообщение %s отправлено в очередь %s', message, pattern)

    @async_generator
    async def load_wishlists(self, sender: str):
        """Выгружает wishlist из Redis"""
        key = pattern_to_key(sender, pattern=RedisController.WISH_PATTERN)
        cursor = b'0'

        while cursor:
            cursor, wishlist = await self.redis.sscan(key=key, cursor=cursor, match='*')

            for wish in wishlist:
                await yield_(wish)

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
            Logger.info('Демон остановлен')
        except Exception as e:
            Logger.error('Демон завершил свою работу с ошибкой %r', e)
