import logging
from asyncio import CancelledError, get_event_loop

from aiohttp import web, WSMsgType
from aiohttp.abc import Request
from aioredis import ConnectionClosedError, PoolClosedError

from status_daemon.constants import PUB_PATTERN, LAST_PATTERN
from status_daemon.exceptions import base_exception_handler, call_exception_handler
from status_daemon.messages import Message, MessageParser
from status_daemon.redis.redis import RedisController
from status_daemon.servers.utils import flush_and_publish_statuses, has_subscribers
from status_daemon.utils import pattern_to_key


class SyncReceiver:
    """Приемник статусов сервера синхронизации
    (принимает сообщения от подключившегося сервера)"""

    def __init__(
            self,
            redis: RedisController,
            request: Request,
            suv_name: str
    ):
        self._request = request  # type: Request
        self._redis = redis  # type: RedisController
        self._ws = web.WebSocketResponse(autoping=False)
        self._suv_name = suv_name
        self._suv_last_status_pattern = pattern_to_key(self._suv_name, '*', pattern=LAST_PATTERN)
        self._suv_pub_pattern = pattern_to_key(self._suv_name, '*', pattern=PUB_PATTERN)
        self._loop = get_event_loop()
        self._loop.set_exception_handler(base_exception_handler)

    @property
    def loop(self):
        return self._loop

    @property
    def redis(self):
        return self._redis.pool

    async def __aenter__(self):
        await self._ws.prepare(self._request)
        await self._redis.flush_keys_by_pattern(pattern=self._suv_last_status_pattern)
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        # Обнуляем статусы абонентов с сервера и уведомляем об этом всех, кто подписан
        logging.info('Запущена очистка последних статусов встречных абонентов сервера %s', self._suv_name)
        await flush_and_publish_statuses(
            redis=self.redis,
            last_pattern=self._suv_last_status_pattern,
        )
        logging.info('Окончено прослушивание сообщений сервера %s', self._suv_name)

    async def listen_statuses(self):
        """Прослушивает сообщения от встречного сервера"""
        logging.info('Запущена синхронизация статусов с %s', self._suv_name)
        try:
            while True:
                msg = await self._ws.receive()
                if msg.type != WSMsgType.TEXT:
                    continue

                try:
                    logging.debug(
                        'От сервера %s получено сообщение %s', self._suv_name, msg.data
                    )
                    message_parser = MessageParser(msg.data)
                    if message_parser.is_package:  # если сообщение пакетное
                        message_items = message_parser.package_status_data
                        for message in message_items:
                            self.loop.create_task(self._publish_received_status(message=message))
                    else:  # если сообщение одиночное
                        self.loop.create_task(
                            self._publish_received_status(message=message_parser.single_status_data)
                        )
                except (ConnectionClosedError, PoolClosedError) as e:
                    raise ValueError(
                        'Закрыто соединение с сервером Redis %s: %s' % (self.redis, e)
                    )
                except Exception as e:
                    raise ValueError(
                        'Ошибка при попытке обработать сообщение %s: %s' % (msg.data, e)
                    )
        except CancelledError:
            pass
        except Exception as e:
            raise ValueError('Закрыта синхронизация с сервером %s по причине: %s', self._suv_name, e)

    async def _publish_received_status(
            self, message: Message
    ):
        """
        Отправляет полученный статус в очередь
        """
        try:
            has_subs = await has_subscribers(redis=self.redis, uid=message.uid)
            pipe = self.redis.pipeline()
            if has_subs:
                pipe.publish(
                    channel=pattern_to_key(message.uid, pattern=self._suv_pub_pattern),
                    message=Message.create_message(message)
                )
            pipe.set(
                key=pattern_to_key(message.uid, pattern=self._suv_last_status_pattern),
                value=message.status.value
            )
            await pipe.execute()
        except Exception as e:
            call_exception_handler(
                loop=self.loop,
                message='Ошибка при попытке опубликовать сообщения %s с сервера %s: %s' % (
                    message, self._suv_name, e
                )
            )
        else:
            logging.debug(
                'Успешная публикация сообщения %s с сервера %s',
                message, self._suv_name
            )
