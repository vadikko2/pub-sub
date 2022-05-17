import logging
from asyncio import get_event_loop
from typing import Set, Optional, Iterable

from aiohttp import WSMsgType, web

from status_daemon import AsyncRunnableMixin
from status_daemon.constants import (
    WISH_PATTERN, LOCAL_PUB_PATTERN,
    LOCAL_LAST_PATTERN, LAST_PATTERN
)
from status_daemon.exceptions import call_exception_handler, base_exception_handler
from status_daemon.messages import MessageParser, Message
from status_daemon.privileges.privileges import Privileges
from status_daemon.redis.redis import RedisController
from status_daemon.servers.status.constants import BATCH_SIZE
from status_daemon.servers.status.publisher.utils import catch_subscribe_events
from status_daemon.servers.status.users import User
from status_daemon.servers.status.utils import divide_to_equal_batches
from status_daemon.servers.utils import has_subscribers
from status_daemon.status_daemon.constants import Status, Operation
from status_daemon.utils import extract_uid_from_key, pattern_to_key


class Publisher(AsyncRunnableMixin):
    """Публикант статусов"""

    def __init__(
            self,
            user: User,
            privileges: Privileges,
            redis: RedisController
    ):
        self._redis = redis  # type: RedisController
        self._ws = web.WebSocketResponse(autoping=False)
        self._privileges = privileges  # type: Privileges
        self._user = user  # type: User

        self._pub_channel_name = pattern_to_key(self.user.uid, pattern=LOCAL_PUB_PATTERN)

        self._loop = get_event_loop()
        self._loop.set_exception_handler(base_exception_handler)

    async def __aenter__(self):
        await self._ws.prepare(self.user.request)
        await self._clear_user_subscriptions()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self._close_connection_callback()
        if not self._ws.closed:
            await self._ws.close()
        logging.info('Окончено прослушивание сообщений от пользователя %r', self.user)

    @property
    def user(self):
        return self._user

    @property
    def redis(self):
        return self._redis.pool

    @property
    def loop(self):
        return self._loop

    async def _publish(
            self, message: Message
    ) -> None:
        """Публикация сообщения из ws в redis."""
        try:
            if message.status:
                msg = Message.create_message(message=message)
                has_subs = await has_subscribers(redis=self.redis, uid=self.user.uid)
                pipe = self.redis.pipeline()
                if has_subs:
                    pipe.publish(channel=self._pub_channel_name, message=msg)
                pipe.set(
                    key=pattern_to_key(self.user.uid, pattern=LOCAL_LAST_PATTERN),
                    value=message.status.value
                )
                await pipe.execute()
        except Exception as e:
            raise ValueError(
                'Ошибка при отправке сообщения %r пользователя %r в Redis %r: %s' % (
                    message, self.user, self.redis, e
                )
            )

    async def _close_connection_callback(self) -> None:
        """Callback, выполняющийся при закрытии соединения клиентом сервиса статусов"""
        try:
            status = Status.NOT_REGISTERED  # type: Status

            # Получаем последний статус абонента
            last_status_key = await self._get_last_status_key(uid=self.user.uid)

            if last_status_key is None:
                last_status = Status.NOT_REGISTERED.value
            else:
                last_status = int(
                    await self.redis.get(key=last_status_key) or Status.NOT_REGISTERED.value
                )

            # если статус не ПЕРЕАДРЕСАЦИЯ
            if last_status and (Status(last_status) == Status.FORWARDING):
                # Публикуем статус NOT_REGISTERED при отключении абонента
                status = Status.FORWARDING

            # отсылаем последний статус подписантам
            await self._publish(message=Message(status=status))
            await self._clear_user_subscriptions()
            logging.info('Остановлено прослушивание сообщений от %r', self.user)
        except Exception as e:
            logging.error(
                'Некорректное завершение закрытия соединения с абонентом %r: %s',
                self.user, e
            )

    async def _clear_user_subscriptions(self) -> None:
        """Отписываем отключенного абонента от всех"""
        cursor = b'0'
        try:
            while cursor:
                cursor, wish_keys = await self.redis.scan(cursor=cursor, match=WISH_PATTERN)
                if wish_keys:
                    wish_pipe = self.redis.pipeline()
                    for key in wish_keys:
                        try:
                            wish_pipe.srem(key=key, member=self.user.uid)
                        except Exception as e:
                            logging.error(
                                'Ошибка при попытке удалить абонента %r из подписки %s: %s',
                                self.user, key, e
                            )
                    self.loop.create_task(wish_pipe.execute())
        except Exception as e:
            logging.error(
                'Ошибка при очистке подписок пользователя %r: %s',
                self.user, e
            )
        else:
            logging.info('Подписки абонента %r очищены', self.user)

    async def run(self):
        """Прослушивает WS на предмет сообщений от клиента"""
        logging.info(
            'Запущено прослушивание сообщений от пользователя %r', self.user
        )
        async for msg in self._ws:  # type: WSMsgType
            if msg.type != WSMsgType.TEXT:
                continue
            if msg.data == WSMsgType.TEXT.closed:
                await self._ws.close()
                break

            self.loop.create_task(self._do(msg=msg))

    async def _do(self, msg: WSMsgType):
        """Отрабатывает полученное сообщение"""
        try:
            message = MessageParser(msg.data).single_status_data
            if message.status:
                await self._publish(message)

        except Exception as e:
            call_exception_handler(
                loop=self.loop,
                message='Ошибка при публикации сообщения %s в redis %r: %s' % (msg.data, self._redis, e)
            )
            return

        else:
            logging.debug(
                'Пользователь %r опубликовал новое сообщение %s', self.user, msg.data
            )

        if message.events:
            # ловим сообщения SUBSCRIBE
            subscribe_events_uids = catch_subscribe_events(message.events)  # type: Set[str]

            try:
                await self._send_last_statuses(  # отправляем последние статусы
                    uids=subscribe_events_uids,
                )
                await self._update_wishlist(  # Обновляем wishlist
                    message=message
                )
            except Exception as e:
                call_exception_handler(
                    loop=self.loop,
                    message='Ошибка при попытке осуществить подписку %s абонентом %r: %s' % (
                        message.events, self.user, e
                    )
                )

    async def _update_wishlist(self, message: Message):
        """Обновляет wishlist"""

        subscribe_list = {
            uuid for uuid, event in message.events.items() if event == Operation.SUBSCRIBE
        }  # type: Set[str]
        unsubscribe_list = {
            uuid for uuid, event in message.events.items() if event == Operation.UNSUBSCRIBE
        }  # type: Set[str]

        unsubscribe_pipe = self.redis.pipeline()
        for publisher in unsubscribe_list:
            wishlist_key = pattern_to_key(publisher, pattern=WISH_PATTERN)

            # Пытаемся удалить запись из таблицы wishlist
            try:
                unsubscribe_pipe.srem(wishlist_key, self.user.uid)
            except Exception as e:
                logging.error(
                    'Ошибка при попытке удаления записи %s из %s: %s',
                    self.user.uid, wishlist_key, e
                )
            else:
                logging.debug('Из %s удалена запись %s', wishlist_key, self.user.uid)
        self.loop.create_task(unsubscribe_pipe.execute())

        subscribe_pipe = self.redis.pipeline()
        for publisher in subscribe_list:
            wishlist_key = pattern_to_key(publisher, pattern=WISH_PATTERN)

            # пытаемся добавить новую запись в wishlist
            try:
                subscribe_pipe.sadd(wishlist_key, self.user.uid)
            except Exception as e:
                raise ValueError(
                    'Ошибка при добавлении %s в %s: %s' % (
                        self.user.uid, wishlist_key, e
                    )
                )
            else:
                logging.debug('В %s добавлена завись %s.', wishlist_key, self.user.uid)
        self.loop.create_task(subscribe_pipe.execute())

    async def _send_last_statuses(
            self, uids: Iterable[str]
    ):
        """Отправляет актуальные статусы для абонентов, на которые произошла подписка"""

        statuses_list = set()  # type: Set[Message]

        last_statuses_keys = set()  # type: Set[str] # набор uid-ов для которых необходимо будет запросить статусы
        not_registered_uids = set()  # набор uid-ов, чьи статусы не были опубликованы

        try:
            keys = await self._get_last_status_keys(uids)
            for uid, key in zip(uids, keys):
                if key is None:
                    not_registered_uids.add(uid)
                else:
                    last_statuses_keys.add(key)

            if last_statuses_keys:
                statuses = await self.redis.mget(*last_statuses_keys)
                for key, status in zip(last_statuses_keys, statuses):
                    uid = extract_uid_from_key(key=key, index=2)
                    pr_ = await self._privileges.get_privilege(
                        uid, self.user.uid
                    )  # type: bool
                    try:
                        if pr_:  # если привилегия есть - отправляем последний статус
                            status = Status(int(status))
                        else:
                            status = Status.UNKNOWN
                        statuses_list.add(Message(status=status, uid=uid))
                    except Exception as e:
                        logging.error(
                            'Ошибка формирования последнего статуса %s абонента %s: %s', status, uid, e
                        )
                        statuses_list.add(Message(status=Status.UNKNOWN, uid=uid))

            if not_registered_uids:
                for uid in not_registered_uids:
                    is_local = await self._privileges.is_local(uid=uid)  # type: bool
                    # если пользователь локальный - NOT_REGISTERED, если нет - UNKNOWN
                    status = Status.UNKNOWN
                    if is_local:
                        pr_ = await self._privileges.get_privilege(
                            uid, self.user.uid
                        )  # type: bool
                        if pr_:
                            status = Status.NOT_REGISTERED
                    statuses_list.add(Message(status=status, uid=uid))
        except Exception as e:
            raise ValueError(
                'Ошибка при попытке выгрузить последние статусы абонентов по ключам %s: %s' % (
                    last_statuses_keys, e
                )
            )
        self.send_by_batches(data=statuses_list)

    def send_by_batches(
            self, data: Iterable[Message],
    ) -> None:
        """Отправляет данные по WS батчами"""
        batches = divide_to_equal_batches(data, BATCH_SIZE)

        try:
            for batch in batches:
                self.loop.create_task(
                    self.send_message(message=Message.create_package(messages=batch))
                )
        except Exception as e:
            raise ValueError(
                'Ошибка при пакетной отправке абоненту %r пакета %r: %s' %
                (self.user, batches, e)
            )

    async def send_message(self, message: str) -> None:
        """Отправляет сообщение"""
        try:
            await self._ws.send_str(message)
        except Exception as e:
            call_exception_handler(
                loop=self.loop,
                message='Ошибка при попытке отправить пользователю %r сообщение %s: %s' %
                        (self.user, message, e)
            )
        else:
            logging.debug(
                'Пользователю %r отправлено сообщение %s',
                self.user, message
            )

    async def _get_last_status_key(self, uid: str) -> Optional[str]:
        try:
            last_status_pattern = pattern_to_key('*', uid, pattern=LAST_PATTERN)
            keys = await self.redis.keys(pattern=last_status_pattern)
            if not keys:
                return None
            return keys[0]
        except Exception as e:
            raise ValueError(
                'Ошибка получения ключа последнего '
                'статуса пользователя %s: %s' % (uid, e)
            )

    async def _get_last_status_keys(self, uids: Iterable[str]) -> Iterable[Optional[str]]:
        keys_pipe = self.redis.pipeline()
        for uid in uids:
            last_status_pattern = pattern_to_key('*', uid, pattern=LAST_PATTERN)
            keys_pipe.keys(pattern=last_status_pattern)
        keys_candidates = await keys_pipe.execute()

        result = []
        for item in keys_candidates:
            if not item:
                result.append(None)
            else:
                result.append(item[0])
        return result
