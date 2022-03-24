import asyncio
import logging
from asyncio import Queue, QueueEmpty
from typing import Any, Dict, Iterable, Callable, Optional

from aiohttp import ClientSession, ClientWebSocketResponse, ClientConnectionError
from async_timeout import timeout

from status_daemon import AsyncRunnableMixin, AsyncConnectableMixin
from status_daemon.messages import Message
from status_daemon.servers.status.constants import SYNC_RETRY
from status_daemon.servers.synchronize.sender.utils import default_callback
from status_daemon.utils import async_retry


class RemoteServerObserver(AsyncRunnableMixin, AsyncConnectableMixin):
    """Прослушивает входящую очередь и пересылает на сервер все, что в нее приходит"""

    def __init__(self, host: str, port: Any, name: str, server_name: str, session: ClientSession):

        self._RECONNECT_CALLBACK = default_callback  # type: Callable
        self._RECONNECT_CALLBACK_ARGS = ()  # type: Iterable
        self._RECONNECT_CALLBACK_KWARGS = {}  # type: Dict

        self._host = host
        self._port = port
        self._name = name
        self._server_name = server_name
        self._url = 'http://{}:{}/sync'.format(self._host, self._port)
        self._queue = None  # type: Optional[Queue]
        self._session = session  # type: ClientSession

    @property
    def name(self):
        """Имя сервера, на который отсылаются сообщения"""
        return self._name

    async def put(self, message: Message):
        """Отправляет сообщение в очередь на отправку на сервер"""
        if isinstance(self._queue, Queue):
            await self._queue.put(message)

    async def _listen_queue(self, ws_: ClientWebSocketResponse):
        """Прослушивает очередь"""
        while True:
            try:
                # ждем получения сообщения на отправку
                async with timeout(SYNC_RETRY):
                    try:
                        message = await self._queue.get()
                    except QueueEmpty:
                        continue
                    # отправка сообщения на встречный сервер
                    await self._send_message(ws_, Message.create_message(message))
            except asyncio.TimeoutError:
                # проверяем не потерялась ли связь с сервером
                if ws_._conn.closed:
                    raise ClientConnectionError(
                        'Разрыв связи с сервером %s (%s)' % (self._name, self._url)
                    )

    async def _send_message(self, ws_: ClientWebSocketResponse, str_message: str):
        """Отправляет сообщение на встречный сервер"""
        try:
            await ws_.send_str(data=str_message)
            logging.debug(
                'Сообщение %s успешно передано на сервер %s (%s)',
                str, self._name, self._url
            )
        except Exception as e:
            logging.error(
                'Ошибка при попытке отправить сообщение %s на сервер %s (%s): %s',
                str_message, self._name, self._url, e
            )

    async def listen(self):
        """
        Прослушивает собственную очередь и перенаправляет все данные из нее в ws
        """
        try:
            async with self._session.ws_connect(self._url, headers={'SUV': self._server_name}) as ws_:
                self._queue = Queue()
                try:
                    messages = self._RECONNECT_CALLBACK(
                        *self._RECONNECT_CALLBACK_ARGS,
                        **self._RECONNECT_CALLBACK_KWARGS
                    )
                    async for message in messages:
                        try:
                            await self._send_message(ws_, Message.create_package(message))
                        except Exception as e:
                            logging.error(
                                'Ошибка при пересылке сообщения %s на сервер %s (%s): %s',
                                message, self._name, self._url, e
                            )
                except Exception as e:
                    raise ValueError(
                        'Ошибка при выполнении callback-а %s для получения данных '
                        'при установлении связи с сервером %s (%s): %s' % (
                            self._RECONNECT_CALLBACK.__name__, self._name, self._url, e
                        )
                    )

                logging.info(
                    'Запущено перенаправление статусов на сервер %s (%s)',
                    self._name, self._url
                )
                await self._listen_queue(ws_)
        except Exception as e:
            if isinstance(self._queue, Queue):
                for _ in range(self._queue.qsize()):
                    self._queue.get_nowait()
                    self._queue.task_done()
                self._queue = None
                logging.debug('Очередь для сервера %s очищена', self._name)
            raise ValueError(
                'Отсутствует соединение с сервером %s (%s): %s' %
                (self._name, self._url, e)
            )

    @classmethod
    async def connect(cls, host: str, port: Any, name: str, server_name: str):
        session = ClientSession()
        return cls(
            host=host, port=port, name=name,
            server_name=server_name, session=session
        )

    async def disconnect(self):
        await self._session.close()

    @async_retry()
    async def run(
            self,
            reconnect_callback: Callable,
            *reconnect_callback_args,
            **reconnect_callback_kwargs
    ):
        """Запуск"""
        self._RECONNECT_CALLBACK = reconnect_callback
        self._RECONNECT_CALLBACK_ARGS = reconnect_callback_args
        self._RECONNECT_CALLBACK_KWARGS = reconnect_callback_kwargs
        try:
            await self.listen()
        except Exception as e:
            raise ValueError(
                'Ошибка при перенаправлении статусов на сервер %s (%s): %s' % (
                    self._name, self._url, e
                )
            )
