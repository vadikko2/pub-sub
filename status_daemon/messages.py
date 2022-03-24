import logging
from typing import Optional, Dict, Set

from ujson import loads, dumps

from status_daemon.exceptions import MessageDecodeException
from status_daemon.status_daemon.constants import (
    Operation, Status, MessageAttributes
)


class Message:

    def __init__(
            self, status: Optional[Status] = None, timestamp: Optional[str] = None,
            uid: Optional[str] = None, events: Dict[str, Operation] = None
    ):
        self._status = status
        self._timestamp = timestamp
        self._uid = uid
        self._events = events

    @property
    def status(self):
        return self._status

    @status.setter
    def status(self, value: Status):
        if isinstance(value, Status):
            self._status = value
        else:
            raise ValueError('%r не является инстансом класса %s' % (value, Status))

    @property
    def uid(self):
        return self._uid

    @uid.setter
    def uid(self, value: str):
        self._uid = value

    @property
    def events(self):
        return self._events

    def __repr__(self):
        return dumps(self.as_dict(), indent=4)

    @classmethod
    def extract(
            cls, message: Dict
    ):
        """Валидирует значения, а потом создает инстанс Message"""
        # извлекаем статус
        status_field = message.get(MessageAttributes.status.value)
        try:
            status = Status(status_field) if status_field else None  # type: Optional[Status]
        except Exception as e:
            raise ValueError('Ошибка разбора значения статуса %s: %r' % (status_field, e))

        # извлекаем перечен команд
        events = {
            key: Operation(value) for key, value in
            message.get(MessageAttributes.events.value, {}).items()
        }  # type: Dict[str, Operation]

        # извлекаем timestamp
        timestamp = message.get(MessageAttributes.timestamp.value)  # type: Optional[float]
        publisher = message.get(MessageAttributes.publisher.value)  # type: Optional[float]
        return cls(status=status, timestamp=timestamp, uid=publisher, events=events)

    def __hash__(self):
        return hash(self._uid)

    def as_dict(self):
        """Создает сообщение в формате Dict"""
        result = {}
        if self._status:
            result[MessageAttributes.status.value] = self._status.value
        if self._timestamp:
            result[MessageAttributes.timestamp.value] = self._timestamp
        if self._uid:
            result[MessageAttributes.publisher.value] = self._uid
        if self._events:
            result[MessageAttributes.events.value] = {uid: op.value for uid, op in self._events.items()}
        return result

    @staticmethod
    def create_message(message):
        """Создает сообщение в формате JSONString"""
        return dumps(message.as_dict())

    @staticmethod
    def create_package(messages: Set) -> str:
        """
        Собирает покет со статусами в формате JSONString
        """
        result = [message.as_dict() for message in messages]
        return dumps(result)


class MessageParser:
    """Парсер сообщений со статусами"""

    def __init__(self, message: str):
        self.message = loads(message)

    @property
    def is_package(self) -> bool:
        """является ли сообщение пакетным"""
        return isinstance(self.message, list)

    @property
    def single_status_data(self) -> Message:
        """
        Извлекает содержимое одиночного сообщения
        """
        try:
            if self.is_package:
                raise ValueError('Сообщение %s содержит пакет статусов, не одиночное значение' % self.message)
            return Message.extract(self.message)
        except Exception as e:
            raise MessageDecodeException(e)

    @property
    def package_status_data(self) -> Set[Message]:
        """
        Извлекает содержимое сообщения с пакетом статусов
        """
        try:
            if not self.is_package:
                raise ValueError('Сообщение %s содержит одиночное значение статуса, не пакет' % self.message)

            result = set()
            for item in self.message:
                try:
                    result.add(Message.extract(item))
                except Exception as e:
                    logging.error(
                        'Ошибка извлечения данных из элемента сообщения %r: %r',
                        item, e
                    )
            return result
        except Exception as e:
            raise MessageDecodeException(e)
