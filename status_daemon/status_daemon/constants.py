from enum import Enum


class Status(Enum):
    """Список всех статусов с их идентификаторами"""
    ENABLE = 1  # доступен
    BUSY = 2  # занят
    NOT_REGISTERED = 3  # не зарегистрирован
    INCOMING = 4  # входящий вызов
    DO_NOT_DISTURB = 5  # не беспокоить
    ABSENT = 6  # отсутствует
    FORWARDING = 7  # включена переадресация
    UNKNOWN = 8  # неизвестен


class Operation(Enum):
    """Список возможных операций с их идентификаторами"""
    SUBSCRIBE = 9
    UNSUBSCRIBE = 10


class MessageAttributes(Enum):
    """Аттрибуты сообщения"""

    # статус абонента (Status)
    status = 'status'

    # словарь UID абонентов и операций
    # (подписка/отписка на получение статусов других абонентов) ({UID: Operation})
    events = 'events'

    # временнАя метка (полезна для замера времени доставки статуса) (Unix Time)
    timestamp = 'timestamp'

    # UID абонента, опубликовавшего статус
    publisher = 'publisher'
