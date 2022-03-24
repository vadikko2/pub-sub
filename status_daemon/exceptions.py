class DropTaskException(Exception):
    """Исключение для отлова события - убить таску"""


class MessageDecodeException(Exception):
    """Ошибка разбора сообщения"""


class NoSpecifiedArgsException(Exception):
    """Модуль, необходимый для сервиса статусов не указан"""
