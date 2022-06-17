from asyncio import AbstractEventLoop

from status_daemon import Logger


class DropTaskException(Exception):
    """Исключение для отлова события - убить таску"""


class MessageDecodeException(Exception):
    """Ошибка разбора сообщения"""


class NoSpecifiedArgsException(Exception):
    """Модуль, необходимый для сервиса статусов не указан"""


class AvailableException(Exception):
    """Ошибка доступа абонента к адресной книге"""


def base_exception_handler(_, context):
    err_msg = context.get('message')
    Logger.error(err_msg)


def call_exception_handler(loop: AbstractEventLoop, message: str):
    context = {
        'message': message
    }
    loop.call_exception_handler(context=context)
