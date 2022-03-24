from enum import Enum

DAEMON_KEYSPACE = '__keyspace@*__:*'
WISH_PATTERN = "WISH@*@"  # таблица с "желаемыми"
# для получения статусов uuid-ов абонентов (ключ - отправитель статуса,
# значение - список получателей)
LAST_PATTERN = "LAST@*@*@"  # Значение последнего статуса
LOCAL_LAST_PATTERN = "LAST@local@*@"  # Значение последнего статуса локального абонента
PUB_PATTERN = "PUB@*@*@"  # очереди для публикации статусов
LOCAL_PUB_PATTERN = "PUB@local@*@"  # очереди для публикации статусов локального абонента
SUB_PATTERN = "SUB@*@"  # очереди для получения статусов


class STATE(Enum):
    """Готовность привилегий"""
    ready = 1
    not_ready = 0
