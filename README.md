# Демон маршрутизации статусов

## Описание

Сервис включает в себя следующие модули:

1. Демон маршрутизации (`./status_daemon/status-daemon/`)

2. Web-socket сервер статусов (`./status_daemon/servers/status/`)

4. Web-socket сервер + клиент междоменной синхронизации статусов (`./status_daemon/servers/synchronize/`)

5. Интеграционный тест. Включает в себя следующие подмодули:
    * Имитатор Web-socket клиента сервиса статусов (`./integration/test_client.py`)
    * Имитатор клиента междоменной синхронизации статусов (`./integration/sync_client.py`)

6. CLI для получения и редактирования статусов (`./status_daemon/cli`). Поддерживает:
   * Редактирование статуса локального абонента по номеру телефона
   * Получение статусов абонентов по перечню номеров (или всех при помощи `*`)
   * Получение номеров телефонов по паттерну
   * Подписка на получение статусов по паттерну (Например `1000*`)
## Развертывание отладочного стенда (`for developers`)

1. Для работы под Astra1.6 использовать Python 3.5.3 (рекомендуется, но не обязательно).
2. Установить все зависимости выполнив команды `bash ./deploy.sh`.
3. Убедиться в правильности настроек в `./config/settings.yml`.
4. Перейти в виртуальное окружение `. ./venv/bin/activate`.
5. Запустить демон маршрутизации `python3 -m run -d`.
6. Запустить Web-socket сервер `python3 -m run -ws`
7. Запустить сервер синхронизации `python3 -m run -rsync`
8. Запустить клиент синхронизации `python3 -m run -ssync`

## Список команд

Ниже приведен результат выполнения `python3 -m run -h`:

```text
usage: run.py [-h] [-d | -ws | -rsync | -ssync] [-sc] [-ssh SYNC_SERVER_HOST]
              [-ssp SYNC_SERVER_PORT] [-name SYNC_CLIENT_NAME]
              [-ufn UIDS_FILENAME] [-ssec SYNC_SECONDS]
              [-msn MAX_STATUS_NUMBER] [-sst SYNC_SLEEP_TIME] [-tc]
              [-rh REMOTE_HOST] [-p PORT]
              [-rn REMOTE_NAMES [REMOTE_NAMES ...]] [-s STATUS [STATUS ...]]
              [-sl SLEEP] [-cli]

Укажите название модуля, который необходимо запустить:

optional arguments:
  -h, --help            show this help message and exit
  -d, --daemon          Запуск демона маршрутизации статусов.
  -ws, --ws-server      Запуск проксирующего Web-socket сервера.
  -rsync, --synchronize-receiver
                        Синхронизация статусов (приемник)
  -ssync, --synchronize-sender
                        Синхронизация статусов (передатчик)
  -cli                  CLI

Тестовый клиент сервера синхронизации статусов:
  -sc, --sync-client    Запуск клиента сервера синхронизации статусов
  -ssh SYNC_SERVER_HOST, --sync-server-host SYNC_SERVER_HOST
                        Адрес сервера синхронизации
  -ssp SYNC_SERVER_PORT, --sync-server-port SYNC_SERVER_PORT
                        Порт сервера синхронизации
  -name SYNC_CLIENT_NAME, --sync-client-name SYNC_CLIENT_NAME
                        имя клиента синхронизации
  -ufn UIDS_FILENAME, --uids-filename UIDS_FILENAME
                        Путь до файла с UID-ами
  -ssec SYNC_SECONDS, --sync-seconds SYNC_SECONDS
                        Время работы клиента (в секундах)
  -msn MAX_STATUS_NUMBER, --max-status-number MAX_STATUS_NUMBER
                        Максимальное количество статусов, отправленных за раз
                        на сервер
  -sst SYNC_SLEEP_TIME, --sync-sleep-time SYNC_SLEEP_TIME
                        Время между попытками массовой пересылки статусов

Тестовый клиент сервиса статусов:
  -tc, --test-client    Запуск тестового клиента сервиса статусов.
  -rh REMOTE_HOST, --remote-host REMOTE_HOST
                        Адрес сервиса статусов
  -p PORT, --port PORT  Порт сервиса статусов
  -rn REMOTE_NAMES [REMOTE_NAMES ...], --remote-names REMOTE_NAMES [REMOTE_NAMES ...]
                        Список доменных имен пользователей, с которыми
                        необходимо организовать обмен статусами.
  -s STATUS [STATUS ...], --status STATUS [STATUS ...]
                        Список статусов, которые будут публиковаться.
                        Возможные значения статусов: { "DO_NOT_DISTURB": 5,
                        "ABSENT": 6, "UNKNOWN": 8, "INCOMING": 4,
                        "NOT_REGISTERED": 3, "FORWARDING": 7, "ENABLE": 1,
                        "BUSY": 2 }.
  -sl SLEEP, --sleep SLEEP
                        Промежуток между публикациями статусов.
```

## Протокол обмена сообщениями

Клиент сервиса статусов обменивается сообщениями с сервером по протоколу Web-socket. Web-socket клиент может
осуществлять следующие действия: публикация сообщений (статусы, команды подписки/отписки на получение статусов других
абонентов), получение статусов других абонентов по подписке. Web-socket сервер поддерживает `GSS` аутентификацию (
обязательна) и осуществляет распространение статусов в соответствии с черно-белыми правилами,
предоставляемыми [сервисом привилегий](http://172.16.254.5/kolosm/privilege).

Пример кода создания соединения с Web-socket сервером (с использованием `GSS` аутентификации):

```python
import kerberos
from aiohttp import ClientSession

from config import settings

service = settings.AUTH.servicename


def get_auth_header():
    __, ctx = kerberos.authGSSClientInit(service)
    kerberos.authGSSClientStep(ctx, "")
    token = kerberos.authGSSClientResponse(ctx)
    return {"Authorization": "Negotiate " + token}


async def get_ws(session: ClientSession):
    return await session.ws_connect(url='http://{}:{}/pubsub'.format(settings.SERVICE.host, settings.SERVICE.port))


async def run_client():
    session = ClientSession(headers=get_auth_header())
    ws = await get_ws(session)
```

NOTE: Web-socket сервера доступен по endpoint-у `/pubsub`.

Для успешного прохождения `GSS` аутентификации, при создании соединения в заголовок запроса необходимо добавить
поле `Authorization` с контекстом безопасности полученным от `krb5` в формате `"Negotiate " + token`.

### Формат сообщения

Обмен между Web-socket сервером и клиентом осуществляется сообщениями следующего формата.

Сообщение может содержать следующие аттрибуты:

```python
class MessageAttributes(Enum):
    """Аттрибуты сообщения"""

    # статус абонента (Status)
    status = 'status'

    # словарь UID абонентов и операций (подписка/отписка на получение статусов других абонентов) ({UID: Operation})
    events = 'events'

    # временнАя метка (полезна для замера времени доставки статуса) (Unix Time)
    timestamp = 'timestamp'

    # UID абонента, опубликовавшего статус
    publisher = 'publisher'


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
```

### Пример публикуемого сообщения

Ниже приведен пример возможного публикуемого сообщения. Все атрибуты публикуемых сообщений опциональны.

```json
{
  "status": 1, # публикация клиентом статуса ENABLE
  "events": {
    "uid1": 9, # публикация команды на получение статусов (SUBSCRIBE) от абонента uid1 
    "uid2": 9, # публикация команды на получение статусов (SUBSCRIBE) от абонента uid2
    "uid3": 10, # публикация команды на остановку получения статусов (UNSUBSCRIBE) от абонента uid3
    "uid4": 10 # публикация команды на остановку получения статусов (UNSUBSCRIBE) от абонента uid4
  },
  "timestamp": "1627128694.693686" # Метка времени публикации сообщения
}
```

Важно заметить, что поле `publisher` при публикации сообщения указывать необязательно. При авторизации абонента сервис
статусов получает его UID автоматически от сервиса привилегий и прикладывает к сообщениям, которые получат
абоненты-потребители статусов.

NOTE: Сообщение должно быть передано по Web-socket в формате JSONString.

### Пример получаемых уведомлений

Ниже приведен пример возможных сообщений, получаемых по подписке.

```json
{
   "status": 1, # Статус ENABLE, опубликованный абонентом uid1 в момент timestamp (опционально) 
   "publisher": "uid1", # UID абонента, опубликовавшего полученный статус
   "timestamp": "1627128694.693686" # Время публикации статуса (опционально)
}
```
```json
{
   "status": 2, # Статус BUSY, опубликованный абонентом uid2. Время публикации не указано
   "publisher": "uid2" # UID абонента, опубликовавшего полученный статус
}
```

### Пример кода публикации и получения сообщений

Пример кода для публикации сообщений и получения уведомлений по подписке на Web-socket:

```python
from ujson import dumps
import logging

from aiohttp import WSMsgType
from asyncio import sleep

from typing import Iterable, List

from config import settings
from status_daemon.status_daemon.constants import Status, MessageAttributes, Operation
from status_daemon.messages import Message
from status_daemon.redis.redis import RedisController
from status_daemon.privileges.privileges import Privileges
from status_daemon.utils import pattern_to_key

remote_uid = '75f7e0ed-3f10-4ba1-8445-3ccbab91d4d8'

async def request_uids_by_names(remote_names: Iterable[str], pr_redis_pool: RedisController) -> Iterable[str]:
    uids = set(await pr_redis_pool.pool.mget(
        *[pattern_to_key(name, pattern=Privileges.DNAME_PATTERN) for name in remote_names]
    ) or [])
    return uids

async def listen_statuses(ws, remote_names: List[str]):
    pr_redis = await RedisController.connect(**settings.PRIVILEGE_REDIS)  # type: RedisController
    try:
        remote_uids = await request_uids_by_names(remote_names=remote_names, pr_redis_pool=pr_redis)

        subscribe_msg = dumps({
            MessageAttributes.events.value: {
                remote_uid: Operation.SUBSCRIBE.value for remote_uid in remote_uids
            }
        })

        await ws.send_str(subscribe_msg)
        logging.debug('Отправлен запрос на подписку на пользователей {}'.format(remote_names))

        while True:
            msg = await ws.receive()  # type: WSMsgType

            if msg.type == WSMsgType.text:
                if msg.data == 'close':
                    await ws.close()
                    break
                else:
                    logging.debug("Получено сообщение: %s" % msg.data)
            elif msg.tp == WSMsgType.closed:
                break
            elif msg.tp == WSMsgType.error:
                break
    finally:
        await pr_redis.disconnect()


async def publish_status(ws, status_list: List[int], sleep_: int = 5):
    while True:
        for item in status_list:
            try:
                status = Status(item)
            except Exception:
                logging.error('Неизвестный статус %s' % item)
                continue
            msg = Message.create_message(Message(status=status))
            await sleep(sleep_)  # ждать перед отправкой
            await ws.send_str(msg)
            logging.info('Опубликовано сообщение: %s' % msg)


```

Полный код клиента приведен [здесь](http://172.16.254.5/kolosm/status-daemon/-/tree/master/integration/test_client.py)

## Управление Redis

Для активации поддержки notify-keyspace - добавить в `/etc/redis/redis.conf` файл строку `notify-keyspace-events KA`

