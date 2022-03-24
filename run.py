import argparse
import logging.config

from ujson import dumps

from integration.exceptions import RemoteNameException
from status_daemon.exceptions import NoSpecifiedArgsException
from status_daemon.status_daemon.constants import Status


def main(args):
    """EntryPoint"""

    if args.daemon:
        from runners.routing import RoutingRunner
        RoutingRunner.run()
    elif args.ws_server:
        from runners.ws_server import WSServerRunner
        WSServerRunner.run()
    elif args.synchronize_receiver:
        from runners.rsync import ReceiverSyncRunner
        ReceiverSyncRunner.run()
    elif args.synchronize_sender:
        from runners.ssync import SenderSyncRunner
        SenderSyncRunner.run()

    elif args.test_client:
        from integration.test_client import run

        if not args.remote_host:
            raise ValueError('Не указан адрес сервиса статусов')
        if not args.remote_names:
            raise RemoteNameException('Не указано ни одного имени удаленного пользователя.')

        host = args.remote_host
        port = args.port
        status_list = args.status
        remote_names = args.remote_names
        sleep_ = args.sleep

        run(host=host, port=port, remote_names=remote_names, status_list=status_list, sleep_=sleep_)
    elif args.sync_client:
        from integration.sync_client import run
        host = args.sync_server_host
        port = args.sync_server_port
        name = args.sync_client_name
        uids_fn = args.uids_filename
        ssec = args.sync_seconds
        max_status_number = args.max_status_number
        sleep_time = args.sync_sleep_time
        run(
            host=host,
            port=port,
            name=name,
            uids_filename=uids_fn,
            seconds=ssec,
            max_status_number=max_status_number,
            sleep_time=sleep_time

        )
    else:
        raise NoSpecifiedArgsException


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Укажите название модуля, который необходимо запустить:'
    )

    service_status_group = parser.add_mutually_exclusive_group()

    # компоненты сервиса статусов
    service_status_group.add_argument(
        '-d', '--daemon', action='store_true',
        help='Запуск демона маршрутизации статусов.'
    )
    service_status_group.add_argument(
        '-ws', '--ws-server', action='store_true',
        help='Запуск проксирующего Web-socket сервера.'
    )
    service_status_group.add_argument(
        '-rsync', '--synchronize-receiver', action='store_true',
        help='Синхронизация статусов (приемник)'
    )
    service_status_group.add_argument(
        '-ssync', '--synchronize-sender', action='store_true',
        help='Синхронизация статусов (передатчик)'
    )

    # клиент сервера синхронизации
    sync_client_group = parser.add_argument_group('Тестовый клиент сервера синхронизации статусов')
    sync_client_group.add_argument(
        '-sc', '--sync-client', action='store_true',
        help='Запуск клиента сервера синхронизации статусов'
    )

    # параметры клиента синхронизации
    sync_client_group.add_argument(
        '-ssh', '--sync-server-host', type=str, help='Адрес сервера синхронизации'
    )

    sync_client_group.add_argument(
        '-ssp', '--sync-server-port', type=int, default=8004, help='Порт сервера синхронизации'
    )

    sync_client_group.add_argument(
        '-name', '--sync-client-name', type=str, default='suvname',
        help='имя клиента синхронизации'
    )

    sync_client_group.add_argument(
        '-ufn', '--uids-filename', type=str, help='Путь до файла с UID-ами'
    )

    sync_client_group.add_argument(
        '-ssec', '--sync-seconds', type=int, default=60, help='Время работы клиента (в секундах)'
    )

    sync_client_group.add_argument('-msn', '--max-status-number', type=int, default=200,
                                   help='Максимальное количество статусов, отправленных за раз на сервер')

    sync_client_group.add_argument('-sst', '--sync-sleep-time', type=int, default=2,
                                   help='Время между попытками массовой пересылки статусов')
    # тестовый клиент
    status_client_group = parser.add_argument_group('Тестовый клиент сервиса статусов')
    status_client_group.add_argument(
        '-tc', '--test-client', action='store_true',
        help='Запуск тестового клиента сервиса статусов.'
    )

    # параметры тестового клиента
    status_client_group.add_argument(
        '-rh', '--remote-host', type=str, help='Адрес сервиса статусов'
    )

    status_client_group.add_argument(
        '-p', '--port', type=int, default=8003, help='Порт сервиса статусов'
    )

    status_client_group.add_argument(
        '-rn', '--remote-names', type=str, nargs='+',
        help='Список доменных имен пользователей, с которыми необходимо организовать обмен статусами.'
    )

    status_client_group.add_argument(
        '-s', '--status', nargs='+', type=int, default=[status.value for status in Status],
        help='Список статусов, которые будут публиковаться. Возможные значения статусов: %s.' % dumps({
            status._name_: status.value for status in Status
        }, indent=4)
    )

    status_client_group.add_argument(
        '-sl', '--sleep', type=int, default=5, help='Промежуток между публикациями статусов.'
    )

    args_ = parser.parse_args()

    try:
        main(args_)
    except KeyboardInterrupt:
        logging.info('Сервис статусов остановлен вручную.')
    except NoSpecifiedArgsException as e:
        logging.error('Неудачная попытка запуска сервиса статусов: %s.' % e)
        parser.print_help()
