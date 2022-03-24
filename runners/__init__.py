import logging
import logging.config
from abc import abstractmethod, ABC
from time import sleep

from config import settings
from status_daemon import BlockingRunnableMixin


class Runner(BlockingRunnableMixin, ABC):
    """Абстрактный класс EntryPoint"""

    ERR_STR = 'Ошибка при работе %s: %s. Сервис %s прекратил свою работу'

    @staticmethod
    def _init():
        logging.config.dictConfig(settings.LOGGING_BASE_CONFIG)
        logging.getLogger('aiohttp').setLevel(logging.WARNING)
        logging.getLogger('aioredis').setLevel(logging.WARNING)
        logging.getLogger('asyncio').setLevel(logging.WARNING)

    @staticmethod
    @abstractmethod
    def _run():
        """Абстрактный метод запуска"""
        pass

    @classmethod
    def run(cls):
        """Запуск"""
        try:
            cls._init()
            cls._run()
        except Exception as e:
            logging.error(Runner.ERR_STR, cls.__name__, e, cls.__name__)
        except KeyboardInterrupt:
            logging.info('%s остановлен вручную.', cls.__name__)
        finally:
            sleep(5)  # нужно для корректного рестарта systemd
