from time import sleep

from status_daemon import BlockingRunnableMixin, Logger


class Runner(BlockingRunnableMixin):
    """Абстрактный класс EntryPoint"""

    ERR_STR = 'Ошибка при работе %s: %s. Сервис %s прекратил свою работу'

    @staticmethod
    def _run():
        """Абстрактный метод запуска"""
        raise NotImplementedError

    @classmethod
    def run(cls):
        """Запуск"""
        try:
            cls._run()
        except Exception as e:
            Logger.error(Runner.ERR_STR, cls.__name__, e, cls.__name__)
        except KeyboardInterrupt:
            Logger.info('%s остановлен вручную.', cls.__name__)
        finally:
            sleep(5)  # нужно для корректного рестарта systemd
