class UnauthorizedException(Exception):
    """Ошибка аутентификации"""


class GSSUnauthorizedException(UnauthorizedException):
    """Ошибка GSS аутентификации"""


class SyncUnauthorizedException(UnauthorizedException):
    """Ошибка аутентификации при синхронизации серверов статусов"""
