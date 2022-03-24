from config import settings

BATCH_SIZE = int(settings.BATCH_SIZE or 100)
RETRY = int(settings.RETRY or 10)
SYNC_RETRY = int(settings.SYNC_RETRY or 10)
