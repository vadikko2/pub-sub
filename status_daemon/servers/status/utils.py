from itertools import islice
from typing import Iterable

from status_daemon.servers.status.constants import BATCH_SIZE


def divide_to_equal_batches(it_: Iterable, batch_size: int = BATCH_SIZE):
    """Разделяет сообщение на батчи равной длины"""
    it_ = iter(it_)
    for i in iter(lambda: tuple(islice(it_, batch_size)), ()):
        yield list(i)
