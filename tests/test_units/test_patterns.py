import string

from status_daemon.constants import (
    WISH_PATTERN, PUB_PATTERN,
    SUB_PATTERN
)
from status_daemon.utils import pattern_to_key


def test_pattern_to_key():
    test_uuid = string.ascii_letters + string.digits
    test_cases = [
        (WISH_PATTERN, 'WISH@{}@'.format(test_uuid)),
        (PUB_PATTERN, 'PUB@{}@'.format(test_uuid)),
        (SUB_PATTERN, 'SUB@{}@'.format(test_uuid))
    ]

    assert pattern_to_key(test_uuid) == test_cases[0][1]
    for pattern, result in test_cases[1:]:
        assert pattern_to_key(test_uuid, pattern=pattern) == result
