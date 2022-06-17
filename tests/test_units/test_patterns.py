import string

from status_daemon.redis.redis import RedisController
from status_daemon.utils import pattern_to_key


def test_pattern_to_key():
    test_uuid = string.ascii_letters + string.digits
    test_cases = [
        (RedisController.WISH_PATTERN, 'WISH@{}@'.format(test_uuid)),
        (RedisController.PUB_PATTERN, 'PUB@{}@'.format(test_uuid)),
        (RedisController.SUB_PATTERN, 'SUB@{}@'.format(test_uuid))
    ]

    for pattern, result in test_cases[1:]:
        assert pattern_to_key(test_uuid, pattern=pattern) == result
