import asyncio
from asyncio import get_event_loop
from cmd import Cmd
from concurrent.futures import Future
from typing import Dict

from async_timeout import timeout

from status_daemon.messages import Message
from status_daemon.privileges.privileges import Privileges
from status_daemon.redis.redis import RedisController
from status_daemon.servers.synchronize.sender.daemon import SyncStatusSenderDaemon
from status_daemon.status_daemon.constants import Status
from status_daemon.utils import pattern_to_key, extract_uid_from_key, parse_keyspace


class StatusDaemonCLI(Cmd):
    MSS_PH_NUMBER_PATTERN = 'MSSPHN@*@'
    ERR_STR = 'ERR: %s'
    PARSE_ERR = 'ArgParse ERR: %s'
    OK = 'ok'

    loop = get_event_loop()
    prompt = 'status> '
    intro = "Welcome to StatusDaemon CLI! Type ? to list commands"

    def __init__(self, *args, **kwargs):
        self._redis = StatusDaemonCLI.loop.run_until_complete(RedisController.connect())  # type: RedisController
        self._privileges = StatusDaemonCLI.loop.run_until_complete(Privileges.connect())  # type: Privileges
        self._tasks = {}  # type: Dict[str, Future]
        self._uid_2_mss_number = {}  # type: Dict[str, str]
        super(StatusDaemonCLI, self).__init__(*args, **kwargs)

    @staticmethod
    def parse(arg):
        """Convert a series of zero or more numbers to an argument tuple"""
        return tuple(map(str, arg.split()))

    def _get_numbers_by_pattern(self, pattern):
        find_pattern = pattern_to_key(pattern or '*', pattern=StatusDaemonCLI.MSS_PH_NUMBER_PATTERN)

        cursor = b'0'
        while cursor:
            cursor, keys = self.loop.run_until_complete(self._privileges.redis.scan(
                cursor=cursor, match=find_pattern
            ))
            for key in keys:
                yield extract_uid_from_key(key)

    def do_numbers(self, pattern: str):
        candidates = StatusDaemonCLI.parse(pattern)
        for candidate in candidates:
            if '*' in candidate:
                for number in self._get_numbers_by_pattern(candidate):
                    print(number)
            else:
                print(candidate)

    @staticmethod
    def help_numbers():
        print('Get mss phone numbers by pattern (* for all numbers)')

    def do_set(self, args):
        try:
            mss_number, status = StatusDaemonCLI.parse(args)
            status = Status(int(status))
        except Exception as e:
            print(StatusDaemonCLI.PARSE_ERR % e)
            print(self.help_set())
            return
        try:
            mss_to_uid_key = pattern_to_key(mss_number, pattern=StatusDaemonCLI.MSS_PH_NUMBER_PATTERN)
            uid = self.loop.run_until_complete(self._privileges.redis.get(mss_to_uid_key))
            if not uid:
                raise ValueError('Unknown mss number %s' % mss_number)
            if not self.loop.run_until_complete(self._privileges.is_local(uid=uid)):
                raise ValueError('You can not set status for not local user')
            status_key = pattern_to_key(uid, pattern=self._redis.LOCAL_LAST_PATTERN)
            self.loop.run_until_complete(self._redis.pool.set(key=status_key, value=status.value))
            message = Message(status=status, uid=uid)

            channel_name = pattern_to_key('local', uid, pattern=self._redis.PUB_PATTERN)
            self.loop.run_until_complete(self._redis.pool.publish(
                channel=channel_name,
                message=Message.create_message(message=message)
            ))

        except Exception as e:
            print(StatusDaemonCLI.ERR_STR % e)
        else:
            print(StatusDaemonCLI.OK)

    def _get_status_value(self, number: str):
        mss_to_uid_key = pattern_to_key(number, pattern=StatusDaemonCLI.MSS_PH_NUMBER_PATTERN)
        uid = self.loop.run_until_complete(self._privileges.redis.get(mss_to_uid_key))
        if not uid:
            raise ValueError('Unknown mss number %s' % number)
        last_status_pattern = pattern_to_key('*', uid, pattern=RedisController.LAST_PATTERN)
        keys = self.loop.run_until_complete(self._redis.pool.keys(pattern=last_status_pattern))
        if not keys:
            if self.loop.run_until_complete(self._privileges.is_local(uid=uid)):
                return Status.NOT_REGISTERED.name
            else:
                return Status.UNKNOWN.name
        status_key = keys[0]
        status = self.loop.run_until_complete(self._redis.pool.get(status_key))
        return Status(int(status)).name

    def do_get(self, mss_number):
        numbers = set()
        candidates = StatusDaemonCLI.parse(mss_number)
        for candidate in candidates:
            if '*' in candidate:
                numbers.update(set(self._get_numbers_by_pattern(candidate)))
            else:
                numbers.add(candidate)

        for number in numbers:
            try:
                status = self._get_status_value(number=number)
                print(number, status)
            except Exception as e:
                print(number, StatusDaemonCLI.ERR_STR % e)

    @staticmethod
    def help_get():
        print('Get status value by mss phone number (* for getting all statuses): *<MSS PHONE NUMBER>')

    def do_pr(self, args):
        try:
            target, remote = StatusDaemonCLI.parse(args)
        except Exception as e:
            print(StatusDaemonCLI.PARSE_ERR % e)
            print(self.help_pr())
            return
        try:
            target_mss_to_uid_key = pattern_to_key(target, pattern=StatusDaemonCLI.MSS_PH_NUMBER_PATTERN)
            remote_mss_to_uid_key = pattern_to_key(remote, pattern=StatusDaemonCLI.MSS_PH_NUMBER_PATTERN)
            target_uid = self.loop.run_until_complete(self._privileges.redis.get(target_mss_to_uid_key))
            remote_uid = self.loop.run_until_complete(self._privileges.redis.get(remote_mss_to_uid_key))
            for uid, number in zip((target_uid, remote_uid), (target, remote)):
                if not uid:
                    raise ValueError('Unknown mss number %s' % number)

            print(
                self.loop.run_until_complete(self._privileges.get_privilege(target_uid, remote_uid)),
                self.loop.run_until_complete(self._privileges.get_privilege(remote_uid, target_uid)),
            )
        except Exception as e:
            print(StatusDaemonCLI.ERR_STR % e)

    @staticmethod
    def help_pr():
        print('Get privilege between 2 mss numbers and revers: <TARGET MSS NUMBER> <REMOTE MSS NUMBER>')

    async def _get_number_uid(self, uid):
        if uid not in self._uid_2_mss_number:
            cursor = b'0'
            while cursor:
                cursor, keys = await self._privileges.redis.scan(
                    cursor=cursor, match=StatusDaemonCLI.MSS_PH_NUMBER_PATTERN, count=100
                )
                if not keys: continue
                values = await self._privileges.redis.mget(*keys)
                for key, value in zip(keys, values):
                    self._uid_2_mss_number[value] = extract_uid_from_key(key)
        return self._uid_2_mss_number.get(uid)

    async def _listen_statuses(self, pattern):
        async with SyncStatusSenderDaemon.get_channel(redis=self._redis.pool, pattern=pattern) as channel:
            while True:
                try:
                    async with timeout(1):
                        keyspace, _ = await channel.get(encoding='utf-8')

                    key = parse_keyspace(keyspace=keyspace)
                    value = int(await self._redis.pool.get(key) or 8)
                    uid = extract_uid_from_key(key, index=2)
                    number = await self._get_number_uid(uid)
                    print(number, Status(value).name)
                except asyncio.TimeoutError:
                    pass
                except KeyboardInterrupt as e:
                    raise e
                except Exception as e:
                    print(StatusDaemonCLI.ERR_STR % e)

    def do_sub(self, pattern):
        try:
            subscribe_key_pattern = pattern_to_key('*', pattern, pattern=self._redis.LAST_PATTERN)
            status_pattern = pattern_to_key(self._redis.pool.db, subscribe_key_pattern,
                                            pattern=self._redis.DAEMON_KEYSPACE)
            self.loop.run_until_complete(self._listen_statuses(status_pattern))
        except KeyboardInterrupt:
            print('stop')

    @staticmethod
    def help_sub():
        print('Subscribe by pattern: for example "*"')

    @staticmethod
    def help_set():
        print('Set status by mss phone number: <MSS PHONE NUMBER> <STATUS>')

    def do_exit(self, _):
        self.loop.run_until_complete(self._redis.disconnect())
        self.loop.run_until_complete(self._privileges.disconnect())
        print("Bye")
        return True

    @staticmethod
    def help_exit():
        print('exit the application.')

    do_EOF = do_exit
    help_EOF = help_exit


if __name__ == '__main__':
    StatusDaemonCLI().cmdloop()
