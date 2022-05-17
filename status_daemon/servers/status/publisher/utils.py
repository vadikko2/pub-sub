from typing import Dict, Set

from status_daemon.status_daemon.constants import Operation


def catch_subscribe_events(events: Dict[str, Operation]) -> Set[str]:
    """Ловит сообщения с командой Operation.SUBSCRIBE"""
    return {uid for uid, operation in events.items() if operation == Operation.SUBSCRIBE}
