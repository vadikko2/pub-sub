from time import time

from ujson import dumps, loads

from status_daemon.messages import MessageParser, Message
from status_daemon.status_daemon.constants import Status, Operation, MessageAttributes


def test_message_creator():
    message = Message(status=Status.BUSY, timestamp=str(time()))
    assert isinstance(message, Message)
    assert message.status == Status.BUSY

    dict_message = loads(Message.create_message(message))
    assert isinstance(dict_message, dict)

    message_ = Message.extract(dict_message)
    assert isinstance(message_, Message)
    assert message.status == Status.BUSY

    message_.status = Status.UNKNOWN
    assert message_.status == Status.UNKNOWN


def test_message_parser():
    test_message = dumps(
        {
            MessageAttributes.events.value: {
                '1': Operation.SUBSCRIBE.value,
                '2': Operation.UNSUBSCRIBE.value
            },
            MessageAttributes.status.value: Status.ENABLE.value,
            MessageAttributes.timestamp.value: str(time())
        }
    )

    message = MessageParser(message=test_message).single_status_data
    assert isinstance(message.status, Status)
    for uuid, operation in message.events.items():
        assert isinstance(uuid, str) and isinstance(operation, Operation)
