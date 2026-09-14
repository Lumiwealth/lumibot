"""Deterministic WebSocket lifecycle checks using the existing stream class."""

import json
from types import SimpleNamespace
from unittest.mock import Mock

from lumibot.brokers.kalshi import KalshiStream
from lumibot.trading_builtins import CustomStream


class Socket:
    def __init__(self, stream, messages):
        self.stream, self.messages = stream, iter(messages)
        self.sent = []
        self.closed = False

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.closed = True

    def send(self, message):
        self.sent.append(json.loads(message))

    def recv(self, timeout):
        try:
            value = next(self.messages)
        except StopIteration:
            self.stream._stop_event.set()
            raise TimeoutError()
        if isinstance(value, Exception):
            raise value
        return json.dumps(value)

    def close(self):
        self.closed = True


def make_stream():
    client = SimpleNamespace(
        ws_url="wss://example.invalid/trade-api/ws/v2",
        WS_PATH="/trade-api/ws/v2",
        auth_headers=Mock(return_value={"KALSHI-ACCESS-KEY": "test"}),
    )
    broker = SimpleNamespace(
        _client=client,
        _stream_established=Mock(),
        logger=Mock(),
        _strategy_name="test",
        polling_interval=1,
        sync_orders=Mock(),
        sync_positions=Mock(),
    )
    return KalshiStream(broker)


def test_stream_uses_custom_stream_and_signed_subscription():
    stream = make_stream()
    assert isinstance(stream, CustomStream)
    socket = Socket(
        stream,
        [
            {"type": "subscribed", "msg": {"channel": "user_orders"}},
            {"type": "subscribed", "msg": {"channel": "fill"}},
            {"type": "user_order", "msg": {"order_id": "one"}},
            {"type": "fill", "msg": {"order_id": "one"}},
        ],
    )
    factory = Mock(return_value=socket)
    stream._websocket_factory = factory
    stream._read_websocket()
    assert socket.sent == [{"id": 1, "cmd": "subscribe", "params": {"channels": ["user_orders", "fill"]}}]
    assert stream._queue.qsize() == 2
    stream.broker._client.auth_headers.assert_called_once_with("GET", "/trade-api/ws/v2")
    assert factory.call_args.kwargs["additional_headers"] == {"KALSHI-ACCESS-KEY": "test"}
    assert socket.closed


def test_idle_receive_timeout_keeps_same_connection():
    stream = make_stream()
    socket = Socket(stream, [TimeoutError(), TimeoutError(), {"type": "subscribed"}])
    stream._websocket_factory = Mock(return_value=socket)
    stream._read_websocket()
    assert stream._websocket_factory.call_count == 1
    stream.broker.logger.warning.assert_not_called()


def test_stream_requires_both_subscription_acknowledgements():
    stream = make_stream()
    stream._receive({"type": "subscribed", "msg": {"channel": "user_orders"}})
    stream.broker._stream_established.assert_not_called()
    stream._receive({"type": "subscribed", "msg": {"channel": "fill"}})
    stream.broker._stream_established.assert_called_once()


def test_reconnect_does_not_reuse_previous_subscription_acknowledgement():
    stream = make_stream()
    first = Socket(stream, [{"type": "subscribed", "msg": {"channel": "user_orders"}}, RuntimeError()])
    second = Socket(stream, [{"type": "subscribed", "msg": {"channel": "fill"}}])
    stream._websocket_factory = Mock(side_effect=[first, second])
    stream._read_websocket()
    stream.broker._stream_established.assert_not_called()
    assert stream.broker._is_stream_subscribed is False


def test_disconnect_reconnects_resigns_resubscribes_without_leaking_error(monkeypatch):
    stream = make_stream()
    first = Socket(stream, [RuntimeError("secret signed headers must not leak")])
    second = Socket(stream, [{"type": "subscribed"}])
    stream._websocket_factory = Mock(side_effect=[first, second])
    stream._read_websocket()
    assert stream._websocket_factory.call_count == 2
    assert stream.broker._client.auth_headers.call_count == 2
    assert len(first.sent) == len(second.sent) == 1
    assert stream._repair.is_set()
    assert "secret" not in str(stream.broker.logger.warning.call_args_list)


def test_subscription_error_triggers_reconnect():
    stream = make_stream()
    stream._websocket_factory = Mock(
        side_effect=[
            Socket(stream, [{"type": "error", "msg": {"msg": "private error"}}]),
            Socket(stream, [{"type": "subscribed"}]),
        ]
    )
    stream._read_websocket()
    assert stream._websocket_factory.call_count == 2
    assert "private error" not in str(stream.broker.logger.warning.call_args_list)


def test_dispatch_loop_processes_updates_and_periodic_repair():
    stream = make_stream()
    stream._read_websocket = Mock()
    updates = []

    @stream.add_action(stream.UPDATE)
    def update(identifier):
        updates.append(identifier)

    stream._receive({"type": "fill", "msg": {"order_id": "one"}})
    stream.broker.sync_positions.side_effect = lambda _: stream._stop_event.set()
    stream._run()
    assert updates == ["one"]
    stream.broker.sync_orders.assert_called_once_with("test")
    stream.broker.sync_positions.assert_called_once_with("test")
    assert stream._queue.unfinished_tasks == 0


def test_dispatch_failure_requests_rest_repair():
    stream = make_stream()
    stream._read_websocket = Mock()

    @stream.add_action(stream.UPDATE)
    def update(identifier):
        raise ValueError("bad event")

    stream._receive({"type": "fill", "msg": {"order_id": "one"}})
    stream.broker.sync_positions.side_effect = lambda _: stream._stop_event.set()
    stream._run()
    stream.broker.sync_orders.assert_called_once()
    assert stream._queue.unfinished_tasks == 0


def test_poll_error_does_not_crash_dispatcher():
    stream = make_stream()
    stream._read_websocket = Mock()

    def fail(_):
        stream._stop_event.set()
        raise ValueError("temporary")

    stream.broker.sync_orders.side_effect = fail
    stream._run()
    assert stream.broker.logger.warning.called


def test_stop_closes_socket_and_workers():
    stream = make_stream()
    socket = Socket(stream, [])
    stream._socket = socket
    stream._reader = Mock()
    stream._thread = Mock()
    stream._thread.is_alive.return_value = True
    stream.stop()
    assert socket.closed
    assert stream._stop_event.is_set()
    stream._reader.join.assert_called_once()
    stream._thread.join.assert_called_once()


def test_irrelevant_or_incomplete_messages_do_not_create_events():
    stream = make_stream()
    for message in [{}, {"type": "ticker"}, {"type": "fill", "msg": {}}]:
        stream._receive(message)
    assert stream._queue.empty()
