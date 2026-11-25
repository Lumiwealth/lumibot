"""Cancelable Webull order-event subscription using the official wire protocol."""
import json
import threading
import time


class WebullOrderEvents:
    def __init__(self, key, secret, account, *, paper):
        self._key = key
        self._secret = secret
        self._account = account
        self._target = "events-api.sandbox.webull.com:443" if paper else "events-api.webull.com:443"
        self._stop = threading.Event()
        self._lock = threading.Lock()
        self._channel = None

    def run(self, on_observation, on_state):
        import grpc
        from webull.core.auth.algorithm import sha_hmac256_new
        from webull.trade.events import events_pb2, events_pb2_grpc
        from webull.trade.events.signature_composer import calc_signature

        delay = 1
        while not self._stop.is_set():
            on_state("connecting")
            with self._lock:
                if self._stop.is_set():
                    break
                channel = grpc.secure_channel(self._target, grpc.ssl_channel_credentials())
                self._channel = channel
            try:
                request = events_pb2.SubscribeRequest(
                    subscribeType=1, timestamp=time.time_ns() // 1_000_000, accounts=[self._account],
                )
                _, metadata = calc_signature(self._key, self._secret, request, sha_hmac256_new)
                replies = events_pb2_grpc.EventServiceStub(channel).Subscribe(request, metadata=metadata)
                for response in replies:
                    if self._stop.is_set():
                        break
                    if response.eventType == events_pb2.SubscribeSuccess:
                        on_state("acknowledged")
                        delay = 1
                    elif response.eventType == events_pb2.AuthError:
                        on_state("authentication_failed")
                        return
                    elif response.eventType in (events_pb2.NumOfConnExceed, events_pb2.SubscribeExpired):
                        break
                    if response.payload:
                        try:
                            payload = json.loads(response.payload)
                        except (ValueError, TypeError):
                            continue
                        if isinstance(payload, dict):
                            on_observation(payload)
            except grpc.RpcError as error:
                if error.code() in (grpc.StatusCode.UNAUTHENTICATED, grpc.StatusCode.PERMISSION_DENIED):
                    on_state("authentication_failed")
                    return
            finally:
                channel.close()
                with self._lock:
                    self._channel = None
            if not self._stop.is_set():
                on_state("recovering")
                self._stop.wait(delay)
                delay = min(delay * 2, 30)
        on_state("stopped")

    def close(self):
        self._stop.set()
        with self._lock:
            if self._channel is not None:
                self._channel.close()
