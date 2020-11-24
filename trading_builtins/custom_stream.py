from queue import Queue
from threading import Thread
import logging

class CustomStream:
    def __init__(self):
        self._queue = Queue()
        self._actions_mapping = {}

    def dispatch(self, event, **payload):
        self._queue.put((event, payload))

    def add_action(self, event_name):
        def add_event_action(f):
            self._actions_mapping[event_name] = f
            return f

        return add_event_action

    def _run(self):
        while True:
            event, payload = self._queue.get()
            if payload is None: payload = {}
            if event in self._actions_mapping:
                action = self._actions_mapping[event]
                action(**payload)

    def run(self):
        thread = Thread(target=self._run, daemon=True)
        thread.start()
