from queue import Queue
from threading import Thread


class CustomStream:
    def __init__(self):
        self._queue = Queue(1)
        self._actions_mapping = {}

    def dispatch(self, event, **payload):
        self._queue.put((event, payload))
        self._queue.join()

    def add_action(self, event_name):
        def add_event_action(f):
            self._actions_mapping[event_name] = f
            return f

        return add_event_action

    def _run(self):
        while True:
            event, payload = self._queue.get()
            if payload is None:
                payload = {}
            if event in self._actions_mapping:
                action = self._actions_mapping[event]
                action(**payload)
            self._queue.task_done()

    def run(self, name):
        thread = Thread(target=self._run, daemon=True, name=name)
        thread.start()
