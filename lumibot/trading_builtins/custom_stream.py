import logging
import queue
import threading
from queue import Queue

class CustomStream:

    def __init__(self):
        self._queue = Queue(100)
        self._actions_mapping = {}
        self._stop_event = threading.Event()
        self._thread = None

    def dispatch(self, event, wait_until_complete=False, **payload):
        # Don't put events if we're stopping
        if self._stop_event.is_set():
            return

        try:
            self._queue.put((event, payload), block=False)
        except queue.Full:
            logging.warning(f"Queue full, dropping event {event}")
            return

        # Primarily used for backtesting. If wait_until_complete is True, the function will block until the queue is
        # empty. This is useful for ensuring that all events have been processed before moving on to the next step.
        if wait_until_complete:
            # Wait for the queue to be processed
            self._queue.join()

    def add_action(self, event_name):
        def add_event_action(f):
            self._actions_mapping[event_name] = f
            return f

        return add_event_action

    def _run(self):
        while not self._stop_event.is_set():
            try:
                # Use timeout to periodically check stop_event
                event, payload = self._queue.get(timeout=0.1)
                self._process_queue_event(event, payload)
                self._queue.task_done()
            except queue.Empty:
                # Check if we should stop
                if self._stop_event.is_set():
                    break
                continue
            except Exception as e:
                logging.error(f"Error processing queue event: {e}")
                if self._stop_event.is_set():
                    break

    def _process_queue_event(self, event, payload):
        if payload is None:
            payload = {}
        if event in self._actions_mapping:
            action = self._actions_mapping[event]
            action(**payload)

    def run(self, name):
        # Threads are spawned by the broker._launch_stream() code
        self._run()

    def stop(self):
        """Stop the stream gracefully"""
        self._stop_event.set()
        # Clear the queue to unblock any waiting operations
        try:
            while not self._queue.empty():
                self._queue.get_nowait()
                self._queue.task_done()
        except queue.Empty:
            pass
        # Wait for thread to finish if it exists
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=1)


class PollingStream(CustomStream):
    """
    A stream that polls an API endpoint at a regular interval and dispatches events based on the response. It is
    required that a polling action is registered with the stream using add_action(). The polling action should make a
    request to the API and dispatch events based on the response. A user can also dispatch events to the stream manually
    using dispatch(), including the poll event to force an off-cycle poll action to occur.
    """
    POLL_EVENT = "poll"

    def __init__(self, polling_interval=5.0):
        """
        Parameters
        ----------
        polling_interval: float
            Number of seconds to wait between polling the API.
        """
        super().__init__()
        self.polling_interval = polling_interval

    def _run(self):
        while not self._stop_event.is_set():
            try:
                # This is a blocking operation until an item is available in the queue or the timeout is reached.
                event, payload = self._queue.get(timeout=max(self.polling_interval, 0.1))
                self._process_queue_event(event, payload)
                self._queue.task_done()
            except queue.Empty:
                # Check if we should stop
                if self._stop_event.is_set():
                    break
                # If the queue is empty and enough time has passed, poll
                if not self._stop_event.is_set():
                    self._poll()
                continue
            # Ensure that the Polling thread does not die if an exception is raised in the event processing.
            except Exception as e: # noqa
                logging.exception(f"An error occurred while processing a queue event. {e}")
                if self._stop_event.is_set():
                    break
                continue

    def _poll(self):
        if self.POLL_EVENT not in self._actions_mapping:
            raise ValueError("No action is defined for the poll event. You must register a polling action with "
                             "add_action()")

        try:
            self._process_queue_event(self.POLL_EVENT, {})
        except queue.Full:
            logging.info("Polling action itself has added too many events to the queue. Skipping this polling cycle, "
                         "(it is incomplete) to allow the queue to drain. The next cycle will occur as scheduled.")
            return
        # Ensure that the Polling thread does not die if an exception is raised in the event processing.
        except Exception as e: # noqa
            logging.exception(f"An error occurred while polling. {e}")
