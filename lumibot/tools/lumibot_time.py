import logging
import re
import time
from threading import currentThread

default_time_sleep = time.sleep
warned_against_calling_time_sleep = False


def warning_time_sleep(sleeptime):
    global warned_against_calling_time_sleep
    if warned_against_calling_time_sleep is False:
        thread_name = currentThread().getName()
        authorized_threads_with_sleep = [r"^.*_requesting_data_.*$"]
        if not any(
            [re.match(expr, thread_name) for expr in authorized_threads_with_sleep]
        ):
            warned_against_calling_time_sleep = True
            # TODO: Look into this warning being handled more gracefully. Right now it
            # is being called too often
            # logging.critical(
            #     "Time.sleep has been called within thread %s."
            #     "time.sleep should be used with caution within lumibot, "
            #     "especially in backtesting mode." % thread_name
            # )

    default_time_sleep(sleeptime)


time.sleep = warning_time_sleep
