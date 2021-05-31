import logging
import re
import time
from threading import currentThread

old_time_sleep = time.sleep
warned_against_calling_time_sleep = False


def lumibot_sleep(sleeptime):
    raise Exception("Needs to be overloaded")


def warning_time_sleep(sleeptime):
    global warned_against_calling_time_sleep

    thread_name = currentThread().getName()
    authorized_threads_with_sleep = [r"^.*_requesting_data_.*$"]
    if not any([re.match(expr, thread_name) for expr in authorized_threads_with_sleep]):
        if warned_against_calling_time_sleep is False:
            warned_against_calling_time_sleep = True
            logging.critical(
                "Time.sleep has been called within thread %s."
                "time.sleep should be used with caution within lumibot, "
                "especially in backtesting mode." % thread_name
            )
    old_time_sleep(sleeptime)
