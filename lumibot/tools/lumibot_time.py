import logging
import re
import time
from threading import currentThread

old_time_sleep = time.sleep


def lumibot_sleep(sleeptime):
    raise Exception("Needs to be overloaded")


def warning_time_sleep(sleeptime):
    thread_name = currentThread().getName()
    authorized_threads_with_sleep = ["^.*_requesting_data_.*$"]
    if not any([re.match(expr, thread_name) for expr in authorized_threads_with_sleep]):
        logging.critical(
            "time.sleep should be used with caution within lumibot, "
            "especially in backtesting mode."
        )
    old_time_sleep(sleeptime)
