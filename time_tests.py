import datetime
import time


def show_time():
    start = datetime.datetime.now()
    return start.strftime("%H:%M:%S")


def show_delta(_from):
    _now = datetime.datetime.now()
    delta = _now-_from
    return delta.seconds


now = datetime.datetime.now()
time.sleep(4)
print(show_delta(now))
