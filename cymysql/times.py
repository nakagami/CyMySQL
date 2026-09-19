from datetime import date, datetime, time, timedelta
from time import localtime

Date = date
Time = time
TimeDelta = timedelta
Timestamp = datetime


def DateFromTicks(ticks: float | int) -> date:
    return date(*localtime(ticks)[:3])


def TimeFromTicks(ticks: float | int) -> time:
    return time(*localtime(ticks)[3:6])


def TimestampFromTicks(ticks: float | int) -> datetime:
    return datetime(*localtime(ticks)[:6])

