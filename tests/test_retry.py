import datetime
from retry import retry


ZERO_OR_ONE = 1
COUNTER = 0

def now():
    return datetime.datetime.now()

def get_next():
    global ZERO_OR_ONE
    if ZERO_OR_ONE == 0:
        ZERO_OR_ONE = 1
    else:
        ZERO_OR_ONE = 0

    return ZERO_OR_ONE

@retry(delay=1)
def test_retry_until_seconds_is_div_10():
    assert(now().second % 10 == 0)

@retry(ZeroDivisionError, tries=2, delay=1)
def test_retry_until_not_zero_division_error():
    i = get_next()
    result = 20 / i
    assert(result == 20)

@retry(tries=10)
def test_retry_10_times():
    global COUNTER
    COUNTER += 1
    if COUNTER != 9:
        raise Exception(f'COUNTER not there yet: {COUNTER}')