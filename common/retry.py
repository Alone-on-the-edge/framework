import sys
sys.path.append("d:\Project\ssot")

import math
from time import sleep

from common.logger import get_logger

class RetryOptions(object):
    def __init__(self, initital_interval_seconds: int = 3,
        backoff_coefficient: int = 2,
        max_attempts: int = 5,
        max_interval_seconds: int = 60,
        retryable_exception = (Exception,)):

        self.initial_interval_seconds = initital_interval_seconds
        self.backoff_coefficient = backoff_coefficient
        self.max_attempts = max_attempts
        self.max_interval_seconds = max_interval_seconds
        self.retryable_exceptions = retryable_exception

    def calculate_sleep_time(self, attempt: int):
        sleep_seconds = math.pow(self.backoff_coefficient,attempt - 1) * self.initial_interval_seconds

        return sleep_seconds if sleep_seconds <= self.max_interval_seconds else self.max_interval_seconds
    

class _RetryDecorator:
    def __init__(self, func, retry_options: RetryOptions = RetryOptions()):
        self.func = func
        self.retry_options = retry_options

    def __call__(self, *args, **kwargs):
        current_attempt = 0
        while True:
            try:
                return self.func(*args, **kwargs)
            except self.retry_options.retryable_exceptions:
                current_attempt += 1
                get_logger("retry logger") \
                .warn(f"Retrying func with name {self.func.__name__} & curr_attempt = {current_attempt}")

                if current_attempt <= self.retry_options.max_attempts:
                    sleep_time_seconds = self.retry_options.calculate_sleep_time(current_attempt)
                    sleep(sleep_time_seconds)
                else:
                    raise

def retry(function=None, retry_options: RetryOptions = RetryOptions()):
    if function:
        return _RetryDecorator(function)
    else:
        def wrapper(func):
            return _RetryDecorator(func, retry_options)

        return wrapper
