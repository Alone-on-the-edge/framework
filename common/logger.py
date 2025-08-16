import logging
from datetime import datetime

from pytz import timezone

def get_logger(name):
    tz = timezone('US/Eastern')

    def timetz(*args):
        return datetime.now(tz).timetuple()
    
    logging.Formatter.converter = timetz
    log = logging.getLogger(name)
    if len(log.handlers) == 0:
        formatter = logging.Formatter('%(asctime)s [%(levelname)s] (%(name)s) - %(message)s')
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)
        log.addHandler(console_handler)
        log.setLevel(logging.INFO)

    return log