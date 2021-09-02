import logging
import logging.config
import time
import json
from functools import wraps

import os 
script_directory = os.path.dirname(os.path.abspath(__file__))
import sys 
sys.path.append(script_directory)

with open(f'{script_directory}/logging.config') as f:
    LOG_CONFIG = json.loads(f.read())

logging.config.dictConfig(LOG_CONFIG)
logger = logging.getLogger("data")


def timed(func):
    """This decorator prints the execution time for the decorated function."""

    @wraps(func)
    def wrapper(*args, **kwargs):
        start = time.time()
        result = func(*args, **kwargs)
        end = time.time()
        logger.debug("{} ran in {}s".format(func.__name__, round(end - start, 2)))
        return result

    return wrapper

