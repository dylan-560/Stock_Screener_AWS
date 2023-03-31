import datetime
import logging
from pytz import timezone
import os

try:
    from get_screens import environment_vars
except:
    pass

ENVIRONMENT = 'AWS production'

if ENVIRONMENT == 'AWS production':
    from get_screens.settings.AWS_prod import *
elif ENVIRONMENT == 'AWS test':
    from get_screens.settings.AWS_test import *
elif ENVIRONMENT == 'local production':
    from get_screens.settings.local_prod import *
elif ENVIRONMENT == 'local test':
    from get_screens.settings.local_test import *
elif ENVIRONMENT == 'local and AWS production':
    from get_screens.settings.local_and_AWS_prod import *

def datetime_now(as_str=False):
    now = datetime.datetime.now(timezone('America/Chicago'))
    now = datetime.datetime.strftime(now,'%Y-%m-%d %H:%M:%S')
    if as_str:
        return now
    else:
        return datetime.datetime.strptime(now,'%Y-%m-%d %H:%M:%S')

# RATE LIMITING / KEY HANDLING
RATE_LIMITING = True        # if true implements rate limiting based off rate settings for each resource key
REQUEST_TIMEOUT = 5         # seconds before timeout from data sources
HOURS_TO_UNEXCLUDE = 47     # number of hours to try a key marked excluded again
MAX_CONSEC_TRIES = 3        # maximum number of consecutive times a key can recieve an error before
                            # its marked as excluded and stops being used\

# DATABASE
PERSISTED_KEYS_TABLE_NAME = '_persisted_keys'
ERRORS_TABLE_NAME = '_errors_log'
KEEP_RAW_SCREEN_DAYS = 3 # number of days to keep raw screen data for

LOCAL_DB_PARAMS = {
    'location':'LOCAL',
    'host':os.environ['LOCAL_DB_HOST'],
    'user':os.environ['LOCAL_DB_USER'],
    'password':os.environ['LOCAL_DB_PASS'],
    'db_name':os.environ['LOCAL_DB_NAME']
}

AWS_DB_PARAMS = {
    'location':'AWS',
    'host':os.environ['AWS_DB_HOST'],
    'user':os.environ['AWS_DB_USER'],
    'password':os.environ['AWS_DB_PASS'],
    'db_name':os.environ['AWS_DB_NAME']
}

# ERROR REPORTING / LOGGING
LOGGING_ARGS = {
    "level": logging.INFO,
    "format": "[%(levelname)s] %(message)s",
    "datefmt": "%d-%b-%y %H:%M",
    "force": True,
}

logging.basicConfig(**LOGGING_ARGS)

KEEP_LOG_DAYS = 3                   # number of days to keep logs for

ERRORS_LIST = []
def record_errors(msg,level='ERROR'):
    if SAVE_ERRORS:
        x = {
            'datetime':datetime_now(),
            'level':level,
            'message':msg.replace('\t','')
        }
        ERRORS_LIST.append(x)
