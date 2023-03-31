"""
yfinance:               https://pypi.org/project/yfinance/
finnhub                 https://finnhub.io/docs/api                                 60/min
alphavantage            https://www.alphavantage.co/                                500/day
polygon                 https://polygon.io/docs/stocks/getting-started
alpaca                  https://alpaca.markets/docs/api-references/trading-api/     200/minute

stock prices            https://rapidapi.com/alphawave/api/stock-prices2            500/month
API stocks:             https://rapidapi.com/api4stocks/api/apistocks
twelve data:            https://rapidapi.com/twelvedata/api/twelve-data1
seeking alpha:          https://rapidapi.com/apidojo/api/seeking-alpha              500/month
fidelity                https://rapidapi.com/apidojo/api/fidelity-investments
alphavantage RAPI       https://rapidapi.com/alphavantage/api/alpha-vantage         500/day
cnbc                    https://rapidapi.com/apidojo/api/cnbc
yahoo_finance_1_RAPI    https://rapidapi.com/sparior/api/yahoo-finance15
morningstar             https://rapidapi.com/apidojo/api/ms-finance                 500/month
mboum                   https://rapidapi.com/sparior/api/mboum-finance              500/month

"""

import copy
import json
import time
from get_screens.settings._settings import *
from db_connect import Database

try:
    import environment_vars
except:
    pass

def get_keys():

    # creates blank key stats
    def build_keys(keys_dict, keys_rates):
        logging.info(f'\t\t...creating new key info')
        ret_keys = {}

        for source, key_list in keys_dict.items():
            rate = keys_rates[source]
            add_keys = []
            for enum, key in enumerate(key_list):
                key['key_num'] = enum + 1
                key['key_name'] = source
                key['rate'] = rate  # number of calls allowed in a 60 second time window, default to 40
                key['key_errors'] = 0  # number of consecutive errors encountered when using key
                key['key_exclude'] = False  # killswitch that determines if the key is even used
                key['last_used'] = None  # datetime the key was last used

                add_keys.append(key)

            ret_keys[source] = copy.deepcopy(add_keys)
        return ret_keys

    # pulls key stats from save file
    def persist_keys(keys_dict,keys_rates):

        def fill_in_keys():

            def test_to_unexclude(key):
                if key['key_exclude']:
                    if key['last_used']:
                        hours_since_last_used = (time.time() - float(key['last_used'])) / 3600
                        if hours_since_last_used > HOURS_TO_UNEXCLUDE:
                            key['key_exclude'] = False
                            key['key_errors'] = 0
                return key

            ########################################################
            ret_keys = {}

            try:
                if 'last_saved' in persisted_keys:
                    del persisted_keys['last_saved']

                for name, key_list in persisted_keys.items():
                    if isinstance(key_list,str):
                        key_list = json.loads(key_list)

                    ret_keys[name] = []

                    name_keys = [k.get('key') for k in keys_dict[name]]
                    name_keys = [k for k in name_keys if k]

                    for enum, key_dict in enumerate(key_list):
                        if name_keys:
                            cutoff_key = key_dict['key']
                            full_key = [k for k in name_keys if k.endswith(cutoff_key)][0]
                            key_dict['key'] = full_key

                        if key_dict['last_used']:
                            key_dict['last_used'] = float(key_dict['last_used'])
                        key_dict = test_to_unexclude(key=key_dict)

                        ret_keys[name].append(key_dict)

            except Exception as e:
                msg = f'PROBLEM FILLING IN KEYS: {e}'
                logging.critical(msg)
                record_errors(msg=msg,level='CRITICAL')

            return ret_keys

        def pull_keys_from_db():

            def pull_from_db(db_params):
                db = Database(**db_params)
                return db.pull_key_info()

            #####################################################
            last_saved = None

            if PULL_FROM_LOCAL:
                keys = pull_from_db(db_params=LOCAL_DB_PARAMS)
            if PULL_FROM_AWS:
                keys = pull_from_db(db_params=AWS_DB_PARAMS)

            return keys

        def need_to_build():

            if len(persisted_keys) == 0:
                return True
            elif not persisted_keys['last_saved']:
                return True
            elif ((datetime_now() - persisted_keys['last_saved']).total_seconds()/60/60) > HOURS_TO_UNEXCLUDE:
                return True
            else:
                return False

        ############################################################################
        logging.info(f'\ttrying to find persisted data...')
        ret_keys = {}
        persisted_keys = pull_keys_from_db()

        if need_to_build():
            ret_keys = build_keys(keys_dict=keys_dict, keys_rates=keys_rates)
        else:
            ret_keys = fill_in_keys()
            if not ret_keys:
                ret_keys = build_keys(keys_dict=keys_dict, keys_rates=keys_rates)

        return ret_keys

    ################################################################################
    logging.info('RETRIEVING KEY DATA...')

    rapid_api_keys = [
        {'key': os.environ['RAPID_API_KEY_1']},
        {'key': os.environ['RAPID_API_KEY_2']}
    ]

    # requests per minute
    keys_rates = {
        'stock_prices_API_keys':9.5,        # is 10
        'API_stocks_keys':59,               # is 60
        'twelve_data_keys': 7.8,            # is 8
        'seeking_alpha_keys':290,           # is 300
        'alphavantage_RAPI_keys':4.9,       # is 5
        'cnbc_keys': 290,                   # is 300
        'YF_RAPI_keys':59,                  # is 60 to 300 depending on the source
        'morningstar_keys':290,             # is 300
        'mboum_keys':9.5,                   # is 10
        'fidelity_keys':290,                # is 300
        'alphavantage_keys':4.9,            # is 5
        'tradingview_keys':4.9,             # is 5
        'schwab_keys':4.9,                  # is 5
        'polygon_keys':4.9,                 # is 5
        'finnhub_keys':300,                 # is 1800
        'alpaca_keys':150,                  # is 200
        'finviz_site':3,                    # is scraper
        'YF_API':50,                        # is scraper
        'marketwatch_site':3,               # is scraper
        'stock_analysis_site':3,            # is scraper
    }

    keys_dict = {
        'stock_prices_API_keys':rapid_api_keys,
        'API_stocks_keys':rapid_api_keys,
        'twelve_data_keys':rapid_api_keys,
        'seeking_alpha_keys':rapid_api_keys,
        'alphavantage_RAPI_keys':rapid_api_keys,
        'cnbc_keys':rapid_api_keys,
        'YF_RAPI_keys':rapid_api_keys,
        'morningstar_keys':rapid_api_keys,
        'mboum_keys':rapid_api_keys,
        'fidelity_keys':rapid_api_keys,
        'tradingview_keys':rapid_api_keys,
        'schwab_keys':rapid_api_keys,
        'alphavantage_keys':[{'key': os.environ['ALPHAVANTAGE_KEY_1'], 'key_num': 1}],
        'polygon_keys':[{'key': os.environ['POLYGON_API_KEY_1'], 'key_num': 1},
                        {'key': os.environ['POLYGON_API_KEY_2'], 'key_num': 2}],

        'finnhub_keys':[{'key': os.environ['FINNHUB_1'], 'key_num': 1},
                        {'key': os.environ['FINNHUB_2'], 'key_num': 2}],

        'alpaca_keys': [{}],
        'YF_API':[{}],
        'finviz_site':[{}],
        'marketwatch_site':[{}],
        'stock_analysis_site':[{}]
    }

    if PERSIST_KEY_INFO:
        ret_keys = persist_keys(keys_dict=keys_dict, keys_rates=keys_rates)
    else:
        ret_keys = build_keys(keys_dict=keys_dict, keys_rates=keys_rates)

    return ret_keys

def save_keys_info(keys_dict):

    def cut_keys(keys_dict):
        # cuts key values down so the entire key isnt saved
        ret_keys = {}
        for name, key_list in keys_dict.items():
            ret_keys[name] = []
            for key in key_list:
                key_str = key.get('key')
                if key_str:
                    cutoff_key = key_str[len(key_str) - 6:]
                    key['key'] = cutoff_key
                if key['last_used']:
                    key['last_used'] = str(key['last_used'])

                ret_keys[name].append(key)

        return ret_keys

    def save_to_db(keys_dict,db_params):

        db = Database(**db_params)
        db.update_key_info(keys=keys_dict)

    ########################################################
    logging.info('-----------------------------------------------------------')
    logging.info('UPDATING KEYS INFO')
    logging.info('-----------------------------------------------------------')

    keys_dict = cut_keys(keys_dict=keys_dict)

    if SAVE_TO_LOCAL:
        save_to_db(keys_dict=keys_dict, db_params=LOCAL_DB_PARAMS)
    if SAVE_TO_AWS:
        save_to_db(keys_dict=keys_dict, db_params=AWS_DB_PARAMS)