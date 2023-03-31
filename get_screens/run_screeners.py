import helper_functions
import random
from save_errors import save_errors_to_DB
from data_sources import Screeners
from db_connect import Database
from get_screens.settings._settings import *
from key_lists import save_keys_info,get_keys

try:
    import environment_vars
except:
    pass

def run_screeners(event=None, context=None):

    def save_screens(save_list):

        def save_to_db(db_params):
            db = Database(**db_params)
            db.insert_daily_raw_screen_results(screens_list=save_list)

        ##################################################
        logging.info('-----------------------------------------------------------')
        logging.info('SAVING DAILY SCREEN DATA')
        logging.info('-----------------------------------------------------------')

        if SAVE_TO_LOCAL:
            save_to_db(db_params=LOCAL_DB_PARAMS)
        if SAVE_TO_AWS:
            save_to_db(db_params=AWS_DB_PARAMS)

    def shuffle_source_list(s_list):
        if not s_list:
            return s_list
        source_indexes = list(range(0, len(s_list)))
        random.shuffle(source_indexes)
        return [s_list[idx] for idx in source_indexes]

    ###############################################################################
    now = datetime_now()
    if not helper_functions.is_trading_day(curr_date=now):
        logging.info('IS NOT A TRADING DAY')
        return

    ERRORS_LIST = []

    logging.info('----------------------------------------------------')
    logging.info('\t\t\t\tRUNNING SCREENERS')
    logging.info('----------------------------------------------------')

    keys = get_keys()

    screeners = Screeners()

    screener_sources = [
        {'source': screeners.schwab_screener, 'keys': keys['schwab_keys']},
        {'source': screeners.tradingview_screener, 'keys': keys['tradingview_keys']},
        {'source': screeners.yahoo_finance_screener, 'keys': keys['YF_RAPI_keys']},
        {'source': screeners.finviz_screener, 'keys': keys['finviz_site']},
    ]
    ################################################################################################################
    # MULTI THREADING

    # import concurrent.futures
    # save_list = []
    # results_list = []
    # with concurrent.futures.ThreadPoolExecutor() as executor:
    #     results = [executor.submit(source['source'], source['keys']) for source in screener_sources]
    #
    #     for f in concurrent.futures.as_completed(results):
    #         results_list.append(f.result())
    #
    #     for result in results_list:
    #         #append keys
    #         keys[result[0][0]['key_name']] = result[0]
    #
    #         # append results
    #         save_list += [r for r in result[1] if r]

    ################################################################################################################
    # NON MULTI THREADING

    save_list = []
    for source in screener_sources:

        source['keys'] = shuffle_source_list(s_list=source['keys'])
        source['keys'], results = source['source'](keys_list=source['keys'])

        if results:
            save_list += results
            source['source_errors'] = 0

    ################################################################################################################

    logging.info('----------------------------------------------------')

    save_screens(save_list=save_list)

    if PERSIST_KEY_INFO:
        save_keys_info(keys_dict=keys)
    if SAVE_ERRORS:
        save_errors_to_DB()

if __name__ == '__main__':
    run_screeners()
