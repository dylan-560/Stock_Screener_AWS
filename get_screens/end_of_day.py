import random
import helper_functions
import pandas as pd
from report_errors import create_report_msg
from save_errors import save_errors_to_DB, delete_old_errors_from_DB
from get_screens.settings._settings import *
from key_lists import save_keys_info, get_keys
from db_connect import Database
from data_sources import QuotesData, StockStats, IntradayCandles

try:
    import environment_vars
except:
    pass

def get_EOD(event=None, context=None):

    def TEST_run_EOD():

        def save_final(screen_list):

            def save_to_db(save_list, db_params):
                db = Database(**db_params)
                db.insert_final_results(screens_list=save_list)

            ##########################################################################
            logging.info('-----------------------------------------------------------------')
            logging.info('SAVING FINAL SCREENER LIST')
            logging.info('-----------------------------------------------------------------')

            if SAVE_TO_LOCAL:
                save_to_db(save_list=screen_list, db_params=LOCAL_DB_PARAMS)
            if SAVE_TO_AWS:
                save_to_db(save_list=screen_list, db_params=AWS_DB_PARAMS)

        ###########################################################################################

        now = datetime_now()

        logging.info('-----------------------------------------------------------------')
        logging.info(f'\t\t\t RUNNING END OF DAY (TEST): {str(now.date())}')
        logging.info('-----------------------------------------------------------------')

        screener_list = [
            {'symbol': 'RETA', 'market_cap': 3421.719, 'prev_close': 31.17, 'shares_outstanding': 31.837, 'sector': 'Healthcare',
             'industry': 'Biotechnology', 'shares_float': 27.448, 'shares_short': 8.828, 'short_perc_float': 40.65,
             'max_pct_gain': 204.78, 'ext_market_activity': 181.81, 'close_to_range': 89.84, 'prev_close_to_200sma': -5.32,
             'ohlc_data': '[{"Date":"2023-03-01 08:30:00","Open":85.75,"High":89.275,"Low":85.125,"Close":85.54,"Volume":1206401.0},'
                          '{"Date":"2023-03-01 08:35:00","Open":84.99,"High":86.47,"Low":84.91,"Close":85.37,"Volume":484251.0},'
                          '{"Date":"2023-03-01 08:40:00","Open":85.49,"High":87.66,"Low":85.49,"Close":86.34,"Volume":371065.0}]',
             'dollar_volume': 1433.084, 'datetime': str(now.date())}]

        keys = get_keys()

        record_errors(msg='test_error 1', level='CRITICAL')
        record_errors(msg='test_error 2')

        save_final(screen_list=screener_list)

        if PERSIST_KEY_INFO:
            save_keys_info(keys_dict=keys)

        if SAVE_ERRORS:
            save_errors_to_DB()

    def run_EOD():
        # CONSTANTS ###############################################################################################
        # prev close to current open and current open to current high
        REV_SPLIT_CUTOFF = {'PCO': 5, 'OH': 7}

        # percentage of curr day high to low to recognize and exlcude a buyout
        BUYOUT_CUTOFF = 5

        # designates metrics that require averaging
        DO_AVERAGE = ['market_cap', 'shares_float', 'short_perc_float', 'shares_short', 'shares_outstanding']

        # designates quote metrics that need to be timestamped after market close before being used for other metrics
        TIME_SENSITIVE_QUOTES = ['high', 'low', 'close', 'volume']

        # hour the market closes
        MARKET_CLOSE_HOUR = 15

        ############################################################################################################

        class ScreenResult():

            def __init__(self, **kwargs):

                self.symbol = kwargs["symbol"].upper()

                if 'timestamp' in kwargs:
                    self.timestamp = datetime.datetime.strptime(kwargs['timestamp'], '%Y-%m-%d %H:%M:%S')
                else:
                    self.timestamp = datetime_now()

                if 'source' in kwargs:
                    self.source = kwargs['source']

                for key, value in kwargs.items():
                    if key in DO_AVERAGE:
                        setattr(self, key, {self.source: value})
                    elif key in TIME_SENSITIVE_QUOTES:
                        setattr(self, key, value)
                        setattr(self,key+'_timestamp',self.timestamp)

                    elif key not in ['symbol','timestamp','source']:
                        setattr(self, key, value)

            def timestamped_after_close(self, attr_names):
                """
                Tests if all attributes and attr_names have a timestamp that is after the market close
                :returns bool
                """

                ret_vals = []
                for name in attr_names:
                    if hasattr(self, name) and name in TIME_SENSITIVE_QUOTES:
                        if getattr(self, name + '_timestamp').hour >= MARKET_CLOSE_HOUR:
                            ret_vals.append(True)
                            continue
                    ret_vals.append(False)

                return all(i for i in ret_vals)

            def to_dict(self):
                """
                Converts class instance to dictionary
                """
                return {key: value for key, value in self.__dict__.items()}

            def conglomerate(self, other):
                """
                combines ScreenResults instances
                """

                for key, value in vars(other).items():
                    key = key.lower()

                    if key == 'pct_change':
                        if hasattr(self, key) and hasattr(other, key):
                            if getattr(self, key) < getattr(other,key):
                                setattr(self, key, value)

                    elif key in DO_AVERAGE:
                        if self.source != other.source:
                            if hasattr(self, key):
                                sources = {**getattr(self, key), **value}
                            else:
                                sources = value

                            setattr(self, key, sources)

                    elif key in TIME_SENSITIVE_QUOTES:
                        if hasattr(other, key):
                            if self.timestamp < other.timestamp:
                                # if self volume is greater than other volume, keep self but set timestamp to other
                                if key.lower() == 'volume' and hasattr(self, 'volume') and other.volume < self.volume:
                                    setattr(self, key + '_timestamp', other.timestamp)
                                else:
                                    setattr(self, key, value)
                                    setattr(self, key+'_timestamp',other.timestamp)

                    elif not hasattr(self, key) and key not in ['source','timestamp','symbol'] \
                            and not key.endswith('_timestamp'):
                        setattr(self, key, value)

                if self.timestamp < other.timestamp:
                    self.timestamp = other.timestamp

            def do_averages(self, attr_name):

                def numeric_stats_weighted_avg(value_list):

                    if not value_list:
                        return None

                    # if the list len <= 2 or all values in the list are the same
                    if len(value_list) <= 2 or all(x == value_list[0] for x in value_list):  # just do regular average
                        avg = sum(value_list) / len(value_list)
                        return avg

                    # if theres 3 or more unique items stored in the value list
                    if len(value_list) >= 3:
                        avg_dict = {}

                        for comp_num in value_list:
                            row = []

                            for next_num in value_list:
                                if value_list.index(comp_num) != value_list.index(next_num) and comp_num != next_num:
                                    diff = 100 / (abs(comp_num - next_num))
                                    row.append(diff)

                            row_sum = sum(row)
                            avg_dict.update({comp_num: row_sum})  # sums all proximity measures

                        val_sum = sum(avg_dict.values())

                        # sums together the weighted avgs
                        avg = 0
                        for k, v in avg_dict.items():
                            try:
                                avg += (k * (v / val_sum))
                            except ZeroDivisionError:
                                pass

                        return avg

                ##############################################################
                try:
                    attr_vals = getattr(self, attr_name)
                    attr_vals = list(attr_vals.values())
                    attr_vals = [v for v in attr_vals if v]

                    for val in attr_vals:
                        val = float(helper_functions.string_num_converter(value=val))

                    cluster_avg = numeric_stats_weighted_avg(value_list=attr_vals)

                    if attr_name != 'short_perc_float':
                        cluster_avg = int(cluster_avg)

                    setattr(self, attr_name, cluster_avg)
                except Exception as e:
                    msg = f"{e}, on ticker: {self.symbol}, problem getting averages"
                    logging.error(f'\t\t{msg}')
                    record_errors(msg=msg)
                    setattr(self, attr_name, None)

            def infer_stats(self):
                """
                Attempts to infer missing metrics from metrics that do exist
                """

                try:
                    # get short perc float if float and shares short
                    if not hasattr(self,'short_perc_float') or not self.short_perc_float:
                        logging.info(f'\t\t\t no short pct float for {self.symbol}, inferring... ')
                        setattr(self, 'short_perc_float', (self.shares_short / self.shares_float) * 100)
                except:
                    logging.info(f'\t\t\t\t cant get')
                    pass

                try:
                    # get float if shares short and short perc float
                    if not hasattr(self,'shares_float') or not self.shares_float:
                        logging.info(f'\t\t\t no float for {self.symbol}, inferring... ')
                        setattr(self, 'shares_float', (self.shares_short / self.short_perc_float) * 100)
                except:
                    logging.info(f'\t\t\t\t cant get')
                    pass

                try:
                    # get shares short if float and short perc float
                    if not hasattr(self,'shares_short') or not self.shares_float:
                        logging.info(f'\t\t\t no shares short for {self.symbol}, inferring... ')
                        setattr(self, 'shares_short', (self.shares_short / self.short_perc_float) * 100)
                        setattr(self, 'shares_short', ((self.short_perc_float / 100) * self.shares_float))
                except:
                    logging.info(f'\t\t\t\t cant get')
                    pass

            def filter_by_volume(self):
                """
                Filters out illiquid tickers
                :return True = dont exclude
                :return False = do exclude
                """
                try:
                    keep = True

                    # if the instance has a volume attribute and its timestamped to after the close
                    if self.timestamped_after_close(attr_names=['volume']):
                        price = 0.0
                        if hasattr(self, 'high') and hasattr(self,'low'):
                            price = (self.high + self.low) / 2

                        if price:
                            if price <= 1 and self.volume < 15_000_000:
                                keep = False

                            elif price > 1 and price <= 5 and self.volume < 6_000_000:
                                keep = False

                            elif price > 5 and price <= 10 and self.volume < 4_000_000:
                                keep = False

                            elif price > 10 and self.volume < 3_000_000:
                                keep = False

                    if not keep:
                        logging.info(f'\t\t {self.symbol} excluded on volume')

                    return keep
                except Exception as e:
                    msg = f"PROBLEM WITH VOLUME FILTER, {self.symbol}: {e}"
                    logging.critical(f'\t\t{msg}')
                    record_errors(msg=msg, level="CRITICAL")

            def filter_buyouts(self):
                """
                Filters out tickers with price action that indicates a buyout
                i.e. price action is flat
                :return True = dont exclude
                :return False = do exclude
                """
                try:
                    if self.timestamped_after_close(attr_names=['high', 'low']):
                        HL_perc_range = ((self.high - self.low) / self.low) * 100
                        if HL_perc_range <= BUYOUT_CUTOFF:
                            logging.info(f'\t\t {self.symbol} excluded because suspected buyout')
                            return False

                    return True
                except Exception as e:
                    msg = f"PROBLEM WITH BUYOUT FILTER, {self.symbol}: {e}"
                    logging.critical(f'\t\t{msg}')
                    record_errors(msg=msg, level="CRITICAL")
                    return False

            def filter_reverse_splits(self):
                """
                Filter out tickers with price action that indicates a potential reverse split
                i.e. no volatility
                :return True = dont exclude
                :return False = do exclude
                """
                try:
                    if hasattr(self, 'prev_close') and self.timestamped_after_close(attr_names=['open', 'high']):
                        OH_range = ((self.high - self.open) / self.open) * 100
                        PCO_range = ((self.open - self.prev_close) / self.prev_close) * 100

                        if PCO_range <= REV_SPLIT_CUTOFF['PCO'] and OH_range < REV_SPLIT_CUTOFF['OH']:
                            logging.info(f'\t\t {self.symbol} excluded because suspected reverse split')
                            return False

                    return True
                except Exception as e:
                    msg = f"PROBLEM WITH REVERSE SPLIT FILTER, {self.symbol}: {e}"
                    logging.critical(f'\t\t{msg}')
                    record_errors(msg=msg, level="CRITICAL")
                    return False

            def filter_ticker(self):
                """
                Filters out wierd tickers

                :return True = dont exclude
                :return False = do exclude
                """
                try:
                    ex_list = ['_', '-', '.']
                    if [e for e in ex_list if e in self.symbol]:
                        logging.info(f'\t\t {self.symbol} excluded because ticker')
                        return False
                    elif len(self.symbol) == 5 and self.symbol[-1] == 'W':
                        logging.info(f'\t\t {self.symbol} excluded because ticker')
                        return False
                    else:
                        return True
                except Exception as e:
                    msg = f"PROBLEM WITH TICKER FILTER, {self.symbol}: {e}"
                    logging.critical(f'\t\t{msg}')
                    record_errors(msg=msg, level="CRITICAL")
                    return False

            def calc_max_pct_gain(self):
                """
                Calculates the largest percentage gain reached for the day
                """
                try:
                    CO_CH = (self.high - self.open) / self.open
                    PC_CH = (self.high - self.prev_close) / self.prev_close
                    self.max_pct_gain = (max(CO_CH, PC_CH)) * 100
                except Exception as e:
                    msg = f"{e}, on ticker: {self.symbol}"
                    logging.critical(f'\t\t{msg}')
                    record_errors(msg=msg,level="CRITICAL")
                    self.max_pct_gain = None

            def ext_market_activity(self):
                """
                Calculates perc gain/loss for previous day aftermarket and current day premarket
                """

                try:
                    self.ext_market_activity = ((self.open - self.prev_close) / self.prev_close) * 100
                except Exception as e:
                    msg = f"{e}, on ticker: {self.symbol}"
                    logging.error(f'\t\t{msg}')
                    record_errors(msg=msg)
                    self.ext_market_activity = None

            def close_to_range(self):
                """
                Calculates the percentage of the days range the ticker closed at
                i.e 0 = closed at low of day, 100 = closed at high of day
                """
                try:
                    self.close_to_range = ((self.close - self.low) / (self.high - self.low)) * 100
                except Exception as e:
                    msg = f"{e}, on ticker: {self.symbol}"
                    logging.error(f'\t\t{msg}')
                    record_errors(msg=msg)
                    self.close_to_range = None

            def sma200_distance(self):
                """
                Calculates the percentage distance away from the 200 day simple moving average
                """
                try:
                    self.prev_close_to_200sma = ((self.prev_close - self.sma200) / self.sma200) * 100
                except Exception as e:
                    msg = f"{e}, on ticker: {self.symbol}"
                    logging.error(f'\t\t{msg}')
                    record_errors(msg=msg)
                    self.prev_close_to_200sma = None

            def calc_dollar_volume(self):
                """
                Goes through each 5 minute candle of the day and takes the midpoint price of the open and close
                and multiplies that by the candles volume to get dollar volume. Then adds each candles dollar volume
                up to get the days dollar volume
                """

                try:
                    candle_dollar_volume = ((self.ohlc_data['Open'] + self.ohlc_data['Close']) / 2) * self.ohlc_data['Volume']
                    quote_dollar_volume = ((self.high + self.low) / 2) * self.volume
                    self.dollar_volume = int(max(candle_dollar_volume.sum(),quote_dollar_volume))
                except Exception as e:
                    msg = f"{e}, on ticker: {self.symbol}"
                    logging.error(f'\t\t{msg}')
                    record_errors(msg=msg)
                    self.dollar_volume = None

            def finalize_data(self):
                """
                Common sizes stock stats into millions of shares and rounds other metrics to certain decimal palces
                """
                def make_adjustment(k, v):
                    if k == 'ohlc_data':
                        v['Date'] = v['Date'].dt.strftime('%Y-%m-%d %H:%M:%S')
                        v = v.to_json(orient='records')
                        return v

                    if not v:
                        return None

                    if k in common_size:
                        v /= 1_000_000
                    if k in round_two:
                        v = round(v, 2)
                    if k in round_three:
                        v = round(v, 3)
                    if k in round_four:
                        v = round(v, 4)

                    return v

                ###########################################################################

                common_size = ['market_cap', 'shares_float', 'shares_short', 'shares_outstanding', 'dollar_volume']
                round_two = ['max_pct_gain', 'short_perc_float', 'ext_market_activity', 'close_to_range','prev_close_to_200sma']
                round_three = ['market_cap', 'shares_float', 'shares_short', 'shares_outstanding', 'dollar_volume']
                round_four = ['prev_close']
                keep_keys = ['symbol', 'datetime', 'short_perc_float', 'shares_short', 'shares_outstanding',
                             'shares_float', 'sector', 'prev_close', 'max_pct_gain', 'market_cap',
                             'industry', 'close_to_range', 'ext_market_activity', 'dollar_volume',
                             'prev_close_to_200sma','ohlc_data']

                if isinstance(self.timestamp,datetime.datetime):
                    setattr(self, 'datetime', datetime.datetime.strftime(self.timestamp, '%Y-%m-%d').split()[0])

                delete_vars = []
                for metric, value in vars(self).items():
                    if metric in keep_keys:
                        try:
                            value = make_adjustment(k=metric, v=value)
                            setattr(self, metric, value)
                        except Exception as e:
                            msg = f"{self.symbol}, PROBLEM FINALIZING KEY: {metric},{e}"
                            logging.critical(f'\t\t{msg}')
                            record_errors(msg=msg,level='CRITICAL')
                    else:
                        delete_vars.append(metric)

                for metric in delete_vars:
                    delattr(self, metric)

        ############################################################################################################
        def shuffle_source_list(s_list):
            if not s_list:
                return s_list
            source_indexes = list(range(0, len(s_list)))
            random.shuffle(source_indexes)
            return [s_list[idx] for idx in source_indexes]

        ############################################################################################################

        def initial_tests():

            def test_trading_day():
                if helper_functions.is_trading_day(curr_date=now):
                    return True
                else:
                    logging.info('IS NOT A TRADING DAY')
                    return False

            def test_db_connections():

                connections_working = []

                if 'AWS' in ENVIRONMENT:
                    db = Database(**AWS_DB_PARAMS)
                    if not db.test_connection():
                        return False

                if 'local' in ENVIRONMENT:
                    db = Database(**LOCAL_DB_PARAMS)
                    if db.test_connection():
                        return False


                return True

            ##############################################################################

            ok_to_run = [test_trading_day(),test_db_connections()]

            if not all(i for i in ok_to_run):
                create_report_msg(func_name='TEST_run_EOD')

                if SAVE_ERRORS:
                    save_errors_to_DB()

                delete_old_errors_from_DB()
                delete_old_raw_screens()

                exit()

        def get_screen_data():

            def pull_from_db(db_params):
                db = Database(**db_params)
                return db.pull_daily_raw_screen_results()

            ############################################################################################

            logging.info('PULLING DAILY SCREEN DATA')
            logging.info('-----------------------------------------------------------------')

            if PULL_FROM_LOCAL:
                return pull_from_db(db_params=LOCAL_DB_PARAMS)
            if PULL_FROM_AWS:
                return pull_from_db(db_params=AWS_DB_PARAMS)

        def populate(screen_list):
            logging.info('POPULATING INSTANCES')
            logging.info('-----------------------------------------------------------------')
            ret_list = []
            try:
                now = datetime_now()
                for screen in screen_list:
                    screen_date = datetime.datetime.strptime(screen['timestamp'],'%Y-%m-%d %H:%M:%S').date()
                    if screen_date == now.date():
                        screen_obj = ScreenResult(**screen)
                        ret_list.append(screen_obj)
            except Exception as e:
                ret_list = []
                msg = f"PROBLEMS POPULATING CLASS INSTANCE, {e}"
                logging.critical(f'\t{msg}')
                record_errors(msg=msg, level='CRITICAL')

            return ret_list

        def elminate_screen_list_redundancies(screen_list):
            logging.info('REMOVING SCREEN LIST REDUNDANCIES')
            logging.info('-----------------------------------------------------------------')

            found = {}
            try:
                for screen in screen_list:

                    if screen.symbol in found:
                        found[screen.symbol].conglomerate(screen)
                    else:
                        found[screen.symbol] = screen

                ret_list = [v for v in found.values()]
                ret_list = sorted(ret_list, key=lambda d: d.pct_change, reverse=True)
            except Exception as e:
                ret_list = []
                msg = f"PROBLEM ELIMINATING REDUNDANCIES, {e}"
                logging.critical(f'\t{msg}')
                record_errors(msg=msg, level='CRITICAL')

            return ret_list

        def prelim_screen_filter(screen_list):
            logging.info('RUNNING PRELIM SCREEN ON SCREENS LIST')
            logging.info('-----------------------------------------------------------------')
            ret_list = []

            for screen in screen_list:
                if screen.filter_ticker() and \
                        screen.filter_buyouts() and \
                        screen.filter_by_volume() and \
                        screen.filter_reverse_splits():

                    ret_list.append(screen)

            return ret_list

        def pull_quotes_data(screen_list):

            ###################################################################
            logging.info(f'GETTING QUOTES FOR {len(screen_list)} TICKERS')
            logging.info('-----------------------------------------------------------------')

            quotes = QuotesData()

            quotes_APIs = [
                {'API': quotes.YF_quotes,'keys':keys['YF_API']},
                {'API': quotes.alpaca_quotes, 'keys': keys['alpaca_keys']},
                {'API': quotes.alphavantage_quotes, 'keys': keys['alphavantage_keys']},
                {'API': quotes.alphavantage_RAPID_API_quotes, 'keys': keys['alphavantage_RAPI_keys']},
                {'API': quotes.finnhub_quotes, 'keys': keys['finnhub_keys']},
                {'API': quotes.twelve_data_quotes, 'keys': keys['twelve_data_keys']},
                {'API': quotes.stock_prices_API_quotes, 'keys': keys['stock_prices_API_keys']},
                {'API': quotes.fidelity_quotes, 'keys': keys['fidelity_keys']},
            ]

            quotes_APIs = shuffle_source_list(s_list=quotes_APIs)

            for enum, screen in enumerate(screen_list):

                # if screen already has quote data
                if screen.timestamped_after_close(attr_names=['high', 'low', 'close', 'volume']) \
                        and hasattr(screen,'prev_close') and hasattr(screen,'open'):
                    logging.info('-----------------------------------------------------------------')
                    logging.info(f'\t\t {enum+1}. {screen.symbol} has complete quote data')
                    logging.info('-----------------------------------------------------------------')

                else:
                    logging.info('-----------------------------------------------------------------')
                    logging.info(f'\t\t {enum+1}. {screen.symbol}')
                    logging.info('-----------------------------------------------------------------')

                    quotes_APIs = shuffle_source_list(s_list=quotes_APIs)
                    quotes_found = False

                    for API in quotes_APIs:

                        API['keys'] = shuffle_source_list(s_list=API['keys'])
                        API['keys'], results = API['API'](ticker=screen.symbol,
                                                          keys_list=API['keys'])

                        if results:
                            quotes_instance = ScreenResult(**results)
                            screen.conglomerate(quotes_instance)
                            API['source_errors'] = 0
                            quotes_found = True
                            break

                    if not quotes_found:
                        msg = f'ALL RESOURCES EXHAUSTED FINDING QUOTES FOR {screen.symbol}'
                        logging.critical(f'{msg}')
                        record_errors(msg=msg, level="CRITICAL")

            return screen_list

        def filter_ticker_list(screen_list):
            logging.info('FILTERING OUT ILLIQUIDS AND BUYOUTS')
            logging.info('-----------------------------------------------------------------')

            ret_list = []
            try:
                for screen in screen_list:
                    if screen.filter_buyouts() and screen.filter_by_volume() and screen.filter_reverse_splits():
                        ret_list.append(screen)
            except Exception as e:
                ret_list = []
                msg = f"PROBLEM FILTERING TICKERS, {e}"
                logging.critical(f'\t{msg}')
                record_errors(msg=msg, level="CRITICAL")

            return ret_list

        def get_max_pct_gain(screen_list):

            logging.info('CALCULATING MAX PCT GAINS')
            logging.info('-----------------------------------------------------------------')

            ret_list = []
            for screen in screen_list:
                screen.calc_max_pct_gain()
                if hasattr(screen, 'max_pct_gain') and screen.max_pct_gain:
                    ret_list.append(screen)
                else:
                    msg = f"CANT CALCUATE MAX PCT GAIN FOR {screen.symbol}"
                    logging.critical(f'{msg}')
                    record_errors(msg=msg, level="CRITICAL")

            return ret_list

        def get_top_x(screen_list,top_x):
            logging.info('SORTING AND RETURNING TOP GAINERS')
            logging.info('-----------------------------------------------------------------')

            ret_list = sorted(screen_list, key=lambda d: d.max_pct_gain, reverse=True)
            ret_list = ret_list[:top_x]

            logging.info('\t Returning Tickers:')
            for enum,screen in enumerate(ret_list):
                logging.info(f'\t\t{enum+1}. {screen.symbol} {round(screen.max_pct_gain,2)}%')

            logging.info('-----------------------------------------------------------------')
            logging.info('-----------------------------------------------------------------')
            logging.info('-----------------------------------------------------------------')
            return ret_list

        def pull_stats_data(screen_list):

            def get_stats(screen,stats_APIs):

                stats_found = False
                for API in stats_APIs:

                    # if all keys for that source are marked as excluded move on to the next source
                    if all(k['key_exclude'] for k in API['keys']):
                        continue

                    API['keys'] = shuffle_source_list(s_list=API['keys'])
                    API['keys'], results = API['API'](ticker=screen.symbol,keys_list=API['keys'])

                    if results:
                        stats_found = True
                        stats_instance = ScreenResult(**results)
                        screen.conglomerate(stats_instance)

                if not stats_found:
                    msg = f"{screen.symbol}: ALL PRIMARY STATS SOURCES FAILED"
                    logging.critical(f'\t{msg}')
                    record_errors(msg=msg, level="CRITICAL")

                return screen, stats_APIs

            def get_stats_contingency(screen, scrapers):

                def stats_complete():
                    return [s for s in test_stats if hasattr(screen,s) and getattr(screen,s)]

                ################################################

                test_stats = [
                    'market_cap',
                    'shares_oustanding',
                    'shares_float',
                    'short_perc_float',
                    'shares_short'
                ]

                if not stats_complete():
                    logging.info(f'\t\t\t{screen.symbol}: stats incomplete, running contingencies')

                    scrapers = shuffle_source_list(s_list=scrapers)
                    stats_found = False
                    for site in scrapers:
                        site['keys'], results = site['source'](ticker=screen.symbol,keys_list=site['keys'])

                        if results:
                            stats_found = True
                            stats_instance = ScreenResult(**results)
                            screen.conglomerate(stats_instance)

                            if stats_complete():
                                logging.info(f'\t\t\t\t{screen.symbol}: stats completed')
                                break

                    if not stats_found:
                        msg = f"{screen.symbol}: ALL CONTINGENCY STATS SOURCES FAILED"
                        logging.critical(f'\t\t\t\t{msg}')
                        record_errors(msg=msg, level="CRITICAL")

                return screen, scrapers

            def get_sma_contingency(screen, sma_APIs):
                # TODO test if the company even has 200 days of trading history before running this

                if not hasattr(screen,'sma200') or not screen.sma200:
                    logging.info(f'\t\t{screen.symbol} missing 200sma, running contingencies...')

                    sma_APIs = shuffle_source_list(s_list=sma_APIs)
                    sma_found = False

                    for API in sma_APIs:
                        API['keys'] = shuffle_source_list(s_list=API['keys'])

                        API['keys'], results = API['API'](ticker=screen.symbol,
                                                          keys_list=API['keys'])

                        if results:
                            logging.info(f'\t\t\t{screen.symbol} 200sma found')
                            # setattr(screen,'sma200',results['sma200'])
                            screen.sma200 = results['sma200']
                            sma_found = True
                            break

                    if not sma_found:
                        msg = f"{screen.symbol}: CANT GET 200SMA FROM CONTINGENCY STATS SOURCES"
                        logging.error(f'\t\t\t{msg}')
                        record_errors(msg=msg, level="ERROR")

                return screen, sma_APIs

            ####################################################################################

            logging.info('GETTING STATS DATA')
            logging.info('-----------------------------------------------------------------')

            if not screen_list:
                return screen_list

            stats = StockStats()

            stats_APIs = [
                {'API': stats.alphavantage_stats, 'keys': keys['alphavantage_keys']},
                {'API': stats.alphavantage_RAPID_API_stats, 'keys': keys['alphavantage_RAPI_keys']},
                {'API': stats.cnbc_stats, 'keys': keys['cnbc_keys']},
                {'API': stats.yahoo_finance_1_RAPI_stats, 'keys': keys['YF_RAPI_keys']},
                {'API': stats.polygon_stats, 'keys': keys['polygon_keys']},
                {'API': stats.morningstar_API_stats, 'keys': keys['morningstar_keys']},
                {'API': stats.mboum_API_stats, 'keys': keys['mboum_keys']},
                #{'API': stats.seeking_alpha_stats, 'keys': keys['seeking_alpha_keys'], **source_ext},
            ]

            sma_APIs = [
                {'API': stats.contingency_polygon_200sma, 'keys': keys['polygon_keys']},
                {'API': stats.contingency_twelve_data_200sma, 'keys': keys['twelve_data_keys']},
            ]

            scrapers = [
                {'source':stats.contingency_marketwatch_stats, 'keys': keys['marketwatch_site']},
                {'source':stats.congingency_stock_analysis_stats, 'keys': keys['stock_analysis_site']}
            ]

            for enum, screen in enumerate(screen_list):
                logging.info('-----------------------------------------------------------------')
                logging.info(f'\t\t {enum+1}.  {screen.symbol}')
                logging.info('-----------------------------------------------------------------')

                screen, stats_APIs = get_stats(screen=screen,stats_APIs=stats_APIs)
                screen, scrapers = get_stats_contingency(screen=screen, scrapers=scrapers)
                screen, sma_APIs = get_sma_contingency(screen=screen, sma_APIs=sma_APIs)

            return screen_list

        def do_averages(screen_list):
            logging.info('GETTING AVERAGES FOR STATS')
            logging.info('-----------------------------------------------------------------')
            for screen in screen_list:
                for metric in DO_AVERAGE:
                    screen.do_averages(metric)
                    screen.infer_stats()

            return screen_list

        def infer_external_market_activity(screen_list):
            logging.info('CALCULATING PRE/POST MARKET ACTIVITY')
            logging.info('-----------------------------------------------------------------')

            for screen in screen_list:
                screen.ext_market_activity()

            return screen_list

        def get_close_to_range(screen_list):
            logging.info('CALCULATING CLOSE FROM HOD')
            logging.info('-----------------------------------------------------------------')

            for screen in screen_list:
                screen.close_to_range()

            return screen_list

        def get_dist_from_200sma(screen_list):
            logging.info('CALCULATING DISTANCE FROM 200 SMA')
            logging.info('-----------------------------------------------------------------')

            for screen in screen_list:
                screen.sma200_distance()

            return screen_list

        def pull_intraday_candles(screen_list):
            logging.info('PULLING INTRADAY CANDLE DATA')
            logging.info('-----------------------------------------------------------------')

            if not screen_list:
                return screen_list

            ohlc = IntradayCandles()

            ohlc_APIs = [
                {'API': ohlc.API_stocks_intraday, 'keys': keys['API_stocks_keys']},
                {'API': ohlc.YF_intraday, 'keys': keys['YF_API']},
                {'API': ohlc.twelve_data_intraday, 'keys': keys['twelve_data_keys']},
                {'API': ohlc.seeking_alpha_intraday, 'keys': keys['seeking_alpha_keys']},
            ]

            for enum, screen in enumerate(screen_list):
                logging.info('-----------------------------------------------------------------')
                logging.info(f'\t\t{enum+1}. {screen.symbol}')
                logging.info('-----------------------------------------------------------------')

                ohlc_APIs = shuffle_source_list(s_list=ohlc_APIs)
                ohlc_found = False

                for API in ohlc_APIs:

                    API['keys'] = shuffle_source_list(s_list=API['keys'])
                    API['keys'], results = API['API'](ticker=screen.symbol,
                                                      keys_list=API['keys'])
                    if results:
                        if isinstance(results['ohlc_df'],pd.DataFrame) and not results['ohlc_df'].empty:
                            screen.ohlc_data = results['ohlc_df']
                            ohlc_found = True
                            break

                if not ohlc_found:
                    msg = f"ALL RESOURCES EXHAUSTED FOR {screen.symbol}, EXITING"
                    logging.critical(f'{msg}')
                    record_errors(msg=msg, level="CRITICAL")

            return screen_list

        def get_dollar_volume(screen_list):
            logging.info('GETTING DOLLAR VOLUME')
            logging.info('-----------------------------------------------------------------')

            for screen in screen_list:
                screen.calc_dollar_volume()
            return screen_list

        def finalize_results(screen_list):
            logging.info('FINALIZING RESULTS')
            logging.info('-----------------------------------------------------------------')

            for screen in screen_list:
                screen.finalize_data()

            return screen_list

        def save_final(screen_list):

            def save_to_db(save_list, db_params):
                db = Database(**db_params)
                db.insert_final_results(screens_list=save_list)

            ##########################################################################
            logging.info('-----------------------------------------------------------------')
            logging.info('SAVING FINAL SCREENER LIST')
            logging.info('-----------------------------------------------------------------')

            save_list = [screen.to_dict() for screen in screen_list]

            if SAVE_TO_LOCAL:
                save_to_db(save_list=save_list, db_params=LOCAL_DB_PARAMS)
            if SAVE_TO_AWS:
                save_to_db(save_list=save_list, db_params=AWS_DB_PARAMS)

        def delete_old_raw_screens():

            def delete_from_db(db_params):
                db = Database(**db_params)
                db.delete_daily_screen_results()

            ###########################################################

            if SAVE_TO_LOCAL:
                delete_from_db(db_params=LOCAL_DB_PARAMS)
            if SAVE_TO_AWS:
                delete_from_db(db_params=AWS_DB_PARAMS)

        ###############################################################
        now = datetime_now()

        initial_tests()

        keys = get_keys()

        logging.info('-----------------------------------------------------------------')
        logging.info(f'\t\t\t RUNNING END OF DAY: {str(now.date())}')
        logging.info('-----------------------------------------------------------------')

        screener_list = get_screen_data()
        screener_list = populate(screen_list=screener_list)
        screener_list = elminate_screen_list_redundancies(screen_list=screener_list)
        screener_list = prelim_screen_filter(screen_list=screener_list)

        screener_list = pull_quotes_data(screen_list=screener_list)

        screener_list = filter_ticker_list(screen_list=screener_list)
        screener_list = get_max_pct_gain(screen_list=screener_list)
        screener_list = get_top_x(screen_list=screener_list,top_x=10)

        screener_list = pull_stats_data(screen_list=screener_list)

        screener_list = do_averages(screen_list=screener_list)
        screener_list = infer_external_market_activity(screen_list=screener_list)
        screener_list = get_close_to_range(screen_list=screener_list)
        screener_list = get_dist_from_200sma(screen_list=screener_list)

        screener_list = pull_intraday_candles(screen_list=screener_list)

        screener_list = get_dollar_volume(screen_list=screener_list)
        screener_list = finalize_results(screen_list=screener_list)

        if screener_list:
            save_final(screen_list=screener_list)

            if PERSIST_KEY_INFO:
                save_keys_info(keys_dict=keys)

        else:
            msg = f"NO SCREENS FOR {now.date()} FOUND"
            logging.critical(f'{msg}')
            record_errors(msg=msg, level="CRITICAL")

        if SAVE_ERRORS:
            save_errors_to_DB()

        delete_old_errors_from_DB()
        delete_old_raw_screens()

    ##############################################

    ERRORS_LIST = []

    if RUN_AS_TEST:
        TEST_run_EOD()
        create_report_msg(func_name='TEST_run_EOD')
    else:
        run_EOD()
        create_report_msg(func_name='run_EOD')

if __name__ == '__main__':
    get_EOD()
