import time
import pandas as pd
import requests
import xml.etree.ElementTree as ET
from bs4 import BeautifulSoup
from user_agent import generate_user_agent
import yfinance as yf
import json
import helper_functions
from get_screens.settings._settings import *
from save_errors import record_errors



def rate_limiter(key):
    logging.info(f'{key["key_name"].replace("_keys","")} key {key["key_num"]}')

    if RATE_LIMITING:
        try:
            # set last used if there is no value for it
            if not key['last_used']:
                logging.info(f'\t{key["key_name"]}: no time set for last used, setting...')
                key['last_used'] = time.time()

            # wait appropriate seconds given when the key was last used and the assigned rate limit
            elif key['last_used']:
                key['last_used'] = float(key['last_used'])

                passed_secs = time.time() - key['last_used']
                rate_secs = 60/key['rate']
                wait_secs = rate_secs - passed_secs

                if wait_secs > 0 and wait_secs < 20:
                    logging.info(f'\t\t{key["key_name"]}: RATE LIMITER: sleeping for {wait_secs + 0.5} seconds')
                    time.sleep(wait_secs + 0.5)

                # in case wait seconds are calculated at a crazy large time because of datetime discrepencies
                elif wait_secs >= 20:
                    msg = f'RATE LIMITER WAIT SECONDS > 20, THERE MAY BE SOME PROBLEMS WITH DATETIMES'
                    logging.error(f'{msg}')
                    record_errors(msg=msg)
                    time.sleep(13)

                key['last_used'] = time.time()

        except Exception as e:
            msg = f'{key["key_name"]}:RATE LIMITER ERROR: {e}'
            logging.error(f'{msg}')
            record_errors(msg=msg)
            time.sleep(5)

    return key

def pull_from_source(keys_list, source_name, pull_method, ticker=None):

    def handle_key_attempts():

        def add_error(key):
            key['key_errors'] += 1
            if key['key_errors'] >= MAX_CONSEC_TRIES:
                logging.info(f'\t\t\t MAX TRIES EXCEEDED for {source_name} for {ticker}, key:{key["key_num"]}')
                key['key_exclude'] = True
            return key

        ######################################################################

        for key in keys_list:
            if not key['key_exclude']:

                key = rate_limiter(key=key)
                results = pull_method(ticker=ticker, key=key)

                # if resulst were returned
                if results['results']:
                    key['key_errors'] = 0
                    return results['results']

                # if there is response code information
                elif results['resp_code'] and results['resp_code'] != 200:

                    # if the API is having a server error exclude every key for that source
                    if results['resp_code'] >= 500 and results['resp_code'] <= 599:
                        [k.update({'key_exclude': True,'last_used':time.time()}) for k in keys_list]

                    # if rate limits exceeded
                    elif results['resp_code'] == 429:
                        key['key_exclude'] = True

                    # if the info was not found
                    elif results['resp_code'] >= 300 and results['resp_code'] <= 499:
                        key = add_error(key=key)

                    # otherwise mark this key as excluded
                    else:
                        msg = f'{source_name}: {ticker}: key: {key["key_num"]}, ' \
                              f'resp code: {results["resp_code"]} marking to exclude'

                        logging.info(f'\t\t\t{msg}')
                        key['key_exclude'] = True

                # otherwise mark it as an error
                else:
                    key = add_error(key=key)

            else:
                logging.info(f'\t\t\t {source_name}: {ticker}: key: {key["key_num"]} previously marked excluded')

        return None

    ###########################################################################
    if ticker:
        ticker = ticker.upper()

    results = handle_key_attempts()

    ret_vals = {}
    if results:
        if 'screener' in source_name:
            ret_vals = results

        else:
            ret_vals = {
                'symbol': ticker,
                'source': source_name,
                'timestamp': datetime_now(as_str=True),
                **results
            }

    return keys_list, ret_vals

def try_stats(source_dict, ret_vals, key_names):

    def convert_to_capitalized(input):
        input = input.split(" ")
        input = [p.capitalize() for p in input]
        input = " ".join(input)

        return input

    ###########################################################################

    try:
        source_value = source_dict[key_names['source_key']]
        if source_value:

            if 'to_type' in key_names:
                if key_names['to_type'] == 'adjust to millions':
                    source_value *= 1_000_000
                if key_names['to_type'] == 'adjust perc':
                    source_value *= 100
                if key_names['to_type'] == 'capitalize':
                    source_value = convert_to_capitalized(input=source_value)
                if key_names['to_type'] == 'string to num':
                    source_value = helper_functions.string_num_converter(value=source_value)
                if key_names['to_type'] == 'int':
                    source_value = int(source_value)
                if key_names['to_type'] == 'float':
                    source_value = float(source_value)

            ret_vals['results'][key_names['local_key']] = source_value
    except:
        pass

    return ret_vals

#######################################################################################

class Screeners():
    """
    returns either the screenlist or None
    """
    def __display_ret_val_errors(self, r_vals ,info):
        if r_vals['results']:
            logging.info(f'\t\t ...{info} complete')
        else:
            msg = f'{info}, NO SCREENER DATA FOUND'
            logging.error(f'\t\t ...{msg}')
            record_errors(msg=msg)

    def __label_dicts_in_list(self,source_name, input_list):
        ret_list = []
        for symbol_dict in input_list:

            save_dict = {
                'source': source_name,
                'timestamp':datetime_now(as_str=True),
            }

            save_dict = {**save_dict, **symbol_dict}
            ret_list.append(save_dict)
        return ret_list

    def __normalize_names(self, input_list, names_conversion_dict):
        ret_list = []

        for symbol_dict in input_list:
            temp_dict = {}
            for k, v in symbol_dict.items():
                if k in names_conversion_dict.keys():
                    k = names_conversion_dict[k]
                temp_dict[k] = v

            ret_list.append(temp_dict)

        return ret_list

    ###############################################################################
    # scraper
    def finviz_screener(self, keys_list, source_name='finviz_screener'):

        def get_screener(ticker,key):

            def convert_strings_to_nums(input_list, num_convert_names):
                # convert strings to int/floats
                ret_list = []
                for symbol_dict in input_list:
                    for name in num_convert_names:
                        symbol_dict[name] = helper_functions.string_num_converter(value=symbol_dict[name])
                    ret_list.append(symbol_dict)
                return ret_list

            def get_data(resp,ret_vals):
                data = resp.text
                screen_results_df = pd.read_html(data)[9]
                screen_results_df.columns = screen_results_df.iloc[0]
                screen_results_df = screen_results_df.iloc[1:]

                ret_list = screen_results_df.to_dict(orient='records')

                ret_list = convert_strings_to_nums(input_list=ret_list,
                                                   num_convert_names=num_convert_keys)

                ret_list = self.__normalize_names(input_list=ret_list,
                                                  names_conversion_dict=name_conversions)

                ret_list = self.__label_dicts_in_list(source_name='finviz', input_list=ret_list)

                ret_vals['results'] = ret_list

                return ret_vals

            def TEST_get_data(ret_vals):

                ret_list = [
                    {'source': 'finviz', 'timestamp': datetime_now(as_str=True), 'symbol': 'PRVB', 'sector': 'Healthcare',
                     'industry': 'Biotechnology', 'market_cap': 2270000000, 'shares_outstanding': 83120000,
                     'shares_float': 81800000, 'short_perc_float': 10.02, 'shares_short': 8199999, 'volume': 35979597,
                     'prev_close': 6.7, 'close': 24.1, 'pct_change': 259.7},
                    {'source': 'finviz', 'timestamp': datetime_now(as_str=True), 'symbol': 'CIFR', 'sector': 'Financial',
                     'industry': 'Capital Markets', 'market_cap': 484650000, 'shares_outstanding': 247510000,
                     'shares_float': 40980000, 'short_perc_float': 17.23, 'shares_short': 7060000, 'volume': 2478685,
                     'prev_close': 1.32, 'close': 1.82, 'pct_change': 37.88},
                    {'source': 'finviz', 'timestamp': datetime_now(as_str=True), 'symbol': 'EUDA', 'sector': 'Healthcare',
                     'industry': 'Health Information Services', 'market_cap': 40020000, 'shares_outstanding': 11070000,
                     'shares_float': 7070000, 'short_perc_float': 0.33, 'shares_short': 20000, 'volume': 27975266,
                     'prev_close': 2.02, 'close': 2.76, 'pct_change': 36.63}
                ]

                ret_vals['resp_code'] = 200
                ret_vals['results'] = ret_list
                return ret_vals

            ##############################################################
            function_name = 'finviz_screener'
            info = f'{function_name}, key:{key["key_num"]}'

            logging.info(f'\t trying {info}')

            ret_vals = {'resp_code': None, 'results': {}}
            if RUN_AS_TEST:
                logging.info(f'\t\t{info}: TEST')
                ret_vals = TEST_get_data(ret_vals=ret_vals)
                return ret_vals

            name_conversions = {'Ticker': 'symbol',
                                'Sector': 'sector',
                                'Float': 'shares_float',
                                'Outstanding': 'shares_outstanding',
                                'Industry': 'industry',
                                'Float Short': 'short_perc_float',
                                'Short Interest': 'shares_short',
                                'Market Cap': 'market_cap',
                                'Volume': 'volume',
                                'Prev Close': 'prev_close',
                                'Price': 'close',
                                'Change': 'pct_change'}

            num_convert_keys = ['Market Cap', 'Outstanding', 'Volume', 'Price', 'Change', 'Prev Close',
                                'Short Interest', 'Float Short', 'Float']

            url = 'https://finviz.com/screener.ashx?v=152&s=ta_topgainers&f=sh_curvol_o500,ta_perf_d15o&ft=4&o=-change&c=1,3,4,6,24,25,30,84,67,81,65,66'
            headers = {'User-Agent': generate_user_agent()}

            try:
                response = requests.get(url=url, headers=headers, timeout=5)
                ret_vals['resp_code'] = response.status_code
                if response.status_code == 200:

                    ret_vals = get_data(resp=response,ret_vals=ret_vals)
                    self.__display_ret_val_errors(r_vals=ret_vals,info=info)

                else:
                    msg = f'{info}: response code {response.status_code}'
                    logging.error(f'\t\t{msg}')
                    record_errors(msg=msg)

            except Exception as e:
                msg = f'{info}: problem getting data: {e}'
                logging.error(f'\t\t{msg}')
                record_errors(msg=msg)

            return ret_vals

        ##########################################################################

        return pull_from_source(keys_list=keys_list,
                                source_name=source_name,
                                pull_method=get_screener)

    # RAPI
    def tradingview_screener(self, keys_list, source_name='tradingview_screener'):

        def get_screener(ticker,key):

            def clean_tradingview_screener(input_dict):

                ret_list = []
                for row_dict in input_dict['symbols']:
                    symbol_dict = {}

                    symbol_dict['symbol'] = row_dict['s'].split(":")[-1]
                    symbol_dict["perc change"] = row_dict['f'][0]
                    symbol_dict['day volume'] = row_dict['f'][1]
                    ret_list.append(symbol_dict)

                return ret_list

            def get_data(resp,ret_vals):
                resp_dict = json.loads(response.text)
                ret_list = clean_tradingview_screener(input_dict=resp_dict)
                ret_list = self.__normalize_names(input_list=ret_list,
                                                  names_conversion_dict=name_conversions)
                ret_list = self.__label_dicts_in_list(source_name='tradingview', input_list=ret_list)

                ret_vals['results'] = ret_list
                return ret_vals

            def TEST_get_data(ret_vals):

                ret_list = [
                    {'source': 'tradingview', 'timestamp':datetime_now(as_str=True), 'symbol': 'PRVB', 'pct_change': 259.70149253731347, 'volume': 36019932},
                    {'source': 'tradingview', 'timestamp':datetime_now(as_str=True), 'symbol': 'EUDA', 'pct_change': 36.63366336633662, 'volume': 28051138},
                    {'source': 'tradingview', 'timestamp':datetime_now(as_str=True), 'symbol': 'CALT','pct_change': 33.22875816993465, 'volume': 164051}
                ]

                ret_vals['results'] = ret_list
                return ret_vals

            ##############################################################
            function_name = 'tradingview_screener'
            info = f'{function_name}, key:{key["key_num"]}'

            logging.info(f'\t trying {info}')

            ret_vals = {'resp_code': None, 'results': {}}
            if RUN_AS_TEST:
                logging.info(f'\t\t{info}: TEST')
                ret_vals = TEST_get_data(ret_vals=ret_vals)
                return ret_vals

            name_conversions = {'perc change': 'pct_change', 'day volume': 'volume'}

            url = "https://trading-view.p.rapidapi.com/market/get-movers"

            querystring = {"exchange": "US", "name": "percent_change_gainers", "locale": "en"}
            headers = {
                "X-RapidAPI-Key": key['key'],
                "X-RapidAPI-Host": "trading-view.p.rapidapi.com"
            }

            try:
                response = requests.request("GET", url, headers=headers, params=querystring, timeout=REQUEST_TIMEOUT)
                ret_vals['resp_code'] = response.status_code
                if response.status_code == 200:

                    ret_vals = get_data(resp=response, ret_vals=ret_vals)
                    self.__display_ret_val_errors(r_vals=ret_vals,info=info)

                else:
                    msg = f"{info}: response code {response.status_code}"
                    logging.error(f'\t\t{msg}')
                    record_errors(msg=msg)

            except Exception as e:
                msg = f"{info}: problem getting data: {e}"
                logging.error(f'\t\t{msg}')
                record_errors(msg=msg)

            return ret_vals

        ##########################################################################

        return pull_from_source(keys_list=keys_list,
                                source_name=source_name,
                                pull_method=get_screener)

    # RAPI
    def yahoo_finance_screener(self, keys_list, source_name='yahoo_finance_screener'):

        def get_screener(ticker,key):

            def clean_yahoo_finance_screener_results(input_list):
                ret_list = []
                key_list = ['symbol',
                            'marketCap',
                            'sharesOutstanding',
                            'regularMarketVolume',
                            'regularMarketChangePercent',
                            'regularMarketPreviousClose',
                            'regularMarketOpen',
                            'regularMarketDayHigh',
                            'regularMarketDayLow',
                            'regularMarketPrice',
                            'twoHundredDayAverage']

                for result in input_list:

                    filtered_dict = {k: v for (k, v) in result.items() if k in key_list}

                    cleaned_dict = {}
                    for k, v in filtered_dict.items():
                        if isinstance(v, dict) and 'raw' in v.keys():
                            cleaned_dict[k] = v['raw']
                        else:
                            cleaned_dict[k] = v

                    ret_list.append(cleaned_dict)
                return ret_list

            def get_data(resp,ret_vals):
                resp_dict = json.loads(resp.text)
                resp_dict = resp_dict['finance']['result'][0]['quotes']

                ret_list = clean_yahoo_finance_screener_results(input_list=resp_dict)
                ret_list = self.__normalize_names(input_list=ret_list,
                                                  names_conversion_dict=name_conversions)
                ret_list = self.__label_dicts_in_list(source_name='yahoofinance', input_list=ret_list)

                ret_vals['results'] = ret_list
                return ret_vals

            def TEST_get_data(ret_vals):

                ret_list = [
                    {'source': 'yahoofinance', 'timestamp': datetime_now(as_str=True), 'symbol': 'PRVB', 'high': 24.23,
                     'sma200': 6.71135, 'market_cap': 2166667264, 'volume': 36019932, 'low': 22.7, 'open': 24.08,
                     'pct_change': 259.7015, 'shares_outstanding': 89903200, 'prev_close': 6.7, 'close': 24.1},
                    {'source': 'yahoofinance', 'timestamp': datetime_now(as_str=True), 'symbol': 'CIFR', 'high': 1.85,
                     'sma200': 1.3901, 'market_cap': 450484608, 'volume': 2502935, 'low': 1.4, 'open': 1.42,
                     'pct_change': 37.878788, 'shares_outstanding': 247519008, 'prev_close': 1.32, 'close': 1.82},
                    {'source': 'yahoofinance', 'timestamp': datetime_now(as_str=True), 'symbol': 'EUDA', 'high': 3.2,
                     'sma200': 6.7059, 'market_cap': 61742856, 'volume': 28051138, 'low': 1.6, 'open': 1.6,
                     'pct_change': 36.633663, 'shares_outstanding': 22370600, 'prev_close': 2.02, 'close': 2.76}
                ]

                ret_vals['results'] = ret_list
                return ret_vals

            #####################################################################

            function_name = 'yahoo_finance_screener'
            info = f'{function_name}, key:{key["key_num"]}'

            logging.info(f'\ttrying {info}')

            ret_vals = {'resp_code': None, 'results': {}}
            if RUN_AS_TEST:
                logging.info(f'\t\t{info}: TEST')
                ret_vals = TEST_get_data(ret_vals=ret_vals)
                return ret_vals

            name_conversions = {'marketCap': 'market_cap',
                                'regularMarketVolume': 'volume',
                                'regularMarketChangePercent': 'pct_change',
                                'sharesOutstanding': 'shares_outstanding',
                                'regularMarketPreviousClose': 'prev_close',
                                'regularMarketOpen': 'open',
                                'regularMarketDayHigh': 'high',
                                'regularMarketDayLow': 'low',
                                'regularMarketPrice': 'close',
                                'twoHundredDayAverage': 'sma200'}

            url = "https://yh-finance.p.rapidapi.com/screeners/list"

            querystring = {"quoteType": "EQUITY",
                           "sortField": "percentchange",
                           "region": "US",
                           "size": "12",
                           "offset": "0",
                           "sortType": "DESC"}

            payload = [
                {
                    "operator": "gt",
                    "operands": ["dayvolume", 500000]
                },
                {
                    "operator": "gt",
                    "operands": ["intradayprice", 0.3]
                },
                {
                    "operator": "gt",
                    "operands": ["percentchange", 10]
                },
                {
                    "operator": "eq",
                    "operands": ["region", "us"]
                }
            ]

            headers = {
                "content-type": "application/json",
                "X-RapidAPI-Key": key['key'],
                "X-RapidAPI-Host": "yh-finance.p.rapidapi.com"
            }

            try:
                response = requests.request("POST", url, json=payload, headers=headers, params=querystring, timeout=REQUEST_TIMEOUT)
                ret_vals['resp_code'] = response.status_code
                if response.status_code == 200:

                    ret_vals = get_data(resp=response, ret_vals=ret_vals)
                    self.__display_ret_val_errors(r_vals=ret_vals,info=info)
                else:
                    msg = f"{info}: response code {response.status_code}"
                    logging.error(f'\t\t{msg}')
                    record_errors(msg=msg)

            except Exception as e:
                msg = f'{info}: problem getting data: {e}'
                logging.error(f'\t\t{msg}')
                record_errors(msg=msg)

            return ret_vals

        ##########################################################################

        return pull_from_source(keys_list=keys_list,
                                source_name=source_name,
                                pull_method=get_screener)

    # RAPI
    def schwab_screener(self, keys_list, source_name='schwab_screener'):

        def get_screener(ticker,key):

            def clean_scwab_screener_results(input_list):
                keep_metrics = ['Symbol', 'PriceLast', 'PriceChangePercent', 'Volume']
                ret_list = []
                for symbol_dict in input_list:
                    temp_dict = {k: v for (k, v) in symbol_dict.items() if k in keep_metrics}
                    ret_list.append(temp_dict)

                return ret_list

            def get_data(resp,ret_vals):
                resp_dict = json.loads(resp.text)
                resp_dict = resp_dict['CompanyMovers']
                ret_list = clean_scwab_screener_results(input_list=resp_dict)
                ret_list = self.__normalize_names(input_list=ret_list,
                                                  names_conversion_dict=name_conversions)
                ret_list = self.__label_dicts_in_list(source_name='schwab', input_list=ret_list)

                ret_vals['results'] = ret_list
                return ret_vals

            def TEST_get_data(ret_vals):

                ret_list = [
                    {'source': 'schwab', 'timestamp': datetime_now(as_str=True), 'symbol': 'PRVB', 'close': 24.1,'pct_change': 259.70149253731347, 'volume': 36019932},
                    {'source': 'schwab', 'timestamp': datetime_now(as_str=True), 'symbol': 'CIFR', 'close': 1.82,'pct_change': 37.878787878787875, 'volume': 2502935},
                    {'source': 'schwab', 'timestamp': datetime_now(as_str=True), 'symbol': 'EUDA', 'close': 2.76,'pct_change': 36.63366336633662, 'volume': 28051138}
                ]

                ret_vals['results'] = ret_list
                return ret_vals

            #####################################################################
            function_name = 'schwab_screener'
            info = f'{function_name}, key:{key["key_num"]}'

            logging.info(f'\t trying {info}')

            ret_vals = {'resp_code': None, 'results': {}}
            if RUN_AS_TEST:
                logging.info(f'\t\t{info}: TEST')
                ret_vals = TEST_get_data(ret_vals=ret_vals)
                return ret_vals

            name_conversions = {
                'Symbol': 'symbol',
                'PriceLast': 'close',
                'PriceChangePercent': 'pct_change',
                'Volume': 'volume'
            }

            url = "https://schwab.p.rapidapi.com/market/get-movers"

            querystring = {"rankType": "PctChgGainers", "exchange": "US", "sectorCusip": "ALL"}

            headers = {
                "X-RapidAPI-Key": key['key'],
                "X-RapidAPI-Host": "schwab.p.rapidapi.com"
            }

            try:
                response = requests.request("GET", url, headers=headers, params=querystring, timeout=REQUEST_TIMEOUT)
                ret_vals['resp_code'] = response.status_code
                if response.status_code == 200:

                    ret_vals = get_data(resp=response, ret_vals=ret_vals)
                    self.__display_ret_val_errors(r_vals=ret_vals,info=info)

                else:
                    msg = f'{info}: response code {response.status_code}'
                    logging.error(f'\t\t{msg}')
                    record_errors(msg=msg)

            except Exception as e:
                msg = f'{info}: problem getting data: {e}'
                logging.error(f'\t\t{msg}')
                record_errors(msg=msg)

            return ret_vals

        ##########################################################################

        return pull_from_source(keys_list=keys_list,
                                source_name=source_name,
                                pull_method=get_screener)

    ################# UNTESTED #####################################################
    def seeking_alpha_screener(self):
        url = "https://seeking-alpha.p.rapidapi.com/market/get-day-watch"

        headers = {
            "X-RapidAPI-Key": "d075b32cc2msh747d682c0370de7p1adb18jsn5affd77413e8",
            "X-RapidAPI-Host": "seeking-alpha.p.rapidapi.com"
        }

        response = requests.request("GET", url, headers=headers, timeout=REQUEST_TIMEOUT)

        print(response.text)

class IntradayCandles():
    """
     returns
         bool: True = got results, False = couldnt get results
         results: either a datatype or None if bool = False

     """
    # alpaca free tier only gets IEX data
    # IEX no longer free
    # alphavantage doesnt get current day data
    # polygon doesnt get current day data
    # finnhub not giving current day candles, need to confirm
    # STOCK_DATA gives wierd/incomplete data

    def __display_ret_val_errors(self, r_vals, info):
        if r_vals['results']:
            logging.info(f'\t\t ...{info} complete')
        else:
            msg = f'{info} CANT GET DATA'
            logging.error(f'\t\t ...{msg}')
            record_errors(msg=msg)

    def __candle_resampler(self,input_df, timeframe=str):
        """
        inputs origonal dataframe and selected timeframe and outputs dataframe of desired output conversion timeframe

        input dataframe input:

        Date, Open, High, Low, Close, Volume

        timeframe : '15min' , 1hr:'60min', 4hr:'240min', 1day:'1440min'

        """

        def fill_in(cols):
            volume = cols[0]
            target = cols[1]
            stock_close = cols[2]

            if volume == 0:
                target = stock_close

            return target

        #########################################

        if input_df['Date'].dtype != 'datetime64[ns]':
            # conver to datetime
            input_df['Date'] = pd.to_datetime(input_df['Date'])  # , unit='ms'))

        input_df = input_df.set_index(pd.DatetimeIndex(input_df['Date']))

        data_ohlc = input_df.resample(timeframe).agg({'Open': 'first',
                                                      'High': 'max',
                                                      'Low': 'min',
                                                      'Close': 'last',
                                                      'Volume': 'sum'})

        data_ohlc = data_ohlc.reset_index()

        data_ohlc['Close'] = data_ohlc['Close'].fillna(method='ffill')
        data_ohlc['Open'] = data_ohlc[['Volume', 'Open', 'Close']].apply(fill_in, axis=1)
        data_ohlc['High'] = data_ohlc[['Volume', 'High', 'Close']].apply(fill_in, axis=1)
        data_ohlc['Low'] = data_ohlc[['Volume', 'Low', 'Close']].apply(fill_in, axis=1)

        return data_ohlc

    ###############################################################################

    # non RAPI
    def YF_intraday(self, ticker, keys_list, source_name='yahoo_finance'):

        def pull_candles(ticker,key=None):
            function_name = 'YF_intraday'
            info = f'{function_name},{ticker}, key:{key["key_num"]}'

            logging.info(f'\t trying {info}')

            now = datetime_now()

            start_time = datetime.datetime.combine(now, datetime.time.min)
            end_time = datetime.datetime.combine(now, datetime.time.max)

            ret_vals = {'resp_code': None, 'results': {}}
            try:
                df = yf.Ticker(ticker).history(start=start_time, end=end_time, interval='5m')
                if not df.empty:
                    df.reset_index(inplace=True)
                    df.rename(columns={'Datetime': 'Date'}, inplace=True)
                    df['Date'] = pd.to_datetime(df['Date'])
                    df.drop(['Dividends', 'Stock Splits'], axis=1, inplace=True)
                    df['Date'] = df['Date'].dt.tz_localize(None)
                    df['Date'] = df['Date'] - datetime.timedelta(hours=1)

                    ret_vals['results']['ohlc_df'] = df
                    ret_vals['resp_code'] = 200

                    self.__display_ret_val_errors(r_vals=ret_vals,info=info)

                else:
                    msg = f'{info}: NO RETURN VALUE'
                    logging.error(f'\t\t{msg}')
                    record_errors(msg=msg)

            except Exception as e:
                msg =  f'{info}: ERROR: {e}'
                logging.error(f'\t\t{msg}')
                record_errors(msg=msg)

            return ret_vals

        #################################################################################

        return pull_from_source(ticker=ticker,
                                keys_list=keys_list,
                                source_name=source_name,
                                pull_method=pull_candles)

    # RAPI
    def API_stocks_intraday(self, ticker, keys_list, source_name='API_stocks'):

        def pull_candles(ticker,key):

            def get_df(resp,ret_vals):
                resp_dict = resp.json()

                df = pd.DataFrame(resp_dict['Results'])
                if not df.empty:
                    now = datetime_now()
                    start = now.strftime('%Y-%m-%d') + '  00:00:00'
                    end = now.strftime('%Y-%m-%d') + ' 23:59:59'

                    df['Date'] = pd.to_datetime(df['Date'])
                    df['Date'] = df['Date'] - datetime.timedelta(hours=1)

                    df = df.loc[(df['Date'] >= start) & (df['Date'] < end)]

                    ret_vals['results']['ohlc_df'] = df

                return ret_vals

            #############################################################################

            function_name = "API_stocks_intraday"
            info = f'{function_name},{ticker}, key:{key["key_num"]}'

            logging.info(f'\t trying {info}')

            url = "https://apistocks.p.rapidapi.com/intraday"
            querystring = {"symbol": ticker, "interval": "5min", "maxreturn": "85"}

            headers = {
                "X-RapidAPI-Key": key['key'],
                "X-RapidAPI-Host": "apistocks.p.rapidapi.com"
            }

            ret_vals = {'resp_code': None, 'results': {}}

            try:
                response = requests.request("GET", url, headers=headers, params=querystring, timeout=REQUEST_TIMEOUT)
                ret_vals['resp_code'] = response.status_code
                if response.status_code == 200:

                    ret_vals = get_df(resp=response,ret_vals=ret_vals)
                    self.__display_ret_val_errors(r_vals=ret_vals, info=info)

                else:
                    msg = f"{info}: response code {response.status_code}"
                    logging.error(f'\t\t{msg}')
                    record_errors(msg=msg)


            except Exception as e:
                msg = f'{info}: ERROR: {e}'
                logging.error(f'\t\t{msg}')
                record_errors(msg=msg)

            return ret_vals

        ################################################################

        return pull_from_source(ticker=ticker,
                                keys_list=keys_list,
                                source_name=source_name,
                                pull_method=pull_candles)

    # RAPI
    def twelve_data_intraday(self,ticker, keys_list, source_name='twelve_data'):

        def pull_candles(ticker,key):

            def get_df(resp,ret_vals):
                resp_dict = resp.json()['values']
                now = datetime_now()
                start = now.strftime('%Y-%m-%d') + '  00:00:00'
                end = now.strftime('%Y-%m-%d') + ' 23:59:59'

                df = pd.DataFrame(resp_dict)
                if not df.empty:
                    df.rename(columns={
                        'datetime': 'Date',
                        'open': 'Open',
                        'high': 'High',
                        'low': 'Low',
                        'close': 'Close',
                        'volume': 'Volume'}, inplace=True)

                    df['Open'] = pd.to_numeric(df['Open'])
                    df['High'] = pd.to_numeric(df['High'])
                    df['Low'] = pd.to_numeric(df['Low'])
                    df['Close'] = pd.to_numeric(df['Close'])
                    df['Volume'] = pd.to_numeric(df['Volume'])

                    df['Date'] = pd.to_datetime(df['Date'])
                    df['Date'] = df['Date'] - datetime.timedelta(hours=1)

                    df = df.loc[(df['Date'] >= start) & (df['Date'] < end)]
                    ret_vals['results']['ohlc_df'] = df

                return ret_vals

            ###############################################################
            function_name = "twelve_data_intraday"
            info = f'{function_name},{ticker}, key:{key["key_num"]}'

            logging.info(f'\t trying {info}')

            url = "https://twelve-data1.p.rapidapi.com/time_series"

            querystring = {"symbol": ticker,
                           "interval": "5min",
                           "outputsize": "85",
                           "format": "json"}

            headers = {
                "X-RapidAPI-Key": key['key'],
                "X-RapidAPI-Host": "twelve-data1.p.rapidapi.com"
            }

            ret_vals = {'resp_code': None, 'results': {}}

            try:
                response = requests.request("GET", url, headers=headers, params=querystring, timeout=REQUEST_TIMEOUT)
                ret_vals['resp_code'] = response.status_code
                if response.status_code == 200:

                    ret_vals = get_df(resp=response,ret_vals=ret_vals)
                    self.__display_ret_val_errors(r_vals=ret_vals,info=info)

                else:
                    msg = f'{info}: response code {response.status_code}'
                    logging.error(f'\t\t{msg}')
                    record_errors(msg=msg)

            except Exception as e:
                msg = f'{info}: ERROR: {e}'
                logging.error(f'\t\t{msg}')
                record_errors(msg=msg)

            return ret_vals

        ##################################################################################
        return pull_from_source(ticker=ticker,
                                keys_list=keys_list,
                                source_name=source_name,
                                pull_method=pull_candles)

    # RAPI
    def seeking_alpha_intraday(self, ticker, keys_list, source_name='seeking_alpha'):

        def pull_candles(ticker,key):

            def get_df(resp,ret_vals):

                resp_dict = resp.json()['attributes']

                df = pd.DataFrame(resp_dict)
                if not df.empty:
                    df = df.T
                    df = df.drop(['adj'], axis=1)
                    df.reset_index(inplace=True)

                    df.rename(columns={'index': 'Date',
                                       'open': 'Open',
                                       'high': 'High',
                                       'low': 'Low',
                                       'close': 'Close',
                                       'volume': 'Volume'}, inplace=True)

                    df['Date'] = pd.to_datetime(df['Date'])
                    df['Date'] = df['Date'] - datetime.timedelta(hours=1)

                    df = self.__candle_resampler(input_df=df, timeframe='5min')
                    ret_vals['results']['ohlc_df'] = df

                return ret_vals

            #######################################################
            function_name = "seeking_alpha_intraday"
            info = f'{function_name},{ticker}, key:{key["key_num"]}'

            logging.info(f'\t trying {info}')

            url = "https://seeking-alpha.p.rapidapi.com/symbols/get-chart"

            querystring = {
                "symbol": ticker,
                "period": "1D"
            }

            headers = {
                "X-RapidAPI-Key": key['key'],
                "X-RapidAPI-Host": "seeking-alpha.p.rapidapi.com"
            }
            ret_vals = {'resp_code': None, 'results': {}}

            try:
                response = requests.request("GET", url, headers=headers, params=querystring, timeout=REQUEST_TIMEOUT)
                ret_vals['resp_code'] = response.status_code
                if response.status_code == 200:

                    ret_vals = get_df(resp=response,ret_vals=ret_vals)
                    self.__display_ret_val_errors(r_vals=ret_vals,info=info)

                else:
                    msg = f'{info}: response code {response.status_code}'
                    logging.error(f'\t\t{msg}')
                    record_errors(msg=msg)


            except Exception as e:
                msg = f'{info}: ERROR: {e}'
                logging.error(f'\t\t{msg}')
                record_errors(msg=msg)

            return ret_vals

        ###################################################################
        return pull_from_source(ticker=ticker,
                                keys_list=keys_list,
                                source_name=source_name,
                                pull_method=pull_candles)

    # RAPI
    def fidelity_intraday(self, ticker, keys_list, source_name='fidelity'):

        def pull_candles(ticker,key):

            def get_df(resp, ret_vals):
                root = ET.fromstring(resp.content)
                resp_dict = {
                    'timestamps': None,
                    'open': None,
                    'high': None,
                    'low': None,
                    'close': None,
                    'volume': None
                }

                for child in root.iter('*'):
                    tag_name = child.tag.lower()

                    if tag_name in resp_dict.keys():
                        data = child.text.split()
                        resp_dict[tag_name] = [float(x) for x in data]

                df = pd.DataFrame.from_dict(resp_dict, orient='index').transpose()
                df['timestamps'] = pd.to_datetime(df['timestamps'], unit='s')
                df.rename(columns={
                    'timestamps': 'Date',
                    'open': 'Open',
                    'high': 'High',
                    'low': 'Low',
                    'close': 'Close',
                    'volume': 'Volume'}, inplace=True)

                df['Date'] = df['Date'] - datetime.timedelta(hours=6)
                df['Volume'] = df['Volume'].astype(int)
                ret_vals['results']['ohlc_df'] = df

                return ret_vals

            #################################################################################

            function_name = "fidelity_intraday"
            info = f'{function_name},{ticker}, key:{key["key_num"]}'

            logging.info(f'\t trying {info}')

            url = "https://fidelity-investments.p.rapidapi.com/quotes/get-chart"

            now = datetime_now()

            str_date = now.date().strftime('%Y/%m/%d')
            querystring = {"symbol": ticker, "startDate": f"{str_date}-00:00:00", "endDate": f"{str_date}-23:59:59",
                           "intraday": "Y", "granularity": "3"}

            headers = {
                "X-RapidAPI-Key": key['key'],
                "X-RapidAPI-Host": "fidelity-investments.p.rapidapi.com"
            }

            ret_vals = {'resp_code': None, 'results': {}}

            try:
                response = requests.request("GET", url, headers=headers, params=querystring, timeout=REQUEST_TIMEOUT)
                ret_vals['resp_code'] = response.status_code
                if response.status_code == 200:

                    ret_vals = get_df(resp=response, ret_vals=ret_vals)
                    self.__display_ret_val_errors(r_vals=ret_vals,info=info)

                else:
                    msg = f'{info}: response code {response.status_code}'
                    logging.error(f'\t\t{msg}')
                    record_errors(msg=msg)

            except Exception as e:
                msg = f'{info}: ERROR: {e}'
                logging.error(f'\t\t{msg}')
                record_errors(msg=msg)

            return ret_vals

        ###################################################################

        return pull_from_source(ticker=ticker,
                                keys_list=keys_list,
                                source_name=source_name,
                                pull_method=pull_candles)

    #################################################################################

    # works but gets wierd data
    def finnhub_intraday(self,ticker):
        # 60 calls a minute

        ticker = ticker.upper()
        import finnhub
        finnhub_client = finnhub.Client(api_key='cf56hs2ad3i7dbfi4ft0cf56hs2ad3i7dbfi4ftg')
        now = datetime_now()

        start = datetime.datetime.combine(now, datetime.time.min)
        end = datetime.datetime.combine(now, datetime.time.max)

        resp = finnhub_client.stock_candles(
            symbol=ticker,
            resolution='5',
            _from=int(start.timestamp()),
            to=int(end.timestamp())
        )

        if resp['s'] == 'ok':
            df = pd.DataFrame(resp)
            df.rename(columns={'c': 'Close', 'h': 'High', 'l': 'Low', 'o': 'Open', 't': 'Date', 'v': 'Volume'},
                      inplace=True)
            df['Date'] = pd.to_datetime(df['Date'], unit='s')
            df['Date'] = df['Date'] - datetime.timedelta(hours=6)

            start_time = now.strftime('%Y-%m-%d') + '  08:30:00'
            end_time = now.strftime('%Y-%m-%d') + ' 15:00:00'

            df = df.loc[(df['Date'] >= start_time) & (df['Date'] <= end_time)]

            return True, df

class QuotesData():
    # https://rapidapi.com/Horuz/api/stockexchangeapi doesnt work
    # https://rapidapi.com/api4stocks/api/apistocks does get current day OHLCV

    ##################################################################
    def __display_ret_val_errors(self, r_vals, info):
        if r_vals['results']:
            logging.info(f'\t\t...{info} complete')
        else:
            msg = f'{info}: NO QUOTES DATA FOUND'
            logging.error(f'\t\t...{msg}')
            record_errors(msg=msg)

    ################################################################################################

    # non RAPI
    def YF_quotes(self, ticker, keys_list, source_name='yahoo_finance'):

        def pull_quotes(ticker,key=None):

            def get_quotes(ret_vals):
                df = yf.Ticker(ticker).history(start=start, end=end, interval='1D')
                df.reset_index(inplace=True)

                data = df.to_dict(orient='records')

                quotes_dict = {
                    'symbol': ticker,
                    'prev_close': data[0]['Close'],
                    'open': data[1]['Open'],
                    'high': data[1]['High'],
                    'low': data[1]['Low'],
                    'close': data[1]['Close'],
                    'volume': data[1]['Volume'],
                    'timestamp': datetime_now(as_str=True)
                }

                if all(quotes_dict[k] for k in quotes_dict.keys()):
                    ret_vals['resp_code'] = 200
                    ret_vals['results'] = quotes_dict

                return ret_vals

            ###############################################################
            import yfinance as yf
            function_name = "YF_quotes"
            info = f'{function_name},{ticker}, key:{key["key_num"]}'

            logging.info(f'\t trying {info}')

            now = datetime_now()
            prev_day = helper_functions.get_previous_trading_day(curr_date=now)
            curr_day = now + datetime.timedelta(days=1)

            start = prev_day.strftime('%Y-%m-%d')
            end = curr_day.strftime('%Y-%m-%d')

            ret_vals = {'resp_code': None, 'results': {}}

            try:

               ret_vals = get_quotes(ret_vals=ret_vals)
               self.__display_ret_val_errors(r_vals=ret_vals,info=info)

            except Exception as e:
                msg = f'{info}: ERROR: {e}'
                logging.error(f'\t\t{msg}')
                record_errors(msg=msg)

            return ret_vals
        ###########################################################################
        return pull_from_source(ticker=ticker,
                                keys_list=keys_list,
                                source_name=source_name,
                                pull_method=pull_quotes)

    # non RAPI
    def alpaca_quotes(self, ticker, keys_list, source_name='alpaca'):

        def pull_quotes(ticker,key=None):

            def get_quotes(ret_vals):
                api = REST(key_id=os.environ['ALPACA_API_KEY'],
                           secret_key=os.environ['ALPACA_SECRET_KEY'])

                df = api.get_bars(ticker, TimeFrame.Day, start=start + 'T00:00:00Z', end=end + 'T15:30:00Z').df
                df.reset_index(inplace=True)

                data = df.to_dict(orient='records')

                quotes_dict = {
                    'symbol': ticker,
                    'prev_close': data[0]['close'],
                    'open': data[1]['open'],
                    'high': data[1]['high'],
                    'low': data[1]['low'],
                    'close': data[1]['close'],
                    'volume': data[1]['volume'],
                    'timestamp': datetime_now(as_str=True)
                }

                if all(quotes_dict[k] for k in quotes_dict.keys()):
                    ret_vals['resp_code'] = 200
                    ret_vals['results'] = quotes_dict

                return ret_vals

            #######################################################################
            from alpaca_trade_api.rest import REST, TimeFrame

            function_name = "alpaca_quotes"
            info = f'{function_name},{ticker}, key:{key["key_num"]}'

            logging.info(f'\t trying {info}')
            now = datetime_now()
            prev_day = helper_functions.get_previous_trading_day(curr_date=now)

            start = prev_day.strftime('%Y-%m-%d')
            end = now.strftime('%Y-%m-%d')

            ret_vals = {'resp_code': None, 'results': {}}

            try:

                ret_vals = get_quotes(ret_vals=ret_vals)
                self.__display_ret_val_errors(r_vals=ret_vals,info=info)

            except Exception as e:
                msg = f'{info}: ERROR: {e}'
                logging.error(f'\t\t{msg}')
                record_errors(msg=msg)

            return ret_vals

        #############################################################
        return pull_from_source(ticker=ticker,
                                keys_list=keys_list,
                                source_name=source_name,
                                pull_method=pull_quotes)

    # non RAPI
    def alphavantage_quotes(self, ticker, keys_list, source_name='alphavantage'):

        def pull_quotes(ticker,key):
            function_name = "alphavantage_quotes"
            info = f'{function_name},{ticker}, key:{key["key_num"]}'

            logging.info(f'\t trying {info}')

            ticker = ticker.upper()
            url = f'https://www.alphavantage.co/query?function=GLOBAL_QUOTE&symbol={ticker}&apikey={key["key"]}'

            ret_vals = {'resp_code': None, 'results': {}}

            try:
                response = requests.get(url, timeout=REQUEST_TIMEOUT)
                ret_vals['resp_code'] = response.status_code
                if response.status_code == 200:
                    resp_dict = response.json()['Global Quote']

                    ret_vals['results'] = {
                        'symbol': ticker,
                        'prev_close': float(resp_dict['08. previous close']),
                        'open': float(resp_dict['02. open']),
                        'high': float(resp_dict['03. high']),
                        'low': float(resp_dict['04. low']),
                        'close': float(resp_dict['05. price']),
                        'volume': int(resp_dict['06. volume']),
                        'timestamp': datetime_now(as_str=True)
                    }

                    self.__display_ret_val_errors(r_vals=ret_vals,info=info)

                else:
                    msg = f'{info}: response code {response.status_code}'
                    logging.error(f'\t\t{msg}')
                    record_errors(msg=msg)

            except Exception as e:
                msg = f'{info}: ERROR: {e}'
                logging.error(f'\t\t{msg}')
                record_errors(msg=msg)

            return ret_vals

        ####################################################################################
        return pull_from_source(ticker=ticker,
                                keys_list=keys_list,
                                source_name=source_name,
                                pull_method=pull_quotes)

    # non RAPI
    def finnhub_quotes(self, ticker, keys_list, source_name='finnhub'):

        def pull_quotes(ticker,key):

            def get_quotes(ret_vals):

                resp = finnhub_client.stock_candles(
                    symbol=ticker,
                    resolution='D',
                    _from=int(start),
                    to=int(end)
                )

                if resp['s'] == 'ok':
                    quotes_dict = {
                        'symbol':ticker,
                        'prev_close': resp['c'][0],
                        'open': resp['o'][-1],
                        'high': resp['h'][-1],
                        'low': resp['l'][-1],
                        'close': resp['c'][-1],
                        'volume': resp['v'][-1],
                        'timestamp':datetime_now(as_str=True)
                    }

                    if all(quotes_dict[k] for k in quotes_dict.keys()):
                        ret_vals['resp_code'] = 200
                        ret_vals['results'] = quotes_dict

                return ret_vals

            ###########################################################################
            import finnhub
            function_name = "alphavantage_quotes"
            info = f'{function_name},{ticker}, key:{key["key_num"]}'

            logging.info(f'\t trying {info}')

            finnhub_client = finnhub.Client(api_key=key['key'])

            now = datetime_now()

            start = (now - datetime.timedelta(days=1)).timestamp()
            end = now.timestamp()

            ret_vals = {'resp_code': None, 'results': {}}

            try:

                ret_vals = get_quotes(ret_vals=ret_vals)
                self.__display_ret_val_errors(r_vals=ret_vals,info=info)

            except Exception as e:
                msg = f'{info}: ERROR: {e}'
                logging.error(f'\t\t{msg}')
                record_errors(msg=msg)

            return ret_vals

        #####################################################################
        return pull_from_source(ticker=ticker,
                                keys_list=keys_list,
                                source_name=source_name,
                                pull_method=pull_quotes)

    # RAPI
    def twelve_data_quotes(self, ticker, keys_list, source_name='twelve_data'):

        def pull_quotes(ticker,key):

            def get_quotes(resp,ret_vals):
                resp_dict = resp.json()

                quotes_dict = {
                    'symbol': ticker,
                    'prev_close': float(resp_dict['previous_close']),
                    'open': float(resp_dict['open']),
                    'high': float(resp_dict['high']),
                    'low': float(resp_dict['low']),
                    'close': float(resp_dict['close']),
                    'volume': int(resp_dict['volume']),
                    'timestamp': datetime_now(as_str=True)
                }

                if all(quotes_dict[k] for k in quotes_dict.keys()):
                    ret_vals['results'] = quotes_dict

                return ret_vals

            #####################################################################

            function_name = "twelve_data_quotes"
            info = f'{function_name},{ticker}, key:{key["key_num"]}'

            logging.info(f'\t trying {info}')

            ticker = ticker.upper()

            url = "https://twelve-data1.p.rapidapi.com/quote"

            querystring = {"symbol": ticker, "interval": "1day", "outputsize": "30", "format": "json"}

            headers = {
                "X-RapidAPI-Key": key['key'],
                "X-RapidAPI-Host": "twelve-data1.p.rapidapi.com"
            }

            ret_vals = {'resp_code': None, 'results': {}}

            try:
                response = requests.request("GET", url, headers=headers, params=querystring, timeout=REQUEST_TIMEOUT)
                ret_vals['resp_code'] = response.status_code
                if response.status_code == 200:

                    ret_vals = get_quotes(resp=response,ret_vals=ret_vals)
                    self.__display_ret_val_errors(r_vals=ret_vals,info=info)

                else:
                    msg = f'{info}: response code {response.status_code}'
                    logging.error(f'\t\t{msg}')
                    record_errors(msg=msg)

            except Exception as e:
                msg = f'{info}: ERROR: {e}'
                logging.error(f'\t\t{msg}')
                record_errors(msg=msg)

            return ret_vals

        #####################################################################################
        return pull_from_source(ticker=ticker,
                                keys_list=keys_list,
                                source_name=source_name,
                                pull_method=pull_quotes)

    # RAPI
    def stock_prices_API_quotes(self, ticker, keys_list, source_name='stock_prices_API'):

        def pull_quotes(ticker,key):

            def get_quotes(resp,ret_vals):
                resp_dict = resp.json()

                dates = list(resp_dict.keys())
                dates.sort(key = lambda date: datetime_now())

                quotes_dict = {
                    'symbol': ticker,
                    'prev_close': resp_dict[dates[-2]]['Close'],
                    'open': resp_dict[dates[-1]]['Open'],
                    'high': resp_dict[dates[-1]]['High'],
                    'low': resp_dict[dates[-1]]['Low'],
                    'close': resp_dict[dates[-1]]['Close'],
                    'volume': resp_dict[dates[-1]]['Volume'],
                    'timestamp': datetime_now(as_str=True)
                }

                if all(quotes_dict[k] for k in quotes_dict.keys()):
                    ret_vals['results'] = quotes_dict

                return ret_vals

            ##############################################################################
            function_name = "stock_prices_API"
            info = f'{function_name},{ticker}, key:{key["key_num"]}'

            logging.info(f'\t trying {info}')

            url = "https://stock-prices2.p.rapidapi.com/api/v1/resources/stock-prices/5d"

            querystring = {"ticker": ticker}

            headers = {
                "X-RapidAPI-Key": key['key'],
                "X-RapidAPI-Host": "stock-prices2.p.rapidapi.com"
            }

            ret_vals = {'resp_code': None, 'results': {}}

            try:
                response = requests.request("GET", url, headers=headers, params=querystring, timeout=REQUEST_TIMEOUT)
                ret_vals['resp_code'] = response.status_code
                if response.status_code == 200:

                    ret_vals = get_quotes(resp=response,ret_vals=ret_vals)
                    self.__display_ret_val_errors(r_vals=ret_vals,info=info)
                else:
                    msg = f'{info}: response code {response.status_code}'
                    logging.error(f'\t\t{msg}')
                    record_errors(msg=msg)

            except Exception as e:
                msg = f'{info}: ERROR: {e}'
                logging.error(f'\t\t{msg}')
                record_errors(msg=msg)

            return ret_vals
        ############################################################################
        return pull_from_source(ticker=ticker,
                                keys_list=keys_list,
                                source_name=source_name,
                                pull_method=pull_quotes)

    # RAPI
    def alphavantage_RAPID_API_quotes(self, ticker, keys_list, source_name='alphavantage_RAPI'):

        def pull_quotes(ticker,key):

            def get_quotes(resp,ret_vals):
                resp_dict = resp.json()['Global Quote']

                quotes_dict = {
                    'symbol':ticker,
                    'prev_close':float(resp_dict['08. previous close']),
                    'open': float(resp_dict['02. open']),
                    'high': float(resp_dict['03. high']),
                    'low': float(resp_dict['04. low']),
                    'close': float(resp_dict['05. price']),
                    'volume': int(resp_dict['06. volume']),
                }

                if all(quotes_dict[k] for k in quotes_dict.keys()):
                    ret_vals['results'] = quotes_dict

                return ret_vals

            #####################################################################

            function_name = "alphavantage_RAPID_API_quotes"
            info = f'{function_name},{ticker}, key:{key["key_num"]}'

            logging.info(f'\t trying {info}')

            url = "https://alpha-vantage.p.rapidapi.com/query"

            querystring = {"function": "GLOBAL_QUOTE", "symbol": ticker, "datatype": "json"}

            headers = {
                "X-RapidAPI-Key": key['key'],
                "X-RapidAPI-Host": "alpha-vantage.p.rapidapi.com"
            }

            ret_vals = {'resp_code': None, 'results': {}}

            try:
                response = requests.request("GET", url, headers=headers, params=querystring, timeout=REQUEST_TIMEOUT)
                ret_vals['resp_code'] = response.status_code
                if response.status_code == 200:

                    ret_vals = get_quotes(resp=response,ret_vals=ret_vals)
                    self.__display_ret_val_errors(r_vals=ret_vals,info=info)

                else:
                    msg = f'{info}: response code {response.status_code}'
                    logging.error(f'\t\t{msg}')
                    record_errors(msg=msg)

            except Exception as e:
                msg = f'{e} in {function_name}, on ticker: {ticker}, key: {key["key_num"]}'
                logging.error(f'\t\t{msg}')
                record_errors(msg=msg)

            return ret_vals
        ########################################################
        return pull_from_source(ticker=ticker,
                                keys_list=keys_list,
                                source_name=source_name,
                                pull_method=pull_quotes)

    # RAPI
    def fidelity_quotes(self, ticker, keys_list, source_name='fidelity'):
        # 5 requests per second 500/month

        def pull_quotes(ticker,key):
            function_name = "fidelity_quotes"
            info = f'{function_name},{ticker}, key:{key["key_num"]}'

            logging.info(f'\t trying {info}')

            url = "https://fidelity-investments.p.rapidapi.com/quotes/get-details"

            querystring = {"symbols": ticker}

            headers = {
                "X-RapidAPI-Key": key["key"],
                "X-RapidAPI-Host": "fidelity-investments.p.rapidapi.com"
            }

            names = [
                {'local_key': 'symbol', 'source_key': 'SYMBOL'},
                {'local_key': 'sector', 'source_key': 'SECTOR'},
                {'local_key': 'industry', 'source_key': 'INDUSTRY_GROUP'},
                {'local_key': 'shares_short', 'source_key': 'SHORT_INTEREST', 'to_type': 'float'},  # times 1000
                {'local_key': 'shares_float', 'source_key': 'FREE_FLOAT_SHARES', 'to_type': 'float'},
                {'local_key': 'short_perc_float', 'source_key': 'PCT_FLOAT', 'to_type': 'float'},  # no adjustment
                {'local_key': 'prev_close', 'source_key': 'PREVIOUS_CLOSE', 'to_type': 'float'},
                {'local_key': 'open', 'source_key': 'OPEN_PRICE', 'to_type': 'float'},
                {'local_key': 'high', 'source_key': 'DAY_HIGH', 'to_type': 'float'},
                {'local_key': 'low', 'source_key': 'DAY_LOW', 'to_type': 'float'},
                {'local_key': 'close', 'source_key': 'LAST_PRICE', 'to_type': 'float'},
                {'local_key': 'volume', 'source_key': 'VOLUME', 'to_type': 'float'},
                {'local_key': 'market_cap', 'source_key': 'MARKET_CAP', 'to_type': 'float'},
                {'local_key': 'shares_outstanding', 'source_key': 'SHARES_OUTSTANDING', 'to_type': 'float'}
            ]

            ret_vals = {'resp_code': None, 'results': {}}

            try:
                response = requests.request("GET", url, headers=headers, params=querystring, timeout=REQUEST_TIMEOUT)
                ret_vals['resp_code'] = response.status_code
                if response.status_code == 200:

                    root = ET.fromstring(response.content)
                    resp_dict = {}
                    for child in root.iter('*'):
                        resp_dict[child.tag] = child.text

                    for name in names:
                        ret_vals = try_stats(source_dict=resp_dict, ret_vals=ret_vals, key_names=name)
                    ret_vals['results']['shares_short'] *= 1000
                    logging.info(f'\t ...{function_name} complete')

                else:
                    msg = f'{info}: response code {response.status_code}'
                    logging.error(f'\t\t{msg}')
                    record_errors(msg=msg)

            except Exception as e:
                msg = f'{info}: ERROR: {e}'
                logging.error(f'\t\t{msg}')
                record_errors(msg=msg)

            return ret_vals

        ##############################################################################
        return pull_from_source(ticker=ticker,
                                keys_list=keys_list,
                                source_name=source_name,
                                pull_method=pull_quotes)

    # scraper
    def contingency_marketwatch_quotes(self,ticker, keys_list, source_name='marketwatch'):
        "https://www.marketwatch.com/investing/stock/apgn/download-data?startDate=2/13/2023&endDate=02/17/2023"
        return

class StockStats():
    # IEX no longer free
    # polygon only has shares outstanding

    def __display_ret_val_errors(self, r_vals, info):
        if r_vals['results']:
            logging.info(f'\t\t ...{info} complete')
        else:
            msg = f'{info}, NO STATS FOUND'
            logging.error(f'\t\t ...{msg}')
            record_errors(msg=msg)

    ##############################################################################
    #non RAPI
    def alphavantage_stats(self, ticker, keys_list, source_name='alphavantage'):
        """
        limits to 5 requests/minute and 500 requests/day
        """

        def pull_stats(ticker,key):

            function_name = "alphavantage_stats"
            info = f'{function_name},{ticker}, key:{key["key_num"]}'

            logging.info(f'\t trying {info}')

            url = f'https://www.alphavantage.co/query?function=OVERVIEW&symbol={ticker}&apikey={key["key"]}'

            ret_vals = {'resp_code': None, 'results': {}}

            try:
                response = requests.get(url, timeout=REQUEST_TIMEOUT)
                ret_vals['resp_code'] = response.status_code
                if response.status_code == 200:

                    resp_dict = response.json()

                    names = [
                        {'local_key': 'sector', 'source_key': 'Sector','to_type':'capitalize'},
                        {'local_key': 'industry', 'source_key': 'Industry','to_type':'capitalize'},
                        {'local_key': 'market_cap', 'source_key': 'MarketCapitalization','to_type':'int'},
                        {'local_key': 'sma200', 'source_key': '200DayMovingAverage','to_type':'float'},
                        {'local_key': 'shares_outstanding', 'source_key': 'SharesOutstanding','to_type':'int'},
                    ]

                    for name in names:
                        ret_vals = try_stats(source_dict=resp_dict, ret_vals=ret_vals, key_names=name)

                    self.__display_ret_val_errors(r_vals=ret_vals,info=info)

                else:
                    msg = f'{info}: response code {response.status_code}'
                    logging.error(f'\t\t{msg}')
                    record_errors(msg=msg)

            except Exception as e:
                msg = f'{info}: ERROR: {e}'
                logging.error(f'\t\t{msg}')
                record_errors(msg=msg)

            return ret_vals

        ############################################################################################
        return pull_from_source(ticker=ticker,
                                keys_list=keys_list,
                                source_name=source_name,
                                pull_method=pull_stats)

    # RAPI
    def alphavantage_RAPID_API_stats(self, ticker, keys_list, source_name='alphavantage'):
        # 5 requests/minute
        def pull_stats(ticker,key):

            function_name = "alphavantage_RAPID_API_stats"
            info = f'{function_name},{ticker}, key:{key["key_num"]}'

            logging.info(f'\t trying {info}')

            url = "https://alpha-vantage.p.rapidapi.com/query"

            querystring = {"function": "OVERVIEW", "symbol": ticker}

            headers = {
                "X-RapidAPI-Key": key['key'],
                "X-RapidAPI-Host": "alpha-vantage.p.rapidapi.com"
            }

            ret_vals = {'resp_code': None, 'results': {}}

            try:
                response = requests.request("GET", url, headers=headers, params=querystring, timeout=REQUEST_TIMEOUT)
                ret_vals['resp_code'] = response.status_code

                if response.status_code == 200:
                    resp_dict = response.json()

                    names = [
                        {'local_key': 'sector', 'source_key': 'Sector','to_type':'capitalize'},
                        {'local_key': 'industry', 'source_key': 'Industry','to_type':'capitalize'},
                        {'local_key': 'market_cap', 'source_key': 'MarketCapitalization','to_type':'int'},
                        {'local_key': 'sma200', 'source_key': '200DayMovingAverage','to_type':'float'},
                        {'local_key': 'shares_outstanding', 'source_key': 'SharesOutstanding','to_type':'int'},
                    ]

                    for name in names:
                        ret_vals = try_stats(source_dict=resp_dict, ret_vals=ret_vals, key_names=name)

                    self.__display_ret_val_errors(r_vals=ret_vals,info=info)

                else:
                    msg = f'{info}: response code {response.status_code}'
                    logging.error(f'\t\t{msg}')
                    record_errors(msg=msg)

            except Exception as e:
                msg = f'{info}: ERROR: {e}'
                logging.error(f'\t\t{msg}')
                record_errors(msg=msg)

            return ret_vals

        #####################################################################
        return pull_from_source(ticker=ticker,
                                keys_list=keys_list,
                                source_name=source_name,
                                pull_method=pull_stats)

    # RAPI
    def cnbc_stats(self,ticker, keys_list, source_name='cnbc'):

        def pull_stats(ticker,key):

            def get_issue_id(ticker):

                ret_vals = {'resp_code': None, 'results': None}

                url = "https://cnbc.p.rapidapi.com/symbols/translate"

                querystring = {"symbol": ticker}

                headers = {
                    "X-RapidAPI-Key": key['key'],
                    "X-RapidAPI-Host": "cnbc.p.rapidapi.com"
                }

                try:
                    response = requests.request("GET", url, headers=headers, params=querystring, timeout=REQUEST_TIMEOUT)
                    if response.status_code == 200:
                        ret_vals['resp_code'] = response.status_code
                        ret_vals['results'] = json.loads(response.content)['issueId']

                except Exception as e:
                    logging.info(f'\t\t ...{function_name}: cant get issue id - ERROR:{e}')

                return ret_vals
            #################################################################################
            function_name = "cnbc_stats"
            info = f'{function_name},{ticker}, key:{key["key_num"]}'

            logging.info(f'\t trying {info}')

            url = "https://cnbc.p.rapidapi.com/symbols/get-profile"

            ID_results = get_issue_id(ticker=ticker)

            if not ID_results['results'] or ID_results['resp_code'] != 200:
                return ID_results

            querystring = {"issueId": ID_results['results']}

            headers = {
                "X-RapidAPI-Key": key["key"],
                "X-RapidAPI-Host": "cnbc.p.rapidapi.com"
            }

            ret_vals = {'resp_code': None, 'results': {}}

            try:
                response = requests.request("GET", url, headers=headers, params=querystring, timeout=REQUEST_TIMEOUT)
                ret_vals['resp_code'] = response.status_code

                if response.status_code == 200:
                    resp_dict = response.json()

                    names = [
                        {'local_key': 'sector', 'source_key': 'Sector'},
                        {'local_key': 'industry', 'source_key': 'Industry'},
                        {'local_key': 'market_cap', 'source_key': 'MarketCap','to_type':'string to num'},
                        {'local_key': 'shares_outstanding', 'source_key': 'SharesOutstanding','to_type':'string to num'}
                    ]

                    for name in names:
                        ret_vals = try_stats(source_dict=resp_dict, ret_vals=ret_vals, key_names=name)

                    self.__display_ret_val_errors(r_vals=ret_vals,info=info)

                else:
                    msg = f'{info}: response code {response.status_code}'
                    logging.error(f'\t\t{msg}')
                    record_errors(msg=msg)

            except Exception as e:
                msg = f'{info}: ERROR: {e}'
                logging.error(f'\t\t{msg}')
                record_errors(msg=msg)

            return ret_vals

        ##########################################################################################
        return pull_from_source(ticker=ticker,
                                keys_list=keys_list,
                                source_name=source_name,
                                pull_method=pull_stats)

    # RAPI
    def yahoo_finance_1_RAPI_stats(self, ticker, keys_list, source_name='yahoofinance'):
        """
        https://rapidapi.com/sparior/api/yahoo-finance15/
        500/month
        """

        def pull_stats(ticker,key):
            function_name = "yahoo_finance_stats"
            info = f'{function_name},{ticker}, key:{key["key_num"]}'

            logging.info(f'\t trying {info}')

            url = f"https://yahoo-finance15.p.rapidapi.com/api/yahoo/qu/quote/{ticker}/default-key-statistics"

            headers = {
                "X-RapidAPI-Key": key["key"],
                "X-RapidAPI-Host": "yahoo-finance15.p.rapidapi.com"
            }

            ret_vals = {'resp_code': None, 'results': {}}

            try:
                response = requests.request("GET", url, headers=headers, timeout=REQUEST_TIMEOUT)
                if response.status_code == 200:
                    ret_vals['resp_code'] = response.status_code
                    resp_dict = json.loads(response.text)['defaultKeyStatistics']

                    for k,v in resp_dict.items():
                        if isinstance(v,dict) and 'raw' in v:
                            resp_dict[k] = v['raw']

                    names = [
                        {'local_key': 'shares_float', 'source_key': 'floatShares'},
                        {'local_key': 'shares_outstanding', 'source_key': 'sharesOutstanding'},
                        {'local_key': 'shares_short', 'source_key': 'sharesShort'},
                        {'local_key': 'short_perc_float', 'source_key': 'shortPercentOfFloat','to_type':'adjust perc'}
                    ]

                    for name in names:
                        ret_vals = try_stats(source_dict=resp_dict, ret_vals=ret_vals, key_names=name)

                    self.__display_ret_val_errors(r_vals=ret_vals,info=info)

                else:
                    msg = f'{info}: response code {response.status_code}'
                    logging.error(f'\t\t{msg}')
                    record_errors(msg=msg)

            except Exception as e:
                msg = f'{info}: ERROR: {e}'
                logging.error(f'\t\t{msg}')
                record_errors(msg=msg)

            return ret_vals

        #########################################################################################
        return pull_from_source(ticker=ticker,
                                keys_list=keys_list,
                                source_name=source_name,
                                pull_method=pull_stats)

    #non RAPI
    def polygon_stats(self, ticker, keys_list,source_name='polygon'):

        def pull_stats(ticker,key):
            function_name = "polygon_stats"
            info = f'{function_name},{ticker}, key:{key["key_num"]}'

            logging.info(f'\t trying {info}')

            url = f'https://api.polygon.io/v3/reference/tickers/{ticker}?apiKey={key["key"]}'

            ret_vals = {'resp_code': None, 'results': {}}

            try:
                response = requests.request("GET", url, timeout=REQUEST_TIMEOUT)
                ret_vals['resp_code'] = response.status_code
                if response.status_code == 200:
                    resp_dict = response.json()['results']

                    names = [
                        {'local_key': 'market_cap', 'source_key': 'market_cap'},
                        {'local_key': 'shares_outstanding', 'source_key': 'share_class_shares_outstanding'}
                    ]

                    for name in names:
                        ret_vals = try_stats(source_dict=resp_dict, ret_vals=ret_vals, key_names=name)

                    self.__display_ret_val_errors(r_vals=ret_vals,info=info)

                else:
                    msg = f'{info}: response code {response.status_code}'
                    logging.error(f'\t\t{msg}')
                    record_errors(msg=msg)

            except Exception as e:
                msg = f'{info}: ERROR: {e}'
                logging.error(f'\t\t{msg}')
                record_errors(msg=msg)

            return ret_vals

        #########################################################################################
        return pull_from_source(ticker=ticker,
                                keys_list=keys_list,
                                source_name=source_name,
                                pull_method=pull_stats)

    # RAPI
    def morningstar_API_stats(self,ticker, keys_list,source_name='morningstar'):

        def pull_stats(ticker,key):

            def get_performance_id(ticker,key):
                PID_ret_vals = {'resp_code': None, 'results': {}}
                try:
                    url = "https://ms-finance.p.rapidapi.com/market/v2/auto-complete"

                    querystring = {"q": ticker}

                    headers = {
                        "X-RapidAPI-Key": key,
                        "X-RapidAPI-Host": "ms-finance.p.rapidapi.com"
                    }

                    response = requests.request("GET", url, headers=headers, params=querystring, timeout=REQUEST_TIMEOUT)
                    PID_ret_vals['resp_code'] = response.status_code

                    if response.status_code == 200:
                        PID_ret_vals['results'] = json.loads(response.content)['results'][0]['performanceId']
                    else:
                        msg = f'{info}: response code {response.status_code} on getting performance ID'
                        logging.error(f'\t\t{msg}')
                        record_errors(msg=msg)

                except Exception as e:
                    msg = f'{info}: problem getting performance id - ERROR: {e}'
                    logging.info(f'\t\t{msg}')
                    record_errors(msg=msg)

                return PID_ret_vals

            ####################################################################################
            function_name = "morningstar_API_stats"
            info = f'{function_name},{ticker}, key:{key["key_num"]}'

            logging.info(f'\t trying {info}')

            PID_results = get_performance_id(ticker=ticker,key=key['key'])

            if not PID_results['results'] or PID_results['resp_code'] != 200:
                return PID_results

            url = "https://ms-finance.p.rapidapi.com/stock/v2/get-short-interest"
            querystring = {"performanceId": PID_results['results']}
            headers = {
                "X-RapidAPI-Key": key["key"],
                "X-RapidAPI-Host": "ms-finance.p.rapidapi.com"
            }

            ret_vals = {'resp_code': None, 'results': {}}

            try:
                response = requests.request("GET", url, headers=headers, params=querystring, timeout=REQUEST_TIMEOUT)
                ret_vals['resp_code'] = response.status_code

                if response.status_code == 200:
                    resp_dict = json.loads(response.content)

                    names = [
                        {'local_key': 'shares_float', 'source_key': 'floatShares','to_type':'adjust to millions'},
                        {'local_key': 'shares_outstanding', 'source_key': 'sharesOutstanding','to_type':'adjust to millions'},
                        {'local_key': 'shares_short', 'source_key': 'sharesShorted'},
                        {'local_key': 'short_perc_float', 'source_key': 'floatSharesShorted'},
                    ]

                    for name in names:
                        ret_vals = try_stats(source_dict=resp_dict, ret_vals=ret_vals, key_names=name)

                    self.__display_ret_val_errors(r_vals=ret_vals,info=info)

                else:
                    msg = f'{info}: response code {response.status_code}'
                    logging.error(f'\t\t{msg}')
                    record_errors(msg=msg)

            except Exception as e:
                msg = f'{info}: ERROR: {e}'
                logging.error(f'\t\t{msg}')
                record_errors(msg=msg)

            return ret_vals

        #########################################################################################
        return pull_from_source(ticker=ticker,
                                keys_list=keys_list,
                                source_name=source_name,
                                pull_method=pull_stats)

    # RAPI
    def mboum_API_stats(self,ticker, keys_list,source_name='mboum'):

        def pull_stats(ticker,key):
            function_name = "mboum_API_stats"
            info = f'{function_name},{ticker}, key:{key["key_num"]}'

            logging.info(f'\t trying {info}')

            url = "https://mboum-finance.p.rapidapi.com/qu/quote/default-key-statistics"

            querystring = {"symbol": ticker}

            headers = {
                "X-RapidAPI-Key": key["key"],
                "X-RapidAPI-Host": "mboum-finance.p.rapidapi.com"
            }

            ret_vals = {'resp_code': None, 'results': {}}

            try:
                response = requests.request("GET", url, headers=headers, params=querystring, timeout=REQUEST_TIMEOUT)
                ret_vals['resp_code'] = response.status_code
                if response.status_code == 200:
                    resp_dict = json.loads(response.content)['defaultKeyStatistics']

                    for k,v in resp_dict.items():
                        if isinstance(v,dict) and 'raw' in v:
                            resp_dict[k] = v['raw']

                    names = [
                        {'local_key': 'shares_float', 'source_key': 'floatShares'},
                        {'local_key': 'shares_outstanding', 'source_key': 'sharesOutstanding'},
                        {'local_key': 'shares_short', 'source_key': 'sharesShort'},
                        {'local_key': 'short_perc_float', 'source_key': 'shortPercentOfFloat','to_type':'adjust perc'},
                    ]

                    for name in names:
                        ret_vals = try_stats(source_dict=resp_dict, ret_vals=ret_vals, key_names=name)

                    self.__display_ret_val_errors(r_vals=ret_vals,info=info)

                else:
                    msg = f'{info}: response code {response.status_code}'
                    logging.error(f'\t\t{msg}')
                    record_errors(msg=msg)

            except Exception as e:
                msg = f'{info}: ERROR: {e}'
                logging.error(f'\t\t{msg}')
                record_errors(msg=msg)

            return ret_vals

        #########################################################################################
        return pull_from_source(ticker=ticker,
                                keys_list=keys_list,
                                source_name=source_name,
                                pull_method=pull_stats)

    ## RAPI !!! DOESNT WORK ANYMORE, API METHOD DEPRECATED
    def seeking_alpha_stats(self,ticker, keys_list, source_name='seeking_alpha'):

        def pull_stats(ticker,key):
            function_name = "seeking_alpha_stats"
            info = f'{function_name},{ticker}, key:{key["key_num"]}'

            logging.info(f'\t trying {info}')

            url = "https://seeking-alpha.p.rapidapi.com/symbols/get-key-data"

            querystring = {"symbol": ticker}

            headers = {
                "X-RapidAPI-Key": key['key'],
                "X-RapidAPI-Host": "seeking-alpha.p.rapidapi.com"
            }

            ret_vals = {'resp_code': None, 'results': {}}

            try:
                response = requests.request("GET", url, headers=headers, params=querystring, timeout=REQUEST_TIMEOUT)
                ret_vals['resp_code'] = response.status_code
                if response.status_code == 200:
                    resp_dict = json.loads(response.content)['data'][0]['attributes']

                    names = [
                        {'local_key': 'market_cap', 'source_key': 'marketCap'},
                        {'local_key': 'sma200', 'source_key': 'movAvg200d'},
                        {'local_key': 'shares_float', 'source_key': 'shares'}
                    ]

                    for name in names:
                        ret_vals = try_stats(source_dict=resp_dict, ret_vals=ret_vals, key_names=name)

                    self.__display_ret_val_errors(r_vals=ret_vals,info=info)

                else:
                    msg = f'{info}: response code {response.status_code}'
                    logging.error(f'\t\t{msg}')
                    record_errors(msg=msg)

            except Exception as e:
                msg = f'{info}: ERROR: {e}'
                logging.error(f'\t\t{msg}')
                record_errors(msg=msg)

            return ret_vals

        ##########################################################################
        return pull_from_source(ticker=ticker,
                                keys_list=keys_list,
                                source_name=source_name,
                                pull_method=pull_stats)

    ######### CONTINGENCIES #################################################################
    #non RAPI
    def contingency_polygon_200sma(self, ticker, keys_list, source_name='polygon'):

        def pull_stats(ticker,key):
            function_name = "polygon_200_sma"
            info = f'{function_name},{ticker}, key:{key["key_num"]}'

            logging.info(f'\t trying {info}')

            url = f'https://api.polygon.io/v1/indicators/sma/{ticker}?timespan=day&adjusted=' \
                  f'true&window=200&series_type=close&order=desc&limit=1&apiKey={key["key"]}'

            ret_vals = {'resp_code': None, 'results': {}}

            try:
                response = requests.request("GET", url, timeout=REQUEST_TIMEOUT)
                ret_vals['resp_code'] = response.status_code
                if response.status_code == 200:
                    resp_dict = response.json()['results']['values'][0]

                    names = [{'local_key': 'sma200', 'source_key': 'value'}]
                    for name in names:
                        ret_vals = try_stats(source_dict=resp_dict, ret_vals=ret_vals, key_names=name)

                    self.__display_ret_val_errors(r_vals=ret_vals,info=info)

                else:
                    msg = f'{info}: response code {response.status_code}'
                    logging.error(f'\t\t{msg}')
                    record_errors(msg=msg)

            except Exception as e:
                msg = f'{info}: ERROR: {e}'
                logging.error(f'\t\t{msg}')
                record_errors(msg=msg)

            return ret_vals

        ##########################################################################################
        return pull_from_source(ticker=ticker,
                                keys_list=keys_list,
                                source_name=source_name,
                                pull_method=pull_stats)

    # RAPI
    def contingency_twelve_data_200sma(self, ticker, keys_list, source_name='twelve_data'):

        def pull_stats(ticker,key):
            function_name = "twelve_data_200sma"
            info = f'{function_name},{ticker}, key:{key["key_num"]}'

            logging.info(f'\t trying {info}')

            url = "https://twelve-data1.p.rapidapi.com/sma"

            querystring = {"interval": "1day",
                           "symbol": ticker,
                           "time_period": "200",
                           "outputsize": "1",
                           "format": "json",
                           "series_type": "close"}

            headers = {
                "X-RapidAPI-Key": key["key"],
                "X-RapidAPI-Host": "twelve-data1.p.rapidapi.com"
            }

            ret_vals = {'resp_code': None, 'results': {}}

            try:
                response = requests.request("GET", url, headers=headers, params=querystring, timeout=REQUEST_TIMEOUT)
                ret_vals['resp_code'] = response.status_code

                if response.status_code == 200:
                    resp_dict = response.json()['values'][0]

                    names = [{'local_key':'sma200','source_key':'sma','to_type':'float'}]

                    for name in names:
                        ret_vals = try_stats(source_dict=resp_dict, ret_vals=ret_vals, key_names=name)

                    self.__display_ret_val_errors(r_vals=ret_vals,info=info)

                else:
                    msg = f'{info}: response code {response.status_code}'
                    logging.error(f'\t\t{msg}')
                    record_errors(msg=msg)

            except Exception as e:
                msg = f'{info}: ERROR: {e}'
                logging.error(f'\t\t{msg}')
                record_errors(msg=msg)

            return ret_vals

        ###########################################################################
        return pull_from_source(ticker=ticker,
                                keys_list=keys_list,
                                source_name=source_name,
                                pull_method=pull_stats)

    # scraper
    def contingency_marketwatch_stats(self,ticker, keys_list, source_name='marketwatch'):

        def pull_stats(ticker, key=None):

            def get_stats(resp,ret_vals):
                soup = BeautifulSoup(resp.content, 'lxml')

                list_items = soup.find_all('li')
                for i in list_items:
                    strings_list = i.text.lower().strip(' ').split('\n')
                    strings_list = [x for x in strings_list if x]

                    if strings_list and \
                            len(strings_list) <= 4 and \
                            len(strings_list) > 1 and \
                            [t for t in search_tags.values() if t in strings_list]:

                        try:
                            key = [k for k, v in search_tags.items() if strings_list[0] in v][0]
                            value = helper_functions.string_num_converter(value=strings_list[1])
                            if key and value:
                                ret_vals['results'][key] = value

                        except:
                            pass

                return ret_vals

            ###############################################################
            function_name = "contingency_marketwatch_stats"
            info = f'{function_name},{ticker}, key:{key["key_num"]}'

            logging.info(f'\t trying {info}')

            url = f"https://www.marketwatch.com/investing/stock/{ticker.lower()}?mod=search_symbol"

            ret_vals = {'resp_code': None, 'results': {}}

            search_tags = {
                'market_cap': 'market cap',
                'shares_oustanding': 'shares outstanding',
                'shares_float': 'public float',
                'short_perc_float': 'of float shorted',
                'shares_short': 'short interest'
            }

            try:
                response = requests.get(url=url, headers={'User-Agent': generate_user_agent()}, timeout=REQUEST_TIMEOUT)
                ret_vals['resp_code'] = response.status_code
                if response.status_code == 200:

                    ret_vals = get_stats(resp=response,ret_vals=ret_vals)
                    self.__display_ret_val_errors(r_vals=ret_vals, info=info)

                else:
                    msg = f'{info}: response code {response.status_code}'
                    logging.error(f'\t\t{msg}')
                    record_errors(msg=msg)

            except Exception as e:
                msg = f'{info}: ERROR: {e}'
                logging.error(f'\t\t{msg}')
                record_errors(msg=msg)

            return ret_vals

        ###############################################################################

        return pull_from_source(ticker=ticker,
                                keys_list=keys_list,
                                source_name=source_name,
                                pull_method=pull_stats)

    # scraper
    def congingency_stock_analysis_stats(self,ticker, keys_list, source_name='stock_analysis'):

        def pull_stats(ticker,key=None):

            def get_stats(resp,ret_vals):
                soup = BeautifulSoup(response.content, 'lxml')

                list_items = soup.find_all('tr')
                for i in list_items:
                    strings_list = i.text.lower().strip(' ').replace('\t', '').split('\n')
                    strings_list = [x for x in strings_list if x]
                    if len(strings_list) <= 4 and [t for t in search_tags.values() if t in strings_list]:

                        try:
                            key = [k for k, v in search_tags.items() if strings_list[0] in v][0]
                            value = helper_functions.string_num_converter(value=strings_list[1])
                            if key and value:
                                ret_vals['results'][key] = value
                        except:
                            pass

                return ret_vals

            #################################################################################
            function_name = "contingency_stock_analysis_stats"
            info = f'{function_name},{ticker}, key:{key["key_num"]}'

            logging.info(f'\t trying {info}')

            url = f"https://stockanalysis.com/stocks/{ticker.lower()}/statistics/"

            ret_vals = {'resp_code': None, 'results': {}}

            search_tags = {
                'market_cap': 'market cap',
                'shares_oustanding': 'shares outstanding',
                'shares_float': 'float',
                'short_perc_float': 'short % of float',
                'shares_short': 'short interest',
                'sma200': '200-day moving average'
            }

            try:
                response = requests.get(url=url, headers={'User-Agent': generate_user_agent()}, timeout=REQUEST_TIMEOUT)
                ret_vals['resp_code'] = response.status_code
                if response.status_code == 200:

                    ret_vals = get_stats(resp=response, ret_vals=ret_vals)
                    self.__display_ret_val_errors(r_vals=ret_vals, info=info)

                else:
                    msg = f'{info}: response code {response.status_code}'
                    logging.error(f'\t\t{msg}')
                    record_errors(msg=msg)

            except Exception as e:
                msg = f'{info}: ERROR: {e}'
                logging.error(f'\t\t{msg}')
                record_errors(msg=msg)

            return ret_vals

        ##################################################################

        return pull_from_source(ticker=ticker,
                                keys_list=keys_list,
                                source_name=source_name,
                                pull_method=pull_stats)


