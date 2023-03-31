from get_screens.settings._settings import *

try:
    import environment_vars
except:
    pass

def string_num_converter(value, convert_to='num'):
    if value == '-' or value == '' or value == None or value == 'N/A':  # or math.isnan(string) == True:
        return None

    if convert_to == 'num':
        if isinstance(value,(int,float)):
            return value
        # convert string to number
        multipliers = {'K': 1000, 'k': 1000, 'M': 1000000, 'm': 1000000,
                       'B': 1000000000, 'b': 1000000000, 'T': 1000000000000, 't': 1000000000000}

        # # check if getting passed an integer or float
        # test = isinstance(value, (int, float))
        # if test == True:
        #     return value

        # gets rid of unwanted characters
        char_set = [' ', '$', ',']
        for char in char_set:
            if char in value:
                value = value.replace(char, '')

        # check if value is a percentage
        if value[-1] == '%':
            value = value.replace('%', '')
            value = float(value)
            return value

        # check if theres a suffix at the end i.e (5.89M, 600K, ect)
        if value[-1].isalpha():
            mult = multipliers[value[-1]]  # look up suffix to get multiplier
            value = int(float(value[:-1]) * mult)  # convert number to float, multiply by multiplier, then make int
            return value

        # else if theres nothing else that needs to be done return string as number
        else:
            value = float(value)
            if value % 1 == 0:
                return int(value)
            else:
                return value

    if convert_to == 'str':  # convert number to string
        # if the number isn't a percentage
        if value >= 1:
            value = '{:,}'.format(value)
            return value

        if value < 1 and value > -1:
            value = str(round((value * 100), 2)) + '%'
            return value

def get_last_x_trading_days(curr_date=None, days=5):
    import pandas_market_calendars as mcal

    def get_dates_list():
        start = (curr_date - datetime.timedelta(days=lookback))

        dates_df = nyse.schedule(start_date=start.strftime('%Y-%m-%d'),
                                 end_date=curr_date.strftime('%Y-%m-%d'))

        dates_list = list(dates_df.to_dict(orient='index'))
        ret_list = [x.to_pydatetime() for x in dates_list]
        return ret_list

    ######################################################
    if not curr_date:
        curr_date = datetime_now()

    nyse = mcal.get_calendar('NYSE')

    lookback = days

    ret_list = get_dates_list()

    while len(ret_list) < days:
        lookback += 1
        ret_list = get_dates_list()

    return ret_list

def get_previous_trading_day(curr_date):
    curr_date = datetime.datetime.combine(curr_date, datetime.time.min)
    dates_list = get_last_x_trading_days(curr_date=curr_date)

    if curr_date == dates_list[-1]:
        return dates_list[-2]
    else:
        return dates_list[-1]

def is_trading_day(curr_date):
    """
    returns true if markets are open today
    """
    curr_date = curr_date.date()

    dates_list = get_last_x_trading_days(curr_date=curr_date)

    if curr_date == dates_list[-1].date():
        return True
    else:
        return False
