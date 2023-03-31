import pandas as pd
from django.shortcuts import render
from display_screens.models import FinalScreenResults
import display_screens.helper_functions as hf
import datetime
from functools import reduce

# https://startbootstrap.com/template/sb-admin

TIMEFRAMES = {'last_week': 5,
              'last_two_weeks': 10,
              'last_month': 23,
              'last_three_months': 70,
              'last_six_months': 140,
              'last_year': 260}

TOP_X = 10

def handle_date_exception(dates_result_len,num_days,timeframe):
    ret_msg = None
    if dates_result_len < num_days and timeframe:
        ret_msg = f'REQUESTED LOOKBACK PERIOD EXCEEDS EARLIEST AVAILABLE DATE, ' \
                  f'DATA ONLY AVAILABLE FOR LAST {str(dates_result_len)} DAYS'
    return ret_msg

def share_stats_template(tv,timeframe,num_days):
    """
    num_days
    stat_label
    stat_name
    y_axis_title
    data_suffix
    """
    dates_result = list(
        FinalScreenResults.objects.order_by('-datetime').values('datetime').distinct()[:tv['num_days']].values('datetime'))

    start_date = datetime.datetime.strftime(dates_result[-1]['datetime'], '%Y-%m-%d')
    end_date = datetime.datetime.strftime(dates_result[0]['datetime'], '%Y-%m-%d')

    results = list(FinalScreenResults.objects.filter(datetime__gte=start_date, datetime__lte=end_date)
                   .values('datetime', 'max_pct_gain', tv['stat_label']))

    df = pd.DataFrame(results)
    df['datetime'] = pd.to_datetime(df['datetime'])
    df = df.sort_values(['max_pct_gain'], ascending=False).groupby('datetime').head(TOP_X)

    avg_df = hf.get_df_average(input_df=df, base_col_name='datetime', target_col_name=tv['stat_label'])
    med_df = hf.get_df_median(input_df=df, base_col_name='datetime', target_col_name=tv['stat_label'])
    cluster_avg_df = hf.get_df_cluster_average(input_df=df, base_col_name='datetime', target_col_name=tv['stat_label'])
    std_dev_df = hf.get_df_std_dev(input_df=df, base_col_name='datetime', target_col_name=tv['stat_label'])

    stats_df = reduce(lambda left, right: pd.merge(left, right, on=['datetime'], how='outer'),
                      [avg_df, med_df, cluster_avg_df,std_dev_df])

    charting = hf.Charting()

    avg_chart = charting.create_basic_chart(df=stats_df,
                                            col_name=tv['stat_label'] + '_average',
                                            name=tv['stat_name'] + ' Average',
                                            color='red',
                                            y_axis_title=tv['y_axis_title'])

    med_chart = charting.create_basic_chart(df=stats_df,
                                            col_name=tv['stat_label'] + '_median',
                                            name=tv['stat_name'] + ' Median',
                                            color='red',
                                            y_axis_title=tv['y_axis_title'])

    cluster_avg_chart = charting.create_basic_chart(df=stats_df,
                                                    col_name=tv['stat_label'] + '_cluster_avg',
                                                    name=tv['stat_name'] + ' Cluster Average',
                                                    color='red',
                                                    y_axis_title=tv['y_axis_title'])

    std_dev_chart = charting.create_basic_chart(df=stats_df,
                                                    col_name=tv['stat_label'] + '_std',
                                                    name=tv['stat_name'] + ' Standard Deviation',
                                                    color='red',
                                                    y_axis_title=tv['y_axis_title'])

    stats_df = stats_df.sort_values(by='datetime', ascending=False)
    stats = stats_df.to_dict(orient='records')

    stats_list = []
    for stat in stats:
        stat['datetime'] = datetime.datetime.strftime(stat['datetime'], '%Y-%m-%d')
        stat[tv['stat_label'] + '_average'] = str(round(stat[tv['stat_label'] + '_average'], 2)) + tv['data_suffix']
        stat[tv['stat_label'] + '_median'] = str(round(stat[tv['stat_label'] + '_median'], 2)) + tv['data_suffix']
        stat[tv['stat_label'] + '_cluster_avg'] = str(round(stat[tv['stat_label'] + '_cluster_avg'], 2)) + tv['data_suffix']
        stat[tv['stat_label'] + '_std'] = str(round(stat[tv['stat_label'] + '_std'], 2)) + tv['data_suffix']
        stats_list.append(stat)

    context = {
        'top_x': TOP_X,
        'start_date': start_date,
        'end_date': end_date,
        'date_msg': handle_date_exception(dates_result_len=len(dates_result),
                                          num_days=num_days,
                                          timeframe=timeframe),
        'stats': stats_list,
        'med_chart': med_chart,
        'avg_chart': avg_chart,
        'cluster_avg_chart': cluster_avg_chart,
        'std_dev_chart':std_dev_chart,
        'stat_label':tv['stat_label'],
        'stat_name':tv['stat_name']
    }

    return context

def price_related_stats_template(tv,timeframe,num_days):
    dates_result = list(
        FinalScreenResults.objects.order_by('-datetime').values('datetime').distinct()[:num_days].values('datetime'))

    start_date = datetime.datetime.strftime(dates_result[-1]['datetime'], '%Y-%m-%d')
    end_date = datetime.datetime.strftime(dates_result[0]['datetime'], '%Y-%m-%d')

    results = list(FinalScreenResults.objects.filter(datetime__gte=start_date, datetime__lte=end_date)
                   .values('datetime', tv['stat_label'], 'max_pct_gain'))

    df = pd.DataFrame(results)
    df['datetime'] = pd.to_datetime(df['datetime'])
    df = df.sort_values(['max_pct_gain'], ascending=False).groupby('datetime').head(TOP_X)

    avg_df = hf.get_df_average(input_df=df, base_col_name='datetime', target_col_name=tv['stat_label'])
    med_df = hf.get_df_median(input_df=df, base_col_name='datetime', target_col_name=tv['stat_label'])
    cluster_avg_df = hf.get_df_cluster_average(input_df=df, base_col_name='datetime', target_col_name=tv['stat_label'])
    std_df = hf.get_df_std_dev(input_df=df, base_col_name='datetime', target_col_name=tv['stat_label'])

    stats_df = reduce(lambda left, right: pd.merge(left, right, on=['datetime'],
                                                   how='outer'), [avg_df, med_df, cluster_avg_df, std_df])

    charting = hf.Charting()

    avg_chart = charting.create_basic_chart(df=stats_df,
                                            col_name=tv['stat_label']+'_average',
                                            name=tv['stat_name']+' Average',
                                            color='red',
                                            y_axis_title=tv['y_axis_title'])

    med_chart = charting.create_basic_chart(df=stats_df,
                                            col_name=tv['stat_label']+'_median',
                                            name=tv['stat_name']+' Median',
                                            color='red',
                                            y_axis_title=tv['y_axis_title'])

    cluster_avg_chart = charting.create_basic_chart(df=stats_df,
                                                    col_name=tv['stat_label']+'_cluster_avg',
                                                    name=tv['stat_name']+' Cluster Average',
                                                    color='red',
                                                    y_axis_title=tv['y_axis_title'])

    std_dev_chart = charting.create_basic_chart(df=stats_df,
                                                col_name=tv['stat_label']+'_std',
                                                name=tv['stat_name']+' Standard Deviation',
                                                color='red',
                                                y_axis_title=tv['y_axis_title'])

    stats_df = stats_df.sort_values(by='datetime', ascending=False)
    stats = stats_df.to_dict(orient='records')

    stats_list = []
    for stat in stats:
        stat['datetime'] = datetime.datetime.strftime(stat['datetime'], '%Y-%m-%d')
        stat[tv['stat_label']+'_average'] = str(round(stat[tv['stat_label']+'_average'], 2))
        stat[tv['stat_label']+'_median'] = str(round(stat[tv['stat_label']+'_median'], 2))
        stat[tv['stat_label']+'_cluster_avg'] = str(round(stat[tv['stat_label']+'_cluster_avg'], 2))
        stat[tv['stat_label']+'_std'] = str(round(stat[tv['stat_label']+'_std'], 2))
        stats_list.append(stat)

    context = {
        'top_x': TOP_X,
        'start_date': start_date,
        'end_date': end_date,
        'date_msg': handle_date_exception(dates_result_len=len(dates_result),
                                          num_days=num_days,
                                          timeframe=timeframe),
        'stats': stats_list,
        'med_chart': med_chart,
        'avg_chart': avg_chart,
        'cluster_avg_chart': cluster_avg_chart,
        'std_dev_chart': std_dev_chart
    }

    return context
###########################################################

def home(request,selected_date=None,ticker=None):

    dates_result = FinalScreenResults.objects.order_by('-datetime').values('datetime').distinct()[:10]
    dates_result = [datetime.datetime.strftime(x['datetime'],'%Y-%m-%d') for x in dates_result]

    if not selected_date:
        selected_date = dates_result[0]

    top_results_qs = list(FinalScreenResults.objects.filter(datetime__exact=selected_date).order_by('-max_pct_gain')
                       .values('symbol','datetime','prev_close','max_pct_gain','ext_market_activity','market_cap',
                               'shares_float','short_perc_float','shares_short','shares_outstanding','dollar_volume',
                               'sector','industry','close_to_range'))

    top_results = []
    for symbol in top_results_qs:
        symbol['datetime'] = datetime.datetime.strftime(symbol['datetime'],'%Y-%m-%d')
        top_results.append(symbol)

    if not ticker:
        ticker = top_results[0]['symbol'].upper()



    #ticker_results = list(filter(lambda x: x['symbol'] == ticker, top_results))

    tr = list(FinalScreenResults.objects.filter(datetime__exact=selected_date,symbol=ticker).values())[0]
    ticker_candle_df = pd.DataFrame(tr['ohlc_data'])

    ticker_result = [
        [['Symbol',tr['symbol']],['Date',str(tr['datetime']).split()[0]]],
        [['Max Pct Gain', str(tr['max_pct_gain']) + '%'],['Prev Close', tr['prev_close']]],
        [['Pre/Post Mkt Gain', str(tr['ext_market_activity']) + '%'], ['Market Cap', str(tr['market_cap']) + "M"]],
        [['Shares Outstanding',str(tr['shares_outstanding']) + "M"],['Float',str(tr['shares_float']) + "M"]],
        [['Shares Short',str(tr['shares_short']) + "M"],['Short Pct Float',str(tr['short_perc_float']) + '%']],
        [['Dollar Volume',str(tr['dollar_volume']) + "M"],['Close off HOD',str(tr['close_to_range']) + '%']],
        [['Sector',tr['sector']],['Industry',tr['industry']]],
    ]

    # df['Date'] = pd.to_datetime(df['Date'],unit = 's',utc=True)

    charting = hf.Charting()
    chart = charting.chart_days_top_gainer(df=ticker_candle_df)

    context = {
        'all_dates':dates_result,
        'selected_date':selected_date,
        'top_results':top_results,
        'ticker':ticker.upper(),
        'ticker_result':ticker_result,
        'chart':chart
    }

    return render(request,'home.html', context)

def gainer_stats(request, timeframe='last_two_weeks',top_x=TOP_X):

    date_exception_msg = None
    if not timeframe:
        num_days = 14
    else:
        num_days = TIMEFRAMES[timeframe]

    dates_result = list(FinalScreenResults.objects.order_by('-datetime').values('datetime').distinct()[:num_days].values('datetime'))

    start_date = datetime.datetime.strftime(dates_result[-1]['datetime'],'%Y-%m-%d')
    end_date = datetime.datetime.strftime(dates_result[0]['datetime'],'%Y-%m-%d')

    results = list(FinalScreenResults.objects.filter(datetime__gte=start_date,datetime__lte=end_date)
                   .values('datetime', 'max_pct_gain'))


    df = pd.DataFrame(results)
    df['datetime'] = pd.to_datetime(df['datetime'])
    df = df.sort_values(['max_pct_gain'], ascending=False).groupby('datetime').head(top_x)

    avg_df = hf.get_df_average(input_df=df, base_col_name='datetime', target_col_name='max_pct_gain')
    med_df = hf.get_df_median(input_df=df, base_col_name='datetime', target_col_name='max_pct_gain')
    max_df = hf.get_df_max(input_df=df, base_col_name='datetime', target_col_name='max_pct_gain')

    stats_df = reduce(lambda left, right: pd.merge(left, right, on=['datetime'],
                                          how='outer'), [avg_df,med_df,max_df])



    charting = hf.Charting()
    max_chart = charting.create_basic_chart(df=stats_df,
                                            col_name='max_pct_gain_max',
                                            name='Max',
                                            color='red',
                                            y_axis_title='Pct Gain')

    med_chart = charting.create_basic_chart(df=stats_df,
                                            col_name='max_pct_gain_median',
                                            name='Median',
                                            color='red',
                                            y_axis_title='Pct Gain')

    avg_chart = charting.create_basic_chart(df=stats_df,
                                            col_name='max_pct_gain_average',
                                            name='Cluster Average',
                                            color='red',
                                            y_axis_title='Pct Gain')

    stats_df = stats_df.sort_values(by='datetime',ascending=False)
    stats = stats_df.to_dict(orient='records')

    stats_list = []
    for stat in stats:
        stat['datetime'] = datetime.datetime.strftime(stat['datetime'], '%Y-%m-%d')
        stat['max_pct_gain_max'] = str(round(stat['max_pct_gain_max'],2))+'%'
        stat['max_pct_gain_average'] = str(round(stat['max_pct_gain_average'], 2)) + '%'
        stat['max_pct_gain_median'] = str(round(stat['max_pct_gain_median'], 2)) + '%'
        stats_list.append(stat)

    context = {
        'top_x':top_x,
        'start_date':start_date,
        'end_date':end_date,
        'date_msg':handle_date_exception(dates_result_len=len(dates_result),
                                         num_days=num_days,
                                         timeframe=timeframe),
        'stats':stats_list,
        'max_chart':max_chart,
        'med_chart':med_chart,
        'avg_chart':avg_chart
    }

    return render(request,'gainer_stats.html', context)

def price_stats(request, timeframe=None):

    if not timeframe:
        num_days = 14
    else:
        num_days = TIMEFRAMES[timeframe]

    template_vars = {
        'num_days':num_days,
        'stat_label':'prev_close',
        'stat_name':'Prev Close to 200 SMA',
        'y_axis_title':'Price',
        'data_suffix':''
    }

    context = price_related_stats_template(tv=template_vars,
                                           timeframe=timeframe,
                                           num_days=num_days)

    return render(request, 'price_stats.html', context)

def dist_from_200_sma_stats(request, timeframe=None):

    if not timeframe:
        num_days = 14
    else:
        num_days = TIMEFRAMES[timeframe]

    template_vars = {
        'num_days':num_days,
        'stat_label':'prev_close_to_200sma',
        'stat_name':'Prev Close to 200 SMA',
        'y_axis_title':'Percent',
        'data_suffix':'%'
    }

    context = price_related_stats_template(tv=template_vars,
                                           timeframe=timeframe,
                                           num_days=num_days)

    return render(request, 'dist_from_200_sma.html', context)

def close_to_range_stats(request, timeframe=None):

    if not timeframe:
        num_days = 14
    else:
        num_days = TIMEFRAMES[timeframe]

    template_vars = {
        'num_days':num_days,
        'stat_label':'close_to_range',
        'stat_name':'Close to Day Price Range',
        'y_axis_title':'Percent of Days Range',
        'data_suffix':'%'
    }

    context = price_related_stats_template(tv=template_vars,
                                           timeframe=timeframe,
                                           num_days=num_days)

    return render(request,'close_to_range_stats.html', context)

def dollar_volume_stats(request,timeframe=None):
    """
    average and total dollar volume for last x days of top y stocks
    """

    if not timeframe:
        num_days = 14
    else:
        num_days = TIMEFRAMES[timeframe]

    dates_result = list(
        FinalScreenResults.objects.order_by('-datetime').values('datetime').distinct()[:num_days].values('datetime'))

    start_date = datetime.datetime.strftime(dates_result[-1]['datetime'], '%Y-%m-%d')
    end_date = datetime.datetime.strftime(dates_result[0]['datetime'], '%Y-%m-%d')

    results = list(FinalScreenResults.objects.filter(datetime__gte=start_date, datetime__lte=end_date)
                   .values('datetime', 'dollar_volume', 'max_pct_gain'))

    df = pd.DataFrame(results)

    df = df.sort_values(['max_pct_gain'], ascending=False).groupby('datetime').head(TOP_X)
    df.drop(df.columns.difference(['datetime', 'dollar_volume']), 1, inplace=True)

    avg_df = hf.get_df_average(input_df=df, base_col_name='datetime', target_col_name='dollar_volume')
    sum_df = hf.get_df_sum(input_df=df, base_col_name='datetime', target_col_name='dollar_volume')

    stats_df = pd.merge(avg_df,sum_df)

    charting = hf.Charting()

    avg_chart = charting.create_basic_chart(df=stats_df,
                                            col_name='dollar_volume_average',
                                            name='Average',
                                            color='red',
                                            y_axis_title='Dollar Volume (Millions)')

    sum_chart = charting.create_basic_chart(df=stats_df,
                                            col_name='dollar_volume_sum',
                                            name='Median',
                                            color='red',
                                            y_axis_title='Dollar Volume (Millions)')

    stats = stats_df.to_dict(orient='records')

    stats_list = []
    for stat in stats:
        stat['datetime'] = datetime.datetime.strftime(stat['datetime'], '%Y-%m-%d')
        stat['dollar_volume_average'] = str(round(stat['dollar_volume_average'], 2)) + 'M'
        stat['dollar_volume_sum'] = str(round(stat['dollar_volume_sum'], 2)) + 'M'
        stats_list.append(stat)

    context = {
        'top_x': TOP_X,
        'start_date': start_date,
        'end_date': end_date,
        'date_msg': handle_date_exception(dates_result_len=len(dates_result),
                                          num_days=num_days,
                                          timeframe=timeframe),
        'stats': stats_list,
        'sum_chart': sum_chart,
        'avg_chart': avg_chart
    }

    return render(request,'dollar_volume_stats.html', context)

def market_cap(request,timeframe=None):

    if not timeframe:
        num_days = 14
    else:
        num_days = TIMEFRAMES[timeframe]

    template_vars = {
        'num_days':num_days,
        'stat_label':'market_cap',
        'stat_name':'Market Cap',
        'y_axis_title':'Millions of Dollars',
        'data_suffix':'M'
    }

    context = share_stats_template(tv=template_vars,
                                   timeframe=timeframe,
                                   num_days=num_days)

    return render(request, 'market_cap.html', context)

def shares_float(request, timeframe=None):

    if not timeframe:
        num_days = 14
    else:
        num_days = TIMEFRAMES[timeframe]

    template_vars = {
        'num_days':num_days,
        'stat_label':'shares_float',
        'stat_name':'Shares Float',
        'y_axis_title':'Millions of Shares',
        'data_suffix':'M'
    }

    context = share_stats_template(tv=template_vars,
                                   timeframe=timeframe,
                                   num_days=num_days)

    return render(request, 'shares_float.html', context)

def shares_outstanding(request, timeframe=None):

    if not timeframe:
        num_days = 14
    else:
        num_days = TIMEFRAMES[timeframe]

    template_vars = {
        'num_days':num_days,
        'stat_label':'shares_outstanding',
        'stat_name':'Shares Outstanding',
        'y_axis_title':'Millions of Shares',
        'data_suffix':'M'
    }

    context = share_stats_template(tv=template_vars,
                                   timeframe=timeframe,
                                   num_days=num_days)

    return render(request, 'shares_outstanding.html', context)

def shares_short(request, timeframe=None):

    if not timeframe:
        num_days = 14
    else:
        num_days = TIMEFRAMES[timeframe]

    template_vars = {
        'num_days':num_days,
        'stat_label':'shares_short',
        'stat_name':'Shares Short',
        'y_axis_title':'Millions of Shares',
        'data_suffix':'M'
    }

    context = share_stats_template(tv=template_vars,
                                   timeframe=timeframe,
                                   num_days=num_days)

    return render(request, 'shares_short.html', context)

def short_perc_float(request, timeframe=None):

    if not timeframe:
        num_days = 14
    else:
        num_days = TIMEFRAMES[timeframe]

    template_vars = {
        'num_days':num_days,
        'stat_label':'short_perc_float',
        'stat_name':'Percentage of Float Short',
        'y_axis_title':'Percent of Float',
        'data_suffix':'%'
    }

    context = share_stats_template(tv=template_vars,
                                   timeframe=timeframe,
                                   num_days=num_days)

    return render(request, 'short_perc_float.html', context)

def time_window_performance(request,timeframe=None):

    # TODO put a resample in here and tie it to the number of periods you want to split the day into
    if not timeframe:
        num_days = 14
    else:
        num_days = TIMEFRAMES[timeframe]

    dates_result = list(
        FinalScreenResults.objects.order_by('-datetime').values('datetime').distinct()[:num_days].values('datetime'))

    start_date = datetime.datetime.strftime(dates_result[-1]['datetime'], '%Y-%m-%d')
    end_date = datetime.datetime.strftime(dates_result[0]['datetime'], '%Y-%m-%d')

    results = list(FinalScreenResults.objects.filter(datetime__gte=start_date, datetime__lte=end_date)
                   .values('symbol', 'datetime', 'max_pct_gain', 'ohlc_data'))

    df = pd.DataFrame(results)
    df['datetime'] = pd.to_datetime(df['datetime'])

    df = df.sort_values(['max_pct_gain'], ascending=False).groupby('datetime').head(TOP_X).sort_values(['datetime'])
    df = df.drop(['symbol', 'datetime', 'max_pct_gain'], axis=1)
    ohlc_data = df['ohlc_data'].tolist()

    ohlc_list = []
    for x_list in ohlc_data:
        for x in x_list:
            ohlc_list.append(x)

    ohlc_df = pd.DataFrame(ohlc_list)
    ohlc_df['Date'] = pd.to_datetime(ohlc_df['Date'])

    ohlc_df['Time'] = pd.to_datetime(ohlc_df['Date'], "%H:%M:%S").dt.time

    ohlc_df['dollar_volume'] = ((abs(ohlc_df['Open'] + ohlc_df['Close']) / 2) * ohlc_df['Volume'])/1_000_000
    ohlc_df['candle_gain'] = ((ohlc_df['Close'] - ohlc_df['Open']) / ohlc_df['Open']) * 100
    ohlc_df = ohlc_df.drop(['Date','Open','High','Low','Close','Volume'], axis=1)

    cluster_avg_ohlc_df = hf.get_df_cluster_average_for_time_charts(input_df=ohlc_df,
                                                                   group_col='Time',
                                                                   col_names=['candle_gain', 'dollar_volume'])
    cluster_avg_ohlc_df.rename(columns={'dollar_volume': 'cluster_avg_dollar_volume',
                                       'candle_gain': 'cluster_avg_candle_gain'}, inplace=True)

    avg_ohlc_df = ohlc_df.groupby('Time').mean().reset_index()
    avg_ohlc_df.rename(columns={'dollar_volume': 'avg_dollar_volume',
                                'candle_gain':'avg_candle_gain'}, inplace=True)

    med_ohlc_df = ohlc_df.groupby('Time').median().reset_index()
    med_ohlc_df.rename(columns={'dollar_volume': 'med_dollar_volume',
                                'candle_gain': 'med_candle_gain'}, inplace=True)

    std_ohlc_df = ohlc_df.groupby('Time').std().reset_index()
    std_ohlc_df.rename(columns={'dollar_volume': 'std_dollar_volume',
                                'candle_gain': 'std_candle_gain'}, inplace=True)

    charting = hf.Charting()
    avg_chart = charting.create_timeframe_chart(df=avg_ohlc_df,
                                                col_names=['avg_dollar_volume', 'avg_candle_gain'],
                                                names=['Average Dollar Volume', 'Average Candle Gain'],
                                                colors=['orange', 'blue'],
                                                y_axis_titles=['$ Volume (Millions)', 'Pct Gain'])

    med_chart = charting.create_timeframe_chart(df=med_ohlc_df,
                                                col_names=['med_dollar_volume', 'med_candle_gain'],
                                                names=['Median Dollar Volume', 'Median Candle Gain'],
                                                colors=['orange', 'blue'],
                                                y_axis_titles=['$ Volume (Millions)', 'Pct Gain'])

    std_chart = charting.create_timeframe_chart(df=std_ohlc_df,
                                                col_names=['std_dollar_volume', 'std_candle_gain'],
                                                names=['Std Deviation Dollar Volume', 'Std Deviation Candle Gain'],
                                                colors=['orange', 'blue'],
                                                y_axis_titles=['$ Volume (Millions)', 'Pct Gain'])

    cluster_avg_chart = charting.create_timeframe_chart(df=cluster_avg_ohlc_df,
                                                col_names=['cluster_avg_dollar_volume', 'cluster_avg_candle_gain'],
                                                names=['Cluster Avg Dollar Volume', 'Cluster Avg Candle Gain'],
                                                colors=['orange', 'blue'],
                                                y_axis_titles=['$ Volume (Millions)', 'Pct Gain'])


    context = {'top_x': TOP_X,
               'start_date': start_date,
               'end_date': end_date,
               'date_msg': handle_date_exception(dates_result_len=len(dates_result),
                                                 num_days=num_days,
                                                 timeframe=timeframe),
               'avg_chart':avg_chart,
               'med_chart':med_chart,
               'std_chart':std_chart,
               'cluster_avg_chart':cluster_avg_chart}

    return render(request,'time_window_performance.html', context)

def day_of_week_performance(request,timeframe=None):
    if not timeframe:
        num_days = 14
    else:
        num_days = TIMEFRAMES[timeframe]

    dates_result = list(
        FinalScreenResults.objects.order_by('-datetime').values('datetime').distinct()[:num_days].values('datetime'))

    start_date = datetime.datetime.strftime(dates_result[-1]['datetime'], '%Y-%m-%d')
    end_date = datetime.datetime.strftime(dates_result[0]['datetime'], '%Y-%m-%d')

    results = list(FinalScreenResults.objects.filter(datetime__gte=start_date, datetime__lte=end_date)
                   .values('symbol', 'datetime', 'max_pct_gain', 'dollar_volume'))

    df = pd.DataFrame(results)
    df['datetime'] = pd.to_datetime(df['datetime'])
    df['day_of_week'] = pd.to_datetime(df['datetime'], "%H:%M:%S").dt.day_name()
    df = df.sort_values(['max_pct_gain'], ascending=False).groupby('datetime').head(TOP_X).sort_values(['datetime'])
    df = df.drop(['symbol', 'datetime'], axis=1)

    cluster_avg_dow_df = hf.get_df_cluster_average_for_time_charts(input_df=df,
                                                                   group_col='day_of_week',
                                                                   col_names=['max_pct_gain','dollar_volume'])
    cluster_avg_dow_df.rename(columns={'dollar_volume': 'cluster_avg_dollar_volume',
                                       'max_pct_gain': 'cluster_avg_pct_gain'}, inplace=True)
    cluster_avg_dow_df = hf.order_by_days_of_week(input_df=cluster_avg_dow_df)


    avg_dow_df = df.groupby('day_of_week').mean().reset_index()
    avg_dow_df.rename(columns={'dollar_volume': 'avg_dollar_volume',
                                'max_pct_gain': 'avg_pct_gain'}, inplace=True)
    avg_dow_df = hf.order_by_days_of_week(input_df=avg_dow_df)

    med_dow_df = df.groupby('day_of_week').median().reset_index()
    med_dow_df.rename(columns={'dollar_volume': 'med_dollar_volume',
                                'max_pct_gain': 'med_pct_gain'}, inplace=True)
    med_dow_df = hf.order_by_days_of_week(input_df=med_dow_df)

    std_dow_df = df.groupby('day_of_week').std().reset_index()
    std_dow_df.rename(columns={'dollar_volume': 'std_dollar_volume',
                                'max_pct_gain': 'std_pct_gain'}, inplace=True)
    std_dow_df = hf.order_by_days_of_week(input_df=std_dow_df)

    charting = hf.Charting()
    avg_chart = charting.create_day_of_week_chart(df=avg_dow_df,
                                                  col_names=['avg_dollar_volume', 'avg_pct_gain'],
                                                  names=['Average Dollar Volume', 'Average Percent Gain'],
                                                  colors=['orange', 'blue'],
                                                  y_axis_titles=['$ Volume (Millions)', 'Pct Gain'])

    med_chart = charting.create_day_of_week_chart(df=med_dow_df,
                                                  col_names=['med_dollar_volume', 'med_pct_gain'],
                                                  names=['Median Dollar Volume', 'Median Percent Gain'],
                                                  colors=['orange', 'blue'],
                                                  y_axis_titles=['$ Volume (Millions)', 'Pct Gain'])

    std_chart = charting.create_day_of_week_chart(df=std_dow_df,
                                                  col_names=['std_dollar_volume', 'std_pct_gain'],
                                                  names=['Std Deviation Dollar Volume', 'Std Deviation Percent Gain'],
                                                  colors=['orange', 'blue'],
                                                  y_axis_titles=['$ Volume (Millions)', 'Pct Gain'])

    cluster_avg_chart = charting.create_day_of_week_chart(df=cluster_avg_dow_df,
                                                          col_names=['cluster_avg_dollar_volume', 'cluster_avg_pct_gain'],
                                                          names=['Cluster Average Dollar Volume',
                                                                 'Cluster Average Percent Gain'],
                                                          colors=['orange', 'blue'],
                                                          y_axis_titles=['$ Volume (Millions)', 'Pct Gain'])

    context = {'top_x': TOP_X,
               'start_date': start_date,
               'end_date': end_date,
               'date_msg': handle_date_exception(dates_result_len=len(dates_result),
                                                 num_days=num_days,
                                                 timeframe=timeframe),
               'avg_chart': avg_chart,
               'med_chart': med_chart,
               'std_chart': std_chart,
               'cluster_avg_chart':cluster_avg_chart}

    return render(request, 'day_of_week_performance.html', context)

def about(request):
    return render(request, 'about.html', {})

def methodology(request):
    return render(request, 'methodology.html', {})

def handle_not_found(request,exception):
    return render(request,'not_found.html',{}),