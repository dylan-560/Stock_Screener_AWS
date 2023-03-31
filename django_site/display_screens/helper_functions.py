import datetime

import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots

def get_last_x_completed_trading_days(curr_date=datetime.date.today(), days=5):
    # TODO write something to exlcude the current day if time is before ~7pm (or whenever the EOD screen finishes)
    import pandas as pd
    import pandas_market_calendars as mcal

    def get_dates_list():
        start = (curr_date - datetime.timedelta(days=lookback))

        dates_df = nyse.schedule(start_date=start.strftime('%Y-%m-%d'),
                                 end_date=curr_date.strftime('%Y-%m-%d'))

        dates_list = list(dates_df.to_dict(orient='index'))
        ret_list = [x.to_pydatetime().strftime('%Y-%m-%d') for x in dates_list]
        return ret_list

    ######################################################

    nyse = mcal.get_calendar('NYSE')

    lookback = days

    ret_list = get_dates_list()

    while len(ret_list) < days:
        lookback += 1
        ret_list = get_dates_list()

    return ret_list

def get_previous_trading_day(curr_date):
    dates_list = get_last_x_completed_trading_days(curr_date=curr_date)
    if curr_date == dates_list[-1].date():
        return dates_list[-2]
    else:
        return dates_list[-1]

def numeric_stats_weighted_avg(value_list):
    # inputs a list of numeric values to average
    if isinstance(value_list,pd.Series):
        value_list = value_list.tolist()

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

def get_df_max(input_df, base_col_name, target_col_name):
    input_df.drop(input_df.columns.difference([base_col_name, target_col_name]), 1, inplace=True)
    input_df = input_df.groupby(base_col_name).max().reset_index()
    input_df.rename(columns={target_col_name: target_col_name + '_max'}, inplace=True)
    return input_df

def get_df_average(input_df, base_col_name, target_col_name):
    input_df.drop(input_df.columns.difference([base_col_name, target_col_name]), 1, inplace=True)
    input_df = input_df.groupby(base_col_name).mean().reset_index()
    input_df.rename(columns={target_col_name: target_col_name + '_average'}, inplace=True)
    return input_df

def get_df_median(input_df, base_col_name, target_col_name):
    input_df.drop(input_df.columns.difference([base_col_name, target_col_name]), 1, inplace=True)
    input_df = input_df.groupby(base_col_name).median().reset_index()
    input_df.rename(columns={target_col_name: target_col_name + '_median'}, inplace=True)
    return input_df

def get_df_std_dev(input_df, base_col_name, target_col_name):
    input_df.drop(input_df.columns.difference([base_col_name, target_col_name]), 1, inplace=True)
    input_df = input_df.groupby(base_col_name).std().reset_index()
    input_df.rename(columns={target_col_name: target_col_name + '_std'}, inplace=True)
    return input_df

def get_df_cluster_average(input_df, base_col_name, target_col_name):

    input_df = input_df.dropna(axis=0)
    input_df = input_df.groupby([base_col_name])[target_col_name].apply(list)
    input_df = input_df.apply(numeric_stats_weighted_avg).to_frame().reset_index()
    input_df.rename(columns={target_col_name: target_col_name + '_cluster_avg'}, inplace=True)

    return input_df

def get_df_cluster_average_for_time_charts(input_df,group_col, col_names):

    ret_df = pd.DataFrame()

    for col in col_names:
        temp_df = input_df.groupby(group_col).apply(lambda x: numeric_stats_weighted_avg(x[col])).reset_index()
        temp_df.rename(columns={0: col}, inplace=True)
        if ret_df.empty:
            ret_df = temp_df
        else:
            ret_df[col] = temp_df[col]

    return ret_df

    dv_CA_dow_df = df.groupby('day_of_week').apply(lambda x: hf.numeric_stats_weighted_avg(x['dollar_volume'])).reset_index()
    dv_CA_dow_df.rename(columns={0: 'cluster_avg_dollar_volume'}, inplace=True)

    cluster_avg_dow_df = reduce(lambda left, right: pd.merge(left, right, on=['day_of_week'],
                                                             how='outer'), [gain_CA_dow_df, dv_CA_dow_df])

def get_df_sum(input_df, base_col_name, target_col_name):
    input_df.drop(input_df.columns.difference([base_col_name, target_col_name]), 1, inplace=True)
    input_df = input_df.groupby(base_col_name).sum().reset_index()
    input_df.rename(columns={target_col_name: target_col_name + '_sum'}, inplace=True)
    return input_df

def order_by_days_of_week(input_df):
    CATS = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']

    input_df['day_of_week'] = pd.Categorical(input_df['day_of_week'], categories=CATS, ordered=True)
    input_df = input_df.sort_values('day_of_week')
    return input_df

class Charting:

    def chart_days_top_gainer(self,df):
        fig = make_subplots(rows=1, cols=1)

        # graph candlestick
        fig.add_trace(
            go.Candlestick(x=df['Date'],
                           open=df['Open'],
                           high=df['High'],
                           low=df['Low'],
                           close=df['Close'],
                           name='Candlestick Data'),
            row=1, col=1)

        fig.update_layout(yaxis_title='Price',
                          xaxis_title='Date',
                          xaxis_rangeslider_visible=False,
                          margin_b=0,
                          margin_l=0,
                          margin_r=0,
                          margin_t=0)

        return fig.to_html(config={"displayModeBar": False})

    def create_basic_chart(self, df, col_name, name, color, y_axis_title):
        fig = make_subplots(rows=1, cols=1)

        fig.add_trace(
            go.Scatter(x=df['datetime'],
                       y=df[col_name],
                       marker=dict(color=color, size=9),
                       name=name),
            row=1, col=1)

        fig.update_layout(yaxis_title=y_axis_title,
                          xaxis_title='Date',
                          xaxis_rangeslider_visible=False,
                          margin_b=0,
                          margin_l=0,
                          margin_r=0,
                          margin_t=0)

        return fig.to_html(config={"displayModeBar": False})

    def create_timeframe_chart(self, df, col_names, names, colors, y_axis_titles):
        fig = make_subplots(rows=1, cols=1, specs=[[{"secondary_y": True}]])

        fig.add_trace(
            go.Bar(x=df['Time'],
                   y=df[col_names[0]],
                   name=names[0],
                   marker_color=colors[0]),
                   row=1, col=1, secondary_y=False)

        fig.add_trace(
            go.Scatter(x=df['Time'],
                       y=df[col_names[1]],
                       mode='lines',
                       name=names[1],
                       line=dict(color=colors[1])),
            row=1, col=1, secondary_y=True)

        fig.update_yaxes(title_text=y_axis_titles[0], secondary_y=False)
        fig.update_yaxes(title_text=y_axis_titles[1], secondary_y=True)

        fig.update_layout(xaxis_title='Time of Day',
                          xaxis_rangeslider_visible=False,
                          margin_b=0,
                          margin_l=0,
                          margin_r=0,
                          margin_t=0)

        return fig.to_html(config={"displayModeBar": False})

    def create_day_of_week_chart(self, df, col_names, names, colors, y_axis_titles):
        fig = make_subplots(rows=1, cols=1, specs=[[{"secondary_y": True}]])

        fig.add_trace(
            go.Bar(x=df['day_of_week'],
                   y=df[col_names[0]],
                   name=names[0],
                   marker_color=colors[0]),
                   row=1, col=1, secondary_y=False)

        fig.add_trace(
            go.Scatter(x=df['day_of_week'],
                       y=df[col_names[1]],
                       mode='lines',
                       name=names[1],
                       line=dict(color=colors[1])),
            row=1, col=1, secondary_y=True)

        fig.update_yaxes(title_text=y_axis_titles[0], secondary_y=False)
        fig.update_yaxes(title_text=y_axis_titles[1], secondary_y=True)

        fig.update_layout(xaxis_title='Time of Day',
                          xaxis_rangeslider_visible=False,
                          margin_b=0,
                          margin_l=0,
                          margin_r=0,
                          margin_t=0)

        return fig.to_html(config={"displayModeBar": False})