from django.test import TestCase

from helper_functions import Charting
import pandas as pd

df = pd.read_csv(r'C:\Users\dlnbl\PycharmProjects\django_screener_app\QS_result.csv')
df['datetime'] = pd.to_datetime(df['datetime'])
do_chart = Charting()
do_chart.chart_days_top_gainer(df=df)

# Create your tests here.
