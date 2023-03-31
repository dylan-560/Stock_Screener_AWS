from django.urls import path
from . import views

urlpatterns = [
    path('', views.home, name='home'),
    path('home/', views.home, name='home'),
    path('home/<str:selected_date>/', views.home, name='home'),
    path('home/<str:selected_date>/<str:ticker>/', views.home, name='home'),

    path('gainer_stats/', views.gainer_stats, name='gainer_stats'),
    path('gainer_stats/<str:timeframe>/', views.gainer_stats, name='gainer_stats'),

    path('price_stats/', views.price_stats, name='price_stats'),
    path('price_stats/<str:timeframe>/', views.price_stats, name='price_stats'),

    path('dist_from_200_sma/', views.dist_from_200_sma_stats, name='dist_from_200_sma'),
    path('dist_from_200_sma/<str:timeframe>/', views.dist_from_200_sma_stats, name='dist_from_200_sma'),

    path('close_to_range_stats/', views.close_to_range_stats, name='close_to_range_stats'),
    path('close_to_range_stats/<str:timeframe>/', views.close_to_range_stats, name='close_to_range_stats'),

    path('dollar_volume_stats/', views.dollar_volume_stats, name='dollar_volume_stats'),
    path('dollar_volume_stats/<str:timeframe>/', views.dollar_volume_stats, name='dollar_volume_stats'),

    path('market_cap/', views.market_cap, name='market_cap'),
    path('market_cap/<str:timeframe>/', views.market_cap, name='market_cap'),

    path('shares_float/', views.shares_float, name='shares_float'),
    path('shares_float/<str:timeframe>/', views.shares_float, name='shares_float'),

    path('shares_outstanding/', views.shares_outstanding, name='shares_outstanding'),
    path('shares_outstanding/<str:timeframe>/', views.shares_outstanding, name='shares_outstanding'),

    path('shares_short/', views.shares_short, name='shares_short'),
    path('shares_short/<str:timeframe>/', views.shares_short, name='shares_short'),

    path('short_perc_float/', views.short_perc_float, name='short_perc_float'),
    path('short_perc_float/<str:timeframe>/', views.short_perc_float, name='short_perc_float'),

    path('time_window_performance/', views.time_window_performance, name='time_window_performance'),
    path('time_window_performance/<str:timeframe>/', views.time_window_performance, name='time_window_performance'),

    path('day_of_week_performance/', views.day_of_week_performance, name='day_of_week_performance'),
    path('day_of_week_performance/<str:timeframe>/', views.day_of_week_performance, name='day_of_week_performance'),

    path('about/', views.about, name='about'),
    path('methodology/', views.methodology, name='methodology')

]