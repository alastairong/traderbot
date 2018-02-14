"""
Kraken Data Collection and Processing
For a selected crypto-fiat pair, Collect OHLC candlestick data at the specified interval level and return it
"""

# Import packages
import pandas
import time
import requests
from datetime import datetime, timedelta

class GDAX(Preprocessor):
    def __init__(self, topic, interval=5, start_time, end_time):
        """
        Initialise shared parameters.
        :topic: this will be the API specific target. E.g. a reddit subreddit or GDAX currency pair
        :interval: the time interval at which the training data will be collected and batched
        :start_time: earliest point from which data will be collected, as a datetime object
        :end_time: final point at which data will be collected, as a datetime object
        """
        self.interval = interval
        self.start_time = start_time
        self.end_time = end_time
        self.currency_pair = topic
        self.retries = 3 # For API rate-limiting
        self.url = 'https://api.gdax.com/products/{currency_pair}/candles'.format(currency_pair=self.currency_pair) # URL for candle

    def get_training_data(self):
        """
        Breaks up gdax trade data requests into chunks of 200 candlesticks to download in 1 second intervals, to comply with GDAX API rules
        :currency_pair: string with requested crypto-fiat pair
        :start: start of time period as datetime object
        :end: end of time period as datetime object
        :interval: candlestick intervals in ninutes
        Returns an array with rows of candlestick data in the following format: [timestamp, low, high, open, close, volume]
        """

        return data_frame

    def get_test_data(self):
        """
        Call API to collect data for 1 time period only starting from now. Returns fully formatted data in dataframe.
        Note that this function will be significantly simpler than get_training_data since there is no need to loop through
        multiple time periods and aggregate multiple API calls
        """
        raise NotImplementedError("{} must override step()".format(self.__class__.__name__))
