"""
GDAX Data Collection and Processing
For a selected crypto-fiat pair, Collect OHLC candlestick data at the specified interval level and return it
"""
# Import packages
import pandas
import time
import requests
from datetime import datetime, timedelta
from base_class import Preprocessor
from helpers import date_to_iso8601

class GDAX(Preprocessor):
    def __init__(self, topic, interval, start_time, end_time):
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
        Call API to collect target dataset over the defined time period. Returns fully formatted data as a
        dataframe and summarized into intervals.
        Note that training data here has not yet been split into data vs. targets
        """
        raise NotImplementedError("{} must override step()".format(self.__class__.__name__))

    def get_test_data(self):
        """
        Call API to collect data for 1 time period only starting from now. Returns fully formatted data in dataframe.
        Note that this function will be significantly simpler than get_training_data since there is no need to loop through
        multiple time periods and aggregate multiple API calls
        """
        raise NotImplementedError("{} must override step()".format(self.__class__.__name__))


    def request_trade_slice(self):
        """
        Single HTTP request function with error catching and management for server error responses
        Response is in the format: [[time, low, high, open, close, volume], ...]
        """

        # Change dates to iso8601 format as specified
        iso_start = date_to_iso8601(self.start_time)
        iso_end = self.date_to_iso8601(self.end_time)

        for retry_count in range(0, self.retries):
            response = requests.get(self.url, {
              'start': iso_start,
              'end': iso_end,
              'granularity': self.interval * 60 # Converting to seconds for API
            })
            if response.status_code != 200:
                if retry_count + 1 == self.retries:
                    raise Exception('Failed to get exchange data for ({}, {})! Error message: {}'.format(self.start_time, self.end_time, response.text))
                else:
                    # Exponential back-off.
                    time.sleep(1.5 ** retry_count)
            else:
                # Sort the historic rates (in ascending order) based on the timestamp.
                result = sorted(response.json(), key=lambda x: x[0])
                return result
