"""
GDAX Data Collection and Processing
For a selected crypto-fiat pair, Collect OHLC candlestick data at the specified interval level and return it
"""
# Import packages
import pandas as pd
import time
import requests
from datetime import datetime, timedelta
from base_class import Preprocessor
from helpers import date_to_iso8601

class GDAX(Preprocessor):
    def __init__(self, interval, start_time, end_time):
        """
        Initialise shared parameters.
        :interval: the time interval at which the training data will be collected and batched
        :start_time: earliest point from which data will be collected, as a datetime object
        :end_time: final point at which data will be collected, as a datetime object
        """
        self.interval = interval
        self.start_time = start_time
        self.end_time = end_time
        self.retries = 3 # For API rate-limiting
        self.url = 'https://api.gdax.com/products/{currency_pair}/candles'.format(currency_pair=self.currency_pair) # URL for candle

    def get_training_data(self, topic):
        """
        Breaks up gdax trade data requests into chunks of 200 candlesticks to download in 1 second intervals, to comply with GDAX API rules
        :topic: this will be the API specific target. E.g. a reddit subreddit or GDAX currency pair
        Returns an dataframe with rows of candlestick data in the following format: [timestamp, low, high, open, close, volume]
        """
        data = [] # Empty list to append data
        currency_pair = topic
        delta = timedelta(minutes=self.interval * 200) # 200 intervals per request
        slice_start = self.start_time
        while slice_start != self.end_time:
            slice_end = min(slice_start + delta, self.end_time)
            print("downloading {} data from {} to {}".format(self.currency_pair, slice_start, slice_end))
            data += self.request_trade_slice(
                    start=slice_start,
                    end=slice_end,
            )
            slice_start = slice_end
            time.sleep(0.5)

        dataframe = pd.DataFrame(data=data, columns=['time', 'low', 'high', 'open', 'close', 'volume'])
        dataframe.set_index('time', inplace=True)
        return dataframe

    def get_test_data(self, topic):
        """
        Call API to collect data for 1 time period only starting from now. Returns fully formatted data in dataframe.
        Note that this function will be significantly simpler than get_training_data since there is no need to loop through
        multiple time periods and aggregate multiple API calls
        :topic: this will be the API specific target. E.g. a reddit subreddit or GDAX currency pair
        """
        raise NotImplementedError("{} must override step()".format(self.__class__.__name__))


    def request_trade_slice(self, start, end):
        """
        Single HTTP request function with error catching and management for server error responses
        Response is in the format: [[time, low, high, open, close, volume], ...]
        """

        # Change dates to iso8601 format as specified
        iso_start = date_to_iso8601(start)
        iso_end = date_to_iso8601(end)

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
