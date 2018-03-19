"""
Kraken Data Collection and Processing
For a selected crypto-fiat pair, Collect OHLC candlestick data at the specified interval level and return it
"""

# Import packages
import numpy as np
import pandas as pd
import time
import requests
from datetime import datetime, timedelta
import krakenex
import pytz
from pykrakenapi import KrakenAPI
from Preprocessing.base_class import Preprocessor
from Preprocessing.helpers import date_to_iso8601, date_to_interval
import config

class Kraken(Preprocessor):
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
        # Initialise krakenex library
        api = krakenex.API(config.key, config.secret)
        self.k = KrakenAPI(api)


    def get_training_data(self, topic):
        """
        Loops through Kraken data requests over the whole period. Kraken API only takes a start date and ends
        :topic: this will be the API specific target. E.g. a reddit subreddit or GDAX currency pair
        Returns an dataframe with rows of candlestick data in the following format: [timestamp, low, high, open, close, volume]
        """
        currency_pair = topic
        slice_start = self.start_time
        end_timestamp = self.end_time.replace(tzinfo=pytz.utc).timestamp() # last is returned a an epoch timestamp so end_time needs to be reformatted
        trades, last = self.request_trade_slice(currency_pair, slice_start)
        while last < end_timestamp:
            slice_start = datetime.utcfromtimestamp(last)
            new_data, last = self.request_trade_slice(currency_pair, slice_start)
            trades = trades.append(new_data)
            time.sleep(1)
            print("time period from {} to {}".format(slice_start, last))
        dataframe = self.to_ohlc(trades)
        return dataframe


    def get_test_data(self, topic):
        """
        Call API to collect data for 1 time period only starting from now. Returns fully formatted data in dataframe.
        Note that this function will be significantly simpler than get_training_data since there is no need to loop through
        multiple time periods and aggregate multiple API calls
        :topic: this will be the API specific target. E.g. a reddit subreddit or GDAX currency pair
        """
        raise NotImplementedError("{} must override step()".format(self.__class__.__name__))


    def request_trade_slice(self, currency_pair, start):
        """
        Calls krakenex get_trade function to get line-by-line trade data for an individual time slice
        :start: start of API request time period
        Returns a tuple of (trades, last). Last = last trade timestamp. Used to set timestamp for next request
        trades is a list of trades with format of [time, price, volume]
        """
        timestamp = int(start.replace(tzinfo=pytz.utc).timestamp()) * 1000000000
        trades, last = self.k.get_recent_trades(currency_pair, since=timestamp, ascending=True)
        return trades, last/1000000000


    def to_ohlc(self, trades):
        """
        Groups and processes individual trade data to return OHLC (candle) data
        :trades: trades pandas dataframe as returned by request_trade_slice function
        Returns an array with rows of candlestick data in the following format: ['time', 'low', 'high', 'open', 'close', 'volume']
        """
        # Converts unix timestamp data into candle interval periods. Period time corresponds to beginning of period
        trades['datetime'] = pd.to_datetime(trades['time'], unit='s') # Reformat unix timestamp as datetime
        trades['period'] = trades['datetime'].map(lambda x: date_to_interval(x, self.interval))

        # Group trades into periods to aggregate volume and get low/high price
        trade_agg = trades.groupby('period')
        trade_agg = trade_agg.agg({
            'price': {'low': np.min, 'high': np.max},
            'volume': np.sum
        })

        # Create a fresh pandas dataframe and copies data from aggregated trades dataframe above
        ohlc = pd.DataFrame(index=trade_agg.index.values, columns=['low', 'high', 'open', 'close', 'volume'])
        ohlc['low'] = trade_agg['price']['low']
        ohlc['high'] = trade_agg['price']['high']
        ohlc['volume'] = trade_agg['volume']['sum']

        # Iterate through each candle period and searches original trades dataframe for the first and last price, then set that
        for i, row in ohlc.iterrows():
            selection = trades.loc[trades['period'] == i] # Return all rows for the current period
            first_trade_index = selection['time'].idxmax()
            first_trade_price = selection.loc[first_trade_index]['price']
            ohlc.at[i, 'open'] = first_trade_price
            last_trade_index = selection['time'].idxmin()
            last_trade_price = selection.loc[last_trade_index]['price']
            ohlc.at[i, 'close'] = last_trade_price

        return ohlc
