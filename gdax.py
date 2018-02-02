"""
GDAX API Import Functions
"""
# Import packages
import pandas
import time
import requests
from datetime import datetime, timedelta

class GDAX:
    def __init__(self):
        pass

    def self.date_to_iso8601(date):
        return '{year}-{month:02d}-{day:02d}T{hour:02d}:{minute:02d}:{second:02d}'.format(
          year=date.year,
          month=date.month,
          day=date.day,
          hour=date.hour,
          minute=date.minute,
          second=date.second)


    def request_trade_slice(self.product_id, start, end, granularity):
        """
        Single HTTP request function with error catching and management for server error responses
        Response is in the format: [[time, low, high, open, close, volume], ...]
        """
        # Allow 3 retries (we might get rate limited).
        retries = 3

        # Change dates to iso8601 format as specified
        iso_start = self.date_to_iso8601(start)
        iso_end = self.date_to_iso8601(end)

        # Set uri
        uri = 'https://api.gdax.com/products/{currency_pair}/candles'.format(currency_pair=product_id)

        for retry_count in range(0, retries):
            response = requests.get(uri, {
              'start': iso_start,
              'end': iso_end,
              'granularity': granularity * 60 # Converting to seconds for API
            })
            if response.status_code != 200:
                if retry_count + 1 == retries:
                    raise Exception('Failed to get exchange data for ({}, {})! Error message: {}'.format(start, end, response.text))
                else:
                    # Exponential back-off.
                    time.sleep(1.5 ** retry_count)
            else:
                # Sort the historic rates (in ascending order) based on the timestamp.
                result = sorted(response.json(), key=lambda x: x[0])
                return result


    def request_order_book(self.product_id, level):
        """
        Returns the current order book for a currency pair.
        :Level: Granularity level 1, 2, or 3 as defined by API. Default = 1, preferred for my usage is 2.
        Level 2 response example:
        {
            "sequence": "3",
            "bids": [
                [ price, size, num-orders ],
                [ "295.96", "4.39088265", 2 ],
                ...
            ],
            "asks": [
                [ price, size, num-orders ],
                [ "295.97", "25.23542881", 12 ],
                ...
            ]
        }
        """
        # Allow 3 retries (we might get rate limited).
        retries = 3

        # Set uri
        uri = 'https://api.gdax.com/products/{currency_pair}/book'.format(currency_pair=product_id)

        for retry_count in range(0, retries):
            response = requests.get(uri, {
              'level': level
            })
            if response.status_code != 200 or not len(response.json()):
                if retry_count + 1 == retries:
                    raise Exception('Failed to get order book data! Error message: {}'.format(response.text))
                else:
                    # Exponential back-off.
                    time.sleep(1.5 ** retry_count)
            else:
                return result


    def trade_downloader(self.currency_pair, start, end, interval):
        """
        Breaks up gdax trade data requests into chunks of 200 candlesticks to download in 1 second intervals, to comply with GDAX API rules
        :currency_pair: string with requested crypto-fiat pair
        :start: start of time period as datetime object
        :end: end of time period as datetime object
        :interval: candlestick intervals in ninutes
        Returns an array with rows of candlestick data in the following format: [timestamp, low, high, open, close, volume]
        """
        data = [] # Empty list to append data
        delta = timedelta(minutes=interval * 200) # 200 intervals per request
        slice_start = start
        while slice_start != end:
            slice_end = min(slice_start + delta, end)
            print("downloading {} data from {} to {}".format(currency_pair, slice_start, slice_end))
            data += self.request_trade_slice(
                    product_id=currency_pair,
                    start=slice_start,
                    end=slice_end,
                    granularity=interval
            )
            slice_start = slice_end
            time.sleep(0.5)

        data_frame = pandas.DataFrame(data=data, columns=['time', 'low', 'high', 'open', 'close', 'volume'])
        data_frame.set_index('time', inplace=True)
        return data_frame


    def order_book_downloader(self, currency_pair, interval, start_time, end_time):
        """
        Script to download order book at regular intervals. Will work out how to use this data with deep learning network later :)
        :currency_pair: string with requested crypto-fiat pair
        :interval: request intervals in minutes
        :start_time: datetime object - the time at which this operation should start requesting data
        :end_time: datetime object - the time at which this operation should finish
        Returns something...
        """
        data = [] # Empty list to append data

        # Calculate time to next interval
        time_to_start =  min(start_time - datetime.datetime.now(),0) # Start when specified, or now if specified time is in the past
        time.sleep(time_to_start)

        while datetime.datetime.now() < end_time:
            data += self.request_order_book(
                    product_id=currency_pair,
                    level=2
            )
            time.sleep(interval*60) # sleep takes time in seconds

        return data
