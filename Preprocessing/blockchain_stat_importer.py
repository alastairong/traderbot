"""
Class to download blockchain stats such as addresses, transactions/minute, and mining hashrate. Due to lack of suitable APIs for
historical data, get_training_data() will only import and validate a manually created CSV

"""


import numpy as np
import pandas as pd
from datetime import datetime, timedelta

from Preprocessing.helpers import date_to_datestring, date_to_iso8601, date_to_interval
from Preprocessing.base_class import Preprocessor

class Blockchain_Stats(Preprocessor):

    def __init__(self, interval, start_time, end_time):
        """
        Initialise shared parameters.
        :interval: The time interval at which the training data will be collected and batched
        :start_time: earliest point from which data will be collected, as a datetime object
        :end_time: final point at which data will be collected, as a datetime object
        """

        self.interval = interval
        self.start_time = start_time
        self.end_time = end_time


    def get_training_data(self, csv_path):
        """
        Reads in manually compiled CSV data for blockchain stats
        :csv_path: filepath to a CSV with format of []
        Returns a dataframe of format []
        """
        data = pd.read_csv(csv_path)
        data[['Hashrate', 'Addresses', 'Supply', 'Trx_Fee', 'Daily_Trx']] = data[['Hashrate', 'Addresses', 'Supply', 'Trx_Fee', 'Daily_Trx']].apply(pd.to_numeric)
        data[['Timestamp']] = data[['Timestamp']].apply(pd.to_datetime)
        data = data[data['Timestamp'] < self.end_time]
        data = data[data['Timestamp'] > self.start_time]

        return data


    def get_test_data(self, topic):
        """
        Call API to collect data for 1 time period only starting from now. Returns fully formatted data in dataframe.
        Note that this function will be significantly simpler than get_training_data since there is no need to loop through
        multiple time periods and aggregate multiple API calls
        :topic: this will be the API specific target. E.g. a reddit subreddit or GDAX currency pair
        """
        raise NotImplementedError("{} must override step()".format(self.__class__.__name__))
