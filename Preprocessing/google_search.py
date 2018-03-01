from pytrends.request import TrendReq
import pandas as pd
from datetime import datetime, timedelta

from traderbot.Preprocessing.helpers import date_to_datestring
from traderbot.Preprocessing.base_class import Preprocessor

class Searchtrends(Preprocessor):

    def __init__(self, interval, start_time, end_time):
        """
        Initialise shared parameters.
        :interval: the time interval at which the training data will be collected and batched
        :start_time: earliest point from which data will be collected, as a datetime object
        :end_time: final point at which data will be collected, as a datetime object
        """
        self.pytrends = TrendReq(hl='en-US', tz=0)
        self.interval = interval
        self.start_time = start_time
        self.end_time = end_time


    def get_training_data(self, topic):
        """
        Call API to collect target dataset over the defined time period. Returns fully formatted data as a
        dataframe and summarized into intervals.
        Note that training data here has not yet been split into data vs. targets
        :topic: this will be the API specific target. E.g. a reddit subreddit or GDAX currency pair
        """
        # Get topic suggestion
        suggestion = self.pytrends.suggestions(topic)[0]['mid']

        # For each time period chunk, call downloader
        data = pd.DataFrame() # Empty list to append data
        delta = timedelta(days=180) # Keep to 6 month periods to ensure daily intervals
        slice_start = self.start_time
        while slice_start != self.end_time:
            slice_end = min(slice_start + delta, self.end_time)
            print("downloading {} data from {} to {}".format(topic, slice_start, slice_end))
            df = self.trend_downloader(
                    topic=[topic],
                    start=slice_start,
                    end=slice_end,
            )
            slice_start = slice_end
            data = data.append(df)

        return data


    def trend_downloader(self, topic, start, end):
        """
        For a specific time slice, requests search trend data by region normalized to 100 and combines it
        :start: in datetime format
        :end: in datetime format
        """
        timeframe = date_to_datestring(start) + " " + date_to_datestring(end)

        # Global data
        self.pytrends.build_payload(topic, cat=0, timeframe=timeframe, geo='', gprop='')
        WW_data = self.pytrends.interest_over_time()
        WW_data.columns= ['datetime', 'Worldwide']
        data = WW_data['Worldwide'].to_frame()

        # US Data
        self.pytrends.build_payload(topic, cat=0, timeframe=timeframe, geo='US', gprop='')
        US_data = self.pytrends.interest_over_time()
        US_data.columns= ['datetime', 'US']
        data = data.join(US_data['US'])

        # UK Data
        self.pytrends.build_payload(topic, cat=0, timeframe=timeframe, geo='GB', gprop='')
        GB_data = self.pytrends.interest_over_time()
        GB_data.columns= ['datetime', 'GB']
        data = data.join(GB_data['GB'])

        # UK Data
        self.pytrends.build_payload(topic, cat=0, timeframe=timeframe, geo='FR', gprop='')
        FR_data = self.pytrends.interest_over_time()
        FR_data.columns= ['datetime', 'FR']
        data = data.join(FR_data['FR'])

        # Germany Data
        self.pytrends.build_payload(topic, cat=0, timeframe=timeframe, geo='DE', gprop='')
        DE_data = self.pytrends.interest_over_time()
        DE_data.columns= ['datetime', 'DE']
        data = data.join(DE_data['DE'])

        # Russia Data
        self.pytrends.build_payload(topic, cat=0, timeframe=timeframe, geo='RU', gprop='')
        RU_data = self.pytrends.interest_over_time()
        RU_data.columns= ['datetime', 'RU']
        data = data.join(RU_data['RU'])

        # Korea Data
        self.pytrends.build_payload(topic, cat=0, timeframe=timeframe, geo='KR', gprop='')
        KR_data = self.pytrends.interest_over_time()
        KR_data.columns= ['datetime', 'KR']
        data = data.join(KR_data['KR'])

        return data

    def get_test_data(self, topic):
        """
        Call API to collect data for 1 time period only starting from now. Returns fully formatted data in dataframe.
        Note that this function will be significantly simpler than get_training_data since there is no need to loop through
        multiple time periods and aggregate multiple API calls
        :topic: this will be the API specific target. E.g. a reddit subreddit or GDAX currency pair
        """
        raise NotImplementedError("{} must override step()".format(self.__class__.__name__))
