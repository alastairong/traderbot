"""
Data Processing
Functions to download, consolidate, and process data from the various sources
"""


class processor:
    def historical_download(config, start_time, end_time):
        # Downloads data for all the different APIs and combines them into a dataframe
        # Test should be the most recent data always
        # Returns dataframe of format XXX
        pass

    def live_download(config, window):
        # Similar to historical_download but downloads for the most recent {window} intervals
        # Returns dataframe in format XXX
        pass

    def generate_x_y(data):
        # Takes a downloaded dataset and splits it into data and targets. Targets are the kraken prices
        # interval in the future
        # Returns a tuple of data, target
        pass
