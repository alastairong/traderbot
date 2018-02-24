"""
Data Processing
Functions to download, consolidate, and process data from the various sources
"""

from kraken import Kraken

class processor:
    """
    Downloads training and live data for deep learning and prediction. Also includes data processor helper functions
    """
    def historical_download(config = None, start_time, end_time):
        """
        Downloads and aggregates historical data for training
        :config: config file if needed
        :start_time: beginning of download period in Datetime format
        :end_time: end of download period in Datetime format
        Returns a 2-dimensional numpy array of shape (num_periods, num_datapoints)
        """
        # Downloads data for all the different APIs and combines them into a dataframe
        K = Kraken(start_time, end_time)
        K_ETH_USD = K.get_training_data('XETHZUSD')
        K_BTC_USD = K.get_training_data('XXBTZUSD')
        K_LTC_USD = K.get_training_data('XLTCZUSD')

        G = GDAX(start_time, end_time)
        G_ETH_USD = G.get_training_data('ETH-USD')
        G_BTC_USD = G.get_training_data('BTC-USD')
        G_LTC_USD = G.get_training_data('LTC-USD')

        # Download from reddit
        client_ID='GK8bm-Gl2VD5SA'
        client_secret='pHtlAyxKuYt1JAGir_Gn9XOXlJc'
        topic= 'ethtrader'
        interval= 5
        start_time=datetime(2018,2,21,10)
        end_time=datetime(2018,2,21,15)

        reddit = Reddit_Scanner(interval, start_time, end_time)
        reddit.authenticate(client_ID, client_secret, include_sentiment_analysis=False)

        # Convert absolute prices to % change


        # Normalize values to 0 to 1


        # Aggregate data into 2D tensor


        # Returns numpy array

        pass

    def live_download(config, window = 1):
        # Similar to historical_download but downloads for the most recent {window} intervals
        # Returns dataframe in format XXX
        pass

    def generate_x_y(data, target = "Kraken_BTC_Close"):
        # Takes a downloaded dataset and splits it into data and targets. Targets are the kraken prices
        # interval in the future
        # Returns a tuple of data, target
        pass
