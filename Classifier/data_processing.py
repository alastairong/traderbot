"""
Data Processing
Functions to download, consolidate, and process data from the various sources
"""
import numpy as np
import pandas as pd
import sys
from datetime import datetime
sys.path.append('../..')
import pickle

from traderbot.Preprocessing import kraken, gdax, reddit, google_search, blockchain_stat_importer

class processor:
    """
    Downloads training and live data for deep learning and prediction. Also includes data processor helper functions
    """
    def historical_download(start_time, end_time, interval, include_sentiment_analysis=False, normalize=False, load_pickle=True):
        """
        Downloads and aggregates historical data for training
        :config: config file if needed
        :start_time: beginning of download period in Datetime format
        :end_time: end of download period in Datetime format
        :interval: time interval at which the training data will be collected and batched, in minutes
        Returns a dataframe
        """
        # TODO: Make loading of different datasets programmatic and customisable based on a list of some kind
        if load_pickle:
            try:
                with open('kraken_data.pickle', 'rb') as f:
                        K_ETH_USD, K_BTC_USD, K_LTC_USD = pickle.load(f)
            except:
                print("No Kraken pickle found")
            try:
                with open('gdax_data.pickle', 'rb') as f:
                        G_ETH_USD, G_BTC_USD, G_LTC_USD = pickle.load(f)
            except:
                print("No GDAX pickle found")
            try:
                with open('reddit_data.pickle', 'rb') as f:
                        ETH_reddit, BTC_reddit, LTC_reddit = pickle.load(f)
            except:
                print("No Reddit pickle found")
            try:
                with open('google_data.pickle', 'rb') as f:
                        ETH_trends, BTC_trends, LTC_trends = pickle.load(f)
            except:
                print("No Google trends pickle found")
            try:
                with open('blockchain_data.pickle', 'rb') as f:
                        ETH_blockchain, BTC_blockchain, LTC_blockchain = pickle.load(f)
            except:
                print("No Blockchain pickle found")
        else:
            K = kraken.Kraken(interval, start_time, end_time)
            K_ETH_USD = K.get_training_data('XETHZUSD')
            K_BTC_USD = K.get_training_data('XXBTZUSD')
            K_LTC_USD = K.get_training_data('XLTCZUSD')
            with open('kraken_data.pickle', 'wb') as f:
                pickle.dump((K_ETH_USD, K_BTC_USD, K_LTC_USD), f)

            # Get GDAX market data
            G = gdax.GDAX(interval, start_time, end_time)
            G_ETH_USD = G.get_training_data('ETH-USD')
            G_BTC_USD = G.get_training_data('BTC-USD')
            G_LTC_USD = G.get_training_data('LTC-USD')
            with open('gdax_data.pickle', 'wb') as f:
                pickle.dump((G_ETH_USD, G_BTC_USD, G_LTC_USD), f)

            # Get Reddit data
            client_ID='GK8bm-Gl2VD5SA'
            client_secret='pHtlAyxKuYt1JAGir_Gn9XOXlJc'
            reddit = reddit.Reddit_Scanner(interval, start_time, end_time)
            reddit.authenticate(client_ID, client_secret, include_sentiment_analysis=False)
            ETH_reddit =reddit.get_training_data('Ethereum')
            BTC_reddit =reddit.get_training_data('Bitcoin')
            LTC_reddit =reddit.get_training_data('Litecoin')
            with open('reddit_data.pickle', 'a') as f:
                pickle.dump((ETH_reddit, BTC_reddit, LTC_reddit), f)

            # Get Google search data
            Search = google_search.Searchtrends(interval, start_time, end_time)
            ETH_trends = Search.get_training_data('Ethereum')
            BTC_trends = Search.get_training_data('Bitcoin')
            LTC_trends = Search.get_training_data('Litecoin')
            with open('google_data.pickle', 'wb') as f:
                pickle.dump((ETH_trends, BTC_trends, LTC_trends), f)

            # Get Blockchain stats
            Blockchain = blockchain_stat_importer.Blockchain_Stats(interval, start_time, end_time)
            ETH_Blockchain = Blockchain.get_training_data('../Blockchain_Data/Blockchain Stats - ETH_Clean.csv')
            BTC_Blockchain = Blockchain.get_training_data('../Blockchain_Data/Blockchain Stats - BTC_Clean.csv')
            LTC_Blockchain = Blockchain.get_training_data('../Blockchain_Data/ltc_blockchain.csv')
            with open('blockchain_data.pickle', 'wb') as f:
                pickle.dump((ETH_Blockchain, BTC_Blockchain, LTC_Blockchain), f)

        # Convert GDAX timestamps to datetimes
        G_ETH_USD = G_ETH_USD.rename(index=lambda x:x*1000000000).rename(index=pd.to_datetime)
        G_BTC_USD = G_BTC_USD.rename(index=lambda x:x*1000000000).rename(index=pd.to_datetime)
        G_LTC_USD = G_LTC_USD.rename(index=lambda x:x*1000000000).rename(index=pd.to_datetime)

        # Set blockchain data index to timestamp (datetime format)
        ETH_blockchain = ETH_blockchain.set_index('Timestamp')
        BTC_blockchain = BTC_blockchain.set_index('Timestamp')
        LTC_blockchain = LTC_blockchain.set_index('Timestamp')

        # Renaming to ensure unique column names. TODO: Make renaming programmatic
        G_ETH_USD = G_ETH_USD.rename(columns={"low": "Eth_gdax_low", "high": "Eth_gdax_high", "open": "Eth_gdax_open", "close": "Eth_gdax_close", "volume": "Eth_gdax_vol"})
        G_BTC_USD = G_BTC_USD.rename(columns={"low": "Btc_gdax_low", "high": "Btc_gdax_high", "open": "Btc_gdax_open", "close": "Btc_gdax_close", "volume": "Btc_gdax_vol"})
        G_LTC_USD = G_LTC_USD.rename(columns={"low": "Ltc_gdax_low", "high": "Ltc_gdax_high", "open": "Ltc_gdax_open", "close": "Ltc_gdax_close", "volume": "Ltc_gdax_vol"})
        K_ETH_USD = K_ETH_USD.rename(columns={"low": "Eth_kraken_low", "high": "Eth_kraken_high", "open": "Eth_kraken_open", "close": "Eth_kraken_close", "volume": "Eth_kraken_vol"})
        K_BTC_USD = K_BTC_USD.rename(columns={"low": "Btc_kraken_low", "high": "Btc_kraken_high", "open": "Btc_kraken_open", "close": "Btc_kraken_close", "volume": "Btc_kraken_vol"})
        K_LTC_USD = K_LTC_USD.rename(columns={"low": "Ltc_kraken_low", "high": "Ltc_kraken_high", "open": "Ltc_kraken_open", "close": "Ltc_kraken_close", "volume": "Ltc_kraken_vol"})
        ETH_trends = ETH_trends.rename(columns={"Worldwide": "Eth_search_worldwide", "US": "Eth_search_US", "GB": "Eth_search_GB", "FR": "Eth_search_FR", "DE": "Eth_search_DE", "RU": "Eth_search_RU", "KR": "Eth_search_KR"})
        BTC_trends = BTC_trends.rename(columns={"Worldwide": "Btc_search_worldwide", "US": "Btc_search_US", "GB": "Btc_search_GB", "FR": "Btc_search_FR", "DE": "Btc_search_DE", "RU": "Btc_search_RU", "KR": "Btc_search_KR"})
        LTC_trends = LTC_trends.rename(columns={"Worldwide": "Ltc_search_worldwide", "US": "Ltc_search_US", "GB": "Ltc_search_GB", "FR": "Ltc_search_FR", "DE": "Ltc_search_DE", "RU": "Ltc_search_RU", "KR": "Ltc_search_KR"})
        ETH_blockchain = ETH_blockchain.rename(columns={"Hashrate": "Eth_hashrate", "Addresses": "Eth_addresses", "Supply": "Eth_supply", "Trx_Fee": "Eth_trx_fee", "Daily_Trx": "Eth_daily_trx"})
        BTC_blockchain = BTC_blockchain.rename(columns={"Hashrate": "Btc_hashrate", "Addresses": "Btc_addresses", "Supply": "Btc_supply", "Trx_Fee": "Btc_trx_fee", "Daily_Trx": "Btc_daily_trx"})
        LTC_blockchain = LTC_blockchain.rename(columns={"Hashrate": "Ltc_hashrate", "Addresses": "Ltc_addresses", "Supply": "Ltc_supply", "Trx_Fee": "Ltc_trx_fee", "Daily_Trx": "Ltc_daily_trx"})

        # Join dataframes together
        input_data = None
        input_data = G_ETH_USD
        input_data = input_data.join(K_ETH_USD)
        #input_data = input_data.join(ETH_reddit)
        input_data = input_data.join(ETH_trends)
        input_data = input_data.join(ETH_blockchain)

        input_data = input_data.join(G_BTC_USD)
        input_data = input_data.join(K_BTC_USD)
        #input_data = input_data.join(BTC_reddit)
        input_data = input_data.join(BTC_trends)
        input_data = input_data.join(BTC_blockchain)

        input_data = input_data.join(G_LTC_USD)
        input_data = input_data.join(K_LTC_USD)
        #input_data = input_data.join(LTC_reddit)
        input_data = input_data.join(LTC_trends)
        input_data = input_data.join(LTC_blockchain)

        # Do interpolation for any blank cells
        input_data = input_data.interpolate()

        # Create new fee per transaction column
        input_data['Eth_fee_per_trx'] = input_data['Eth_trx_fee'] / input_data['Eth_daily_trx']
        input_data['Btc_fee_per_trx'] = input_data['Btc_trx_fee'] / input_data['Btc_daily_trx']
        input_data['Ltc_fee_per_trx'] = input_data['Ltc_trx_fee'] / input_data['Ltc_daily_trx']

        # Delete redundant columns: Trx fee, maybe some of the country data
        input_data.drop('Eth_trx_fee', axis=1, inplace=True)
        input_data.drop('Btc_trx_fee', axis=1, inplace=True)
        input_data.drop('Ltc_trx_fee', axis=1, inplace=True)

        # Convert everything except trx_fee / trx, reddit sentiment to % change
        dataframe = input_data.pct_change()
        dataframe[['Eth_fee_per_trx', 'Btc_fee_per_trx', 'Ltc_fee_per_trx']] = input_data[['Eth_fee_per_trx', 'Btc_fee_per_trx', 'Ltc_fee_per_trx']]
        # TODO: Add reddit sentiment to this list that isn't converted to pct

        # Normalise if required
        if normalize:
            # dataframe.normalize(0, 1, in_place = True)
            pass

        return dataframe

    def live_download(config, window = 1):
        # Similar to historical_download but downloads for the most recent {window} intervals
        # Returns dataframe in format XXX
        pass

    def generate_x_y(data, target = "Kraken_BTC_Close", forecast_range=1):
        """
        Converts data into
        """
        # Takes a downloaded dataset and splits it into data and targets
        input_data = np.array(data.drop(target, axis=1))
        target_df = data[target].shift(forecast_range)
        target_data = np.array(target_df)

        return input_data, target_data
