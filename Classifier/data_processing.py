"""
Data Processing
Functions to download, consolidate, and process data from the various sources
"""
import numpy as np
import pandas as pd
import sys
from datetime import datetime, timedelta
sys.path.append('../..')
import pickle

from Preprocessing import kraken, gdax, reddit, google_search, blockchain_stat_importer

class processor:
    """
    Downloads training and live data for deep learning and prediction. Also includes data processor helper functions
    """
    def historical_download(start_time, end_time, interval, include_sentiment_analysis=False):
        """
        Downloads and aggregates historical data for training
        :config: config file if needed
        :start_time: beginning of download period in Datetime format
        :end_time: end of download period in Datetime format
        :interval: time interval at which the training data will be collected and batched, in minutes
        Returns a dataframe
        """
        # Get Kraken USD market data
        K = kraken.Kraken(interval, start_time, end_time)
        K_ETH_USD = K.get_training_data('XETHZUSD')
        K_BTC_USD = K.get_training_data('XXBTZUSD')
        #K_LTC_USD = K.get_training_data('XLTCZUSD')

        # Get Kraken EUR market data
        K_ETH_EUR = K.get_training_data('XETHZEUR')
        K_BTC_EUR = K.get_training_data('XXBTZEUR')
        #K_LTC_EUR = K.get_training_data('XLTCZEUR')

        # Get GDAX USD market data
        G = gdax.GDAX(interval, start_time, end_time)
        G_ETH_USD = G.get_training_data('ETH-USD')
        G_BTC_USD = G.get_training_data('BTC-USD')
        #G_LTC_USD = G.get_training_data('LTC-USD')

        # Get GDAX EUR market data
        #G_ETH_EUR = G.get_training_data('ETH-EUR')
        G_BTC_EUR = G.get_training_data('BTC-EUR')
        #G_LTC_EUR = G.get_training_data('LTC-EUR')

        # Get Reddit data
        #client_ID='GK8bm-Gl2VD5SA'
        #client_secret='pHtlAyxKuYt1JAGir_Gn9XOXlJc'
        #reddit = reddit.Reddit_Scanner(interval, start_time, end_time)
        #reddit.authenticate(client_ID, client_secret, include_sentiment_analysis=False)
        #ETH_reddit =reddit.get_training_data('Ethereum')
        #BTC_reddit =reddit.get_training_data('Bitcoin')
        #LTC_reddit =reddit.get_training_data('Litecoin')
        #with open('reddit_data.pickle', 'a') as f:
        #    pickle.dump((ETH_reddit, BTC_reddit, LTC_reddit), f)

        # Get Google search data
        #Search = google_search.Searchtrends(interval, start_time, end_time)
        #ETH_trends = Search.get_training_data('Ethereum')
        #BTC_trends = Search.get_training_data('Bitcoin')
        #LTC_trends = Search.get_training_data('Litecoin')
        #with open('google_data.pickle', 'wb') as f:
        #    pickle.dump((ETH_trends, BTC_trends, LTC_trends), f)

        # Get Blockchain stats
        #Blockchain = blockchain_stat_importer.Blockchain_Stats(interval, start_time, end_time)
        #ETH_Blockchain = Blockchain.get_training_data('../Blockchain_Data/Blockchain Stats - ETH_Clean.csv')
        #BTC_Blockchain = Blockchain.get_training_data('../Blockchain_Data/Blockchain Stats - BTC_Clean.csv')
        #LTC_Blockchain = Blockchain.get_training_data('../Blockchain_Data/ltc_blockchain.csv')
        #with open('blockchain_data.pickle', 'wb') as f:
        #    pickle.dump((ETH_Blockchain, BTC_Blockchain), f)

        # Convert GDAX timestamps to datetimes
        G_ETH_USD = G_ETH_USD.rename(index=lambda x:x*1000000000).rename(index=pd.to_datetime)
        G_BTC_USD = G_BTC_USD.rename(index=lambda x:x*1000000000).rename(index=pd.to_datetime)
        #G_LTC_USD = G_LTC_USD.rename(index=lambda x:x*1000000000).rename(index=pd.to_datetime)
        #G_ETH_EUR = G_ETH_EUR.rename(index=lambda x:x*1000000000).rename(index=pd.to_datetime)
        G_BTC_EUR = G_BTC_EUR.rename(index=lambda x:x*1000000000).rename(index=pd.to_datetime)
        #G_LTC_EUR = G_LTC_EUR.rename(index=lambda x:x*1000000000).rename(index=pd.to_datetime)

        # Set blockchain data index to timestamp (datetime format)
        #ETH_blockchain = ETH_blockchain.set_index('Timestamp')
        #BTC_blockchain = BTC_blockchain.set_index('Timestamp')
        #LTC_blockchain = LTC_blockchain.set_index('Timestamp')

        # Renaming to ensure unique column names. TODO: Make renaming programmatic
        G_ETH_USD = G_ETH_USD.rename(columns={"low": "Ethusd_gdax_low", "high": "Ethusd_gdax_high", "open": "Ethusd_gdax_open", "close": "Ethusd_gdax_close", "volume": "Ethusd_gdax_vol"})
        G_BTC_USD = G_BTC_USD.rename(columns={"low": "Btcusd_gdax_low", "high": "Btcusd_gdax_high", "open": "Btcusd_gdax_open", "close": "Btcusd_gdax_close", "volume": "Btcusd_gdax_vol"})
        #G_LTC_USD = G_LTC_USD.rename(columns={"low": "Ltcusd_gdax_low", "high": "Ltcusd_gdax_high", "open": "Ltcusd_gdax_open", "close": "Ltcusd_gdax_close", "volume": "Ltcusd_gdax_vol"})
        K_ETH_USD = K_ETH_USD.rename(columns={"low": "Ethusd_kraken_low", "high": "Ethusd_kraken_high", "open": "Ethusd_kraken_open", "close": "Ethusd_kraken_close", "volume": "Ethusd_kraken_vol"})
        K_BTC_USD = K_BTC_USD.rename(columns={"low": "Btcusd_kraken_low", "high": "Btcusd_kraken_high", "open": "Btcusd_kraken_open", "close": "Btcusd_kraken_close", "volume": "Btcusd_kraken_vol"})
        #K_LTC_USD = K_LTC_USD.rename(columns={"low": "Ltcusd_kraken_low", "high": "Ltcusd_kraken_high", "open": "Ltcusd_kraken_open", "close": "Ltcusd_kraken_close", "volume": "Ltcusd_kraken_vol"})
        #G_ETH_EUR = G_ETH_EUR.rename(columns={"low": "Etheur_gdax_low", "high": "Etheur_gdax_high", "open": "Etheur_gdax_open", "close": "Etheur_gdax_close", "volume": "Etheur_gdax_vol"})
        G_BTC_EUR = G_BTC_EUR.rename(columns={"low": "Btceur_gdax_low", "high": "Btceur_gdax_high", "open": "Btceur_gdax_open", "close": "Btceur_gdax_close", "volume": "Btceur_gdax_vol"})
        #G_LTC_EUR = G_LTC_EUR.rename(columns={"low": "Ltceur_gdax_low", "high": "Ltceur_gdax_high", "open": "Ltceur_gdax_open", "close": "Ltceur_gdax_close", "volume": "Ltceur_gdax_vol"})
        K_ETH_EUR = K_ETH_EUR.rename(columns={"low": "Etheur_kraken_low", "high": "Etheur_kraken_high", "open": "Etheur_kraken_open", "close": "Etheur_kraken_close", "volume": "Etheur_kraken_vol"})
        K_BTC_EUR = K_BTC_EUR.rename(columns={"low": "Btceur_kraken_low", "high": "Btceur_kraken_high", "open": "Btceur_kraken_open", "close": "Btceur_kraken_close", "volume": "Btceur_kraken_vol"})
        #K_LTC_EUR = K_LTC_EUR.rename(columns={"low": "Ltceur_kraken_low", "high": "Ltceur_kraken_high", "open": "Ltceur_kraken_open", "close": "Ltceur_kraken_close", "volume": "Ltceur_kraken_vol"})
        #ETH_trends = ETH_trends.rename(columns={"Worldwide": "Eth_search_worldwide", "US": "Eth_search_US", "GB": "Eth_search_GB", "FR": "Eth_search_FR", "DE": "Eth_search_DE", "RU": "Eth_search_RU", "KR": "Eth_search_KR"})
        #BTC_trends = BTC_trends.rename(columns={"Worldwide": "Btc_search_worldwide", "US": "Btc_search_US", "GB": "Btc_search_GB", "FR": "Btc_search_FR", "DE": "Btc_search_DE", "RU": "Btc_search_RU", "KR": "Btc_search_KR"})
        #LTC_trends = LTC_trends.rename(columns={"Worldwide": "Ltc_search_worldwide", "US": "Ltc_search_US", "GB": "Ltc_search_GB", "FR": "Ltc_search_FR", "DE": "Ltc_search_DE", "RU": "Ltc_search_RU", "KR": "Ltc_search_KR"})
        #ETH_blockchain = ETH_blockchain.rename(columns={"Hashrate": "Eth_hashrate", "Addresses": "Eth_addresses", "Supply": "Eth_supply", "Trx_Fee": "Eth_trx_fee", "Daily_Trx": "Eth_daily_trx"})
        #BTC_blockchain = BTC_blockchain.rename(columns={"Hashrate": "Btc_hashrate", "Addresses": "Btc_addresses", "Supply": "Btc_supply", "Trx_Fee": "Btc_trx_fee", "Daily_Trx": "Btc_daily_trx"})
        #LTC_blockchain = LTC_blockchain.rename(columns={"Hashrate": "Ltc_hashrate", "Addresses": "Ltc_addresses", "Supply": "Ltc_supply", "Trx_Fee": "Ltc_trx_fee", "Daily_Trx": "Ltc_daily_trx"})
        # Join dataframes together
        input_data = None
        input_data = G_ETH_USD
        #input_data = input_data.join(G_ETH_EUR)
        input_data = input_data.join(K_ETH_USD)
        input_data = input_data.join(K_ETH_EUR)
        #input_data = input_data.join(ETH_reddit)
        #input_data = input_data.join(ETH_trends)
        #input_data = input_data.join(ETH_blockchain)

        input_data = input_data.join(G_BTC_USD)
        input_data = input_data.join(G_BTC_EUR)
        input_data = input_data.join(K_BTC_USD)
        input_data = input_data.join(K_BTC_EUR)

        #input_data = input_data.join(BTC_reddit)
        #input_data = input_data.join(BTC_trends)
        #input_data = input_data.join(BTC_blockchain)

        #input_data = input_data.join(G_LTC_USD)
        #input_data = input_data.join(G_LTC_EUR)
        #input_data = input_data.join(K_LTC_USD)
        #input_data = input_data.join(K_LTC_EUR)
        #input_data = input_data.join(LTC_reddit)
        #input_data = input_data.join(LTC_trends)
        #input_data = input_data.join(LTC_blockchain)

        # Do interpolation for any blank cells
        input_data = input_data.interpolate()
        # Remove any data from outside correct time period
        #print("slicing by dates")
        input_data = input_data[input_data.index > start_time]
        input_data = input_data[input_data.index < end_time]

        # Create new fee per transaction column
        #input_data['Eth_fee_per_trx'] = input_data['Eth_trx_fee'] / input_data['Eth_daily_trx']
        #input_data['Btc_fee_per_trx'] = input_data['Btc_trx_fee'] / input_data['Btc_daily_trx']
        #input_data['Ltc_fee_per_trx'] = input_data['Ltc_trx_fee'] / input_data['Ltc_daily_trx']

        # Delete redundant columns: Trx fee, maybe some of the country data
        #input_data.drop('Eth_trx_fee', axis=1, inplace=True)
        #input_data.drop('Btc_trx_fee', axis=1, inplace=True)
        #input_data.drop('Ltc_trx_fee', axis=1, inplace=True)

        # TODO: Add reddit sentiment to this list that isn't converted to pct

        return input_data

    def live_download(interval, sequence_length, x_mean, x_std):
        """
        Function to download most recent data, scrub and normalise it, and convert to sequential values for LSTM
        :interval: Time period in minutes between datapoints. Needs to be same as for original training data
        :sequence_length: Number of intervals of "memory" for LSTM network. Data will be fed in time windows of length=sequence_length
        :x_mean: training/validation data mean values for normalisation
        :x_std: training/validation data standard deviation values for normalisation
        Returns tuple of np.array data for last time window. Shape of (1, sequence_length, input_size), and current price for logging purposes
        """

        # Download data for the most recent period
        end_time = datetime.now().replace(microsecond=0,second=0,minute=0)
        start_time = end_time - timedelta(minutes=interval * (sequence_length + 1)) # Adding some historical data in case interpolation needed
        data = processor.historical_download(start_time, end_time, interval)

        # Convert to float and interpolate any missing values
        data = data.astype('float64')
        data = data.interpolate()
        target = "Btcusd_kraken_close" # Only used for training. Must be the same as for training
        current_price = data[target] # For logging purposes

        # Convert to growth rates and np.arrays
        data = data.pct_change()
        x = np.array(data[1:]) # First value removed. Will always be NaN because growth rates

        # Normalise data
        x = (x - x_mean) / x_std
        print("x")
        print(x[0])

        # Reshape data from (num_samples, features) to (num_samples, sequence_length, features)
        seq_x = []
        for ii in range(len(x) - sequence_length + 1):
            print(ii)
            seq_x.append(x[ii : ii + sequence_length])
            print(x[ii : ii + sequence_length])

        seq_x = np.array(seq_x)
        print("seq x")
        print(seq_x)

        input_data = np.reshape(seq_x[-1], (-1, sequence_length, seq_x.shape[2]))
        return input_data, current_price[-1]

    def generate_x_y(data, target="Kraken_BTC_USD_Close", forecast_range=1 ):
        """
        Converts training data into training data and labels, with label currently fixed at 1 interval in the future.
        Returns a numpy array tuple of (train_data, training_target, target_actuals) where target actuals was the $ or EUR value
        """
        # Save target actuals for later comparison
        target_actuals = data[target]
        target_actuals = np.array(target_actuals)

        # Convert everything except trx_fee / trx, reddit sentiment to % change
        data = data.pct_change()
        #data[['Eth_fee_per_trx', 'Btc_fee_per_trx', 'Ltc_fee_per_trx']] = data[['Eth_fee_per_trx', 'Btc_fee_per_trx', 'Ltc_fee_per_trx']]

        # Splits dataset into data and targets
        train_data = np.array(data[:-forecast_range])
        target_df = data[target].shift(-forecast_range)
        target_data = np.array(target_df)

        return train_data[1:], target_data[1:-forecast_range], target_actuals[1:-1] # Remove first line since for % growth it will be NaN. Remove last line for target since it's also NaN because of shifting
