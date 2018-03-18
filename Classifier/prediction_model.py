"""
Classifier Neural Network
Defines the model structure and key functions
"""

import h5py
from keras import optimizers
from keras.models import Sequential
from keras.layers import Dense, Dropout, LSTM, MaxPooling2D, Conv2D
from keras.callbacks import ModelCheckpoint, Callback
import matplotlib.pyplot as plt

# Define prediction model
class LSTM_net:
    """
    RNN using LSTM
    """
    def __init__(self, input_size, learning_rate):
        self.input_size = input_size
        self.learning_rate = learning_rate
        self.build_model()

    def build_model(self):
        self.model = Sequential()
        self.model.add(LSTM(256, return_sequences=True,
                       input_shape=self.input_size))
        #self.model.add(Dropout(0.2))
        self.model.add(LSTM(256))
        #self.model.add(Dropout(0.2))
        self.model.add(Dense(1, activation='linear'))

        # Define optimiser and compile
        optimizer = optimizers.Adam(self.learning_rate)
        self.model.compile(optimizer=optimizer, loss='mse', metrics=['accuracy'])
