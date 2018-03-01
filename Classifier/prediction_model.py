"""
Classifier Neural Network
Defines the model structure and key functions
"""

from keras.models import Sequential
from keras.layers import Dense, Dropout, LSTM, MaxPooling2D, Conv2D

class Neural_Net:
    """
    RNN using LSTM. With only ~60 datapoints per time period, fully connected layers works well
    """

    def __init__(self, architecture = LSTM, learning_rate=0.001, batch_size=64):
        # Sets up the network in Keras, including optimisers
        # Starting point is a 4 layer CNN + 2 layers of LSTM
        self.model = architecture.build_model()


    def train(self):
        # Call model train function and initiate data logging and weight saving
        pass

    def predict(self):
        # Call the model to predict next 5 intervals based on historical data
        # TODO: A probability / confidence score would be very interesting...
        pass


class LSTM:
    """
    Actual model structure. Keep is separate to keep code clean?
    """
    def __init__(self, input_size):
        self.input_size = input_size
        self.build_model()

    def build_model(self):
        model = Keras.Sequential()
        model.add(LSTM(
            input_dim=input_size,
            output_dim=64,
            return_sequences=True,
            stateful=False))
        model.add(Dropout(0.2))
        model.add(LSTM(
            output_dim=64,
            return_sequences=True,
            stateful=False))
        model.add(Dropout(0.2))
        model.add(Dense(units=1, activation='linear'))

        # Define optimiser and compile
        optimizer = optimizers.Adam()
        self.model.compile(optimizer=optimizer, loss=????)
        return model

class Convnet:
    """
    Actual model structure. Keep is separate to keep code clean?
    """
    def __init__(self, input_size):
        self.input_size = input_size
        self.build_model()

    def build_model(self):
        pass

class standard_DNN:
    """
    Actual model structure. Keep is separate to keep code clean?
    """
    def __init__(self, input_size):
        self.input_size = input_size
        self.build_model()

    def build_model(self):
        pass
