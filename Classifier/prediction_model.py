"""
Classifier Neural Network
Defines the model structure and key functions
"""

import h5py
from keras.models import Sequential
from keras.layers import Dense, Dropout, LSTM, MaxPooling2D, Conv2D
from keras.callbacks import ModelCheckpoint, Callback
import matplotlib.pyplot as plt

class Neural_Net:
    """
    Class to build and train neural net
    """

    def __init__(self, input_size, architecture, activation='relu', learning_rate=0.001):
        # Sets up the network in Keras, including optimisers
        if architecture == 'DNN':
            self.model = LSTM(input_size, learning_rate, activation)
        if architecture == 'LSTM':
            self.model = LSTM(input_size, learning_rate)
        else:
            print("model architecture not recognised or defined")

    def train(self, train_data, train_targets, train_mean, train_std, valid_data, valid_labels, epochs, batch_size=64):
        """
        Function to train the model, including logging and weight saving callbacks and results plotting
        :train_data: 2D numpy array of training samples
        :train_targets: 1D numpy array of training targets (prices). Should be time-offset against get_training_data
        :train_mean: the mean of the target series in the training dataset
        :train_std: standard deviation of the target in the training dataset
        """

        # Define weight saving callback
        checkpointer = ModelCheckpoint(filepath='./saved_models/weights1.hdf5',
                               verbose=1, save_best_only=True)

        # Define logging callback
        class train_log(keras.callbacks.Callback):
            def on_train_begin(self, logs={}):
                self.losses = {'train':[], 'validation':[]}

            def on_epoch_end(self, logs={}):
                self.losses['train'].append(logs.get('loss'))
                self.losses['validation'].append(logs.get('val_loss'))

            def on_train_end(self, batch, logs={}):
                plt.plot(losses['train'], label='Training loss')
                plt.plot(losses['validation'], label='Validation loss')
                plt.legend()
                _ = plt.ylim()

        # Call model train function and initiate data logging and weight saving
        self.model.fit(train_data, train_targets,
                       batch_size=batch_size, epochs=epochs,
                       callbacks=[checkpointer, train_log],
                       validation_data=(valid_data, valid_targets))

        # Plot price prediction chart
        prediction = []
        for ii in range(len(valid_data)):
            input_data = valid_data[ii]
            model_output = self.model.predict(input_data)
            prediction.append(model_output * train_std + train_mean)

        plt.plot(prediction, label='Predicted price')
        plt.plot(valid_labels * train_std + train_mean)
        plt.legend()
        _ = plt.ylim()


    def forecast(self):
        # Call the model to predict next 5 intervals based on historical data
        # TODO: A probability / confidence score would be very interesting...
        pass


class LSTM:
    """
    RNN using LSTM
    """
    def __init__(self, input_size, learning_rate):
        self.input_size = input_size
        self.learning_rate = learning_rate
        self.build_model()

    def build_model(self):
        self.model = Sequential()
        self.model.add(LSTM(
            input_dim = self.input_size,
            output_dim=64,
            return_sequences=True,
            stateful=False))
        self.model.add(Dropout(0.2))
        self.model.add(LSTM(
            output_dim=64,
            return_sequences=True,
            stateful=False))
        self.model.add(Dropout(0.2))
        self.model.add(Dense(units=1, activation='linear'))

        # Define optimiser and compile
        optimizer = optimizers.Adam(self.learning_rate)
        self.model.compile(optimizer=optimizer, loss='mse', metrics=['accuracy'])

class Convnet:
    """
    TBD - Model using CNN to capture time series patterns
    """
    def __init__(self, input_size, learning_rate, activation='relu'):
        self.input_size = input_size
        self.learning_rate = learning_rate
        self.build_model()

    def build_model(self):
        self.model = Sequential()
        # Define optimiser and compile
        optimizer = optimizers.Adam(self.learning_rate)
        self.model.compile(optimizer=optimizer, loss='mse', metrics=['accuracy'])

class DNN:
    """
    Standard fully connected MLP as baseline
    """
    def __init__(self, input_size, learning_rate, activation='relu'):
        self.input_size = input_size
        self.learning_rate = learning_rate
        self.activation = activation
        self.build_model()

    def build_model(self):
        self.model = Sequential()
        self.model.add(Dense(32, activation = self.activation, input_dim = self.input_size))
        self.model.add(Dropout(0.2))
        self.model.add(Dense(64, activation = self.activation))
        self.model.add(Dropout(0.2))
        self.model.add(Dense(128, activation = self.activation))
        self.model.add(Dropout(0.2))
        self.model.add(Dense(units=1, activation='linear'))

        # Define optimiser and compile
        optimizer = optimizers.Adam(self.learning_rate)
        self.model.compile(optimizer=optimizer, loss='mse', metrics=['accuracy'])
