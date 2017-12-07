import tensorflow as tf
import pandas as pd


estimator = None

def initialize():
    feature_columns = []
    hidden_units = [40, 60, 20]
    estimator = tf.estimator.DNNRegressor(
        feature_columns=feature_columns,
        hidden_units=hidden_units,
    )

def build_train_input(dataframe):
    pass

def build_test_input(dataframe):
    pass

def train(input):
    estimator.fit()

def predict(input):
    pass

