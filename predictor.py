import tensorflow as tf
import pandas as pd
import numpy as np
import time
import itertools


def split_data(data):
    features = data.drop(["ID_TMDB", "REVENUE"], axis=1)
    labels = data["REVENUE"]
    return features, labels


def read_train_data():
    data = pd.read_feather("train_save.feather", nthreads=24)
    return data


def initialize():
    feature_columns = [tf.feature_column.numeric_column("x", shape=[50485])]

    hidden_units = [1000, 600, 5]

    estimator = tf.estimator.DNNRegressor(
        feature_columns=feature_columns,
        hidden_units=hidden_units,
        model_dir="test3"
    )
    return estimator

def train(estimator, features, labels):
    print("preparing input")
    train_input_fn = tf.estimator.inputs.numpy_input_fn(
        x={"x": features},
        y=labels,
        num_epochs=None,
        shuffle=True
    )
    print("training")
    estimator.train(train_input_fn, steps=10000)
    print("preparing evaluation")
    test_input = tf.estimator.inputs.numpy_input_fn(
        x={"x":features},
        y=labels,
        num_epochs=1,
        shuffle=False
    )
    print("testing eval")
    accuracy = estimator.evaluate(test_input)
    print(accuracy)

def predict(estimator, features, labels):

    input_fn = tf.estimator.inputs.numpy_input_fn(
        x={"x": features},
        num_epochs=1,
        shuffle=False
    )

    prediction = estimator.predict(input_fn=input_fn)
    actual = labels[0]
    predicted = float(list(prediction)[0]["predictions"][0])
    difference = predicted - actual
    print("ACTUAL", actual, "PREDICTED", predicted, "DIFF", difference)

def init():
    ti = time.time()
    data = read_train_data()
    data["ORIGINAL_LANGUAGE"] = data["ORIGINAL_LANGUAGE"].astype("category").cat.codes
    features, labels = split_data(data)
    features = features.as_matrix()
    labels = labels.as_matrix()
    estimator = initialize()
    for i in range(100, 120):
        ft = features[i:i+1]
        lb = labels[i:i+1]
        predict(estimator, ft, lb)
    #train(estimator, features, labels)

    print(time.time() - ti, "s")
if __name__ == "__main__":
    init()

