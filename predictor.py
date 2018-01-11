import tensorflow as tf
import pandas as pd
import numpy as np
import time


def split_data(data):
    features = data.drop(["ID_TMDB", "REVENUE"], axis=1)
    labels = data["REVENUE"]
    return features, labels


def read_train_data():
    data = pd.read_feather("train_save.feather", nthreads=24)
    return data


def initialize(features):
    # possible_features = list(features)[3:]
    #
    # in_collection = tf.feature_column.numeric_column("IN_COLLECTION")
    # language = tf.feature_column.categorical_column_with_vocabulary_list(
    #     "ORIGINAL_LANGUAGE", features["ORIGINAL_LANGUAGE"].unique()
    # )
    # budget = tf.feature_column.numeric_column("BUDGET")
    # act_dir = [tf.feature_column.numeric_column(x) for x in possible_features]

    feature_columns = [tf.feature_column.numeric_column("x", shape=[50485])] #[in_collection, tf.feature_column.indicator_column(language), budget] + act_dir

    hidden_units = [200, 400, 200]

    # po potrebi priredit optimizer
    estimator = tf.estimator.DNNRegressor(
        feature_columns=feature_columns,
        hidden_units=hidden_units,
        model_dir="/tmp/test"
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
    estimator.train(train_input_fn, steps=500)
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

def init():
    ti = time.time()
    data = read_train_data()
    data = data.head(100)
    data["ORIGINAL_LANGUAGE"] = data["ORIGINAL_LANGUAGE"].astype("category").cat.codes
    features, labels = split_data(data)
    features = features.as_matrix()
    labels = labels.as_matrix()
    estimator = initialize(features)
    train(estimator, features, labels)

    print(time.time() - ti, "s")
if __name__ == "__main__":
    init()

