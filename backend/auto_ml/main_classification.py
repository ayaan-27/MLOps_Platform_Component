import os, sys
import re
import pandas as pd

sys.path.append(os.getcwd())

import auto_ml.classification_models as classification_models
from auto_ml.leaderboard import generate_leaderboard_classification
import utils.logs as logs
from sklearn.model_selection import train_test_split
import utils.file_io as io

LOGGER = logs.get_logger()

def main_classification(
    dataset_id,
    version_id,
    target,
    bias=False,
    bias_column="",
    test_split=0.2,
    tuning_metric="accuracy",
    n_passes=50,
):
    """
        Function for running multiple models and getting the best.
    Args
        path : String
            Valid path to dataset

        target : String
            target column in dataset

        test_split : float
            fraction of data to be set aside for testing

        tuning_metric :String
            Metric given for tuning

        n_passes :integer
            number of models to be generated

    Returns

        score_table : pandas DataFrame
            Performance for each model
    """

    try:
        LOGGER.info("check for correct tuning metric")
        if bias is False:
            if tuning_metric in [
                "parity_diff",
                "disparate_impact",
                "eq_odds_ratio",
                "equal_opportunity",
            ]:
                LOGGER.error("incorrect metric chosen")
                raise Exception("incorrect metric chosen")
        if bias is True and tuning_metric not in [
            "parity_diff",
            "disparate_impact",
            "eq_odds_ratio",
            "equal_opportunity",
        ]:
            LOGGER.error("No bias metric chosen")
            raise Exception("No bias metric chosen")
        data = io.read_file(io.download_file(dataset_id = dataset_id, version_id = version_id))
        S_train = ""

        # this line of code solves fatal error with lgbm regressor
        data = data.rename(columns=lambda x: re.sub("[^A-Za-z0-9_]+", "", x))

        LOGGER.info("splitting into train and test data")
        y = data[target]
        X = data.drop([target], axis="columns", inplace=False)
        X_train, X_test, y_train, y_test = train_test_split(
            X, y, test_size=test_split, random_state=5, shuffle=True
        )
        LOGGER.info("defining sensitive column for bias metrics and removing it from the dataset")
        if bias is True:
            S_train = X_test[bias_column]
            X_train.drop([bias_column], axis="columns", inplace=True)
            X_test.drop([bias_column], axis="columns", inplace=True)
        LOGGER.info("Data has been split for training and testing")
        # Collecting all the candidates and thier tuners
        (
            tuners,
            candidates,
            selector,
            selector_dict,
        ) = classification_models.model_definitions(tuning_metric)
        LOGGER.info("Models and tuners successfully defined")
        
        leaderboard_df = generate_leaderboard_classification(
            X_train,
            y_train,
            X_test,
            y_test,
            tuning_metric,
            n_passes,
            tuners,
            candidates,
            selector,
            selector_dict,
            S_train,
            bias
        )
        # leaderboard retrieved
        LOGGER.info("Leaderboard retrieved")
        return leaderboard_df
    except Exception as e:
        LOGGER.error(str(e))
        raise Exception(str(e))
