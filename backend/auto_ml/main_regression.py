import re
import os, sys

sys.path.append(os.getcwd())

import pandas as pd
import auto_ml.regression_models as regression_models
from auto_ml.leaderboard import generate_leaderboard_regression
import utils.logs as logs
from sklearn.model_selection import train_test_split
import utils.file_io as io

LOGGER = logs.get_logger()

def main_regression(
    dataset_id, version_id, target, test_split=0.2, tuning_metric="MSE", n_passes=50
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
        if tuning_metric not in ["MSE", "RMSE", "MAE", "R2", "MSLE", "MAPE"]:
            LOGGER.error("Invalid tuning metric")
            raise Exception("Invalid Tuning metric")


        data = io.read_file(io.download_file(dataset_id = dataset_id, version_id = version_id))

        # this line of code solves fatal error with lgbm regressor
        data = data.rename(columns=lambda x: re.sub("[^A-Za-z0-9_]+", "", x))

        # splitting into train and test data
        y = data[target]
        X = data.drop([target], axis="columns", inplace=False)
        X_train, X_test, y_train, y_test = train_test_split(
            X, y, test_size=test_split, random_state=5, shuffle=True
        )

        # Collecting all the candidates and thier tuners
        tuners, candidates, selector, selector_dict = regression_models.model_definitions(
            tuning_metric
        )
        LOGGER.info("Models and tuners successfully defined")
        (
            leaderborad_df,
            best_run_id,
            best_model,
            best_params,
            best_score,
        ) = generate_leaderboard_regression(
            X_train,
            y_train,
            X_test,
            y_test,
            tuning_metric,
            n_passes,
            tuners,
            candidates,
            selector,
            selector_dict
        )

        LOGGER.info("Leaderboard retrieved")
        return leaderborad_df, best_run_id, best_model, best_params, best_score
    except Exception as e:
        LOGGER.error(str(e))
