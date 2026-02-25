import os,sys
sys.path.append(os.getcwd())
from collections import Counter

import mlflow
import pandas as pd

from augmentation.sampling import Sampling
from utils.file_io import download_file, read_file, save_pickle
from utils.logs import get_logger
from datasets.datasets import create_version

LOGGER = get_logger()


'''
TRACKING_URL = "http://localhost:5000"
mlflow.set_tracking_uri(TRACKING_URL)
mlflow.set_experiment("Sampling Temp")
client = mlflow.tracking.MlflowClient(TRACKING_URL)
'''

def sampling_main(dataset_id, 
        version_id, 
        sampling_method, 
        sampling_strategy="auto", 
        target="Class", 
        categorical_problem=False, 
        categorical_features=[], 
        k_neighbors=5):
    """
        Script to perform sampling on a dataset
    Args:
        path : str
            A valid path to the dataset

        sampling_method : str
            A valid sampling method.
            ["Random Oversampler", "Random Undersampler", "SMOTE",
            "Categorical SMOTE"].

        sampling_strategy : float, str or dict
            Sampling information to resample the data set.
            When 'float' , it corresponds to the desired ratio of the
            number of samples in the minority class over the number of
            samples in the majority class after resampling.
            When str, specify the class targeted by the resampling.
            For oversampling and SMOTE : ['minority', 'not minority',
            'all', 'auto'].
            For undersampling : ['majority', 'not minority', 'not majority',
            'all', 'auto'].
            Defaults to 'auto'.
            When dict, the keys correspond to the targeted classes.
            The values correspond to the desired number of samples
            for each targeted class.

        target : str, optional
            The target column name. Defaults to 'Class'.

        categorical_problem : bool, optional
            Must be 'True' if the dataset has categorical columns.
            Defaults to False.

        categorical_features : list(int), optional
            The column numbers of all categorical columns part of the dataset.
            Defaults to [].

        k_neighbors : int or object, optional, default=5
            Only applicable to SMOTE and Categorical SMOTE
            If int, number of nearest neighbours to used to construct
            synthetic samples.
            If object, an estimator that inherits from KNeighborsMixin
            that will be used to find the k_neighbors.
    """
    try:
        LOGGER.info("inside sampling master")

        LOGGER.info("reading dataset with dataset_id  "+str(dataset_id)+" version_id "+str(version_id))
        dataset = read_file(download_file(dataset_id = dataset_id, version_id = version_id))
        

        X = dataset.drop(target, axis=1)
        Y = dataset[target]

        if len(Counter(Y)) > 2:
            ptype = "multi"
            LOGGER.info("Multi class problem")
        elif len(Counter(Y)) == 2:
            ptype = "binary"
            LOGGER.info("Binary class problem")
        else:
            LOGGER.error("Not a classification problem")
            raise Exception("Invalid classification problem, only 1 class detected")

        # If the strategy is of dictionary type, need to check if no ok keys
        # match number of classes present in the dictionary
        if type(sampling_strategy) == dict:
            if len(sampling_strategy.keys()) == len(Counter(Y)):
                pass
            else:
                LOGGER.error(
                    "Number of keys in dictionary do not match \
                    the number of classes present"
                )
                raise Exception(
                    "Invalid Sampling strategy, the number of keys in the strategy \
                        do not corrospond to the number of classes present"
                )

        valid = False

        # Sampling strategy and type of classification
        # should match.
        if ptype == "multi" and (
            type(sampling_strategy) == str or type(sampling_strategy) == dict
        ):
            valid = True
        elif ptype == "binary" and (
            type(sampling_strategy) == float
            or type(sampling_strategy) == str
            or type(sampling_strategy) == dict
        ):
            valid = True

        if valid is False:
            LOGGER.error(
                "Invalid combination of sampling strategy and \
                type of classification problem"
            )
            raise Exception("Invalid Sampling strategy and type of classification problem")

        sampling_strategies_over = ["minority", "not minority", "all", "auto"]
        sampling_strategies_under = ["majority", "not minority", "not majority", "all", "auto"]

        sampling_methods = [
            "Random Oversampler",
            "Random Undersampler",
            "SMOTE",
            "Categorical SMOTE",
        ]

        # Sampling strategy and Sampling method should be compatible
        if (
            (type(sampling_strategy) == str)
            and (sampling_strategy in sampling_strategies_under)
            and (sampling_method == sampling_methods[1])
        ):
            valid = True
        elif (
            (type(sampling_strategy) == str)
            and (sampling_strategy in sampling_strategies_over)
            and (sampling_method != sampling_methods[1])
        ):
            valid = True
        else:
            valid = False
            LOGGER.error(
                "This sampling strategy cannot be applied to \
                this sampling method"
            )
            raise Exception("Invalid sampling strategy and sampling method combination")

        sampler = Sampling(X, Y, sampling_strategy)
        X_resampled = []
        Y_resampled = []

        if sampling_method in sampling_methods and valid:

            if sampling_method == sampling_methods[0]:
                X_resampled, Y_resampled = sampler.randomOverSampler()

            if sampling_method == sampling_methods[1]:
                X_resampled, Y_resampled = sampler.randomUnderSampler()

            if sampling_method == sampling_methods[2]:
                X_resampled, Y_resampled = sampler.smote(k_neighbors)

            if (
                len(categorical_features) > 1
                and sampling_method == "Categorical SMOTE"
                and categorical_problem
            ):
                X_resampled, Y_resampled = sampler.smotenc(
                    categorical_features, sampling_strategy, k_neighbors
                )

            if (
                len(categorical_features) < 1
                and sampling_method == "Categorical SMOTE"
                and categorical_problem
            ):
                LOGGER.error("Number of categorical features too small")
                raise Exception("Invalid Categorical SMOTE operation")

        else:
            LOGGER.error("Invalid sampling method")
            raise Exception("Invalid sampling method")

        X_resampled[target] = Y_resampled


        LOGGER.info(X_resampled[target].value_counts())
        LOGGER.info("Data Augmented using Sampling")
        
        
        '''
        resampled_path = "Sampled-" + os.path.split(path)[-1]
        X_resampled.to_csv(resampled_path, index=False)
        '''

        '''
        settings = {
            "Path": path,
            "Sampling_method": sampling_method,
            "Target": target,
            "Categorical_problem": categorical_problem,
            "Categorical_columns": categorical_features,
        }
        
        with mlflow.start_run():
            LOGGER.info("ML Flow running")
            mlflow.log_dict(settings, "Settings.json")
            mlflow.log_artifact(resampled_path)
            mlflow.end_run()
        '''

        return X_resampled
    except Exception as e:
        print("ERROR : ", e)




if __name__ == "__main__":
    sampling_main(dataset_id = 1,version_id = 26,sampling_method = "Random Oversampler")
