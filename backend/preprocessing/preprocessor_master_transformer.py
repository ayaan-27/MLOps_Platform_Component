#!/usr/bin/env python
# coding: utf-8
from os import path
from pickle import load

import mlflow
from mlflow.tracking import MlflowClient
import pandas as pd
from utils.file_io import download_file, read_file, save_pickle
import utils.logs as logs
from preprocessing import dataframe_handler as dh

LOGGER = logs.get_logger()


def pre_processor_controller(
    dataset_id: int, 
    version_id: int, 
    pre_process_dict: dict, 
    project_id: int, 
    user_id: int = 1, 
    job_id: int = 1, 
    removedup= False
    ):

    client = MlflowClient()
    try:
        experiment_id = client.create_experiment("preprocessing_"+dataset_id+"_"+version_id)
        LOGGER.info("Created new experiment")
    except BaseException:
        experiment_id = client.get_experiment_by_name(
            "preprocessing_"+dataset_id+"_"+version_id
        ).experiment_id
        LOGGER.info("Using existing experiment")
        
    pre_process_df = read_file(download_file(dataset_id = dataset_id, version_id = version_id))

    # removing target feature
    # independent_feature_data_frame= data_frame_test.iloc[:,:-1]

    try:
        # loading data_transformer object from pickle file
        #TODO - How to download Pickle File - Problem with dataset_id and version_id
        data_transformer = load(open("preprocess_transformer.pkl", "rb"))
        LOGGER.info("Pickled object read successfully.")

        # transform test dataset
        new_data = data_transformer.transform(pre_process_df)
        new_feature_name = dh.get_feature_names(data_transformer)
        new_df = pd.DataFrame(data=new_data, columns=new_feature_name)
        
        #TODO - create new version and upload to s3
        new_df.to_csv("Preprocessed_Data.csv", index=False)
        mlflow.log_artifact("Preprocessed_Data.csv")
        LOGGER.info("Test dataset transformed successfully.")
        print("After Transform")
        print(pre_process_df, "\n", new_df)

    except Exception as er:
        LOGGER.error(
            "Exception raised. Error in pickle file or test data transformation. Error is: {}".format(
                er
            )
        )

        
