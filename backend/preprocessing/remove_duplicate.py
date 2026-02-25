#!/usr/bin/env python
# coding: utf-8
from os import path

import pandas as pd

import utils.logs as logs

LOGGER = logs.get_logger()


def removeduplicate_dataframe(df):
    """Function to remove duplicate rows in dataframe

    Args:
        df (DataFrame): Input Dataframe

    Returns:
        DataFrame: De-Duplicated Dataframe
    """
    LOGGER.info("Remove Duplicate Rows: In Progress")
    new_df = df.copy()
    new_df.drop_duplicates(inplace=True)
    LOGGER.info("Remove Duplicate Rows: Completed")
    return new_df


def base_removedup(filepath, removedup=False):

    if filepath:
        if path.isfile(filepath):
            df = pd.read_csv(filepath)
        else:
            raise Exception("File Path not defined")

    if removedup:
        action = "Remove Duplicate Rows"
        print(action)
        new_df = removeduplicate_dataframe(df)

    print(df, "\n", new_df)


# base_removedup(filepath = "C:/Users/Karupaiya/Downloads/0000_Refactored_Py_DS_ML_Bootcamp-master/000_WORKOUT_KP/cars1.csv", removedup = True)
