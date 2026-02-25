#!/usr/bin/env python
# coding: utf-8
from os import path

import pandas as pd

import utils.logs as logs
from preprocessing import dataframe_handler as dh

LOGGER = logs.get_logger()


def removenull_column(df, col_name):
    """Function to remove null rows in a column of dataframe

    Args:
        df (DataFrame): Input Dataframe

    Returns:
        DataFrame: Dataframe with Non Null rows in specified column
    """
    LOGGER.info("Remove Null Values from Column [" + str(col_name) + "]: In Progress")
    new_df = df.copy()
    new_df = new_df[new_df[col_name].notnull()]
    LOGGER.info("Remove Null Values from Column [" + str(col_name) + "]: Completed")
    return new_df


def base_removenull(filepath, removenull=False, ucol_removenull=[]):

    if filepath:
        if path.isfile(filepath):
            df = pd.read_csv(filepath)
        else:
            raise Exception("File Path not defined")

    if removenull:
        for i in range(0, len(ucol_removenull)):
            action = "Remove Null"
            print(action, i)
            dh.check_nulldf(df, ucol_removenull[i], action)
            df = removenull_column(df, ucol_removenull[i])
    print(df.isnull().sum())
    print(df)


# base_removenull(filepath = "C:/Users/Karupaiya/Documents/00_MLPractice/cars1.csv", removenull = True, ucol_removenull = ['Car','MPG'])
