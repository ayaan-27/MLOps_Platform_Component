#!/usr/bin/env python
# coding: utf-8
from os import path

import pandas as pd
from sklearn import preprocessing
from sklearn.compose import ColumnTransformer

import utils.logs as logs
from preprocessing import dataframe_handler as dh

LOGGER = logs.get_logger()


class ColumnScaler:
    def __init__(self, scale_method):
        """Constructor to initialise method for Scaling

        Args:
            scale_method (Str): Method for Scaling
        """
        LOGGER.info(
            "Class ColumnScaler Instantiated with, Scale Method:" + str(scale_method)
        )
        if scale_method == "":
            self.scale_method = "standardscaler"
        else:
            self.scale_method = scale_method
        self.scale_para_val()

    def scale_para_val(self):
        """Function to validate Scaling Parameters defined by user

        Raises:
            Exception: When Method defined by user is not supported by Functionality
        """
        LOGGER.info("Scale Column Parameter Validation: In Progress")
        if self.scale_method.lower() not in [
            "maxabsscaler",
            "minmaxscaler",
            "robustscaler",
            "standardscaler",
        ]:
            LOGGER.error("Scale Column Parameter Validation: Failed")
            raise Exception(
                "Undefined method: Method defined by user is not supported by Functionality"
            )
        LOGGER.info("Scale Column Parameter Validation: Validated")

    def scale_transformer(self, col_name):
        """Function to return tuple with skLearn Scaler and suitable inputs based on scale method

        Args:
            col_name (Str): Name of Column to be Scale

        Returns:
            Tuple: Tuple with object, transformer and column name
        """
        LOGGER.info("Scale Column Transformers created successfully for " + str(col_name))
        if self.scale_method.lower() == "maxabsscaler":
            # print('maxabsscaler')
            return (col_name + "_scaler", preprocessing.MaxAbsScaler())
        elif self.scale_method.lower() == "minmaxscaler":
            # print('minmaxscaler')
            return (col_name + "_scaler", preprocessing.MinMaxScaler())
        elif self.scale_method.lower() == "robustscaler":
            # print('robustscaler')
            return (col_name + "_scaler", preprocessing.RobustScaler())
        elif self.scale_method.lower() == "standardscaler":
            # print('default')
            return (col_name + "_scaler", preprocessing.StandardScaler())


def base_scale(filepath, scale=True, ucol_scale=[], method_scale=[]):

    # READ THE CSV FILE AND STORE IT AS DATA FRAME
    if filepath:
        if path.isfile(filepath):
            df = pd.read_csv(filepath)
        else:
            raise Exception("File Path not defined")

    transformers = []

    if scale:
        if len(ucol_scale) == 0:
            raise Exception()
        if len(ucol_scale) != len(method_scale):
            raise Exception()
        for i in range(0, len(ucol_scale)):
            action = "Scale"
            print(action, i)
            dh.check_coldtype(df, ucol_scale[i], action)
            t = ColumnScaler(method_scale[i])
            t.scale_para_val()
            transformers.append(t.scale_transformer(ucol_scale[i]))

    print(transformers)
    column_trans = ColumnTransformer(transformers, remainder="passthrough")
    # print(column_trans.fit_transform(df))
    output_array = pd.DataFrame(column_trans.fit_transform(df))
    # print(output_array[0])
    new_df = df.copy()
    for i in range(0, len(ucol_scale)):
        new_df[ucol_scale[i]] = output_array[i]

    # print(df.isnull().sum(),new_df.isnull().sum())
    print(df, new_df)


# base_scale(filepath = "C:/Users/Karupaiya/Documents/00_MLPractice/cars1.csv",  scale = True,
# ucol_scale = ['MPG','Cylinders'], method_scale =
# ['standardscaler','robustscaler'])
