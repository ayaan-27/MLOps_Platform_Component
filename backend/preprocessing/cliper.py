#!/usr/bin/env python
# coding: utf-8
from os import path

import numpy as np
import pandas as pd
from sklearn.base import BaseEstimator, TransformerMixin
from sklearn.compose import ColumnTransformer

import preprocessing.dataframe_handler as dh
import utils.logs as logs

LOGGER = logs.get_logger()


class ColumnCliper(BaseEstimator, TransformerMixin):
    def __init__(self, flag, u_min, u_max):
        """Constructor to initialise flag, minimum value and maximum value for Clipping

        Args:
            flag (Str): Flag to carry the information about user preference for Clipping
            u_min (Int/Float): Minimum Value for Clipping
            u_max (Int/Float): Maximum Value for Clipping
        """
        LOGGER.info(
            "Class ColumnCliper Instantiated with, Flag:"
            + str(flag)
            + " User Minimum: "
            + str(u_min)
            + " User Maximum: "
            + str(u_max)
        )
        if flag == "":
            self.flag = "value"
        else:
            self.flag = flag
        if u_min == "":
            self.u_min = "null"
        else:
            self.u_min = u_min
        if u_max == "":
            self.u_max = "null"
        else:
            self.u_max = u_max
        self.clip_para_val()

    def clip_para_val(self):
        """Function to validate Clipping Parameters defined by user

        Raises:
            Exception: When Flag defined by user is not supported by Functionality
        """
        LOGGER.info("Clip Column Parameter Validation: In Progress")
        if self.flag.lower() not in ["percentile", "value"]:
            LOGGER.error("Clip Column Parameter Validation: Failed")
            raise Exception(
                "Undefined flag: Flag defined by user is not supported by Functionality"
            )
        LOGGER.info("Clip Column Parameter Validation: Validated")
        # if (isinstance(self.u_min,str) or isinstance(self.u_max,str)):
        #     raise Exception("Undefined Value type for Clip Operation")

    def clip_transformer(self, col_name):
        """Function to return tuple with Custom Clip transformer and suitable inputs

        Args:
            col_name (Str): Name of Column to be Clipped

        Returns:
            Tuple: Tuple with object, transformer and column name
        """
        LOGGER.info("Clip Column Transformers created successfully for " + str(col_name))
        return (col_name + "_cliper", self)

    def clip_column(self, clip_df, u_min, u_max):
        """Function to perform cliping

        Args:
            clip_df (DataFrame): Input Dataframe with only required column data for clipping
            u_min (Int/Float): Minimum Value for Clipping
            u_max (Int/Float): Maximum Value for Clipping

        Returns:
            DataFrame: Clipped data frame
        """
        LOGGER.info(
            "Clip Column Custom Transform Function - Clip Minimum:"
            + str(u_min)
            + " Clip Maximum:"
            + str(u_max)
        )
        col_name = clip_df.columns.tolist()
        col_data = clip_df[col_name[0]]
        cliped_col = np.clip(col_data.tolist(), a_min=u_min, a_max=u_max)
        return pd.DataFrame(cliped_col)

    def fit(self, X, y=None):
        """Function to FIT the dataframe for transformation and initialise/assign values for fitting if null

        Args:
            X (DataFrame): Input Dataframe
            y (optional): [description]. Defaults to None.

        Raises:
            Exception: When Minimum Value to Clip is greater than Maximum Value

        Returns:
            Object: Class Object
        """
        LOGGER.info("Clip Column Custom Fit Function: In Progress")
        col_name = X.columns.tolist()
        col_data = X[col_name[0]]
        if self.flag.lower() == "percentile":
            if self.u_min == "null":
                # print('i am here')
                self.u_min = 0
            if self.u_max == "null":
                # print('i am there')
                self.u_max = 100
            self.u_max = np.percentile(col_data.tolist(), self.u_max)
            self.u_min = np.percentile(col_data.tolist(), self.u_min)
        elif self.flag.lower() == "value":
            if self.u_min == "null":
                # print('i am here')
                self.u_min = col_data.min()
            if self.u_max == "null":
                # print('i am there')
                self.u_max = col_data.max()
        # print(self.u_min,self.u_max)
        if self.u_min > self.u_max:
            LOGGER.info("Clip Column Custom Fit Function: Failed")
            raise Exception(
                "Minimum Clip Value is not Lesser than or Equal to Maximum Clip Value"
            )
        LOGGER.info("Clip Column Custom Fit Function: Completed")
        return self

    def transform(self, X):
        """Function to transform the input dataframe

        Args:
            X (DataFrame): Input Dataframe

        Returns:
            Function: clip_column function with inputs
        """
        LOGGER.info("Clip Column Custom Transform Function: In Progress")
        return self.clip_column(X, self.u_min, self.u_max)


def base_clip(filepath, clip=True, ucol_clip=[], clip_flag=[], clip_min=[], clip_max=[]):

    # READ THE CSV FILE AND STORE IT AS DATA FRAME
    if filepath:
        if path.isfile(filepath):
            df = pd.read_csv(filepath)
        else:
            raise Exception("File Path Not defined")

    transformers = []

    if clip:
        if len(ucol_clip) == 0:
            raise Exception()
        for i in range(0, len(ucol_clip)):
            action = "Clip"
            print(action, i)
            dh.check_nulldf(df, ucol_clip[i], action)
            dh.check_coldtype(df, ucol_clip[i], action)
            t = ColumnCliper(clip_flag[i], clip_min[i], clip_max[i])
            t.clip_para_val()
            transformers.append(t.clip_transformer(ucol_clip[i]))

    print(transformers)
    column_trans = ColumnTransformer(transformers, remainder="passthrough")
    # print(column_trans.fit_transform(df))
    output_array = pd.DataFrame(column_trans.fit_transform(df))
    # print(output_array[0])
    new_df = df.copy()
    for i in range(0, len(ucol_clip)):
        new_df[ucol_clip[i]] = output_array[i]

    # print(df.isnull().sum(),new_df.isnull().sum())
    print(df, new_df)


# base_clip(filepath = "C:/Users/Karupaiya/Documents/00_MLPractice/cars1.csv", clip = True, ucol_clip = ['Acceleration','Model'],
#       clip_flag = ['percentile',''] ,clip_min = ['',''], clip_max = [60,82])
