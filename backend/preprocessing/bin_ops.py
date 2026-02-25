#!/usr/bin/env python
# coding: utf-8

from math import ceil
from os import path

import pandas as pd
from sklearn import preprocessing
from sklearn.base import BaseEstimator, TransformerMixin
from sklearn.compose import ColumnTransformer

# INCLUDED
import utils.logs as logs

LOGGER = logs.get_logger()


class ColumnBin(BaseEstimator, TransformerMixin):

    no_bins = est = 0

    def __init__(self, bin_method, value):
        """Constructor to initialise method, value for binning operations

        Args:
            bin_method (Str): Bin Method defined by user to be performed on the Column
            value (Int/Float): Supporting Value for the binning
        """
        LOGGER.info(
            "Class ColumnBin Instantiated with, Bin Method:"
            + str(bin_method)
            + " Bin Value: "
            + str(value)
        )
        if bin_method == "":
            self.bin_method = "kbins"
        else:
            self.bin_method = bin_method
        if value == "":
            self.value = 5
        else:
            self.value = value
        self.bin_para_val()

    def bin_para_val(self):
        """Function to validate Math Ops Parameters defined by user

        Raises:
            Exception: When Method defined by user is not supported by Functionality
            Exception: When unsupported Value is provided by user for the corresponding operation
        """
        LOGGER.info("Binning Operation Parameter Validation: In Progress")
        if self.bin_method.lower() not in ["equal frequency", "equal length", "kbins"]:
            LOGGER.error("Binning Operation Parameter Validation: Failed")
            raise Exception(
                "Undefined method: Method defined by user is not supported by Functionality"
            )
        if isinstance(self.value, str):
            LOGGER.error("Binning Operation Parameter Validation: Failed")
            raise Exception("Undefined Value type for Bin Operation")
        LOGGER.info("Binning Operation Parameter Validation: Validated")

    def bin_transformer(self, col_name):
        """Function to return tuple with Custom bin transformer and suitable inputs

        Args:
            col_name (Str): Name of Column for Binning

        Returns:
            Tuple: Tuple with object, transformer and column name
        """
        LOGGER.info(
            "Binning Operation Transformers created successfully for " + str(col_name)
        )
        return (col_name + "_binning", self)

    def bin_column(self, bin_df, bin_method, value):
        """Function to perform mathematical operation with suitable inputs

        Args:
            bin_df (DataFrame): Input Dataframe with only required column data for binning
            bin_method (Str): Bin Method defined by user to be performed on the Column
            value (Int/Float): Supporting Value for the binning

        Returns:
            DataFrame: Data frame after performing MathOps
        """
        col_name = bin_df.columns.tolist()
        col_data = bin_df[col_name[0]]
        if bin_method.lower() == "equal frequency":
            # Sort Column Values from Smallest to Largest
            sort_col = pd.DataFrame(col_data.sort_values())
            # Resets index of the data frame
            sort_col.reset_index(inplace=True)
            # Calculates no of bins required for defined frequency
            l_col = len(sort_col)
            no_Bins = l_col / value
            LOGGER.info(
                "Binning Operation Custom Transform Function - Bin Method: "
                + str(bin_method)
                + " Bins: "
                + str(no_Bins)
            )
            # Labels the corresponding item in column starting from 0 and updates
            # based on frequnecy
            label = 0
            for i in range(0, l_col):
                if i % value == 0 and i != 0:
                    label = label + 1
                sort_col.loc[i, col_name[0]] = label
            # Resets the index with the old index
            sort_col.set_index("index", inplace=True)
            # Sorts based on Index
            sort_col.sort_index(inplace=True)
            # print(sortCol.value_counts())
            # Returns Dataframe with Labeled Binned Column
            return pd.DataFrame(sort_col)
        elif bin_method.lower() == "equal length" or bin_method.lower() == "kbins":
            # Applies Transform function for binning
            LOGGER.info(
                "Binning Operation Custom Transform Function - Bin Method: "
                + str(bin_method)
                + " Bins: "
                + str(self.no_Bins)
            )
            bin_v = self.est.transform(bin_df[col_name[0]].to_numpy().reshape(-1, 1))
            return pd.DataFrame(bin_v)

    def fit(self, X, y=None):
        """Function to FIT the dataframe for transformation and finalise number of bins

        Args:
            X (DataFrame): Input Dataframe
            y (optional): [description]. Defaults to None.

        Returns:
            Object: Class Object
        """
        # print(X)
        LOGGER.info("Binning Operation Custom Fit Function: In Progress")
        col_name = X.columns.tolist()
        col_data = X[col_name[0]]
        if self.bin_method.lower() == "equal frequency":
            pass
        elif self.bin_method.lower() == "equal length":
            # Calculates no of bins based on Minimum and Maximum Value of the column
            try:
                mini = col_data.min()
                maxi = col_data.max()
                self.no_Bins = (maxi - mini) / self.value
                # Passes Inputs to transformer for binning
                self.est = preprocessing.KBinsDiscretizer(
                    n_bins=ceil(self.no_Bins), encode="ordinal", strategy="uniform"
                )
                self.est.fit(X[col_name[0]].to_numpy().reshape(-1, 1))
            except TypeError as e:
                LOGGER.error("Binning Operation Custom Fit Function: Failed")
                raise Exception(e)
        elif self.bin_method.lower() == "kbins":
            try:
                self.no_Bins = self.value
                # Passes Inputs to transformer for binning
                self.est = preprocessing.KBinsDiscretizer(
                    n_bins=ceil(self.no_Bins), encode="ordinal", strategy="uniform"
                )
                self.est.fit(X[col_name[0]].to_numpy().reshape(-1, 1))
            except TypeError as e:
                LOGGER.error("Binning Operation Custom Fit Function: Failed")
                raise Exception(e)
        LOGGER.info("Binning Operation Custom Fit Function: Completed")
        return self

    def transform(self, X):
        """Function to transform the input dataframe

        Args:
           X (DataFrame): Input Dataframe

        Returns:
            Function: bin_column function with inputs
        """
        LOGGER.info("Binning Operation Custom Transform Function: In Progress")
        return self.bin_column(X, self.bin_method, self.value)


def base_bin(filepath, binv=True, ucol_binv=[], binv_method=[], binv_value=[]):

    # READ THE CSV FILE AND STORE IT AS DATA FRAME
    if filepath:
        if path.isfile(filepath):
            df = pd.read_csv(filepath)
        else:
            raise Exception("File Path Not defined")

    transformers = []

    if binv:
        if len(ucol_binv) == 0:
            raise Exception()
        # if len(ucol_binv) == len(binv_method):
        #   raise Exception()
        for i in range(0, len(ucol_binv)):
            action = "Binning"
            print(action, i)
            t = ColumnBin(binv_method[i], binv_value[i])
            t.bin_para_val()
            transformers.append(t.bin_transformer(ucol_binv[i]))

    print(transformers)
    column_trans = ColumnTransformer(transformers, remainder="passthrough")
    # print(column_trans.fit_transform(df))
    output_array = pd.DataFrame(column_trans.fit_transform(df))
    # print(output_array[0])
    new_df = df.copy()
    for i in range(0, len(ucol_binv)):
        new_df[ucol_binv[i]] = output_array[i]

    # print(df.isnull().sum(),new_df.isnull().sum())
    print(df, new_df)


# base_bin(filepath = "C:/Users/Karupaiya/Documents/00_MLPractice/cars1.csv", binv = True, ucol_binv = ['Car','Model'],
#        binv_method = ['equal frequency','equal length'] ,binv_value = [10,''])
