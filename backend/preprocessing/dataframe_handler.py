#!/usr/bin/env python
# coding: utf-8
import numpy as np
import pandas as pd
import sklearn

import utils.logs as logs

LOGGER = logs.get_logger()


def get_coldf(df, col_name):
    """Function to get the column dataframe from the master dataframe

    Args:
        df (DataFrame): Input DataFrame
        col_name (Str): Column Name

    Raises:
        Exception: When Column doesnt not exist in Dataframe

    Returns:
        DataFrame: Column Dataframe
    """
    LOGGER.info("Dataframe Handler - Subset Columndata as Dataframe: In Progress")
    try:
        col_df = df[col_name]
        LOGGER.info("Dataframe Handler - Subset Columndata as Dataframe: Successfuk")
        return col_df
    except Exception as e:
        print(e)
        LOGGER.exception("Dataframe Handler - Subset Columndata as Dataframe: Failed")
        raise Exception("Column name not defined in Dataframe")


def check_nulldf(df, col_name, action):
    """Function to check if null values are present in a column of DataFrame

    Args:
        df (DataFrame): Input DataFrame
        col_name (Str): Column Name
        action (Str): Preprocessing Action performed

    Raises:
        Exception: When Null Values are present in Column Dataframe
    """
    LOGGER.info("Dataframe Handler - Check Null Values: In Progress")
    col_df = get_coldf(df, col_name)
    if (col_df.isnull().values.any() == False) or (action in ["impute", "Remove Null"]):
        LOGGER.info("Dataframe Handler - No Null Values: Successful")
        pass
    else:
        LOGGER.exception(
            "Error: "
            + action
            + "()\nNull Values present in Column: "
            + col_name
            + " {count:"
            + str(col_df.isnull().sum())
            + "}\n"
        )
        raise Exception("Null Value Present")


def check_coldtype(df, col_name, action):
    """Function to validate the column with numeric datatype in DataFrame

    Args:
        df (DataFrame): Input DataFrame
        col_name (Str): Column Name
        action (Str): Preprocessing Action performed

    Raises:
        Exception: When column is of object datatype
    """
    LOGGER.info("Dataframe Handler - Check Datatype of columndata: In Progress")
    col_df = get_coldf(df, col_name)
    if col_df.dtype != "object":
        LOGGER.info("Dataframe Handler - No Object Datatype for columndata: Successful")
        pass
    else:
        LOGGER.exception(
            "Error: "
            + action
            + "()\nUnsupported Column Datatype\nColumn Name: "
            + col_name
            + " {Datatype:"
            + str(col_df.dtype)
            + "}\n"
        )
        raise Exception("Column Datatype not supported")


class ndarrtodf:
    def __init__(self, col_name):
        """Constructor to initialise column name

        Args:
            col_name (Str): Column Name
        """
        LOGGER.info(
            "Dataframe Handler - Ndarray to Dataframe convertor class: Initialised"
        )
        self.col_name = col_name

    def fit(self, X, y=None):
        """Function to FIT the dataframe for transformation

        Args:
            X (DataFrame): Input Dataframe
            y (optional): [description]. Defaults to None.

        Returns:
            Object: Class Object
        """
        LOGGER.info(
            "Dataframe Handler - Ndarray to Dataframe convertor class: Fit Function()"
        )
        return self

    def transform(self, X):
        """Function to transform the input dataframe

        Args:
           X (DataFrame): Input Dataframe

        Returns:
            DataFrame: Retruns updated column values in input dataframe
        """
        # print(self.df,self.col_name)
        # print(self.df[self.col_name].unique())
        # print(np.unique(X))
        # self.df[self.col_name] = X
        # return self.df
        data_frame = pd.DataFrame(X, columns=[self.col_name])
        LOGGER.info(
            "Dataframe Handler - Ndarray to Dataframe convertor class: Successful"
        )
        return data_frame


def get_feature_names(column_transformer):
    """Get feature names from all transformers.
    Returns
    -------
    feature_names : list of strings
        Names of the features produced by transform.
    """
    # Remove the internal helper function
    # check_is_fitted(column_transformer)

    # Turn loopkup into function for better handling with pipeline later
    def get_names(trans):
        # >> Original get_feature_names() method
        if trans == "drop" or (hasattr(column, "__len__") and not len(column)):
            return []
        if trans == "passthrough":
            if hasattr(column_transformer, "_df_columns"):
                if (not isinstance(column, slice)) and all(
                    isinstance(col, str) for col in column
                ):
                    return column
                else:
                    return column_transformer._df_columns[column]
            else:
                indices = np.arange(column_transformer._n_features)
                return ["x%d" % i for i in indices[column]]
        if not hasattr(trans, "get_feature_names"):
            """
             >>> Change: Return input column names if no method avaiable
             Turn error into a warning
            # warnings.warn("Transformer %s (type %s) does not "
            #                     "provide get_feature_names. "
            #                     "Will return input column names if available"
            #                     % (str(name), type(trans).__name__))
            # For transformers without a get_features_names method, use the input
            # names to the column transformer
            """
            if column is None:
                return []
            else:
                return [name + "__" + f for f in column]

        return [name + "__" + f for f in trans.get_feature_names()]

    # Start of processing
    feature_names = []

    # Allow transformers to be pipelines. Pipeline steps are named
    # differently, so preprocessing is needed
    if isinstance(column_transformer, sklearn.pipeline.Pipeline):
        l_transformers = [
            (name, trans, None, None) for step, name, trans in column_transformer._iter()
        ]
    else:
        # For column transformers, follow the original method
        l_transformers = list(column_transformer._iter(fitted=True))

    for name, trans, column, _ in l_transformers:
        if isinstance(trans, sklearn.pipeline.Pipeline):
            # Recursive call on pipeline
            _names = get_feature_names(trans)
            # if pipeline has no transformer that returns names
            if len(_names) == 0:
                # print(name)
                _names = [name]
            feature_names.extend(_names)
        else:
            feature_names.extend(get_names(trans))

    return feature_names
