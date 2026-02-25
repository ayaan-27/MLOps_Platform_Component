#!/usr/bin/env python
# coding: utf-8
from os import path

import pandas as pd
from sklearn import preprocessing
from sklearn.compose import ColumnTransformer

import utils.logs as logs
from preprocessing import dataframe_handler as dh

LOGGER = logs.get_logger()


class ColumnEncoder:
    def __init__(self, enc_method):
        """Constructor to initialise method for Encoding

        Args:
            enc_method (Str): Method for Encoding
        """
        LOGGER.info(
            "Class ColumnScaler Instantiated with, Encode Method:" + str(enc_method)
        )
        if enc_method == "":
            self.enc_method = "ordinal encoder"
        else:
            self.enc_method = enc_method
        self.encode_para_val()

    def encode_para_val(self):
        """Function to validate Encoding Parameters defined by user

        Raises:
            Exception: When Method defined by user is not supported by Functionality
        """
        LOGGER.info("Encode Column Parameter Validation: In Progress")
        if self.enc_method.lower() not in ["onehot encoder", "ordinal encoder"]:
            LOGGER.error("Encode Column Parameter Validation: Failed")
            raise Exception("Method not defined")
        LOGGER.info("Encode Column Parameter Validation: Validated")

    def encode_transformer(self, col_name):
        """Function to return tuple with skLearn Encoder and suitable inputs based on encode method

        Args:
            col_name (Str): Name of Column to be Encode

        Returns:
            Tuple: Tuple with object, transformer and column name
        """
        LOGGER.info(
            "Encode Column Transformers created successfully for " + str(col_name)
        )
        if self.enc_method.lower() == "ordinal encoder":
            # print('ordinal encoder')
            return ("Trans_" + col_name, preprocessing.OrdinalEncoder(dtype=int))
        elif self.enc_method.lower() == "onehot encoder":
            # print('onehot encoder')
            return (
                "Trans_" + col_name,
                preprocessing.OneHotEncoder(handle_unknown="ignore", sparse=False),
            )


def base_encode(filepath, encode=True, ucol_encode=[], method_encode=[]):

    # READ THE CSV FILE AND STORE IT AS DATA FRAME
    if filepath:
        if path.isfile(filepath):
            df = pd.read_csv(filepath)
        else:
            raise Exception("File Path Not defined")

    transformers = []

    if encode:
        if len(ucol_encode) == 0:
            raise Exception()
        if len(ucol_encode) != len(method_encode):
            raise Exception()
        for i in range(0, len(ucol_encode)):
            action = "Encode"
            print(action, i)
            dh.check_nulldf(df, ucol_encode[i], action)
            t = ColumnEncoder(method_encode[i])
            t.encode_para_val()
            transformers.append(t.encode_transformer(ucol_encode[i]))

    print(transformers)
    column_trans = ColumnTransformer(transformers, remainder="passthrough")
    # print(column_trans.fit_transform(df))
    output_array = pd.DataFrame(column_trans.fit_transform(df))
    # print(output_array[0])
    new_df = df.copy()
    for i in range(0, len(ucol_encode)):
        new_df[ucol_encode[i]] = output_array[i]

    # print(df.isnull().sum(),new_df.isnull().sum())
    print(df, new_df)


# base_encode(filepath = "C:/Users/Karupaiya/Documents/00_MLPractice/cars1.csv",  encode = True,
# ucol_encode = ['Cylinders','Model'], method_encode = ['onehot
# encoder','ordinal encoder'])
