#!/usr/bin/env python
# coding: utf-8
import math as mt
from os import path

import pandas as pd
from scipy.stats import boxcox
from sklearn.base import BaseEstimator, TransformerMixin
from sklearn.compose import ColumnTransformer

import utils.logs as logs
from preprocessing import dataframe_handler as dh

LOGGER = logs.get_logger()


class ColumnMathOps(BaseEstimator, TransformerMixin):
    def __init__(self, operation, value):
        """Constructor to initialise operation, value to perform mathematical operations

        Args:
            operation (Str): Type of Operation to be performed on the Column
            value (Int/Float): Supporting Value for the operation
        """
        LOGGER.info(
            "Class ColumnBin Instantiated with, Operation:"
            + str(operation)
            + " Value: "
            + str(value)
        )
        if operation == "":
            self.operation = "add"
        else:
            self.operation = operation
        if value == "":
            self.value = 10
        else:
            self.value = value
        self.mathops_para_val()

    def mathops_para_val(self):
        """Function to validate Math Ops Parameters defined by user

        Raises:
            Exception: When Operation defined by user is not supported by Functionality
            Exception: When unsupported Value is provided by user for the corresponding operation
        """
        LOGGER.info("Mathematical Operation Parameter Validation: In Progress")
        if self.operation.lower() not in [
            "log",
            "power",
            "add",
            "sub",
            "mul",
            "div",
            "boxcox",
            "reciprocal",
            "nthroot",
            "exp",
        ]:
            LOGGER.error("Mathematical Operation Parameter Validation: Failed")
            raise Exception(
                "Undefined Operation: Operation defined by user is not supported by Functionality"
            )
        if (
            self.operation.lower()
            in ["log", "power", "add", "sub", "mul", "div", "nthroot"]
        ) and (isinstance(self.value, str)):
            LOGGER.error("Mathematical Operation Parameter Validation: Failed")
            raise Exception("Undefined Value type for Mathematical Operation")
        LOGGER.info("Mathematical Operation Parameter Validation: Validated")

    def mathops_transformer(self, col_name):
        """Function to return tuple with Custom MathOps transformer and suitable inputs

        Args:
            col_name (Str): Name of Column for MathOps

        Returns:
            Tuple: Tuple with object, transformer and column name
        """
        LOGGER.info(
            "Mathematical Operation Transformers created successfully for "
            + str(col_name)
        )
        return (col_name + "_mathops", self)

    def mathops_column(self, mathop_df, operation, value):
        """Function to perform mathematical operation with suitable inputs

        Args:
            mathop_df (DataFrame): Input Dataframe with only required column data for MathOps
            operation (Str): Type of Operation to be performed on the Column
            value (Int/Float): Supporting Value for the operation

        Returns:
            DataFrame: Data frame after performing MathOps
        """
        # print("P2",col_data,col_data.loc[1],type(col_data.loc[1]))
        LOGGER.info(
            "Mathematical Operation Custom Transform Function - Operation:"
            + str(operation)
            + " Value:"
            + str(value)
        )
        col_name = mathop_df.columns.tolist()
        col_data = mathop_df[col_name[0]]
        if operation.lower() == "log":
            col_data = col_data.apply(lambda x: mt.log(x, value))
        if operation.lower() == "power":
            col_data = col_data.apply(lambda x: x ** value)
        if operation.lower() == "add":
            col_data = col_data.apply(lambda x: x + value)
        if operation.lower() == "sub":
            col_data = col_data.apply(lambda x: x - value)
        if operation.lower() == "mul":
            col_data = col_data.apply(lambda x: x * value)
        if operation.lower() == "div":
            col_data = col_data.apply(lambda x: x / value)
        if operation.lower() == "boxcox":
            col_data = boxcox(col_data.to_list())
            col_data = col_data[0]
        if operation.lower() == "reciprocal":
            col_data = col_data.apply(lambda x: 1 / x)
        if operation.lower() == "nthroot":
            col_data = col_data.apply(lambda x: x ** (1 / value))
        if operation.lower() == "exp":
            col_data = col_data.apply(lambda x: mt.e ** x)
        return pd.DataFrame(col_data)

    def fit(self, X, y=None):
        """Function to FIT the dataframe for transformation

        Args:
            X (DataFrame): Input Dataframe
            y (optional): [description]. Defaults to None.

        Returns:
            Object: Class Object
        """
        LOGGER.info("Mathematical Operation Custom Fit Function: In Progress")
        LOGGER.info("Mathematical Operation Custom Fit Function: Completed")
        return self

    def transform(self, X):
        """Function to transform the input dataframe

        Args:
           X (DataFrame): Input Dataframe

        Returns:
            Function: mathops_column function with inputs
        """
        LOGGER.info("Mathematical Operation Custom Transform Function: In Progress")
        return self.mathops_column(X, self.operation, self.value)


def base_mathops(
    filepath, mathops=True, ucol_mathops=[], mathops_operation=[], mathops_value=[]
):

    # READ THE CSV FILE AND STORE IT AS DATA FRAME
    if filepath:
        if path.isfile(filepath):
            df = pd.read_csv(filepath)
        else:
            raise Exception("File Path Not defined")

    transformers = []

    if mathops:
        if len(ucol_mathops) == 0:
            raise Exception()
        for i in range(0, len(ucol_mathops)):
            action = "MathOps"
            print(action, i)
            dh.check_coldtype(df, ucol_mathops[i], action)
            t = ColumnMathOps(mathops_operation[i], mathops_value[i])
            t.mathops_para_val()
            transformers.append(t.mathops_transformer(ucol_mathops[i]))

    print(transformers)
    column_trans = ColumnTransformer(transformers, remainder="passthrough")
    # print(column_trans.fit_transform(df))
    output_array = pd.DataFrame(column_trans.fit_transform(df))
    # print(output_array[0])
    new_df = df.copy()
    for i in range(0, len(ucol_mathops)):
        new_df[ucol_mathops[i]] = output_array[i]

    # print(df.isnull().sum(),new_df.isnull().sum())
    print(df, new_df)


# base_mathops(filepath = "C:/Users/Karupaiya/Documents/00_MLPractice/cars1.csv", mathops = True, ucol_mathops = ['Cylinders','Model'],
#         mathops_operation = ['log','mul'] ,mathops_value = [4,''])
