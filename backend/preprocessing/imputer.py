#!/usr/bin/env python
# coding: utf-8
from os import path

import pandas as pd
from sklearn.compose import ColumnTransformer
from sklearn.impute import SimpleImputer

import utils.logs as logs
from preprocessing import dataframe_handler as dh

LOGGER = logs.get_logger()


class ColumnImputer:
    def __init__(self, imp_strategy, fill_value):
        """Constructor to initialise Imputing Strategy and Fill value

        Args:
            imp_strategy (Str): Imputing Strategy for imputing the null values
            fill_value (Int/Float): Fill Value for Constant Imputation Strategy
        """
        LOGGER.info(
            "Class ColumnBin Instantiated with, Imputation Strategy:"
            + str(imp_strategy)
            + " Value: "
            + str(fill_value)
        )
        if imp_strategy == "":
            self.imp_strategy = "most_frequent"
        else:
            self.imp_strategy = imp_strategy
        if fill_value == "":
            self.fill_value = "DEFAULT"
        else:
            self.fill_value = fill_value
        self.impute_para_val()

    def impute_para_val(self):
        """Function to validate Imputation Parameters defined by user

        Raises:
            Exception: When Imputation strategy defined by user is not supported by Functionality
        """
        LOGGER.info("Impute Column Parameter Validation: In Progress")
        if self.imp_strategy.lower() not in [
            "mean",
            "median",
            "most_frequent",
            "constant",
        ]:
            LOGGER.error("Impute Column Parameter Validation: Failed")
            raise Exception("Method not defined")
        LOGGER.info("Impute Column Parameter Validation: Validated")

    def impute_preprocessor(self, imp_df, col_name):
        """Function to preprocess and validates imputation parameters with respect to the data set used

        Args:
            imp_df (DataFrame): Input Tabular Dataframe
            col_name ([type]): Name of Column to be Imputed

        Raises:
            Exception: When Invalid strategy is used for Column Datatype
        """
        if imp_df[col_name].dtype != "object" and self.fill_value == "DEFAULT":
            self.fill_value = 10

        if (
            imp_df[col_name].dtype != "object" and not (isinstance(self.fill_value, str))
        ) or (
            imp_df[col_name].dtype == "object"
            and (
                self.imp_strategy.lower() == "most_frequent"
                or self.imp_strategy.lower() == "constant"
            )
            and (isinstance(self.fill_value, str))
        ):
            pass
        else:
            print(
                "Error: impute_preprocessor()\nInvalid Operation performed on Column data type\nColumn Name:",
                col_name,
                " Datatype:",
                imp_df[col_name].dtype,
                "\n",
            )
            raise Exception("Strategy not defined for the column data type")

    def impute_transformer(self, col_name):
        """Function to return tuple with skLearn Imputer and suitable inputs based on impute strategy

        Args:
            col_name (Str): Name of Column to be Imputed

        Returns:
            Tuple: Tuple with object, transformer and column name
        """
        LOGGER.info(
            "Impute Column Transformers created successfully for " + str(col_name)
        )
        return (
            col_name + "_imputer",
            SimpleImputer(
                strategy=self.imp_strategy.lower(),
                fill_value=self.fill_value,
                copy=False,
            ),
        )


def base_impute(
    filepath, impute=True, ucol_impute=[], method_impute=[], impute_fillvalue=[]
):

    # READ THE CSV FILE AND STORE IT AS DATA FRAME
    if filepath:
        if path.isfile(filepath):
            df = pd.read_csv(filepath)
        else:
            raise Exception("File Path Not defined")

    transformers = []

    if impute:
        if len(ucol_impute) == 0:
            raise Exception()
        if len(ucol_impute) != len(method_impute):
            raise Exception()
        for i in range(0, len(ucol_impute)):
            action = "impute"
            print(action, i)
            dh.check_nulldf(df, ucol_impute[i], action)
            t = ColumnImputer(method_impute[i], impute_fillvalue[i])
            t.impute_para_val()
            t.impute_preprocessor(df, ucol_impute[i])
            transformers.append(t.impute_transformer(ucol_impute[i]))

    print(transformers)
    column_trans = ColumnTransformer(transformers, remainder="passthrough")
    # print(column_trans.fit_transform(df))
    output_array = pd.DataFrame(column_trans.fit_transform(df))
    # print(output_array[0])
    new_df = df.copy()
    for i in range(0, len(ucol_impute)):
        # print(len(output_array),new_df[ucol_impute[i]][j],output_array[i][j],i,j)
        new_df[ucol_impute[i]] = output_array[i]

    # pipe1 = pipeline.Pipeline(steps = transformers)
    # SCALE A COLUMN & ENCODE A COLUMN WITH WITH SPECIFIED METHOD
    # print(pipe1)
    # pipe1.fit(df)
    print(df.isnull().sum(), new_df.isnull().sum())
    print(df, new_df)
    # return new_df


# base_impute(filepath = "C:/Users/Karupaiya/Documents/00_MLPractice/cars1.csv",  impute = True,
# ucol_impute = ['MPG','Car'], method_impute =
# ['Mean','Constant'],impute_fillvalue = ['',''])
