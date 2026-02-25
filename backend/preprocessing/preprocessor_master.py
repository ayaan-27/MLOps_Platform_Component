#!/usr/bin/env python
import os
import sys
from os import path
from pickle import dump

sys.path.append(os.getcwd())
from utils.file_io import download_file, read_file

import mlflow
from mlflow.tracking import MlflowClient
import pandas as pd
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline

import preprocessing.dataframe_handler as dh
import utils.logs as logs
import utils.os_utils as custom_utils
from preprocessing.bin_ops import ColumnBin
from preprocessing.cliper import ColumnCliper
from preprocessing.encoder import ColumnEncoder
from preprocessing.imputer import ColumnImputer
from preprocessing.math_ops import ColumnMathOps
from preprocessing.remove_duplicate import removeduplicate_dataframe
from preprocessing.remove_null import removenull_column
from preprocessing.scaler import ColumnScaler


LOGGER = logs.get_logger()

script_path = os.path.realpath(os.path.dirname(__name__))
os.chdir(script_path)
sys.path.append("..")

#mlflow.set_tracking_uri(custom_utils.get_env_variables("TRACKING_URI"))


def pre_processor_controller(data, confs, target:str=None):
    """Master Function for Preprocessing any tabular dataframe based on inputs provided

    Args:
        data (Str): Input csv file provided by user
        pre_process_dict (Dictionary): Dictionary of Columns and Corresponding actions with settings key
                        If action item is removenull, following params to be provided;
                            settings_key (Bool): Dummy variable [optional]
                        If action item is impute, following params to be provided;
                            imp_strategy (Str): Strategy to Impute [Manadatory]
                                        mean,median,most_frequent,constant
                            fill_value (Int/Float): Fill Value for Constant strategy [Optional]
                        If action item is clip, following params to be provided;
                            flag (Str): Method to Clip Data [Mandatory]
                                        percentile,value
                            u_min (Int/Float): Minimum value for Clipping [Optional]
                            u_max (Int/Float): Maximum value for Clipping [Optional]
                        If action item is scale, following params to be provided;
                            scale_method (Str): Method to Scale [Mandatory]
                                        maxabsscaler,minmaxscaler,robustscaler,standardscaler
                        If action item is mathops, following params to be provided;
                            mathops_operation (Str): Mathematical Operation [Mandatory]
                                        log,power,add,sub,mul,div,boxcox,reciprocal,nthroot,exp
                            mathops_value (Int/Float): Value required to support Operation [Optional]
                        If action item is bin_variables, following params to be provided;
                            bin_method (Str): Method for Binning [Mandatory]
                                        equal frequency, equal length, kbins
                            bin_value (Int/Float): Value required to support method [Optional]
                        If action item is encode, following params to be provided;
                            enc_method (Str): Method to Encode [Mandatory]
                                        onehot encoder, ordinal encoder
        removedup (Bool): Flag to signify if Duplicate rows in column are to be removed

    Raises:
        Expection: When file Path provided by the user is invalid
        Exception: When Imputation action is defined but no strategy provided
        Exception: When Clipping action is defined but flag is undefined
        Exception: When Scaling action is defined but no method provided
        Exception: When Mathematical Operations is defined but no operation provided
        Exception: When Bin Operation is defined but no method provided
        Exception: When Encoding action is defined but no method provided
        Exception: When Value Error encountered during FIT/TRANSFORM operation
    """
    try:
        LOGGER.info("Inside pre_processer functions")
        removedup = confs["removedup"]
        pre_process_dict = confs["pre_process_dict"]

        pre_process_df = None
        pre_process_df_target = None
        if data is not None:
            if target is not None:
                pre_process_df_target = data.pop(target)
                pre_process_df_target = pre_process_df_target.to_frame(name = pre_process_df_target.name)
                
            pre_process_df = data

        else:
            LOGGER.exception("Data is None")
            raise Exception("Data is None")

        LOGGER.info("Dataframe ready for preprocessing")

        if removedup:
            LOGGER.info("Removing Duplicate rows")
            LOGGER.info("Remove Duplicate Rows: Requested")
            pre_process_df = removeduplicate_dataframe(pre_process_df)
            LOGGER.info("Remove Duplicate Rows: Processed")

        LOGGER.info("Initializing Transformers with empty list and index for pipeline")
        col_transformer = []
        col_transformer_target = []


        LOGGER.info(" Processing Dictionary and Removing Null in columns as first step for further preprocessing") 
        for col_name in pre_process_dict.keys():

            col_actions = pre_process_dict[col_name]
            for col_action in col_actions:
                for action in col_action:
                    if action.lower() == "removenull":
                        LOGGER.info(
                            "Remove Null for Column [" + col_name + "]: Requested"
                        )
                        if pre_process_df_target is not None & col_name == pre_process_df_target.columns[0]:
                            dh.check_nulldf(pre_process_df_target, col_name, action)
                            pre_process_df_target = removenull_column(pre_process_df_target, col_name)
                        else:
                            dh.check_nulldf(pre_process_df, col_name, action)
                            pre_process_df = removenull_column(pre_process_df, col_name)
                        LOGGER.info(
                            "Remove Null for Column [" + col_name + "]: Processed"
                        )


        LOGGER.info("Processing Dictionary and extracting column name, corresponding actions and pushing actions as transformers for pipeline\
        Iterationg column names (Keys) from dictionary")


        for col_name in pre_process_dict.keys():
            LOGGER.info("Extracting List of action dictionaries (Values) from dictionary")
            col_actions = pre_process_dict[col_name]
            transformers = []
            col_pipeline = None

            LOGGER.info("Initializing Custom Transformer the particular column to convert ndarray to dataframe")

            LOGGER.info("Custom transformer for Column [" + col_name + "]: Initialised")
            cus_trans = dh.ndarrtodf(col_name)

            LOGGER.info("Iterating action dictionary from list of action dictonaries")
            for col_action in col_actions:

                LOGGER.info("Iterating action (Key) from the action dictory")
                for action in col_action:

                    if action.lower() == "impute":
                        LOGGER.info("IMPUTER - Performs impute action")
                        LOGGER.info(action + " for Column [" + col_name + "]: Requested")

                        LOGGER.info("Extracting List of action variable dictionary (Values) from the action dictionary")
                        action_vars = col_action[action]
                        imp_strategy = None
                        fill_value = None

                        LOGGER.info("Iterating action vairables dictionary from list of action variables dictionaries")
                        for var in action_vars:

                            LOGGER.info("Iterating action variable (Key) from the action vairables dictionary")
                            if "imp_strategy" in var.keys():
                                imp_strategy = var["imp_strategy"]
                            if "fill_value" in var.keys():
                                fill_value = var["fill_value"]

                        LOGGER.info("Checks if any strategy/method/operation is defined for the action")
                        if imp_strategy is None:
                            LOGGER.exception("No Imputation strategy is passed by user")
                            raise Exception("Imputation Strategy not defined in Input")

                        LOGGER.info(action + " for Column [" + col_name + "]: Processing")

                        LOGGER.info("Checks if null values are present in column")
                        LOGGER.info(pre_process_df_target.columns[0])
                        LOGGER.info(col_name)
                        LOGGER.info(pre_process_df_target is not None)

                        LOGGER.info((pre_process_df_target is not None) and (col_name == pre_process_df_target.columns[0]))


                        if (pre_process_df_target is not None) and (col_name == pre_process_df_target.columns[0]):
                            dh.check_nulldf(pre_process_df_target, col_name, action)
                        else:
                            LOGGER.info("line - 186")
                            dh.check_nulldf(pre_process_df, col_name, action)

                        t = ColumnImputer(imp_strategy, fill_value)
                        transformers.append(t.impute_transformer(col_name))
                        LOGGER.info(
                            action + " for Column [" + col_name + "]: Transformer Created"
                        )

                        LOGGER.info("Custom Transformer Appended to update ndarray returned by ColumnTransformer to actual dataframe")
                        transformers.append(("cust" + action, cus_trans))

                    if action.lower() == "clip":
                        LOGGER.info("CLIPER - Performs clip action")
                        LOGGER.info(action + " for Column [" + col_name + "]: Requested")

                        action_vars = col_action[action]
                        method = None
                        u_max = None
                        u_min = None

                        for var in action_vars:
                            if "method" in var.keys():
                                method = var["method"]
                            if "u_min" in var.keys():
                                u_min = var["u_min"]
                            if "u_max" in var.keys():
                                u_max = var["u_max"]

                        if method is None:
                            LOGGER.exception("No method for clipping is defined by user")
                            raise Exception("Clip method not defined in Input")
                    
                        LOGGER.info(action + " for Column [" + col_name + "]: Processing")

                        LOGGER.info("Checks if datatype of column is compatible for action")
                        dh.check_coldtype(pre_process_df, col_name, action)

                        LOGGER.info("Instantiates ColumnCliper Class to use the Action Transformer")
                        t = ColumnCliper(method, u_min, u_max)
                        transformers.append(t.clip_transformer(col_name))
                        LOGGER.info(
                            action + " for Column [" + col_name + "]: Transformer Created"
                        )

                    if action.lower() == "scale":
                        LOGGER.info("SCALER - Performs scale action")
                        LOGGER.info(action + " for Column [" + col_name + "]: Requested")

                        action_vars = col_action[action]
                        scale_method = None

                        for var in action_vars:
                            if "scale_method" in var.keys():
                                scale_method = var["scale_method"]

                        if scale_method is None:
                            LOGGER.exception("No Scale Method is passed by user")
                            raise Exception("Scale Method undefined in Input")

                        LOGGER.info(action + " for Column [" + col_name + "]: Processing")

                        LOGGER.info("Checks if datatype of column is compatible for action")
                        dh.check_coldtype(pre_process_df, col_name, action)

                        LOGGER.info("Instantiates ColumnScaler Class to use the Action Transformer")
                        t = ColumnScaler(scale_method)
                        transformers.append(t.scale_transformer(col_name))
                        LOGGER.info(
                            action + " for Column [" + col_name + "]: Transformer Created"
                        )

                        LOGGER.info("Custom Transformer Appended to update ndarray returned by ColumnTransformer to actual dataframe")
                        transformers.append(("cust" + action, cus_trans))
 
                    if action.lower() == "mathops":
                        LOGGER.info("MATHOPS - Performs mathematical operation")
                        LOGGER.info(action + " for Column [" + col_name + "]: Requested")

                        action_vars = col_action[action]
                        mathops_operation = None
                        mathops_value = None

                        for var in action_vars:
                            if "mathops_operation" in var.keys():
                                mathops_operation = var["mathops_operation"]
                            if "mathops_value" in var.keys():
                                mathops_value = var["mathops_value"]

                        if mathops_operation is None:
                            LOGGER.exception("No Operation is passed by user")
                            raise Exception("Mathematical Opeartion undefined in Input")
                        LOGGER.info(action + " for Column [" + col_name + "]: Processing")

                        LOGGER.info("Checks if datatype of column is compatible for action")
                        dh.check_coldtype(pre_process_df, col_name, action)

                        t = ColumnMathOps(mathops_operation, mathops_value)
                        transformers.append(t.mathops_transformer(col_name))
                        LOGGER.info(
                            action + " for Column [" + col_name + "]: Transformer Created"
                        )

                    if action.lower() == "bin_variables":
                        LOGGER.info("BIN VARIABLES - Performs binning variables")
                        LOGGER.info(action + " for Column [" + col_name + "]: Requested")

                        action_vars = col_action[action]
                        bin_method = None
                        bin_value = None

                        for var in action_vars:
                            if "bin_method" in var.keys():
                                bin_method = var["bin_method"]
                            if "bin_value" in var.keys():
                                bin_value = var["bin_value"]

                        if bin_method is None:
                            LOGGER.exception("No Binning Method is passed by user")
                            raise Exception("Binning Method undefined in Input")
                        LOGGER.info(action + " for Column [" + col_name + "]: Processing")

                        t = ColumnBin(bin_method, bin_value)
                        transformers.append(t.bin_transformer(col_name))
                        LOGGER.info(
                            action + " for Column [" + col_name + "]: Transformer Created"
                        )

                    if action.lower() == "encode":
                        LOGGER.info("ENCODE VARIABLES - Performs encoding of variables")
                        LOGGER.info(action + " for Column [" + col_name + "]: Requested")

                        action_vars = col_action[action]
                        enc_method = None

                        for var in action_vars:
                            if "enc_method" in var.keys():
                                enc_method = var["enc_method"]

                        if enc_method is None:
                            LOGGER.exception("No Encode Method is passed by user")
                            raise Exception("Encode Method undefined in Input")
                        LOGGER.info(action + " for Column [" + col_name + "]: Processing")

                        t = ColumnEncoder(enc_method)
                        transformers.append(t.encode_transformer(col_name))
                        LOGGER.info(
                            action + " for Column [" + col_name + "]: Transformer Created"
                        )

            col_pipeline = Pipeline(steps=transformers)
            if pre_process_df_target.columns[0] == col_name:
                col_transformer_target.append((col_name, col_pipeline, [col_name]))
            else:
                col_transformer.append((col_name, col_pipeline, [col_name]))

        LOGGER.info("Transformers constructed")


        LOGGER.info("Creating Pipeline with list of transformers")

        if pre_process_df_target is not None:
            data_transformer_target = ColumnTransformer(
                transformers=col_transformer_target, remainder="passthrough"
            )
            new_data_target = data_transformer_target.fit(pre_process_df_target)
            new_data_target = data_transformer_target.transform(pre_process_df_target)
            new_feature_name_target = dh.get_feature_names(data_transformer_target)

            data_transformer = ColumnTransformer(
            transformers=col_transformer, remainder="passthrough"
            )
            LOGGER.info(
                "Column Transformer Successfully Constructed: " + str(data_transformer)
            )

            LOGGER.info("Performing FIT & TRANSFORM for the provided data set")
            new_data = data_transformer.fit(pre_process_df)
            new_data = data_transformer.transform(pre_process_df)
            new_feature_name = dh.get_feature_names(data_transformer)

            new_df = pd.DataFrame(data=new_data, columns=new_feature_name)
            new_df_t =pd.DataFrame(data=new_data_target, columns=new_feature_name_target)
            new_df = new_df.join(new_df_t)
        

            LOGGER.info("Operation performed successfully on dataset")

            return new_df, data_transformer
        else:
            data_transformer = ColumnTransformer(
            transformers=col_transformer, remainder="passthrough"
            )
            LOGGER.info(
                "Column Transformer Successfully Constructed: " + str(data_transformer)
            )

            LOGGER.info("Performing FIT & TRANSFORM for the provided data set")
            new_data = data_transformer.fit(pre_process_df)
            new_data = data_transformer.transform(pre_process_df)
            new_feature_name = dh.get_feature_names(data_transformer)

            new_df = pd.DataFrame(data=new_data, columns=new_feature_name)

            LOGGER.info("Operation performed successfully on dataset")

            return new_df, data_transformer

    except Exception as e:
        print(e)
        LOGGER.exception(e)
        raise Exception(e)


def preprocess_main(dataset_id: int, version_id: int, pre_process_dict: dict, target:str=None, removedup= False):
    """Main Function to invode Preprocessor Controller with suitable inputs"""
    try:
        LOGGER.info("Inside preprocessor main function")

        LOGGER.info("downloading dataset id: " + str(dataset_id)+ " and version id: "+str(version_id))        
        data = read_file(download_file(dataset_id = dataset_id, version_id = version_id))

        LOGGER.info("creating configuration dictionary")
        confs = {"pre_process_dict": pre_process_dict, "removedup": removedup}

        LOGGER.info("calling pre_processor_controller function")
        preprocessed_data, data_transformer  = pre_processor_controller(data= data, confs=confs, target=target)
        LOGGER.info("Preprocessing completed")

    except Exception as e:
        LOGGER.exception(e)
    finally:
        return preprocessed_data,data_transformer




