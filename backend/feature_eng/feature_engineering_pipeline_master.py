import os
from pickle import dump

import mlflow
import pandas as pd
from sklearn.decomposition import PCA
from sklearn.pipeline import Pipeline

from feature_eng.datetime_transformer import DateTimeTransformer
from feature_eng.math_ops_transformer import MathOpsTransformer
from feature_eng.multicollinearity_removal_transformer import (
    MulticollinearityRemovalTransformer,
)
from feature_eng.poly_feature_transformer import PolynomialFeatureTransformer
import utils.file_io as io
from utils.logs import get_logger

LOGGER = get_logger()



def feature_engineering_pipeline_fit_transform(data, user_request):
    """Perform feature engineering.
        Based on user request it create a operation sequence.
        The pipeline then tranforms the data set based the operation requested.
        ML Flow configures the user settings and logs the paramters and artifacts of feature
        engineering pipeline.

    Args:
        user_request (dict): deserialised JSON object.
        Ex:{"multiCollinearity":{"vif_Value": null},
        "PCA":{"n_Components": null},
        "DateTime":{"feature": null},
        "polynomial_Features":{"feature_degree":{[{"feature": "# of Bed","degree": 2},
                                                {"feature": "# of Rooms","degree":3}]}},
        "math_Ops":{"features_Operation":[{"feature":["Col1","Col2"],
                                            "operationType": "ratio"},
                                        {"feature":["# of Bed","# of Rooms"],
                                        "operationType": "addition"}]}}

    Raises:
        KeyError: Invalid path input.
        Exception: Cannot perform DateTime transformation and PCA dimensionality reduction together.
        Exception: Invalid datetime input to transformer.
        Exception: Invalid input to polynomial feature generation.
        Exception: Invalid input to Math Operation request.
        Exception: Invalid input to multicollinearity removal among features.
        Exception: Invalid input to dimensionality reduction PCA.
    """
    try:

        LOGGER.info("Loading the data")
        data_frame = data
        df_feature_list = data_frame.columns.tolist()
        dependent_feature = data_frame.iloc[:,-1]
        independent_feature_data_frame = data_frame.iloc[:, :-1]
        LOGGER.info("Dataset successfully loaded.")

        # request of feature operation
        user_input_keys = user_request.keys()

        operation_names = []
        operation_object = []

        # check date time operation request
        if "DateTime" in user_input_keys:
            LOGGER.info("Date time tranformation requested.")
            if "PCA" in user_input_keys:
                LOGGER.error("Exception raised. Cannot perform date transformation and PCA together.")
                # raising exception. cannot perform these two operation in sequence.
                raise Exception("PCA and Date Time manipulation cannot be performed together. Perform date time transformation first\
                                and then perform dimensionality reduction.")

            try:
                date_time_feature = user_request.get("dateTime").get("feature")
            except Exception as er:
                LOGGER.error("Exception raised. Please check date time featiure input. Error is:{} ".format(er))
                raise Exception("Invalid date time feature input. Error is:{} ")
            # checking datetime input and validating them
            if date_time_feature in df_feature_list:
                # date time operation instance
                dt_tranformer = DateTimeTransformer(date_time_feature)
                # adding instance in list
                operation_names.append("Date_Tranformer")
                operation_object.append(dt_tranformer)
                LOGGER.info("Date time transformer appended in operation sequnce.")
            else:
                LOGGER.error("Exception raised. Invalid input to date time transformation")
                raise Exception("Not a valid request for date time manipulation. Please check feature name")
        # checking polynomial feature generation request
        if "polynomial_Features" in user_input_keys:
            LOGGER.info("Polynomial feature tranformation requested.")
            try:
                # getting inputs for poly feature generation
                poly_feature_input = user_request.get("polynomial_Features").get("feature_degree")
                print(poly_feature_input)
            except Exception:
                LOGGER.error("Exception raised. Please check polynomial featiure generaion input.")
            # input list null check
            if len(poly_feature_input) != 0:
                try:
                    # polyfeature instance
                    poly_transformer = PolynomialFeatureTransformer(poly_feature_input)
                    # appending in operation sequnce
                    operation_names.append("Polynomial_Transformer")
                    operation_object.append(poly_transformer)
                    LOGGER.info("Polynomial feature transformer appended in operation sequnce.")
                except Exception as er:
                    LOGGER.exception("Exception raised. Error in polynomial feature transformer. Error is: {}".format(er))
            else:
                LOGGER.exception("Exception raised. Invalid input to polynomial feature transformation.")
                raise Exception("Not a valid request for polynomial feature generation. Please check input values.")
        # MathOps request check
        if "math_Ops" in user_input_keys:
            LOGGER.info("Mathematical operation on multiple feature requested.")
            try:
                # input to MathOps
                math_ops_input = user_request.get("math_Ops").get("features_Operation")
            except Exception:
                LOGGER.error("Exception raised. Please check mathematical operation input.")
                raise Exception("Invalid input to MathOps transformer.")
            # input null check
            if len(math_ops_input) != 0:
                try:
                    # mathops transformer instance
                    mt_op_transformer = MathOpsTransformer(math_ops_input)
                    # appending instance in operation seq list
                    operation_names.append("Mt_Ops_Tranformer")
                    operation_object.append(mt_op_transformer)
                    LOGGER.info("Math Ops transformer appended in operation sequnce.")
                except Exception:
                    print("Invalid input variable in math operation.")
                    LOGGER.error("Exception raised. Error in Math ops transformer object creation.")

            else:
                LOGGER.error("Exception raised. Invalid input to Math Ops transformation.")
                raise Exception("Not a valid input to perform mathematical operations. Please check input values")
        # multicollinearity request check
        if "multiCollinearity" in user_input_keys:
            LOGGER.info("Multicollinearity removal requested.")
            try:
                # input to multicollinearity removal
                mc_vif_threshold = float(user_request.get("multiCollinearity").get("vif_Value"))
            except Exception as er:
                LOGGER.error("Exception raised. Please check multicollinearity removal input. Error is: {}".format(er))

            # threshold value greater than zero check
            if mc_vif_threshold > 0:
                mc_tranformer = MulticollinearityRemovalTransformer(mc_vif_threshold)
                operation_names.append("Mc_removal_Tranformer")
                operation_object.append(mc_tranformer)
                LOGGER.info("Multicollinearity removal transformer appended in operation sequnce.")
        # pca request check
        if "PCA" in user_input_keys:
            LOGGER.info("Dimensionality reduction PCA requested.")
            try:
                # getting input to pca from user input
                pca_principal_dimensions = int(user_request.get("PCA").get("n_Components"))
            except Exception:
                LOGGER.error("Exception raised. Please check PCA input.")
            if pca_principal_dimensions > 0:
                pca = PCA(n_components=pca_principal_dimensions)
                operation_names.append("PCA")
                operation_object.append(pca)
            else:
                raise Exception("Not a valid input to perform pca. Please check input values")
        
        step_seq = list(zip(operation_names, operation_object))

        fe_pipeline = Pipeline(steps=step_seq)

        LOGGER.info("transforming dataset")
        tranformed_dataset = fe_pipeline.fit_transform(
                independent_feature_data_frame)
        
        LOGGER.info("creating final dataframe")
        final_df = pd.DataFrame(data=tranformed_dataset)

        LOGGER.info("Appening target column")
        final_df[dependent_feature.name] = dependent_feature

        return final_df,fe_pipeline


        # # mlflow to log parameter and artifacts
        # with mlflow.start_run():

        #     # creating list of tuples for pipeline step sequence
        #     step_seq = list(zip(operation_names, operation_object))
        #     mlflow.log_param("Operation sequence", step_seq)
        #     LOGGER.info("Operation sequnce added successfully.")

        #     fe_pipeline = Pipeline(steps=step_seq)
        #     LOGGER.info("Pipeline object created successfully.")

        #     tranformed_dataset = fe_pipeline.fit_transform(
        #         independent_feature_data_frame)
        #     LOGGER.info("Pipeline fit and transformed.")

        #     dump(fe_pipeline, open('fe_pipeline.pkl', 'wb'))
        #     LOGGER.info("Pipeline object dumped in pickle file.")

        #     final_df = pd.DataFrame(data=tranformed_dataset)
        #     # saving newly transformed dataset as csv file
        #     final_df.to_csv("Feature_Engineered.csv", index=False)

        #     # logging artifacts. (Unable to log)
        #     mlflow.log_artifact('fe_pipeline.pkl')
        #     mlflow.log_artifact('Feature_Engineered.csv')

        #     # display on local console
        #     print(final_df.head())
        #     print(final_df.columns)
        #     LOGGER.info("Operations in pipeline completed successfully.")

    except Exception as er:
        LOGGER.error("Error: {}".format(er))

def feature_eng_main(dataset_id:int, version_id:int, feature_eng_dict:int):
    
    try:

        dataset = io.read_file(io.download_file(dataset_id = dataset_id, version_id = version_id))
        final_df,fe_pipeline = feature_engineering_pipeline_fit_transform(dataset,feature_eng_dict)

        return final_df,fe_pipeline

    except Exception as exception:
        LOGGER.error(exception)
        print(exception)
