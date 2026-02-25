from pickle import load

import mlflow
import pandas as pd

from utils.logs import get_logger

LOGGER = get_logger()


def feature_engineering_pipeline_transform(path):
    """Transforms the data set based on fit parameters.
    Saves the transformed dataset into a csv file.
    'Feature_Engineered.csv'.

    Args:
        path (str): path to the dataset/CSV file
    """

    try:
        # test dataset path
        try:
            if(path is None):
                path = "C:/Users/amrit/Desktop/ENGG TB/Mphasis/PaceML/Feature_Engg/Pace-ML-interns-Feature-Engineering/DataSets/HouseSales.csv"
            # loading test dataset
            data_frame_test = pd.read_csv(path)
            LOGGER.info("Test data set read successfully.")
        except KeyError as e:
            LOGGER.error("Exception raised. Couldn't read test dataset. Error is: {}".format(e))

        # removing target feature
        independent_feature_data_frame = data_frame_test.iloc[:, :-1]

        try:
            with mlflow.start_run():
                # mlflow log parameters
                mlflow.log_param("Path", path)
                # loading fe_pipeline object from pickle file
                fe_pipeline = load(open('fe_pipeline.pkl', 'rb'))
                mlflow.log_param('Fit_Transform Pickle File', 'fe_pipeline.pkl')
                LOGGER.info("Pickled object read successfully.")
                # transform test dataset
                tranformed_dataset = fe_pipeline.transform(independent_feature_data_frame)
                LOGGER.info("Test dataset transformed successfully.")
                final_df = pd.DataFrame(data=tranformed_dataset)
                final_df.to_csv("Feature_Engineered.csv", index=False)
                mlflow.log_artifact('Feature_Engineered.csv')
        except Exception as er:
            LOGGER.error("Exception raised. Error in pickle file or test data transformation. Error is: {}".format(er))
        # display output on local console
        print("After Transform")
        print(final_df.head())
        print(final_df.columns)
        LOGGER.info("Operation performed successfully on test dataset.")

    except Exception as er:
        LOGGER.error("Exception raised. Error in pickle file or test data transformation. Error is: {}".format(er))
