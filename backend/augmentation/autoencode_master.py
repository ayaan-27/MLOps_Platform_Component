import os,sys
# from datetime import datetime
sys.path.append(os.getcwd())

from utils.file_io import download_file, read_file, save_pickle
from datasets.datasets import create_version
import mlflow
import mlprepare as mlp
import pandas as pd
import torch
from utils.logs import get_logger
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import LabelEncoder
from torch.utils.data import DataLoader, Dataset

import augmentation.autoencoder as autoencode

# TRACKING_URL = "http://localhost:5000"
# mlflow.set_tracking_uri(TRACKING_URL)
# mlflow.set_experiment("Autoencoder Temp")
# client = mlflow.tracking.MlflowClient(TRACKING_URL)

LOGGER = get_logger()

class DataBuilder(Dataset):
    def __init__(self, X_train, X_test, class_val, target="Class", train=True):
        """
            Databuilder for train set and test set

        Args:
            X_train : pandas dataframe
                Training set

            X_test : pandas dataframe
                Test set

            class_val : int
                Target class value

            target : str
                Target column part of the dataset.Defaults to 'Class'.

            train : bool
                Defaults to 'True' - indicates train set.
                'False' indicates test set.
        """
        self.X_train, self.X_test = X_train, X_test
        if train:
            self.X_train[target] = class_val
            self.x = torch.from_numpy(self.X_train.values).type(torch.FloatTensor)
            self.len = self.x.shape[0]
        else:
            self.X_test[target] = class_val
            self.x = torch.from_numpy(self.X_test.values).type(torch.FloatTensor)
            self.len = self.x.shape[0]
        del self.X_train
        del self.X_test

    def __getitem__(self, index):
        """
            Returns the object present at a given index.

        Args:
            index : int
                Index of the object.

        Returns:
            self.x : object
                Object is returned
        """
        return self.x[index]

    def __len__(self):
        """
        Returns length of object
        """
        return self.len


def diff_in_count(dataset, target="Class"):
    """
        Function to find the difference (number of instances) between minority
        classes and the majority class

    Args:
        dataset : pandas dataframe
            Contains the dataset

        target : str, optional
            The target column/label in the dataset.
            Defaults to 'Class'.

    Returns:
        diff_each_class : dict
            A dictionary containing difference (number of instances)
            between minority classes and the majority class
    """
    diff_each_class = {}
    max_val = max(dataset[target].value_counts())
    for i, value in dataset[target].value_counts().items():
        diff_each_class[i] = max_val - value
    return diff_each_class


def augmentation_classification(
    dataset,
    epochs=1400,
    target="Class",
    categorical_problem=False,
    categorical_type=[],
    continuous_type=[],
):
    """
        Classification Augmentation.

    Args:
        dataset : dataframe.

        epoch : int, optional
            Number of epochs.
            Defaults to 1000.

        target : str, optional
            The target column in the dataset.
            Defaults to 'Class'.

        categorical_problem : bool, optional
            'True' if it is a categorical dataset.
            Defaults to False.

        categorical_type : list, optional
            If categorical problem is 'True', then names of the
            categorical columns must be passed as a list.

        continuous_type : list, optional
            If categorical problem is 'True', then names of continuous
            columns must be passed as a list.

    Returns:
        train_dataframe : pandas dataframe
            Augmented dataset is returned.

        settings : dictionary
            JSON Object containing information about what was performed.
    """
    try:

        device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
        
        if target not in dataset.columns:
            LOGGER.error(
                "The target value you specified is not part of \
                        the dataset"
            )
            raise Exception("ERROR : Target column mismatch error")

        # Removing the target column and adding it as the final column in
        # the dataset
        col = dataset.pop(target)
        dataset[target] = col

        # If the target is also a categorical column, it needs to be encoded
        # in the beginning
        flag = 0
        if categorical_problem is True and (target in categorical_type):
            LOGGER.info("Categorical problem with target not encoded")
            flag = 1
            le = LabelEncoder()
            dataset[target] = le.fit_transform(dataset[target])
            categorical_type.remove(target)
        elif categorical_problem is True:
            LOGGER.info("Categorical problem with target encoded")

        cols = dataset.columns
        train_dataframe = dataset

        # Calculating the difference in number of instances between
        # the minority column and majority column
        # Result is a dictionary with key as class and value
        # as difference
        diff_count_each_class = diff_in_count(dataset, target)
        LOGGER.info(diff_count_each_class)

        for key, value in diff_count_each_class.items():
            if value > 0:
                if categorical_problem is True and (
                    len(categorical_type) > 0 and len(continuous_type) > 0
                ):
                    df = mlp.df_to_type(
                        dataset, cont_type=continuous_type, cat_type=categorical_type
                    )
                    train_df, test_df = train_test_split(
                        df[df[target] == key], test_size=0.2, random_state=42
                    )
                    train_df, test_df, dict_list, dict_inv_list = mlp.cat_transform(
                        train_df, test_df, cat_type=categorical_type
                    )
                else:                   
                    train_df, test_df = train_test_split(
                        dataset[dataset[target] == key], test_size=0.2, random_state=42
                    )

                (
                    X_train_fake,
                    X_test_fake,
                    y_train_fake,
                    y_test_fake,
                    scaler_fake_data,
                ) = mlp.cont_standardize(
                    train_df.iloc[:, :-1],
                    test_df.iloc[:, :-1],
                    train_df.iloc[:, -1],
                    test_df.iloc[:, -1],
                    cat_type=None,
                    transform_y=False,
                    standardizer="StandardScaler",
                )

                # Creating training and testt set for the corrosponding class             
                traindata_set = DataBuilder(X_train_fake, X_test_fake, key, target, train=True)
                testdata_set = DataBuilder(X_train_fake, X_test_fake, key, target, train=False)

                trainloader = DataLoader(dataset=traindata_set, batch_size=1024)
                testloader = DataLoader(dataset=testdata_set, batch_size=1024)

                D_in = traindata_set.x.shape[1]
                H = 50
                H2 = 12

                autoenc_model = autoencode.AutoencoderModel(
                    trainloader, testloader, device, D_in, H, H2, latent_dim=5
                )
                autoenc_model_fit = autoenc_model.fit(epochs=epochs)

                cols_fake = cols.to_list()
                cols_fake.remove(target)

                # Fake data is generated
                df_fake_with_noise = autoenc_model_fit.predict_with_noise_df(
                    no_samples=diff_count_each_class[key],
                    cols=cols,
                    mu=0,
                    sigma=0.05,
                    scaler=scaler_fake_data,
                    cont_vars=cols_fake,
                )
                df_fake_with_noise[target] = key

                # If categorical columns were present initially, performing
                # inverse mapping on them
                if categorical_problem is True and (
                    len(categorical_type) > 0 and len(continuous_type) > 0
                ):
                    df_fake_with_noise = df_fake_with_noise.astype(int)
                    for i, col in enumerate(categorical_type):
                        df_fake_with_noise[col] = df_fake_with_noise[col].map(dict_list[i])

                LOGGER.info("Fake data generated")
                # Merge fake data with the original dataset
                train_dataframe = train_dataframe.append(df_fake_with_noise)

        # If the target variable had been encoded initially (in line 53),
        # inverse transforming it here
        if categorical_problem is True and flag == 1:
            train_dataframe[target] = le.inverse_transform(train_dataframe[target])

        # settings = {
        #     # "Path": path,
        #     "Problem Type": "Classification",
        #     "Target Column": target,
        #     "Categorical Problem": categorical_problem,
        #     "Categorical Columns": categorical_type,
        #     "Continuous Columns": continuous_type,
        # }

        LOGGER.info("Augmentation over")
        return train_dataframe

    except Exception as exception:
        LOGGER.error(exception)
        print(exception)


def augmentation_regression(
    dataset,
    epochs=1000,
    no_of_samples=200,
    target="Class",
    categorical_problem=False,
    categorical_type=[],
    continuous_type=[],
):
    """
        Regression Augmentation.

    Args:
        path : str
            A valid path to the dataset.

        epoch : int, optional
            Number of epochs.
            Defaults to 1000.

        no_of_samples : int, optional
            Number of fake data instances to be generated.
            Defaults to 200.

        target : str, optional
            The target column in the dataset.
            Defaults to 'Class'.

        categorical_problem : bool, optional
            'True' if it is a categorical dataset.
            Defaults to False.

        categorical_type : list, optional
            If categorical problem is 'True', then names of the
            categorical columns must be passed as a list.

        continuous_type : list, optional
            If categorical problem is 'True', then names of continuous
            columns must be passed as a list.

    Returns:
        train_dataframe : pandas dataframe
            Augmented dataset is returned.

        settings : dictionary
            JSON Object containing information about what was performed.
    """
    try:

        device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
            

        dataset[target] = 1

        cols = dataset.columns
        train_dataframe = dataset

        # Checking if categorical problem or not and encoding the dataset
        # based on that
        if categorical_problem is True and (len(categorical_type) > 0 and len(continuous_type) > 0):
            df = mlp.df_to_type(dataset, cont_type=continuous_type, cat_type=categorical_type)
            train_df, test_df = train_test_split(
                df[df[target] == 1], test_size=0.2, random_state=42
            )
            # Encode the categorical columns
            train_df, test_df, dict_list, dict_inv_list = mlp.cat_transform(
                train_df, test_df, cat_type=categorical_type
            )
        else:
            # If the dataset has no categorical columns, then split accordingly
            train_df, test_df = train_test_split(
                dataset[dataset[target] == 1], test_size=0.2, random_state=42
            )
        # Performing standardisation.
        (
            X_train_fake,
            X_test_fake,
            y_train_fake,
            y_test_fake,
            scaler_fake_data,
        ) = mlp.cont_standardize(
            train_df.iloc[:, :-1],
            test_df.iloc[:, :-1],
            train_df.iloc[:, -1],
            test_df.iloc[:, -1],
            cat_type=None,
            transform_y=False,
            standardizer="StandardScaler",
        )

        # Creating training and test set for the corrosponding class
        traindata_set = DataBuilder(X_train_fake, X_test_fake, 1, target, train=True)
        testdata_set = DataBuilder(X_train_fake, X_test_fake, 1, target, train=False)

        # Prepare the dataloader
        trainloader = DataLoader(dataset=traindata_set, batch_size=1024)
        testloader = DataLoader(dataset=testdata_set, batch_size=1024)

        D_in = traindata_set.x.shape[1]
        H = 50
        H2 = 12

        autoenc_model = autoencode.AutoencoderModel(
            trainloader, testloader, device, D_in, H, H2, latent_dim=5
        )

        # Training the autoencoder
        autoenc_model_fit = autoenc_model.fit(epochs=epochs)

        # Removing the target class before generating new data
        cols_fake = cols.to_list()
        cols_fake.remove(target)

        LOGGER.info("Reached fake data generation")
        # Fake data is generated here
        df_fake_with_noise = autoenc_model_fit.predict_with_noise_df(
            no_samples=no_of_samples,
            cols=cols,
            mu=0,
            sigma=0.05,
            scaler=scaler_fake_data,
            cont_vars=cols_fake,
        )
        # Since it is a regression problem, adding a column with class value as 1
        df_fake_with_noise[target] = 1

        LOGGER.info("Fake data successfully generated")

        # If categorical columns were present initially,
        # performing inverse mapping on them
        if categorical_problem is True and (len(categorical_type) > 0 and len(continuous_type) > 0):
            df_fake_with_noise = df_fake_with_noise.astype(int)
            for i, col in enumerate(categorical_type):
                df_fake_with_noise[col] = df_fake_with_noise[col].map(dict_list[i])

        dataset.drop(target, axis=1, inplace=True)
        df_fake_with_noise.drop(target, axis=1, inplace=True)

        # Logging the mean values across columns in the original dataset
        LOGGER.info("Original dataset distribution")
        LOGGER.info(dataset.describe().loc["mean"])

        # Logging the mean values across columns in the fake dataset
        LOGGER.info("Fake data distribution")
        LOGGER.info(df_fake_with_noise.describe().loc["mean"])

        # Logging the original dataset and fake dataset's shape
        LOGGER.info(dataset.shape)
        LOGGER.info(df_fake_with_noise.shape)

        # Merge fake data with the original dataset
        train_dataframe = train_dataframe.append(df_fake_with_noise)

        # settings = {
        #     # "Path": path,
        #     "Problem Type": "Regression",
        #     "Number of new samples generated": no_of_samples,
        #     "Categorical Problem": categorical_problem,
        #     "Categorical Columns": categorical_type,
        #     "Continuous Columns": continuous_type,
        # }

        LOGGER.info("Augmentation over")
        return train_dataframe

    except Exception as exception:
        LOGGER.error(exception)
        print(exception)


def autoencode_main(
    dataset_id:int,
    version_id:int,
    epochs=1400,
    target="Class",
    no_of_samples=200,
    categorical_problem=False,
    categorical_type=[],
    continuous_type=[],
):
    """
        Autoencoders based augmentation.

    Args:

        epoch : int, optional
            Number of epochs.
            Defaults to 1000.

        no_of_samples : int, optional
            Number of fake data instances to be generated.
            Defaults to 200.

        target : str, optional
            The target column in the dataset.
            Defaults to 'Class'.

        categorical_problem : bool, optional
            'True' if it is a categorical dataset.
            Defaults to False.

        categorical_type : list, optional
            If categorical problem is 'True', then names of the
            categorical columns must be passed as a list.

        continuous_type : list, optional
            If categorical problem is 'True', then names of continuous
            columns must be passed as a list.
    """
    try:
        
        dataset = read_file(download_file(dataset_id = dataset_id, version_id = version_id))


        No_of_keys = len(dataset[target].value_counts().keys())
        if type(dataset[target]) == object:
            prob = "Classification"
        elif No_of_keys > (dataset.shape[0] * 0.05):
            prob = "Regression"
        else:
            prob = "Classification"

        LOGGER.info(prob)
        if prob == "Regression":
            augmented_dataset = augmentation_regression(
                dataset,
                epochs,
                no_of_samples,
                "Class",  # We create a target class in case of regression.
                categorical_problem,
                categorical_type,
                continuous_type,
            )
            LOGGER.info(augmented_dataset.shape)
            LOGGER.info(augmented_dataset.describe().loc["mean"])
        else:
            augmented_dataset = augmentation_classification(
                dataset, epochs, target, categorical_problem, categorical_type, continuous_type
            )
            
            LOGGER.info(augmented_dataset[target].value_counts()) 
        print(augmented_dataset)
        return augmented_dataset  
    except Exception as exception:
        print(exception)
    # finally:
    #     return augmented_dataset

    


# if __name__ == "__main__":

#     startTime = datetime.now()
#     # main(path="Datasets\house.csv", epochs=1400, target="target")
    # main(
    #     path="Autoencoder/Final/Bank.csv",
    #     epochs=1400,
    #     target="N_CASA_MAX_BALANCE_MTD_tag",
    #     categorical_problem=True,
    #     categorical_type=[
    #         "ACT_TYPE",
    #         "GENDER",
    #         "TOP_CORP_TAG",
    #         "CREDIT_ACT",
    #         "DEBIT_ACT",
    #         "N_CASA_MAX_BALANCE_MTD_tag",
    #         "N_CASA_MIN_BALANCE_MTD_tag"
    #     ],
    #     continuous_type=[
    #         "CUSTOMER_ID",
    #         "AGE",
    #         "N_CASA_MAX_BALANCE_MTD",
    #         "N_CASA_MIN_BALANCE_MTD",
    #         "CRED_NEED_SCORE",
    #         "SAL_MON_01",
    #         "SAL_MON_02",
    #         "SAL_MON_03"

    #     ],
    # )
#     LOGGER.info(datetime.now() - startTime)

