import sys,os,shutil
import pandas as pd
import s3fs
from pickle import dump
import confs.config as conf
import utils.logs as logs
import profiling.df_profile as df_profile

LOGGER = logs.get_logger()
bucket_name = conf.read_config(section="dataset")["bucketname"]
folder_name = conf.read_config(section="dataset")["foldername"]
folder_name_leaderboard = conf.read_config(section="leaderboard")["foldername"]
folder_model = conf.read_config(section="model")["foldername"]
subfolder_model = conf.read_config(section="model")["subfolder"]
subfolder_model_2 = conf.read_config(section="model")["subfolder_2"]

S3_LOCATION = os.path.join(bucket_name, folder_name)
S3_LOCATION_LEADERBOARD = os.path.join(bucket_name, folder_name_leaderboard)
S3_LOCATION_MODEL = os.path.join(bucket_name, folder_model, subfolder_model)
fs = s3fs.S3FileSystem()


def save_file(dataset: pd.DataFrame, dataset_id: int, version_id: int) -> str:
    try:
        s3_location = os.path.join(S3_LOCATION, str(dataset_id), str(version_id))
        os.makedirs(s3_location, exist_ok=True)
        s3_location = os.path.join(s3_location, "dataset.csv")
        dataset.to_csv(s3_location, index=False)
        fs.put(s3_location, s3_location)
        return s3_location
    except Exception as e:
        LOGGER.exception(e)
        raise e


def save_leaderboard(leaderboard: pd.DataFrame, job_id: int) -> str:
    try:
        s3_location = os.path.join(S3_LOCATION_LEADERBOARD, str(job_id))
        os.makedirs(s3_location, exist_ok=True)
        s3_location = os.path.join(s3_location, "leaderboard.csv")
        leaderboard.to_csv(s3_location, index=False)
        fs.put(s3_location, s3_location)
        return s3_location
    except Exception as e:
        LOGGER.exception(e)
        raise e


def save_profile(dataset: pd.DataFrame, dataset_id: int, version_id: int) -> str:
    try:
        s3_location = os.path.join(S3_LOCATION, str(dataset_id), str(version_id))
        os.makedirs(s3_location, exist_ok=True)
        result = df_profile.data_profiling(dataset)
        if result["status"] != 1:
            LOGGER.info("Error during Data Profiling")
            return result        
        else:
            s3_location = os.path.join(s3_location, "dataset_profile.html")
            shutil.copy("dataset_profile.html",s3_location)
            fs.put(s3_location, s3_location)
            fs.chmod(s3_location,acl = 'public-read-write')
            LOGGER.info("Data Profile Upload Successful")
            return {"status":1, "msg":s3_location}
    except Exception as e:
        LOGGER.exception(e)
        raise e

def save_pickle(transformer, dataset_id: int, version_id: int, pickle_type: str = "preprocessor_pickle.pkl") -> str:
    try:
        s3_location = os.path.join(S3_LOCATION, str(dataset_id), str(version_id))
        os.makedirs(s3_location, exist_ok=True)
        s3_location = os.path.join(s3_location, str(pickle_type))
        with open(s3_location,"wb") as f:
            dump(transformer, f)
        fs.put(s3_location,s3_location)
        return s3_location

    except Exception as e:
        LOGGER.exception(e)
        raise e

def download_file(dataset_id: int, version_id: int, local_loc: str = None) -> str:
    try:
        s3_location = os.path.join(
            S3_LOCATION, str(dataset_id), str(version_id), "dataset.csv"
        )
        if local_loc is None:
            local_loc = s3_location
        if not os.path.isfile(local_loc):
            fs.get(s3_location, local_loc)
        return local_loc
    except Exception as e:
        LOGGER.exception(e)
        raise e


def download_leaderboard(job_id: int, local_loc: str = None) -> str:
    try:
        s3_location = os.path.join(
            S3_LOCATION_LEADERBOARD, str(job_id), "leaderboard.csv"
        )
        if local_loc is None:
            local_loc = s3_location
        if not os.path.isfile(local_loc):
            fs.get(s3_location, local_loc)
        return local_loc
    except Exception as e:
        LOGGER.exception(e)
        raise e


def read_file(
    dataset_loc: str,
    nrows: int = None,
    skiprows: int = None,
    header: bool = True,
    sep=",",
    encoding: str = "utf-8",
) -> pd.DataFrame:
    try:
        if header:
            header = 0
        else:
            header = None
        data = pd.read_csv(
            dataset_loc,
            sep=sep,
            header=header,
            nrows=nrows,
            encoding=encoding,
            skiprows=skiprows,
        )
        return data
    except Exception as e:
        LOGGER.exception(e)

def download_ds_eng(dataset_id: int, version_id: int, model_id: int, model_version_id:int, file_type:str) -> str:
    try:

        if "preprocessor_pickle" in file_type:
            s3_location = os.path.join(S3_LOCATION, str(dataset_id), str(version_id),"preprocessor_pickle.pkl")
        elif "fe_eng_pipeline" in file_type:
            s3_location = os.path.join(S3_LOCATION, str(dataset_id), str(version_id),"fe_eng_pipeline.pkl")
        else:
            s3_location = os.path.join(S3_LOCATION, str(dataset_id), str(version_id), file_type)

        local_loc = os.path.join("Models",str(model_id),str(model_version_id), file_type)
        if not os.path.isfile(local_loc):
            fs.get(s3_location, local_loc)
        return local_loc
    except Exception as e:
        LOGGER.exception(e)
        raise e

def download_model(runid:str, model_id:int, model_version_id:int, file_type:str = "finalized_model.sav") -> str:
    try:
        
        s3_location = os.path.join(S3_LOCATION_MODEL, runid, subfolder_model_2, file_type)
        local_loc = os.path.join("Models",str(model_id),str(model_version_id), file_type)
        if not os.path.isfile(local_loc):
            fs.get(s3_location, local_loc)
        return local_loc
    except Exception as e:
        LOGGER.exception(e)
        raise e