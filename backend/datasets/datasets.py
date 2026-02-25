import os

import pandas as pd
from dateutil import parser
from pandas.api.types import is_string_dtype
import confs.config as conf

import db.db as db
import utils.file_io as io
import utils.logs as logs
from pii.pii_masker import pii
from utils.custom_typing import Any, Dict, List
import profiling.df_profile as prof
from jobs.publish_profile import publish_profile

LOGGER = logs.get_logger()

bucket_name = conf.read_config(section="dataset")["bucketname"]
folder_name = conf.read_config(section="dataset")["foldername"]
S3_LOCATION = os.path.join(bucket_name, folder_name)


# & Create Dataset section begins


# ! screen 1 to save information
def create_dataset(
    name: str, ds_desc: str, creation_user_id: int, dataset: pd.DataFrame
) -> Dict[str, Any]:
    # saves dataframe object and create a dataset with internal only visibility
    sess = None
    result = {"status": 0, "msg": "Some error occured!"}
    try:
        sess = db.get_session()

        LOGGER.info("Creating Dataset to get dataset id")
        res_dataset = db.create_dataset(
            session= sess, name= name, user_id= creation_user_id, ds_desc= ds_desc, completed=False, commit=False
        )

        if res_dataset["status"] != 1:
            LOGGER.info("Error in Dataset Creation")
            result["msg"] = res_dataset["msg"]
        else:

            LOGGER.info("Dataset Created")
            dataset_id = res_dataset["msg"]

            LOGGER.info("saving dataset with version id as 0")
            dataset_loc = io.save_file(dataset, dataset_id, version_id=0)

            LOGGER.info("saving the version information in db")
            res_ds_ver = db.create_datasets_versions(
                session= sess, dataset_id= dataset_id, user_id= creation_user_id, location= dataset_loc, commit=False
            )

            if res_ds_ver["status"] != 1:
                LOGGER.info(res_ds_ver["msg"])
                result["msg"] = res_ds_ver["msg"]

            else:

                LOGGER.info("dataset created with version 0")
                sess.commit()
                result["status"] = 1
                result["msg"] = {"dataset_id": dataset_id, "version_id": 0}
    except Exception as e:
        LOGGER.exception(e)
        if sess is not None:
            sess.rollback()
    finally:
        db.close_session(sess)
        return result


# ! scree 2 to load preview
def get_preview(
    dataset_id: int,
    version_id: int,
    nrows: int = 10,
    skiprows: int = None,
    header: bool = True,
    sep=",",
    encoding: str = "utf-8",
) -> Dict[str, Any]:
    sess = db.get_session()
    result = {"status": 0, "msg": "Some error occured!"}
    try:

        LOGGER.info("checking if dataset_id and version_id is valid")
        completed = False if version_id == 0 else True
        LOGGER.info("Completed: "+str(completed))
        res = db.read_datasets_versions(
            session=sess,
            dataset_id=dataset_id,
            version_id=version_id,
            completed=completed,
        )
        if res["status"] != 1:

            result["msg"] = "Dataset version doesn't exist"
            LOGGER.info(result["msg"])
        else:

            LOGGER.info("Downloading dataset from S3")
            dataset_loc = io.download_file(dataset_id, version_id)
            res_data = io.read_file(
                dataset_loc=dataset_loc,
                nrows=nrows,
                skiprows=skiprows,
                header=header,
                sep=sep,
                encoding=encoding,
            )
            result["status"] = 1
            result["msg"] = res_data
            LOGGER.info("Data previewed")
    except Exception as e:
        LOGGER.exception(e)
    finally:
        return result


# ! Screen 2 to save file in our format
def save_dataset_file_encoding(
    dataset_id: int,
    version_id: int,
    skiprows: int = None,
    header: bool = True,
    sep=",",
    encoding: str = "utf-8",
):

    LOGGER.info("saves file using the updated settings")
    sess = None
    result = {"status": 0, "msg": "Some error occured!"}
    try:

        sess = db.get_session()

        LOGGER.info("downloading from s3")
        dataset_loc = io.download_file(dataset_id, version_id)

        LOGGER.info("Reading data with updated settings")
        dataset = io.read_file(
            dataset_loc, skiprows=skiprows, header=header, sep=sep, encoding=encoding
        )
        
        LOGGER.info("Saving updated file to s3")
        dataset_loc = io.save_file(dataset, dataset_id, version_id)

        LOGGER.info("Inserting updated meta in db")
        meta_save = create_meta(dataset_id, version_id, dataset)
        if meta_save["status"] != 1:

            LOGGER.info("Error while saving in db")
            result["msg"] = result["msg"]
        else:
            LOGGER.info("File encoding saved")
            result = meta_save

    except Exception as e:
        LOGGER.exception(e)
        if sess is not None:
            sess.rollback()
    finally:
        db.close_session(sess)
        return result


# ! Screen 3 read meta data
def read_meta(dataset_id: int, version_id: int):
    sess = None
    result = {"status": 0, "msg": "Some error occured!"}
    try:
        sess = db.get_session()

        LOGGER.info("Reading meta")
        res_meta = db.read_ds_meta(sess, dataset_id, version_id)
        if res_meta["status"] != 1:

            LOGGER.info(result["msg"])
            result["msg"] = result["msg"]
        else:
            result = res_meta
    except Exception as e:
        LOGGER.exception(e)
        if sess is not None:
            sess.rollback()
    finally:
        db.close_session(sess)
        return result


# ! Screen 3 save meta data and other details
# TODO update column types
def save_user_provided_schema(
    dataset_id: int,
    version_id: int,
    columns_include: List[str],
    column_types: List[str],
    pii_columns: List[str],
) -> Dict[str, Any]:
    sess = None
    result = {"status": 0, "msg": "Some error occured!"}
    try:
        sess = db.get_session()

        LOGGER.info("downloading file and read it")
        dataset_loc = io.download_file(dataset_id, version_id)
        data_ = io.read_file(dataset_loc)

        LOGGER.info("selecting user specified columns")
        data_ = data_[columns_include]
        # data_ = data_.astype(dict(zip(list(get_col_info(data_).keys()), column_types)))
        #TODO replace astype with proper error handling for changing types per user demand

        LOGGER.info("checking if pii masking needs to be done and do it")
        if len(pii_columns):
            pii_res = db.create_pii(
                sess=sess, dataset_id=dataset_id, pii_columns=pii_columns
            )
            if pii_res["status"] == 1:
                data_ = pii(data_, pii_columns)

        LOGGER.info("geting user id")
        ress = db.read_datasets(sess, dataset_id=dataset_id, completed=False)

        if ress["status"] != 1:
            result["msg"] = ress["msg"]
        else:
            user_id = ress["msg"][0]["user_id"]

            LOGGER.info("Saving data as version 1")
            result = create_version(
                dataset_id=dataset_id,
                user_id=user_id,
                prev_version_id=0,
                job_id=-1,
                dataset=data_,
            )

    except Exception as e:
        LOGGER.exception(e)
        if sess is not None:
            sess.rollback()
    finally:
        db.close_session(sess)
        return result


# ! Screen 4 Set Porject acceess
def project_access(
    dataset_id: int, project_ids: List[int], creation_user_id: int, version_id:int = 1, public: bool = False
) -> Dict[str, Any]:
    sess = None
    result = {"status": 0, "msg": "Some error occured!"}
    try:
        sess = db.get_session()
        flag = False
        if public:

            LOGGER.info("Creating public dataset")
            res_proj_ds = db.create_proj_ds_map(
                session=sess,
                dataset_id=dataset_id,
                public=public,
                creation_user_id=creation_user_id,
                commit=False,
            )
            if res_proj_ds["status"] != 1:
                result["msg"] = result["msg"]
            else:
                flag = True
        else:

            LOGGER.info("Attaching projects to dataset")
            for project_id in project_ids:
                res_proj_ds = db.create_proj_ds_map(
                    session=sess,
                    dataset_id=dataset_id,
                    project_id=project_id,
                    creation_user_id=creation_user_id,
                    commit=False
                )
                if res_proj_ds["status"] != 1:
                    LOGGER.info("Error in attaching project to dataset with project_id "+str(project_id))
                    flag = False
                    result["msg"] = result["msg"]
                    break
                else:
                    proj_ds_id = res_proj_ds["msg"]
                    res_proj_user = db.get_project_users(
                        sess=sess, proj_ids=[project_id])
                    if res_proj_user["status"] != 1:
                        flag = False
                        result["msg"] = res_proj_user["msg"]
                        LOGGER.info(result["msg"])
                        break

                    else:
                        LOGGER.info("Populating PDVU map")
                        proj_user_ids = [x["pk"] for x in res_proj_user["msg"]]
                        for proj_user_id in proj_user_ids:
                            res_pdvu = db.create_pdvu_map(
                                sess=sess, dataset_id=dataset_id, version_id=version_id,
                                project_user_id=proj_user_id, project_ds_id=proj_ds_id,
                                commit=False
                            )
                            if res_pdvu["status"] != 1:
                                result["msg"] = "Error in creating Project Dataset Version User Mapping"
                                LOGGER.info(result["msg"])
                                flag = False
                                break
                            else:
                                flag = True
        if flag:
            sess.commit()
            result["status"] = 1
            result["msg"] = "Successfull"
            LOGGER.info(result["msg"])

    except Exception as e:
        LOGGER.exception(e)
        if sess is not None:
            sess.rollback()
    finally:
        db.close_session(sess)
        return result


# ! Scree 4 on submit finalize the dataset
def visibility_change(dataset_id: int) -> Dict[str, Any]:
    sess = None
    result = {"status": 0, "msg": "Some error occured!"}
    try:
        sess = db.get_session()

        LOGGER.info("Getting max version id")
        max_ver_id = db.get_max_ds_ver_id(sess, dataset_id)["msg"]

        if max_ver_id != -1:
            for ver in range(max_ver_id):
                db.delete_datasets_versions(sess, dataset_id, ver)

        vis = db.update_visibility(sess, dataset_id, completed=True)

        LOGGER.info("Final submit")
        if vis["status"] == 1:
            result = vis
    except Exception as e:
        LOGGER.exception(e)
        if sess is not None:
            sess.rollback()
    finally:
        db.close_session(sess)
        return result


# & Create Dataset section ends


def get_datasets(user_id: int, dataset_id: int = None) -> Dict[str, Any]:
    sess = None
    result = {"status": 0, "msg": "Some error occured!"}
    try:
        sess = db.get_session()
        LOGGER.info("Fetching Datasets")
        
        res = db.list_datasets(session= sess, user_id= user_id, dataset_id= dataset_id, completed= True)
        
        if res["status"] != 1:
            result = res
            LOGGER.info("Some error in fetching Datasets")
            LOGGER.info(result["msg"])
        else:
            result = res
            LOGGER.info(result["msg"])

    except Exception as e:
        LOGGER.exception(e)
    finally:
        db.close_session(sess)
        return result


def get_datasets_versions(
    dataset_id: int = None, version_id: int = None, user_id: int = None
) -> Dict[str, Any]:
    sess = None
    result = {"status": 0, "msg": "Some error occured!"}
    try:
        sess = db.get_session()
        LOGGER.info("Fetching Datasets Versions")
        
        res = db.list_datasets_versions(session= sess, dataset_id = dataset_id, version_id= version_id, user_id = user_id)

        if res["status"] != 1:
            result = res
            LOGGER.info("Some error in fetching dataset versions")
            LOGGER.info(result["msg"])
        else:
            result = res
            LOGGER.info(result["msg"])
    except Exception as e:
        LOGGER.exception(e)
    finally:
        db.close_session(sess)
        return result

def del_dataset(dataset_id: int) -> Dict[str, Any]:
    sess = None
    result = {"status": 0, "msg": "Some error occured!"}
    try:
        sess = db.get_session()
        LOGGER.info("Deleting Dataset")
        res_dataset = db.delete_datasets(session= sess, dataset_id= dataset_id,commit= False)
        if res_dataset["status"] != 1:
            result = res_dataset
            LOGGER.info("Unable to delete dataset")
            LOGGER.info(result["msg"])
            sess.rollback()
        else:
            LOGGER.info("Deleting Dataset versions")
            res_dataset_version = db.delete_datasets_versions(session= sess, dataset_id = dataset_id, commit= False)

            if res_dataset_version["status"] != 1:
                result = res_dataset_version
                LOGGER.info("Unable to delete datasets versions")
                LOGGER.info(result["msg"])
                sess.rollback
            else:
                LOGGER.info("Deleting meta data")
                res_dataset_meta = db.delete_ds_meta(session= sess,dataset_id = dataset_id, commit= False)

                if res_dataset_meta["status"] != 1:
                    result = res_dataset_meta
                    LOGGER.info("Unable to delete dataset meta")
                    LOGGER.info(result["msg"])
                    sess.rollback()
                else:
                    LOGGER.info("Steps to update Pdvu current")
                    LOGGER.info("Fetching project id from proj ds mapping")
                    res_read_proj_ds_map = db.read_proj_ds_map(session= sess, dataset_id= dataset_id)

                    if res_read_proj_ds_map["status"] != 1:
                        result = res_read_proj_ds_map
                        LOGGER.info("Unable to read Proj ds map")
                        LOGGER.info(result["msg"])
                        sess.rollback()
                    else:
                        LOGGER.info("Deleting Proj ds map")
                        res_proj_ds = db.del_proj_ds_map(session= sess, dataset_id = dataset_id, commit= False)

                        if res_proj_ds["status"] != 1:
                            result = res_proj_ds
                            LOGGER.info("Unable to delete Proj ds mapping")
                            LOGGER.info(result["msg"])
                            sess.rollback()
                        else:
                            LOGGER.info("Fetching Project user info")
                            project_ids = [x["project_id"] for x in res_read_proj_ds_map["msg"]]
                            res_read_proj_user = db.get_project_users(sess= sess, proj_ids= project_ids)

                            if res_read_proj_user["status"] != 1:
                                result = res_read_proj_user
                                LOGGER.info("Unable to fetch project user details")
                                LOGGER.info(result["msg"])
                                sess.rollback()
                            else:
                                flag = False
                                LOGGER.info("Updating Pdvu current")
                                project_user_ids = [x["pk"] for x in res_read_proj_user["msg"]]

                                for proj_user_id in project_user_ids:
                                    res_cur_update = db.current_update(
                                        sess= sess,
                                        project_user_id= proj_user_id,
                                        current = False,
                                        commit= False
                                    )

                                    if res_cur_update["status"] != 1:
                                        flag = True
                                        break

                                if flag:
                                    result = res_cur_update
                                    LOGGER.info("Unable to update current status in pdvu map")
                                    LOGGER.info(result["msg"])
                                    sess.rollback()
                                else:
                                    sess.commit()
                                    LOGGER.info("Dataset deleted successfully")
                                    result["status"] = 1
                                    result["msg"] = "Dataset deleted successfully"
    except Exception as e:
        LOGGER.exception(e)
    finally:
        db.close_session(sess)
        return result

def create_version(
    dataset_id: int,
    user_id: int,
    prev_version_id: int,
    job_id: int,
    dataset: pd.DataFrame,
    target_col:str="",
    project_id: int = None
):
    sess = None
    result = {"status": 0, "msg": "Some error occured!"}
    try:
        sess = db.get_session()

        LOGGER.info("get current max id of the dataset")
        res = db.get_max_ds_ver_id(sess, dataset_id)
        version_id = res["msg"] + 1

        LOGGER.info("Saving Dataset with version_id: " +str(version_id))
        loc = io.save_file(dataset, dataset_id, version_id)

        LOGGER.info("creating new version")
        ress = db.create_datasets_versions(
            sess, dataset_id, user_id, loc, prev_version_id, commit=False
        )
        if ress["status"] != 1:
            result["msg"] = ress["msg"]
            LOGGER.info(result["msg"])

        else:
            version_id = ress["msg"]

            LOGGER.info("finding meta data for the given version")
            meta = find_meta_data(dataset)
            col_info = meta["col_info"]
            row_count = meta["row_count"]
            col_count = meta["col_count"]

            LOGGER.info("creating meta data entry in db")
            res_meta = db.create_ds_meta(
                sess, version_id, dataset_id, row_count, col_count, col_info, commit=False
            )

            if res_meta["status"] != 1:
                result["msg"] = res_meta["msg"]
                LOGGER.info(result["msg"])
            else:
                body = {
                    "type": "job_profile",
                    "dataset_id": dataset_id,
                    "version_id":version_id

                }
                LOGGER.info("pushing profile job")
                res_job_profile = publish_profile(body)
                LOGGER.info(res_job_profile)
                if project_id is not None:
                    res_pdvu = create_pdvu_map(sess,dataset_id, version_id, user_id, project_id,
                    target_col=target_col)
                    if res_pdvu["status"] != 1:
                        sess.rollback()
                    else:
                        sess.commit()
                else:
                    sess.commit()

                result["msg"] = version_id
                result["status"] = 1
                LOGGER.info("version created")

    except Exception as e:
        LOGGER.exception(e)
        if sess is not None:
            sess.rollback()
    finally:
        db.close_session(sess)
        return result


def create_meta(
    dataset_id: int, version_id: int, dataset: pd.DataFrame
) -> Dict[str, Any]:
    sess = None
    result = {"status": 0, "msg": "Some error occured!"}
    try:
        sess = db.get_session()
        meta = find_meta_data(dataset)
        col_info = meta["col_info"]
        row_count = meta["row_count"]
        col_count = meta["col_count"]
        res_meta = db.create_ds_meta(
            session=sess,
            dataset_id=dataset_id,
            version_id=version_id,
            row_count=row_count,
            col_count=col_count,
            col_info=col_info,
        )
        if res_meta["status"] != 1:
            result["msg"] = result["msg"]
        else:
            result = res_meta
    except Exception as e:
        LOGGER.exception(e)
        if sess is not None:
            sess.rollback()
    finally:
        db.close_session(sess)
        return result


def find_meta_data(data: pd.DataFrame) -> Dict[str, Any]:
    try:
        col_info = get_col_info(data)
        row_count = data.shape[0]
        col_count = data.shape[1]
        return {"col_info": col_info, "row_count": row_count, "col_count": col_count}
    except Exception as e:
        LOGGER.exception(e)


def get_col_info(data: pd.DataFrame) -> dict:
    data_ = data.dropna().copy()
    datatypes = {}
    # for col, value in data_.apply(lambda x: set(x)).items():
    for col in data_.columns:
        value = set(data_[col])
        if data_[col].dtype != "object":
            if len(value) >= 2 and (
                (len(data_) < 50 and (len(data_) - len(value)) / len(data_) >= 0.7)
                or (len(data_) >= 50 and (len(data_) - len(value)) / len(data_) >= 0.9)
            ):
                datatypes[col] = "ordinal"
            else:
                datatypes[col] = str(data_[col].dtype)
        else:
            if "true" in set(
                str(each_string).strip().lower() for each_string in value
            ) or "false" in set(
                str(each_string).strip().lower() for each_string in value
            ):
                datatypes[col] = "bool"
            elif len(value) >= 1 and (
                (len(data_) < 50 and (len(data_) - len(value)) / len(data_) >= 0.7)
                or (len(data_) >= 50 and (len(data_) - len(value)) / len(data_) >= 0.9)
            ):
                datatypes[col] = "category"
            elif is_string_dtype(data_[col]):
                datatypes[col] = "str"
            else:
                datatypes[col] = "object"
            try:
                data_[col].apply(lambda x: parser.parse(x))
                datatypes[col] = "datetime"
            except BaseException:
                pass
    return datatypes


def create_pdvu_map(
    sess,dataset_id: int, version_id: int, user_id: int, project_id: int, target_col:str=""
) -> Dict[str, Any]:
    # sess = None
    result = {"status": 0, "msg": "Some error occured!"}
    try:
        # sess = db.get_session()

        # get pk from project_user table
        res_proj_user = db.get_project_users(sess, proj_ids = [project_id], user_id = user_id)
        if res_proj_user["status"] != 1:
            result = res_proj_user
        else:
            project_user_id = res_proj_user["msg"][0]["pk"]

            # get pk from proj_ds_map table
            res_proj_ds = db.read_proj_ds_map(sess, dataset_id, project_id)
            if res_proj_ds["status"] != 1:
                result = res_proj_ds
            else:
                project_ds_id = res_proj_ds["msg"][0]["pk"]

                res_pdvu = db.create_pdvu_map(
                    sess, version_id, dataset_id, project_user_id, project_ds_id, target_col
                )
                if res_pdvu["status"] != 1:
                    result["msg"] = result["msg"]
                else:
                    result = res_pdvu

    except Exception as e:
        LOGGER.exception(e)
        if sess is not None:
            sess.rollback()
    finally:
        db.close_session(sess)
        return result


def get_pdvu_map(
    pdvu_id: int = None,
    dataset_id: int = None,
    version_id: int = None,
    user_id: int = None,
    project_id: int = None,
    current: bool = True,
) -> Dict[str, Any]:
    sess = None
    result = {"status": 0, "msg": "Some error occured!"}
    try:
        sess = db.get_session()

        ress = db.read_pdvu_map(
            sess=sess,
            pdvu_id=pdvu_id,
            dataset_id=dataset_id,
            version_id=version_id,
            user_id=user_id,
            project_id=project_id,
            current=current,
        )

        if ress["status"] != 1:
            result["msg"] = result["msg"]
        else:
            result = ress

    except Exception as e:
        LOGGER.exception(e)
        if sess is not None:
            sess.rollback()
    finally:
        db.close_session(sess)
        return result

def get_data(project_id:int, user_id:int) -> Dict[str, Any]:
    sess = None
    result = {"status": 0, "msg": "Some error occured!"}
    try:
        sess = db.get_session()
        res = db.get_ds_ver(session=sess,project_id=project_id, user_id=user_id)

        if res["status"] != 1:
            result["msg"] = result["msg"]
        else:
            dataset_id = res['msg'][0]['dataset_id']
            version_id = res['msg'][0]['version_id']
            s3_location = os.path.join(
            S3_LOCATION, str(dataset_id), str(version_id), "dataset.csv")
            result["status"] = 1
            result["msg"] = s3_location
    except Exception as e:
        LOGGER.exception(e)
        if sess is not None:
            sess.rollback()
    finally:
        db.close_session(sess)
        return result    

def get_ds_ver(project_id:int, user_id:int) -> Dict[str, Any]:
    sess = None
    result = {"status": 0, "msg": "Some error occured!"}
    try:
        sess = db.get_session()
        res = db.get_ds_ver(session=sess,project_id=project_id, user_id=user_id)

        if res["status"] != 1:
            result["msg"] = result["msg"]
        else:
            dataset_id = res['msg'][0]['dataset_id']
            version_id = res['msg'][0]['version_id']
            result["status"] = 1
            result["msg"] = {"dataset_id": dataset_id, "version_id": version_id}
    except Exception as e:
        LOGGER.exception(e)
        if sess is not None:
            sess.rollback()
    finally:
        db.close_session(sess)
        return result 

def get_target_col(project_id:int, user_id:int) -> Dict[str, Any]:
    sess = None
    result = {"status": 0, "msg": "Some error occured!"}
    try:
        sess = db.get_session()
        res = db.get_ds_ver(session=sess,project_id=project_id, user_id=user_id)

        if res["status"] != 1:
            result["msg"] = result["msg"]
        else:
            target_col = res['msg'][0]['target_col']
            result["status"] = 1
            if target_col is None:
                result["msg"] = None
            else:
                result["msg"] = target_col
    except Exception as e:
        LOGGER.exception(e)
        if sess is not None:
            sess.rollback()
    finally:
        db.close_session(sess)
        return result 



def pre_process_map(project_id:int, user_id:int,) -> Dict[str, Any]:
    result = {"status": 0, "msg": "Some error occured!"}
    try:
        sess = db.get_session()
        res = db.get_ds_ver(session=sess,project_id=project_id, user_id=user_id)

        if res["status"] != 1:
            result["msg"] = result["msg"]
        else:
            dataset_id = res['msg'][0]['dataset_id']
            version_id = res['msg'][0]['version_id']
            dataset_loc = io.download_file(dataset_id, version_id)
            df = io.read_file(dataset_loc=dataset_loc)
            res = get_col_info(df)

            res_dict = {}
            for col,cat in res.items():
                if cat == "int64" or cat == "float64":
                    res_dict[col] = ["impute", "scale", "mathops", "clip", "bin_variables", "removenull"]
                elif cat == "category":
                    res_dict[col] = ["encode", "removenull", "impute"]
                elif cat == "ordinal":
                    res_dict[col] = ["encode", "removenull", "impute"]

            result["status"] = 1
            result["msg"] = res_dict
    except Exception as e:
        LOGGER.exception(e)
    finally:
        return result          
