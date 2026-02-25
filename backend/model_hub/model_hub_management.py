import json
import db.db as db
import utils.file_io as io
import utils.logs as logs
from utils.custom_typing import Any, Dict, List

LOGGER = logs.get_logger()

def create_model(
    model_name: str, model_version_name:str, project_id: int, job_id:int, 
    user_id: int, mlflow_runid:str, model_param:json, model_hyperparameters:json
) -> Dict[str, Any]:
    sess = None
    result = {"status": 0, "msg": "Some error occured!"}
    try:
        sess = db.get_session()
        LOGGER.info(" creating entry in model hub")

        res_model = db.create_model_hub(
            session=sess, model_name=model_name, project_id=project_id, user_id=user_id, commit=False)
        
        if res_model["status"] != 1:
            result["msg"] = "Error in model hub entry"
            LOGGER.info(result["msg"])
        else:
            LOGGER.info("model hub entry created")

            LOGGER.info("creating version of model")
            res_model_ver = db.create_model_version(
                session=sess, model_id=res_model["msg"], mlflow_runid=mlflow_runid, job_id=job_id, 
                model_param=model_param, model_hyperparameters=model_hyperparameters,
                model_version_name=model_version_name, commit=False)
            if res_model_ver["status"] != 1:
                result["msg"] = "Erorr in model version entry"
                LOGGER.info(result["msg"])
            else:
                sess.commit()
                result["status"] = 1
                result["msg"] = "Model created with model_id: "+str(res_model["msg"])+" and version_id: "+str(res_model_ver["msg"]) 
                LOGGER.info("creating pipeline dictionary")
                res_pipeline = pipeline_dict(model_id=res_model["msg"], model_version_id=res_model_ver["msg"], user_id=user_id)
                if res_pipeline["status"] != 1:
                    result["status"] = 0
                    result["msg"] = "Error in creating pipeline dict"
                    LOGGER.info(result["msg"])
                else:
                    LOGGER.info("Pipeline dict created")
                    res_update = db.udpate_pipeline_dict(sess, model_id=res_model["msg"], 
                                        version_id=res_model_ver["msg"], pipeline_dict=res_pipeline["msg"])
                    if res_update["status"] != 1:
                        result["status"] = 0
                        result["msg"] = "Error in updating pipeline dict"
                        LOGGER.info(result["msg"])
                    else:
                        io.download_model(runid=mlflow_runid, model_id=res_model["msg"], model_version_id=res_model_ver["msg"])
                        LOGGER.info(result["msg"])
                        LOGGER.info("pipeline udpated")             
    except Exception as e:
        LOGGER.exception(e)
        if sess is not None:
            sess.rollback()
    finally:
        db.close_session(sess)
        return result

def create_model_version(
    model_id:int, model_version_name:str, job_id:int, user_id:int,
    mlflow_runid:str, model_param:json, model_hyperparameters:json
) -> Dict[str, Any]:
    sess = None
    result = {"status": 0, "msg": "Some error occured!"}
    try:
        sess = db.get_session()
        LOGGER.info(" creating entry in model hub")

        res_model_ver = db.create_model_version(
            session=sess, model_id=model_id, mlflow_runid=mlflow_runid, job_id=job_id, 
            model_param=model_param, model_hyperparameters=model_hyperparameters,
            model_version_name=model_version_name, commit=False)
        
        if res_model_ver["status"] != 1:
            result["msg"] = "Error in model version entry"
            LOGGER.info(result["msg"])
        else:
            LOGGER.info("model hub entry created")

            LOGGER.info("creating version of model")
            sess.commit()
            result["status"] = 1
            result["msg"] = "Model version created with version_id: "+str(res_model_ver["msg"]) 
            LOGGER.info("creating pipeline dictionary")
            res_pipeline = pipeline_dict(model_id=model_id, model_version_id=res_model_ver["msg"], user_id=user_id)
            if res_pipeline["status"] != 1:
                result["status"] = 0
                result["msg"] = "Error in creating pipeline dict"
                LOGGER.info(result["msg"])
            else:
                LOGGER.info("Pipeline dict created")
                res_update = db.udpate_pipeline_dict(sess, model_id=model_id, 
                                    version_id=res_model_ver["msg"], pipeline_dict=res_pipeline["msg"])
                if res_update["status"] != 1:
                    result["status"] = 0
                    result["msg"] = "Error in updating pipeline dict"
                    LOGGER.info(result["msg"])
                else:
                    io.download_model(runid=mlflow_runid, model_id=model_id, model_version_id=res_model_ver["msg"])
                    LOGGER.info(result["msg"])
                    LOGGER.info("pipeline udpated")             
    except Exception as e:
        LOGGER.exception(e)
        if sess is not None:
            sess.rollback()
    finally:
        db.close_session(sess)
        return result



def read_models(
   user_id: int
) -> Dict[str, Any]:
    sess = None
    result = {"status": 0, "msg": "Some error occured!"}
    try:
        sess = db.get_session()
        LOGGER.info(" reading models")
        res = db.read_models(session=sess,user_id=user_id)
        if res["status"] != 1:
            result["msg"] = "Error in reading model versions"
            LOGGER.info(result["msg"])
        else:
            
            result = res       
    except Exception as e:
        LOGGER.exception(e)
    finally:
        db.close_session(sess)
        return result

def read_model_version(
   model_id: int
) -> Dict[str, Any]:
    sess = None
    result = {"status": 0, "msg": "Some error occured!"}
    try:
        sess = db.get_session()
        LOGGER.info(" reading models")
        res = db.read_model_version(session=sess, model_id=model_id)
        if res["status"] != 1:
            result["msg"] = "Error in reading model versions"
            LOGGER.info(result["msg"])
        else:
            result = res       
    except Exception as e:
        LOGGER.exception(e)
    finally:
        db.close_session(sess)
        return result


def del_model(model_id: int) -> Dict[str, Any]:
    sess = None
    result = {"status": 0, "msg": "Some error occured!"}
    try:
        sess = db.get_session()
        LOGGER.info("Deleting Model")
        res_model = db.delete_model(session= sess, model_id= model_id,commit= False)
        if res_model["status"] != 1:
            result = res_model
            LOGGER.info("Unable to delete model")
            LOGGER.info(result["msg"])
        else:
            LOGGER.info("Deleting model versions")
            res_model_version = db.delete_model_versions(session= sess, model_id = model_id, commit= False)

            if res_model_version["status"] != 1:
                result = res_model_version
                LOGGER.info("Unable to delete model versions")
                LOGGER.info(result["msg"])
            else:
                sess.commit()
                result = res_model
                LOGGER.info("Model and their version(s) deleted")

    except Exception as e:
        LOGGER.exception(e)
    finally:
        db.close_session(sess)
        return result


def del_model_version(model_id: int, version_id:int) -> Dict[str, Any]:
    sess = None
    result = {"status": 0, "msg": "Some error occured!"}
    try:
        sess = db.get_session()
        LOGGER.info("Deleting Model")
        res_model_version = db.delete_model_versions(session= sess, model_id = model_id, version_id=version_id,commit= False)
        if res_model_version["status"] != 1:
            result = res_model_version
            LOGGER.info("Unable to delete model versions")
            LOGGER.info(result["msg"])
        else:
            sess.commit()
            result = res_model_version
            LOGGER.info("Model version(s) deleted")

    except Exception as e:
        LOGGER.exception(e)
    finally:
        db.close_session(sess)
        return result


def pipeline_dict(model_id:int, model_version_id:int, user_id:int):
    sess = None
    result = {"status": 0, "msg": "Some error occured!"}
    flag = True
    try:
        sess = db.get_session()
        LOGGER.info("Getting job id")
        pipline_dict = {}
        job_id = db.get_job_id(session = sess, model_id=model_id, version_id=model_version_id)
        if job_id["status"] != 1:
            result["msg"] = job_id["msg"]
            LOGGER.info(result["msg"])
        else:
            
            pre_process = {}
            LOGGER.info("job_id:"+str(job_id))
            res_jobs = db.read_jobs(sess=sess, job_id=job_id["msg"])
            pdvu_id = res_jobs["msg"][0]["pdvu_id"]
            LOGGER.info("Reading pdvu mapping for pdvu_id: "+str(pdvu_id))
            res_ds = db.read_pdvu_map(sess=sess, pdvu_id=pdvu_id,user_id=user_id,current=None)

            if res_ds["status"] != 1:
                result["msg"] = res_ds["msg"]
                LOGGER.info(result["msg"])
                flag = False
            else:
                LOGGER.info("Getting dataset_id and Version_id")
                dataset_id = res_ds["msg"][0]["dataset_id"]
                version_id = res_ds["msg"][0]["version_id"]
                LOGGER.info("Dataset_id: " + str(dataset_id) + " Version_id: " + str(version_id))

                LOGGER.info("Traversing all the versions of datasets")
                i = 1
                while version_id > 1:
                    pre_process = {}
                    LOGGER.info("Getting the previous version")
                    res_ds_ver = db.read_datasets_versions(
                        session=sess, dataset_id=dataset_id, version_id=version_id
                    )

                    if res_ds_ver["status"] != 1:
                        result["msg"] = res_ds_ver["msg"]
                        LOGGER.info(result["msg"])
                        flag = False
                        break
                    else:
                        LOGGER.info("saving current version (version_id: {}) for downloading pickle file".format(version_id))
                        temp = version_id
                        LOGGER.info("Setting previous version( version_id: {}) as current version".format(res_ds_ver["msg"][0]["prev_id"]))
                        version_id = res_ds_ver["msg"][0]["prev_id"]

                        LOGGER.info("Reading pdvu map using previous version")
                        res_pdvu = db.read_pdvu_map(
                            sess=sess, dataset_id=dataset_id, version_id=version_id, user_id=user_id,current=None
                            )
                        
                        if res_pdvu["status"] != 1:
                            result["msg"] = res_pdvu["msg"]
                            LOGGER.info(result["msg"])
                            flag = False
                            break
                        else:
                            
                            LOGGER.info("Reading jobs_info using pdvu id (value: {} ) of previous dataset version id".format(res_pdvu["msg"][0]["pk"]))
                            res_jobs = db.read_jobs(sess=sess, pdvu_ids=[res_pdvu["msg"][0]["pk"]], job_status=2)
                            if res_jobs["status"] != 1:
                                result["msg"] = res_jobs["msg"]
                                LOGGER.info(result["msg"])
                                flag = False
                                break
                            else:
                                job_type = res_jobs["msg"][0]["job_type"]
                                if job_type == "preprocess":
                                    LOGGER.info("Adding preprocess to pipeline dictionary ")
                                    pre_process["process_name"] = job_type
                                    pre_process["file_name"] = "preprocessor_pickle_{}.pkl".format(i)
                                    io.download_ds_eng(
                                        dataset_id=dataset_id, version_id=temp, model_id=model_id, model_version_id=model_version_id, file_type = pre_process["file_name"]
                                        )
                                    pipline_dict[i] = pre_process
                                    i=i+1
                                elif job_type == "feature_eng":
                                    LOGGER.info("Adding feature_eng to pipeline dictionary ")
                                    pre_process["process_name"] = job_type
                                    pre_process["file_name"] = "fe_eng_pipeline_{}.pkl".format(i)
                                    io.download_ds_eng(
                                        dataset_id=dataset_id, version_id=temp, model_id=model_id, model_version_id=model_version_id, file_type = pre_process["file_name"]
                                        )
                                    pipline_dict[i] = pre_process
                                    i=i+1
                                else:
                                    LOGGER.info("Skipping as job_type is: "+job_type)
                                    #!TODO handle if job_type is auto_ml
                                    continue

                if flag == True:
                    result["status"] = 1
                    result["msg"] = pipline_dict

            
    except Exception as e:
        LOGGER.exception(e)
        if sess is not None:
            sess.rollback()
    finally:
        db.close_session(sess)
        return result

def get_pipline_dict(model_id:int, version_id:int):
    sess = None
    result = {"status": 0, "msg": "Some error occured!"}
    try:
        sess = db.get_session()
        LOGGER.info("Deleting Model")
        res_pipeline = db.read_model_version(session=sess, model_id = model_id, version_id=version_id)
        if res_pipeline["status"] != 1:
            result = res_pipeline
            LOGGER.info(result["msg"])
        else:
            LOGGER.info("Pipeline dict fetched")
            pipeline_dict = res_pipeline["msg"][0]["pipeline_dict"]
            result["status"] = 1
            result["msg"] = pipeline_dict
    except Exception as e:
        LOGGER.exception(e)
    finally:
        db.close_session(sess)
        return result  
