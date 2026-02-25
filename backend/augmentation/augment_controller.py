import os, sys

sys.path.append(os.getcwd())
from augmentation.autoencode_master import autoencode_main
from augmentation.sampling_master import sampling_main
from datasets.datasets import create_version
import utils.logs as logs
import jobs.job_management as jm
import db.db as db


LOGGER = logs.get_logger()

def start_autoencode_job(body):
    result = {"status": 0, "msg": "Some error occured!"}
    try:
        dataset_id = body["dataset_id"]
        version_id = body["version_id"]
        project_id = body["project_id"]
        job_id = body["job_id"]
        user_id = body["user_id"]
        epochs = body["epochs"]
        target = body["target"]

        LOGGER.info("Getting meta data from db")
        sess = db.get_session()
        res_ds = db.read_ds_meta(sess, dataset_id=dataset_id,version_id=version_id)
        if res_ds["status"] != 1:
            LOGGER.error("Error in reading the meta data")
            result = res_ds
        else:
            categorical_problem = False
            categorical_type = []
            continuous_type = []
            col_dict = res_ds['msg'][0]['col_info']
            for i in col_dict:
                if i[1] == 'category':
                    categorical_problem = True
                    categorical_type.append(i[0])
                else:
                    continuous_type.append(i[0])
            job_options = {"epochs":epochs, "categorical_problem":categorical_problem}
            LOGGER.info("Updating job as in-progress")
            jm.update_job(job_id = job_id, job_status=1, job_options=job_options)

            
            aug_data = autoencode_main(dataset_id=dataset_id, version_id=version_id, epochs=epochs,
                                            target=target, categorical_problem=categorical_problem,
                                            categorical_type=categorical_type, continuous_type=continuous_type
                                )
            if aug_data is not None:
                LOGGER.info("Updating job as completed")
                jm.update_job(job_id = job_id, job_status=2)
                
                res = create_version(
                    dataset_id= dataset_id, 
                    user_id=user_id, 
                    prev_version_id=version_id,
                    job_id=job_id,
                    dataset=aug_data,
                    project_id=project_id
                )
                if res["status"] != 1:
                    LOGGER.info("Unable to create version")
                    result = res
                else:
                    result["status"] = 1
                    result["msg"] = "Autoencode job Completed"
            else:
                jm.update_job(job_id = job_id, job_status=3)
                result["msg"] = "Error in Augmentation algorithm"
                LOGGER.info(result["msg"])
 
    except Exception as e:
        LOGGER.info("Updating job as error")
        res = jm.update_job(job_id = job_id, job_status=3)
        LOGGER.exception(e)
    finally:
        return result

def start_sampling_job(body):
    result = {"status": 0, "msg": "Some error occured!"}
    try:
        LOGGER.info("Parsing data")

        dataset_id = body["dataset_id"]
        version_id = body["version_id"]
        project_id = body["project_id"]
        job_id = body["job_id"]
        user_id = body["user_id"]
        sampling_method = body["sampling_method"]
        sampling_strategy = body["sampling_strategy"]
        target = body["target"]
        categorical_problem = False
        categorical_features = []
        k_neighbors = 5
        job_options = {"sampling_method":sampling_method, "sampling_strategy":sampling_strategy}
        LOGGER.info("Data Parsed")

        LOGGER.info("Updating job as 1")
        jm.update_job(job_id = job_id, job_status=1, job_options=job_options)

        aug_data = sampling_main(dataset_id=dataset_id, version_id=version_id, sampling_method=sampling_method,
                                    sampling_strategy=sampling_strategy, target=target, categorical_problem=categorical_problem, 
                                    categorical_features=categorical_features,k_neighbors=k_neighbors
                        )
        if aug_data is not None:
            LOGGER.info("updating job to 2")
            jm.update_job(job_id = job_id, job_status=2)
            
            res = create_version(
                dataset_id= dataset_id, 
                user_id=user_id, 
                prev_version_id=version_id,
                job_id=job_id,
                dataset=aug_data,
                project_id=project_id
            )
            if res["status"] != 1:
                LOGGER.info("Unable to create version")
                result = res
            else:
                result["status"] = 1
                result["msg"] = "Augmentation - Sampling job Completed"
        else:
            jm.update_job(job_id = job_id, job_status=3)
            result["msg"] = "Error in Augmentation algorithm"
 
    except Exception as e:
        jm.update_job(job_id = job_id, job_status=3)
        LOGGER.exception(e)
    finally:
        return result