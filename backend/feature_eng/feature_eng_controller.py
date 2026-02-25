import os, sys

sys.path.append(os.getcwd())

from feature_eng.feature_engineering_pipeline_master import feature_eng_main
from datasets.datasets import create_version
import utils.logs as logs
import jobs.job_management as jm
from utils.file_io import save_pickle

LOGGER = logs.get_logger()

def start_feature_eng_job(body):
    LOGGER.info("inside feature eng job controller")
    result = {"status": 0, "msg": "Some error occured!"}
    try:
        dataset_id = body["dataset_id"]
        version_id = body["version_id"]
        feature_eng_dict = body["feature_eng_dict"]
        project_id = body["project_id"]
        job_id = body["job_id"]
        user_id = body["user_id"]

        LOGGER.info("Updating job as in-progress")
        jm.update_job(job_id = job_id, job_status=1, job_options=feature_eng_dict)

        fe_data, fe_pipeline = feature_eng_main( dataset_id=dataset_id,
                            version_id=version_id,
                            feature_eng_dict=feature_eng_dict)

        if fe_data is not None:
            
            LOGGER.info("Updating job as completed")
            jm.update_job(job_id = job_id, job_status=2)
            res = create_version(
                dataset_id= dataset_id, 
                user_id=user_id, 
                prev_version_id=version_id,
                job_id=job_id,
                dataset=fe_data,
                project_id=project_id
            )

            if res["status"] != 1:
                LOGGER.info("Unable to create version")
                result = res
            else:
                LOGGER.info("saving pickle file")
                pickle_loc = save_pickle(transformer= fe_pipeline,dataset_id = dataset_id, version_id = res['msg'],pickle_type = "fe_eng_pipeline.pkl")
                LOGGER.info("pickle file saved")  

                result["status"] = 1
                result["msg"] = "Feature engineering Completed and data created with version - " + str(res['msg'])
                LOGGER.info(result["msg"])
        else:
                jm.update_job(job_id = job_id, job_status=3)
                result["msg"] = "Error in Feature engineering algorithm"
                LOGGER.info(result["msg"])
    except Exception as e:
        res = jm.update_job(job_id = job_id, job_status=3)
        LOGGER.exception(e)
    finally:
        return result