import os, sys

sys.path.append(os.getcwd())
from preprocessing.preprocessor_master import preprocess_main
from datasets.datasets import create_version
import utils.logs as logs
import jobs.job_management as jm
from utils.file_io import save_pickle

LOGGER = logs.get_logger()

def start_preprocess_job(body):
    result = {"status": 0, "msg": "Some error occured!"}
    try:
        dataset_id = body["dataset_id"]
        version_id = body["version_id"]
        preprocess_dict = body["pre_process_dict"]
        project_id = body["project_id"]
        job_id = body["job_id"]
        user_id = body["user_id"]
        target = body["target_col"]

        res_jm = jm.update_job(job_id = job_id, job_status=1, job_options = preprocess_dict)
        if res_jm["status"] != 1:
            result["msg"] = "Unable to update job as inprogress"
            LOGGER.info(result["msg"])
        else:
            LOGGER.info("Job updated")
            preprocessed_data, data_transformer = preprocess_main(
                                dataset_id=dataset_id,
                                version_id=version_id,
                                pre_process_dict=preprocess_dict,
                                target=target)

            LOGGER.info("Updating job status as complete")
            res_jm_c = jm.update_job(job_id = job_id, job_status=2)
            if res_jm_c["status"] !=1:
                result["msg"] = "Unable to update job as complete"
                LOGGER.info(result["msg"])
            else:
                LOGGER.info("job updated as complete")
                res = create_version(
                    dataset_id= dataset_id, 
                    user_id=user_id, 
                    prev_version_id=version_id,
                    job_id=job_id,
                    dataset=preprocessed_data,
                    project_id=project_id,
                    target_col = target
                )

                if res["status"] != 1:
                    LOGGER.info("Unable to create version")
                    result = res
                else:
                    result["status"] = 1
                    LOGGER.info("New Version created")
                    pickle_loc = save_pickle(transformer= data_transformer,dataset_id = dataset_id, version_id = res['msg'])
                    result["msg"] = "Preprocessing Completed and data created with version - " + str(res['msg'])
                    LOGGER.info(result["msg"])
 
    except Exception as e:
        res = jm.update_job(job_id = job_id, job_status=3)
        LOGGER.exception(e)
    finally:
        return result