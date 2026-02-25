from auto_ml.main_classification import main_classification
from auto_ml.main_regression import main_regression


import utils.logs as logs
import utils.file_io as io
import jobs.job_management as jm

LOGGER = logs.get_logger()

def start_auto_ml_job(body):
    LOGGER.info("inside auto_ml job controller")
    result = {"status": 0, "msg": "Some error occured!"}
    try:
        
        dataset_id = body["dataset_id"]
        version_id = body["version_id"]
        job_id = body["job_id"]
        ml_type = body["ml_type"]
        target = body["target"]



        job_options = {"ml_type": ml_type, "target": target}
        LOGGER.info("Updating job as in-progress")
        jm.update_job(job_id = job_id, job_status=1, job_options=job_options)

        if ml_type == "regression":
            leaderborad_df = main_regression(
                dataset_id = dataset_id, version_id = version_id, target=target
            )
        
        else:
            leaderborad_df = main_classification(
                dataset_id = dataset_id, version_id = version_id, target=target
            )

        if leaderborad_df is None:
            LOGGER.info("Error in auto_ml code")
            result["msg"] = result["msg"]
        else:
            leaderboard_loc = io.save_leaderboard(leaderborad_df,job_id=job_id)
            res_auto_ml = jm.create_automl(job_id=job_id, auto_ml_type=ml_type, leadboard_loc=leaderboard_loc)
            if res_auto_ml["status"] != 1:
                result["msg"] = result["msg"]
            else:
                jm.update_job(job_id=job_id, job_status=2)
                result["status"] = 1
                result["msg"] = "Auto ml job completed and leaderboard saved"
                LOGGER.info(result["msg"])

    except Exception as e:
        res = jm.update_job(job_id = job_id, job_status=3)
        LOGGER.exception(e)
    finally:
        return result
