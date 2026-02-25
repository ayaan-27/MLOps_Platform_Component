import os, sys
import pandas as pd

sys.path.append(os.getcwd())
import utils.logs as logs
import utils.file_io as io
import db.db as db

LOGGER = logs.get_logger()

def start_profiling_job(body):
    sess = db.get_session()
    result = {"status": 0, "msg": "Some error occured!"}
    try:
        dataset_id = body["dataset_id"]
        version_id = body["version_id"]
        LOGGER.info("downloading and reading dataset with dataset id: {} and version id: {}".format(dataset_id, version_id))
        dataset = pd.read_csv(io.download_file(dataset_id=dataset_id, version_id=version_id))
        LOGGER.info("Dataset read")

        LOGGER.info("Creating profile")
        res_profile = io.save_profile(dataset, dataset_id=dataset_id, version_id=version_id)

        if res_profile["status"] != 1:
            result["msg"] = "Error in saving profile"
            LOGGER.info(result["msg"])
        else:
            LOGGER.info("profiling created and saved in s3")
            LOGGER.info("making db entry")
            res_prof = db.update_profiling_details(
                session = sess,dataset_id=dataset_id,version_id = version_id,profiling_done=True)
            if res_prof["status"] != 0:
                result["msg"] = res_prof["msg"]
                LOGGER.info(result["msg"])
            else:
                result["status"] = 1
                result["msg"] = "Profiling completed"
                LOGGER.info(result["msg"])

    except Exception as e:
        LOGGER.exception(e)
        if sess is not None:
            sess.rollback()

    finally:
        db.close_session(sess)
        return result