# TODO for data engineering:
# 1. Create Job and return job id, add job to queueu, set job to intitiated


import db.db as db
from jobs.celery_tasks import pre_processing
from utils.custom_typing import Any, Dict
from utils.logs import get_logger

LOGGER = get_logger()


def create_job(
    pdvu_mapping: int, job_options: str, parent_job_id: int = None
) -> Dict[str, Any]:
    sess = None
    result = {"status": 0, "msg": "Some error occured!"}
    try:
        sess = db.get_session()
        ress = db.create_job(sess, pdvu_mapping, job_options, parent_job_id, commit=False)
        if ress["status"] != 1:
            result["msg"] = ress["msg"]
        else:
            job = pre_processing.apply_async((ress["msg"],), queue="pre_process")
            if job.status:
                sess.commit()
                result["msg"] = "Job created with id: " + str(ress["msg"])
                result["status"] = 1
    except Exception as e:
        LOGGER.exception(e)
        if sess is not None:
            sess.rollback()
    finally:
        db.close_session(sess)
        return result
