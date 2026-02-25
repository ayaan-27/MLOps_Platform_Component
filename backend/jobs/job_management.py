import json
from collections import Counter

import pandas as pd
from sqlalchemy.sql.functions import user

import db.db as db
import utils.logs as logs

from db.db_jobs_info import create_job, read_jobs, update_job_status

from utils.custom_typing import Any, Dict, List, db_session

LOGGER = logs.get_logger()

def make_job(
    dataset_id:int,
    version_id:int,
    project_id:int,
    user_id:int,
    job_options: json = None,
    job_type: str = None,
    parent_job_id: int = None,
) -> Dict[str, Any]:

    sess = None
    result = {"status": 0, "msg": "Some error occured!"}
    try:
        sess = db.get_session()
        LOGGER.info("Creating Job")

        pdvu_mapping = db.read_pdvu_id(session=sess, project_id=project_id,
                                    dataset_id=dataset_id, version_id=version_id,
                                    user_id=user_id)
        if pdvu_mapping["status"] != 0:
            res = db.create_job(
                sess = sess,
                pdvu_mapping = pdvu_mapping["msg"],
                job_options = job_options,
                parent_job_id = parent_job_id,
                job_type = job_type,
                commit = True
            )

            if res["status"] != 1:
                result = res
                sess.rollback()
                LOGGER.info("Job Creation Failed")
                LOGGER.info(result["msg"])
            else:
                sess.commit()
                result["status"] = 1
                result["msg"] = res["msg"]
        else:
            result=pdvu_mapping

    except Exception as e:
        LOGGER.exception(e)
        if sess is not None:
            sess.rollback()
    finally:
        db.close_session(sess)
        return result

def update_job(
    job_id: int,
    job_status: int,
    job_options: json = None
) -> Dict[str,Any]:

    sess = None
    result = {"status": 0, "msg": "Some error occured!"}

    try:
        sess = db.get_session()
        LOGGER.info("Updating Job")
        if job_options is not None:
            res = db.update_job_status(sess = sess, job_id = job_id, job_status = job_status, job_options=job_options)
        else:
            res = db.update_job_status(sess = sess, job_id = job_id, job_status = job_status)

        if res["status"] != 1:
            result = res
            sess.rollback()
            LOGGER.info("Job Updation Failed")
            LOGGER.info(result["msg"])
        else:
            sess.commit()
            result["status"] = 1
            result["msg"] = "Job status Updated"

    except Exception as e:
        LOGGER.exception(e)
        if sess is not None:
            sess.rollback()
    finally:
        db.close_session(sess)
        return result


def monitor_job(job_id:int):
    sess = None
    result = {"status": 0, "msg": "Some error occured!"}
    try:
        sess = db.get_session()
        LOGGER.info("reading Jobs")
        res = db.monitor(sess = sess, job_id = job_id)

        if res["status"] != 1:
            result = res
            LOGGER.info(result["msg"])
        else:
            result["status"] = 1
            result["msg"] = res["msg"][0]["job_status"]

    except Exception as e:
        LOGGER.exception(e)
        if sess is not None:
            sess.rollback()
    finally:
        db.close_session(sess)
        return result

# List Jobs

def list_jobs(project_id:int, user_id:int
) -> Dict[str, Any]:

    sess = None
    result = {"status": 0, "msg": "Some error occured!"}
    try:
        sess = db.get_session()
        res_pdvu = db.read_pdvu_map(sess=sess,project_id=project_id, user_id=user_id,current = False)
        if res_pdvu["status"] != 1:
            result["msg"] = result["msg"]
        else:
            pdvu_ids = [x['pk'] for x in res_pdvu['msg']]
            res = db.read_jobs(sess=sess, pdvu_ids=pdvu_ids)
            if res["status"] != 1:
                result = res
                LOGGER.info(result["msg"])
            else:
                result["status"] = 1
                result["msg"] = res["msg"]
        
    except Exception as e:
        
        LOGGER.exception(e)
        if sess is not None:
            sess.rollback()
    finally:
        db.close_session(sess)
        return result

def create_automl(job_id:int, auto_ml_type: str, leadboard_loc: str
) -> Dict[str, Any]:

    sess = None
    result = {"status": 0, "msg": "Some error occured!"}
    try:
        sess = db.get_session()
        res = db.create_auto_ml(session=sess,job_id=job_id, auto_ml_type=auto_ml_type, leadboard_loc=leadboard_loc)
        if res["status"] != 1:
            result["msg"] = result["msg"]
        else:
            result["status"] = 1
            result["msg"] = res["msg"]
        
    except Exception as e:
        
        LOGGER.exception(e)
        if sess is not None:
            sess.rollback()
    finally:
        db.close_session(sess)
        return result