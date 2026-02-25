import json
import os, sys
sys.path.append(os.getcwd())
import db.db as db
import utils.file_io as io
import utils.logs as logs
from utils.custom_typing import Any, Dict, List

LOGGER = logs.get_logger()

def create_deployment(
    model_id: int, version_id:int, user_id: int, name:str, status:str, access_lvl:str
) -> Dict[str, Any]:
    sess = None
    result = {"status": 0, "msg": "Some error occured!"}
    try:
        sess = db.get_session()
        LOGGER.info("creating deployment")

        res_deploy = db.create_deployment(
            session=sess, model_id=model_id, version_id=version_id, user_id=user_id, 
            name=name, status=status, access_lvl = access_lvl, commit=False)
        
        if res_deploy["status"] != 1:
            result["msg"] = "Error in Deployment entry"
            LOGGER.info(result["msg"])
        else:
            sess.commit()
            result = res_deploy
            LOGGER.info("Deployment created")

    except Exception as e:
        LOGGER.exception(e)
        if sess is not None:
            sess.rollback()
    finally:
        db.close_session(sess)
        return result

def delete_deployment(d_id: int) -> Dict[str, Any]:
    sess = None
    result = {"status": 0, "msg": "Some error occured!"}
    try:
        sess = db.get_session()
        LOGGER.info("Deleting Deployment")
        res_deploy = db.delete_deployment(session= sess, d_id= d_id,commit= False)
        if res_deploy["status"] != 1:
            result = res_deploy
            LOGGER.info("Unable to delete deployment")
            LOGGER.info(result["msg"])
        else:
            sess.commit()
            result = res_deploy
            LOGGER.info("Deployment deleted")

    except Exception as e:
        LOGGER.exception(e)
    finally:
        db.close_session(sess)
        return result

def read_deployments(
    user_id: int
) -> Dict[str, Any]:
    sess = None
    result = {"status": 0, "msg": "Some error occured!"}
    try:
        sess = db.get_session()
        LOGGER.info("reading deployments")
        res = db.read_deployments(session=sess,user_id=user_id)
        if res["status"] != 1:
            result["msg"] = "Error in reading Deployment(s)"
            LOGGER.info(result["msg"])
        else:
            result = res       
    except Exception as e:
        LOGGER.exception(e)
    finally:
        db.close_session(sess)
        return result