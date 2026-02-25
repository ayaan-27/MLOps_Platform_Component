import json
from collections import Counter
from logging import Logger, info

import pandas as pd
from sqlalchemy.sql.functions import user

import db.db as db
import utils.logs as logs
from db.db_pdvu_mapping import create_pdvu_map
from db.db_proj_ds_mapping import create_proj_ds_map, del_proj_ds_map, read_proj_ds_map
from db.db_proj_user import (
    change_user_role_project,
    delete_project_user,
    get_project_users,
    get_projects,
)
from utils.custom_typing import Any, Dict, List, db_session

LOGGER = logs.get_logger()


def create_proj(
    creation_user_id: int,
    name: str,
    desc: str,
    user_role_ids: Dict[str, Any] = None,
    attach_dataset: bool = False,
    dataset_id: int = None,
    version_id: int = None,
) -> Dict[str, Any]:
    sess = None
    result = {"status": 0, "msg": "Some error occured!"}
    try:
        sess = db.get_session()
        LOGGER.info("Creating Project")
        # TODO create MLflow project/experiment
        mlflow_id = 123
        res = db.create_proj(
            session=sess,
            name=name,
            desc=desc,
            user_id=creation_user_id,
            mlflow_id=mlflow_id,
            commit=False,
        )
        if res["status"] != 1:
            result = res
            sess.rollback()
            LOGGER.info("Project Creation failed.")
            LOGGER.info(result["msg"])
        else:
            LOGGER.info("Adding Users to the project")
            proj_id = res["msg"]
            # TODO set proj ID in MLflow project
            if not user_role_ids:
                user_role_ids = {creation_user_id: -1}
            elif creation_user_id not in user_role_ids.keys() or (
                creation_user_id in user_role_ids.keys()
                and user_role_ids[creation_user_id] != -1
            ):

                user_role_ids.update({str(creation_user_id): -1})

            proj_user_id = []
            flag = False
            for user_role_id in user_role_ids:
                res = db.add_user_project(
                    sess=sess,
                    project_id=proj_id,
                    creation_user_id=creation_user_id,
                    user_id=user_role_id,
                    role_id=user_role_ids[user_role_id],
                    commit=False,
                )
                if res["status"] != 1:
                    result = res
                    sess.rollback()
                    LOGGER.info("Error in adding user to the project")
                    LOGGER.info(result["msg"])
                    flag = True
                    break
                else:
                    proj_user_id.append(res["msg"])

            if not flag and attach_dataset:
                LOGGER.info("Attaching Dataset to the project")
                LOGGER.info("Creating project ds mapping")

                res_proj_ds = db.create_proj_ds_map(
                    session=sess,
                    dataset_id=dataset_id,
                    creation_user_id=creation_user_id,
                    project_id=proj_id,
                    public=False,
                    commit=False,
                )

                if res_proj_ds["status"] != 1:
                    result = res_proj_ds
                    sess.rollback()
                    LOGGER.info("Error in creating project dataset mapping")
                    LOGGER.info(result["msg"])
                else:
                    proj_ds_id = res_proj_ds["msg"]
                    LOGGER.info("Creating Pdvu mapping")

                    flag = False

                    for project_user_id in proj_user_id:
                        res_create_proj_ds_map = db.create_pdvu_map(
                            sess=sess,
                            version_id=version_id,
                            dataset_id=dataset_id,
                            project_user_id=project_user_id,
                            project_ds_id=proj_ds_id,
                            commit=False,
                        )

                        if res_create_proj_ds_map["status"] != 1:
                            result = res_create_proj_ds_map
                            sess.rollback()
                            LOGGER.info("Error in creating pdvu map")
                            LOGGER.info(result["msg"])
                            flag = True
                            break

                    if not flag:
                        sess.commit()
                        result["status"] = 1
                        result["msg"] = "Project Created with Project id " + str(proj_id)
                        LOGGER.info(result["msg"])
            else:
                if flag:
                    sess.rollback()
                    result["status"] = 0
                    result["msg"] = "Unable to create project"
                    LOGGER.info(result["msg"])
                else:
                    sess.commit()
                    result["status"] = 1
                    result["msg"] = "Project Created with Project id " + str(proj_id)
                    LOGGER.info(result["msg"])

    except Exception as e:
        LOGGER.exception(e)
        if sess is not None:
            sess.rollback()
    finally:
        db.close_session(sess)
        return result


def get_proj(proj_id: int = None, user_id: int = None) -> Dict[str, Any]:
    sess = None
    result = {"status": 0, "msg": "Some error occured!"}
    try:
        sess = db.get_session()
        LOGGER.info("Getting Project Information")

        res = db.get_projects(sess=sess, proj_id=proj_id, user_id=user_id)

        if res["status"] != 1:
            result = res
            LOGGER.info("Project(s) not found")
            LOGGER.info(result["msg"])
        else:
            result = res
            LOGGER.info(result["msg"])

    except Exception as e:
        LOGGER.exception(e)
    finally:
        db.close_session(sess)
        return result


def get_proj_users(proj_ids: list = None, user_id: int = None):
    sess = None
    result = {"status": 0, "msg": "Some error occured!"}
    try:
        sess = db.get_session()
        LOGGER.info("Getting all the users assigned to given project(s)")

        res = db.get_project_users(sess=sess, proj_ids=proj_ids, user_id=user_id)

        if res["status"] != 1:
            result = res
            LOGGER.info("Project user combination not found")
            LOGGER.info(result["msg"])
        else:
            result = res
            LOGGER.info(result["msg"])

    except Exception as e:
        LOGGER.exception(e)
    finally:
        db.close_session(sess)
        return result


def del_proj(proj_id: int) -> Dict[str, Any]:
    sess = None
    result = {"status": 0, "msg": "Some error occured!"}
    try:
        sess = db.get_session()
        LOGGER.info("Deleting Project")
        res = db.delete_proj(sess=sess, project_id=proj_id, commit=False)

        if res["status"] != 1:
            result = res
            LOGGER.info("Unable to delete project")
            LOGGER.info(result["msg"])
            sess.rollback()
        else:
            LOGGER.info("Deleting Project_User")
            res_proj_user = db.delete_project_user(
                sess=sess, project_id=proj_id, commit=False
            )

            if res_proj_user["status"] != 1:
                result = res_proj_user
                LOGGER.info("Unable to delete Project user")
                sess.rollback()

            else:
                LOGGER.info("Checking for project ds mapping")
                res_read_proj_ds_map = db.read_proj_ds_map(
                    session=sess, project_id=proj_id
                )

                if res_read_proj_ds_map["status"] != 1:
                    result["status"] = 1
                    result["msg"] = "Project Deleted"
                    sess.commit()
                    LOGGER.info("Project Deleted")
                    LOGGER.info(res_read_proj_ds_map["msg"])

                else:
                    LOGGER.info("Deleting proj ds map")
                    res_del_proj_ds_map = db.del_proj_ds_map(
                        session=sess, project_id=proj_id, commit=False
                    )

                    if res_del_proj_ds_map["status"] != 1:
                        result = res_del_proj_ds_map
                        sess.rollback()
                        LOGGER.info("Unable to delete project ds map")
                        LOGGER.info(result["msg"])
                    else:
                        result["status"] = 1
                        result["msg"] = "Project Deleted"
                        sess.commit()
                        LOGGER.info("Project Deleted")
                        LOGGER.info(res_del_proj_ds_map["msg"])
    except Exception as e:
        LOGGER.exception(e)
    finally:
        db.close_session(sess)
        return result


def edit_proj(
    proj_id: int, creation_user_id: int, desc: str, user_role_ids: Dict[str, Any]
):
    sess = None
    result = {"status": 0, "msg": "Some error occured!"}
    try:
        sess = db.get_session()
        LOGGER.info("Editing Project")
        res = db.edit_project_desc(sess=sess, project_id=proj_id, desc=desc, commit=False)
        if res["status"] != 1:
            result = res
            LOGGER.info("Unable to edit Project")
            LOGGER.info(result["msg"])
            sess.rollback()

        else:
            LOGGER.info("Getting initial User Role mapping")
            res = db.get_project_users(sess=sess, proj_ids=[proj_id])
            if res["status"] != 1:
                result = res
                LOGGER.info("Error in fetching original user role mapping")
                LOGGER.info(result["msg"])
                sess.rollback()
            else:
                LOGGER.info("Creating initial user role mapping dictionary")
                initial_user_id = []
                initial_role_id = []
                for i in range(len(res["msg"])):
                    initial_user_id.append(str(res["msg"][i]["user_id"]))
                    initial_role_id.append(res["msg"][i]["role_id"])
                initial_user_role_ids = dict(zip(initial_user_id, initial_role_id))
                add_user = [
                    user_id
                    for user_id in user_role_ids.keys()
                    if user_id not in initial_user_role_ids
                ]
                del_user = [
                    user_id
                    for user_id in initial_user_role_ids.keys()
                    if user_id not in user_role_ids
                ]
                temp_dict = {}
                for add in add_user:
                    temp_dict[add] = user_role_ids[add]
                change_user = [
                    user_id
                    for user_id in dict(
                        Counter(user_role_ids) - Counter(temp_dict)
                    ).keys()
                    if user_role_ids[user_id] != initial_user_role_ids[user_id]
                ]

                flag = False
                if add_user:
                    LOGGER.info("Adding User to the Project")
                    for user_id in add_user:
                        res = db.add_user_project(
                            sess=sess,
                            project_id=proj_id,
                            creation_user_id=creation_user_id,
                            user_id= user_id,
                            role_id=user_role_ids[user_id],
                            commit=False,
                        )

                        if res["status"] != 1:
                            flag = True
                            break
                    if flag:
                        result = res
                        LOGGER.info("User addition failed")
                        LOGGER.info(result["msg"])
                        sess.rollback()

                if del_user and (not flag):
                    LOGGER.info("Deleting user from the project")
                    for user_id in del_user:
                        res = db.delete_project_user(
                            sess=sess, project_id=proj_id, user_id=user_id, commit=False
                        )

                        if res["status"] != 1:
                            flag = True
                            break
                    if flag:
                        result = res
                        LOGGER.info("User deletion failed")
                        LOGGER.info(result["msg"])
                        sess.rollback()

                if change_user and (not flag):
                    LOGGER.info("Changing exsiting user role in project")
                    for user_id in change_user:
                        res = db.change_user_role_project(
                            sess=sess,
                            project_id=proj_id,
                            user_id= user_id,
                            role_id=user_role_ids[user_id],
                            commit=False,
                        )
                        if res["status"] != 1:
                            flag = True
                            break
                    if flag:
                        result = res
                        LOGGER.info("Change in User(s) role(s) failed")
                        LOGGER.info(result["msg"])
                        sess.rollback()
                if not flag:
                    sess.commit()
                    LOGGER.info("Project Edited Successfully")
                    result["status"] = 1
                    result["msg"] = "Project Edited Successfully"

    except Exception as e:
        LOGGER.exception(e)
        if sess is not None:
            sess.rollback()
    finally:
        db.close_session(sess)
        return result
