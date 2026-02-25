import re

import pandas as pd
from sqlalchemy.orm import session

from db import db
from utils.custom_typing import Any, Dict
from utils.logs import get_logger

# TODO
# license id - requires revisit


PWD_MIN_REQ = (
    "^(?=.*[a-z])(?=.*[A-Z])(?=.*\\d)(?=.*[@$!%*?&\\.])[A-Za-z\\d@$!%*?&\\.]{8,}$"
)
LOGGER = get_logger()


#! Create User
def create_user(
    name: str,
    email_id: str,
    login_id: str,
    pwd: str,
    persona_id: int,
    creation_user_id: int,
) -> Dict[str, Any]:
    sess = None
    result = {"status": 0, "msg": "Some error occured!"}
    try:

        LOGGER.info("Checking if Password meets the requirements")
        """ Password Requiements -  minimum 8 characters
                                    uppercase letters: A-Z
                                    lowercase letters: a-z
                                    numbers: 0-9
                                    any of the special characters: @#$%^&+=
        """
        if (
            re.search(
                PWD_MIN_REQ,
                pwd,
            )
            is None
        ):
            result["msg"] = "Password does not meet minimum requirements!"
            LOGGER.info(result["msg"])

        else:

            LOGGER.info("Password meets minimum requirements")
            sess = db.get_session()
            LOGGER.info("Creating User")

            res = db.create_user(
                sess=sess,
                name=name,
                email_id=email_id,
                login_id=login_id,
                pwd=pwd,
                creation_user_id=creation_user_id,
                commit=False,
            )
            if res["status"] != 1:
                sess.rollback()
                LOGGER.info("Error in creating User")
                result["status"] = result["status"]
            else:

                user_id = res["msg"]
                LOGGER.info("Creating User Persona")

                res_user_persona = db.create_user_persona(
                    sess=sess,
                    role_id=persona_id,
                    user_id=user_id,
                    creation_user_id=creation_user_id,
                    commit=False,
                )
                if res_user_persona["status"] != 1:

                    result["msg"] = "Unable to Create User Persona"
                    LOGGER.info(result["msg"])
                    sess.rollback()
                else:
                    sess.commit()
                    result["status"] = 1
                    result["msg"] = "User Creted with User_id: " + str(user_id)
                    LOGGER.info("User Created")
    except Exception as e:
        LOGGER.exception(e)
    finally:
        db.close_session()
        return result


#! List Users
def get_users(user_id: int = None) -> Dict[str, Any]:
    sess = None
    result = {"status": 0, "msg": "Some error occured!"}
    try:
        sess = db.get_session()

        LOGGER.info("Reading Users")
        res = db.read_users(sess, user_id=user_id)
        if res["status"] != 1:
            result = res
            LOGGER.info(result["msg"])

        else:

            res_login = db.get_login_time(sess, user_id)
            if res_login["status"] != 1:
                LOGGER.info(res_login["msg"])
                result = res
            else:
                LOGGER.info("Joining Logintime with users")
                pd_res_login = pd.DataFrame(
                    res_login["msg"], columns=["login_time", "user_id"]
                )
                pd_user = pd.DataFrame(res["msg"])
                pd_res = pd.merge(pd_user, pd_res_login, on="user_id", how="left")
                result["status"] = 1
                # result["msg"] = pd_res.to_json(orient="records")
                result["msg"] = pd_res.to_dict(orient="records")
                LOGGER.info("User(s) Found")

    except Exception as e:
        LOGGER.exception(e)
    finally:
        db.close_session()
        return result


def edit_user(
    user_id: int,
    creation_user_id: int,
    name: str = None,
    email_id: str = None,
    pwd: str = None,
    persona_id: int = None,
) -> Dict[str, Any]:
    sess = None
    result = {"status": 0, "msg": "Some error occured!"}
    try:
        sess = db.get_session()
        if name is not None or email_id is not None or pwd is not None:
            LOGGER.info("Updating User")
            res = db.update_user(
                sess=sess,
                user_id=user_id,
                creation_user_id=creation_user_id,
                name=name,
                email_id=email_id,
                pwd=pwd,
                commit=False,
            )
            if res["status"] != 1:

                LOGGER.info("Error in Updating User")
                sess.rollback()
                result["status"] = result["status"]
            else:
                if persona_id is not None:

                    LOGGER.info("Updating User Persona")
                    LOGGER.info("Deleting current Persona")
                    del_user_persona = db.delete_user_persona(
                        sess=sess, user_ids=[user_id], commit=False
                    )

                    if del_user_persona["status"] != 1:
                        LOGGER.info("Unable to delete Persona")
                        result["msg"] = res["msg"]
                    else:
                        LOGGER.info("Persona Deleted. Creating Updated Persona")
                        res_persona = db.create_user_persona(
                            sess=sess,
                            user_id=user_id,
                            role_id=persona_id,
                            creation_user_id=creation_user_id,
                            commit=False,
                        )
                        if res_persona["status"] != 1:

                            LOGGER.info("Unable to create new persona")
                            sess.rollback()
                            result["msg"] = res_persona["msg"]
                        else:
                            LOGGER.info("Persona Updated")
                            sess.commit()
                            result["status"] = 1
                            result["msg"] = "User Updated"
                            LOGGER.info(result["msg"])
                else:
                    sess.commit()
                    LOGGER.info("User Updated")
                    result["status"] = 1
                    result["msg"] = "User Updated"
    except Exception as e:
        LOGGER.exception(e)
    finally:
        db.close_session()
        return result


#! Delete User
def delete_user(user_ids: list) -> Dict[str, Any]:
    sess = None
    result = {"status": 0, "msg": "Some error occured!"}
    try:
        sess = db.get_session()

        LOGGER.info("Deleting User")
        res = db.delete_user(sess, user_ids=user_ids, commit=False)
        if res["status"] != 1:

            result = res
            LOGGER.info("Unable to delete user")
            LOGGER.info(result["msg"])
            sess.rollback()
        else:

            flag = False
            LOGGER.info("Deleting User Persona")
            res_user_persona = db.delete_user_persona(
                sess, user_ids=user_ids, commit=False
            )
            if res_user_persona["status"] != 1:

                result = res_user_persona
                LOGGER.info("Unable to delete User Persona")
                sess.rollback()
            else:

                LOGGER.info("Deleting Project_User")
                res_proj = db.delete_project_user(sess, user_ids=user_ids, commit=False)
                if res_proj["status"] != 1:
                    LOGGER.info("Unable to delete Project associated with user")
                    result = res_proj
                    sess.rollback()
                else:
                    LOGGER.info(res_proj["msg"])
                    result["status"] = 1
                    result["msg"] = "User Deleted"
                    sess.commit()
                    LOGGER.info("User Deleted")

                """
                proj_users = db.get_user_project(sess, user_id = user_id)
                if proj_users["status"] == 1:
                    projs = proj_users["msg"]
                    for proj in projs:
                        proj_id = proj["project_id"]
                        project_user_id = proj["pk"]
                        res_proj_user = db.delete_project_user(
                                            sess, project_id = proj_id, user_id = user_id, commit = False)
                        res_pdvu = db.current_update(sess, project_user_id=project_user_id, commit = False)
                        if res_proj_user != 1 and res_pdvu != 1:
                            flag = True
                else:
                    LOGGER.info(proj_users["msg"])
                    flag = False

                if flag:

                    LOGGER.info("Unable to delete all user map")
                    sess.rollback()
                else:

                    sess.commit()
                    result = res
                    LOGGER.info("User Deleted")
                """

    except Exception as e:
        LOGGER.exception(e)
    finally:
        db.close_session()
        return result


def update_password(user_id: int, pwd_cur: str, pwd_update: str) -> Dict[str, Any]:
    sess = None
    result = {"status": 0, "msg": "Some error occured!"}
    try:

        LOGGER.info("Checking if Password meets the requirements")
        if (
            re.search(
                PWD_MIN_REQ,
                pwd_update,
            )
            is None
        ):
            result["msg"] = "New password does not match requirements!"
            LOGGER.info(result["msg"])

        else:
            sess = db.get_session()
            LOGGER.info("Updating password")
            res = db.update_pwd(
                sess, user_id=user_id, pwd_cur=pwd_cur, pwd_update=pwd_update
            )
            if res["status"] != 1:
                result = res
                LOGGER.info(result["msg"])
            else:
                result = res
                LOGGER.info("Password Updated")

    except Exception as e:
        LOGGER.exception(e)
    finally:
        db.close_session()
        return result
