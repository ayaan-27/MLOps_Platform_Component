import pandas as pd

from db import db
from utils.custom_typing import Any, Dict
from utils.logs import get_logger

LOGGER = get_logger()


#! List Roles & View Role
def get_roles(role_id: int = None) -> Dict[str, Any]:
    sess = None
    result = {"status": 0, "msg": "Some error occured!"}
    try:
        sess = db.get_session()
        ress = db.read_roles(sess, role_id)
        if ress["status"] != 1:

            result["msg"] = "Error in reading roles"
            LOGGER.info(result["msg"])
        else:
            result = ress
            LOGGER.info("Roles fetched")

    except Exception as e:
        LOGGER.exception(e)
    finally:
        db.close_session(sess)
        return result


#! View Role
def get_role_permission_map(role_id: int = None) -> Dict[str, Any]:
    sess = None
    result = {"status": 0, "msg": "Some error occured!"}
    try:
        sess = db.get_session()
        ress = db.read_role_permission_map(sess, role_id)
        if ress["status"] != 1:

            result["msg"] = "Error in reading roles"
            LOGGER.info(result["msg"])
        else:
            res_perm = get_permissions()["msg"]
            pd_perm = pd.DataFrame(res_perm)
            pd_role_perm = pd.DataFrame(ress["msg"])
            res_pd = pd.merge(pd_role_perm, pd_perm, how="left", on=["perm_id"])
            result["status"] = 1
            result["msg"] = res_pd.to_dict(orient="records")
            LOGGER.info("Roles fetched")

    except Exception as e:
        LOGGER.exception(e)
    finally:
        db.close_session(sess)
        return result


def get_permissions() -> Dict[str, Any]:
    sess = None
    result = {"status": 0, "msg": "Some error occured!"}
    try:
        sess = db.get_session()

        LOGGER.info("Reading Persmissions")
        perm_res = db.read_permissions(sess)
        if perm_res["status"] != 1:
            result["msg"] = perm_res["msg"]
            LOGGER.info(result["msg"])

        else:

            LOGGER.info("Reading Modules")
            module_res = db.read_modules(sess)
            if module_res["status"] == 1:
                final_res = []
                temp = pd.DataFrame(module_res["msg"])
                for perms in perm_res["msg"]:
                    perms["module_name"] = temp.loc[
                        temp["module_id"] == perms["module_id"], "module_name"
                    ].tolist()[0]
                    final_res.append(perms)
                result["msg"] = final_res
                result["status"] = 1
                result["msg"] = final_res
            else:
                result["msg"] = "Error reading modules"
                LOGGER.info(result["msg"])

    except Exception as e:
        LOGGER.exception(e)
    finally:
        db.close_session(sess)
        return result


#!Create Role
def create_role(
    role_name: str,
    creation_user_id: int,
    module_ids: list,
    perm_lvls: list,
    is_persona: bool = False,
) -> Dict[str, Any]:
    sess = None
    result = {"status": 0, "msg": "Some error occured!"}
    try:
        perms = get_permissions()
        if perms["status"] == 1:
            sess = db.get_session()
            permissions = []
            for perm_lvl, module_id in zip(perm_lvls, module_ids):
                r = db.get_perm_id(sess=sess, perm_lvl=perm_lvl, module_id=module_id)
                permissions.append(r["msg"])

            if role_name == "create new role":

                LOGGER.info(result["msg"])
                result["msg"] = "Invalid Role Name"
            else:

                LOGGER.info("Creating Role")
                res = db.create_role(
                    sess,
                    role_name,
                    creation_user_id,
                    permissions,
                    is_persona=is_persona,
                    commit=False,
                )

                if res["status"] != 1:
                    sess.rollback()
                    result["msg"] = "Unable to create role"
                    LOGGER.info(result["msg"])

                else:
                    sess.commit()
                    result["status"] = 1
                    result["msg"] = "Role created with Role id: " + str(res["msg"])
                    LOGGER.info(result["msg"])

    except Exception as e:
        LOGGER.exception(e)
    finally:
        db.close_session(sess)
        return result


#!Edit Role
def update_roles(
    role_id: int,
    creation_user_id: int,
    module_ids: list,
    perm_lvls: list,
) -> Dict[str, Any]:
    sess = None
    result = {"status": 0, "msg": "Some error occured!"}
    try:
        sess = db.get_session()
        LOGGER.info("Reading the current permisison given to role")
        res_perm = db.read_role_permission_map(sess=sess, role_id=role_id)
        if res_perm["status"] != 1:
            LOGGER.info("Error in reading permission for given role_id")
            result = res_perm
        else:
            permissions = []
            for perm_lvl, module_id in zip(perm_lvls, module_ids):
                r = db.get_perm_id(sess=sess, perm_lvl=perm_lvl, module_id=module_id)
                permissions.append(r["msg"])

            LOGGER.info("Creating initial permission list")
            init_perms = [x["perm_id"] for x in res_perm["msg"]]

            LOGGER.info("Creating list of permissions to be added")
            add_perms = [perms for perms in permissions if perms not in init_perms]

            LOGGER.info("Creating list of permissions to be deleted")
            del_perms = [perms for perms in init_perms if perms not in permissions]
            flag = True
            if add_perms:
                LOGGER.info("Updating role permisison map")
                for perm in add_perms:

                    res_perm_map = db.create_role_persmission_map(
                        sess=sess,
                        role_id=role_id,
                        perm_id=perm,
                        creation_user_id=creation_user_id,
                        commit=False,
                    )
                    if res_perm_map["status"] != 1:
                        LOGGER.info("Error in Updating role permisison map")
                        flag = False
                        result = res_perm_map
                        sess.rollback()
                        break
            if del_perms and flag:

                LOGGER.info("Deleting role permisison map which are not required")
                for perm in del_perms:

                    res_perm_map_del = db.delete_role_permission_map(
                        sess=sess, role_id=[role_id], perm_id=perm, commit=False
                    )
                    if res_perm_map_del["status"] != 1:
                        LOGGER.info(
                            "Error in Deleting role permisison map which are not required"
                        )
                        flag = False
                        result = res_perm_map_del
                        sess.rollback()
                        break

            if flag:
                sess.commit()
                result["status"] = 1
                result["msg"] = "Role permission updated"
                LOGGER.info(result["msg"])

    except Exception as e:
        LOGGER.exception(e)
    finally:
        db.close_session(sess)
        return result


#!Delete Role
def delete_role(role_ids: list) -> Dict[str, Any]:
    sess = None
    result = {"status": 0, "msg": "Some error occured!"}
    try:
        sess = db.get_session()
        if db.read_roles(sess, role_ids)["status"] == 1:
            LOGGER.info("Deleting Role")
            res = db.delete_role(sess, role_ids, commit=False)
            if res["status"] != 1:

                result = res
                sess.rollback()
                LOGGER.info("Unable to delete role")
            else:
                res_perm_map = db.delete_role_permission_map(
                    sess, role_id=role_ids, commit=False
                )
                if res_perm_map["status"] != 1:

                    LOGGER.info("Unable to delete Role permission map")
                    sess.rollback()
                else:
                    LOGGER.info("Role permission map deleted")
                    res_user_persona = db.delete_user_persona(
                        sess=sess, role_ids=role_ids, commit=False
                    )

                    if res_user_persona["status"] != 1:
                        LOGGER.info("No Persona found in User_persona")
                    else:
                        LOGGER.info("User_persona deleted")
                    result = res
                    sess.commit()
                    LOGGER.info("Role Deleted")

    except Exception as e:
        LOGGER.exception(e)
    finally:
        db.close_session(sess)
        return result


def get_modules() -> Dict[str, Any]:
    sess = None
    result = {"status": 0, "msg": "Some error occured!"}
    try:
        sess = db.get_session()
        ress = db.read_modules(sess)
        if ress["status"] != 1:

            result["msg"] = "Error in reading modules"
            LOGGER.info(result["msg"])
        else:
            result = ress
            LOGGER.info("Roles fetched")

    except Exception as e:
        LOGGER.exception(e)
    finally:
        db.close_session(sess)
        return result
