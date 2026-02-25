from time import time

import db.db as db
import utils.logs as logs

LOGGER = logs.get_logger()


# TODO
# 1. validate module and perm access
#   - from user id , check permission in the provided module id, permission id given { 1 - }


def check_permission_level(user_id: int, module_id: int, perm_lvl: int):
    sess = None
    result = {"status": 0, "msg": "Permission level not found because of some error!"}
    try:
        sess = db.get_session()
        usr_res = db.read_user_roles(sess, user_id)
        if usr_res["status"] != 1:
            result["msg"] = usr_res["msg"]
        else:
            role_id = usr_res["msg"][0]["role_id"]
            role_perm_res = db.read_role_permission_map(sess, role_id)
            perm_ids = []
            if role_perm_res["status"] != 1:
                result["msg"] = role_perm_res["msg"]
            else:
                for item in role_perm_res["msg"]:
                    perm_ids.append(item["perm_id"])
                for permission_id in perm_ids:
                    res_per = db.read_permissions(sess, permission_id)
                    if res_per["msg"][0]["perm_lvl"] >= perm_lvl:
                        result = {"status": 1, "msg": "Permission level found"}
                        break
                    else:
                        continue
    except Exception as e:
        LOGGER.exception(e)
    finally:
        db.close_session(sess)
        return result


def login(username: str, password: str):
    sess = None
    result = {"status": 0, "msg": "Login failed because of some error!", "token": 0}
    try:
        sess = db.get_session()
        res = db.val_user(sess, username, password)
        if res["status"] != 1:
            result["msg"] = res["msg"]
        else:
            token = db.set_session_token(sess, res["msg"]["user_id"])
            result = {"status": 1, "msg": "Login Successful", "token": token["msg"]}
    except Exception as e:
        LOGGER.exception(e)
    finally:
        db.close_session(sess)
        return result


def check_session(token):
    sess = None
    result = {"status": 0, "msg": "Session Expired"}
    try:
        token_ = token.split("-")
        if not int(time()) - int(token_[1]) > 1800:
            sess = db.get_session()
            res = db.get_session_token(sess, int(token_[0]))
            if res["status"] == 1:
                if int(token_[1]) == res["msg"]["login_time"]:
                    res = db.read_users(sess, int(token_[0]))
                    if res["status"] == 1:
                        result["status"] = 1
                        result["msg"] = "Session Active"

    except Exception as e:
        LOGGER.exception(e)
    finally:
        db.close_session(sess)
        return result
