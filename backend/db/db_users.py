import time

from sqlalchemy import Column, ForeignKey, Integer
from sqlalchemy.dialects import postgresql
from sqlalchemy.sql.base import NO_ARG
from sqlalchemy.sql.elements import and_

from db.db_license_issued import License_issued
from db.db_metadata import Auto_repr, Base
from utils.custom_typing import Any, Dict, db_session
from utils.logs import get_logger

LOGGER = get_logger()


class Users(Base, Auto_repr):
    __tablename__ = "users"
    user_id = Column(Integer, primary_key=True)
    name = Column(postgresql.VARCHAR(100), nullable=False)
    email_id = Column(postgresql.VARCHAR(100), nullable=False)
    login_id = Column(postgresql.VARCHAR(50), nullable=False, unique=True)
    pwd = Column(postgresql.VARCHAR(30), nullable=False)
    license_id = Column(Integer, ForeignKey("license_issued.license_id"))
    is_deleted = Column(postgresql.BOOLEAN, default=False)
    creation_user_id = Column(Integer, ForeignKey("users.user_id"))
    creation_time = Column(postgresql.BIGINT, nullable=False, default=int(time.time()))
    last_modified = Column(postgresql.BIGINT, nullable=False, default=int(time.time()))


def create_user(
    sess: db_session,
    name: str,
    email_id: str,
    login_id: str,
    pwd: str,
    creation_user_id: int,
    commit: bool = True,
) -> Dict[str, Any]:
    try:
        query = sess.query(Users).filter(Users.login_id == login_id)
        if query.count() != 0:
            return {"status": 0, "msg": "login id already present"}
        user_ = Users()
        user_.name = name
        user_.email_id = email_id
        user_.login_id = login_id
        user_.pwd = pwd
        user_.creation_user_id = creation_user_id
        sess.add(user_)
        if commit:
            sess.commit()
        else:
            sess.flush()
        return {"status": 1, "msg": user_.user_id}
    except Exception as e:
        LOGGER.exception(e)
        sess.rollback()
        raise e


def update_user(
    sess: db_session,
    user_id: int,
    creation_user_id: int,
    name: str = None,
    email_id: str = None,
    pwd: str = None,
    commit: bool = True,
) -> Dict[str, Any]:
    try:
        query = sess.query(Users).filter(
            and_(Users.user_id == user_id, Users.is_deleted == False)
        )
        if query.count() != 0:
            temp = query.first()
            temp.creation_user_id = creation_user_id
            if name is not None:
                temp.name = name
            if email_id is not None:
                temp.email_id = email_id
            if pwd is not None:
                temp.pwd = pwd
            temp.last_modified = int(time.time())
            if commit:
                sess.commit()
            else:
                sess.flush()
            return {"status": 1, "msg": "User Updated"}
        else:
            return {"status": 0, "msg": "No User found"}
    except Exception as e:
        LOGGER.exception(e)
        sess.rollback()
        raise e


def val_user(sess: db_session, login_id: str, pwd: str) -> Dict[str, Any]:
    # validate user for given user_id and password
    try:
        query = sess.query(Users).filter(Users.login_id == login_id)
        if query.count() != 0:
            user = query.first()
            if user.pwd == pwd:
                return {"status": 1, "msg": user.to_dict()}
            else:
                return {"status": 0, "msg": "password does not match"}
        else:
            return {"status": 0, "msg": "user not found"}
    except Exception as e:
        LOGGER.exception(e)
        raise (e)


def delete_user(sess: db_session, user_ids: int, commit: bool = True) -> Dict[str, Any]:
    try:
        query = sess.query(Users).filter(Users.user_id.in_(user_ids))
        if query.count() != 0:
            query.update({"is_deleted": True})
            query.update({"last_modified": int(time.time())})
            if commit:
                sess.commit()
            else:
                sess.flush()
            return {"status": 1, "msg": "User ids deleted"}
        else:
            return {"status": 0, "msg": "user not found"}
    except Exception as e:
        LOGGER.exception(e)
        sess.rollback()
        raise e


def update_pwd(
    sess: db_session, user_id: int, pwd_cur: str, pwd_update: str
) -> Dict[str, Any]:
    try:
        query = sess.query(Users).filter(
            and_(Users.user_id == user_id, Users.is_deleted == False)
        )
        if query.count() != 0:
            user = query.first()
            if user.pwd == pwd_cur:
                query.update({"pwd": pwd_update})
                query.update({"last_modified": int(time.time())})
                sess.commit()
                return {"status": 1, "msg": "password updated"}
            else:
                return {"status": 0, "msg": "current password does not match"}
        else:
            return {"status": 0, "msg": "user not found"}
    except Exception as e:
        LOGGER.exception(e)
        sess.rollback()
        raise e
