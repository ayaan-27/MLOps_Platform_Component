import time

from sqlalchemy import Column, ForeignKey, Integer, and_
from sqlalchemy.dialects import postgresql
from sqlalchemy.orm import query
from sqlalchemy.sql.elements import and_

from db.db_metadata import Auto_repr, Base
from db.db_roles import Roles
from db.db_users import Users
from utils.custom_typing import Any, Dict, db_session
from utils.logs import get_logger

LOGGER = get_logger()


class User_persona(Base, Auto_repr):
    __tablename__ = "user_persona"
    user_id = Column(Integer, ForeignKey("users.user_id"), primary_key=True)
    role_id = Column(Integer, ForeignKey("roles.role_id"), primary_key=True)
    is_deleted = Column(postgresql.BOOLEAN, default=False)
    creation_user_id = Column(Integer, ForeignKey("users.user_id"))
    creation_time = Column(
        postgresql.BIGINT, nullable=False, default=int(time.time()), primary_key=True
    )
    last_modified = Column(postgresql.BIGINT, nullable=False, default=int(time.time()))


def create_user_persona(
    sess: db_session,
    user_id: int,
    role_id: int,
    creation_user_id: int,
    commit: bool = True,
) -> Dict[str, Any]:
    # creates user and role mapping
    try:
        user_persona_ = User_persona()
        user_persona_.user_id = user_id
        user_persona_.creation_user_id = creation_user_id
        user_persona_.role_id = role_id
        sess.add(user_persona_)
        if commit:
            sess.commit()
        else:
            sess.flush()
        return {"status": 1, "msg": user_id}
    except Exception as e:
        LOGGER.exception(e)
        sess.rollback()
        raise (e)


def read_user_persona(sess: db_session, user_id: int = None) -> Dict[str, Any]:
    # returns all comlumns of user_roles
    try:
        subq = sess.query(Users.user_id).filter(Users.is_deleted == False)
        query = sess.query(User_persona).filter(User_persona.user_id.in_(subq))
        if user_id is not None:
            query = query.filter(User_persona.user_id == user_id)
        user_role_list = []
        if query.count() != 0:
            for i in query:
                user_role_list.append(i.to_dict())
            return {"status": 1, "msg": user_role_list}
        else:
            return {"status": 0, "msg": "User Persona not found"}
    except Exception as e:
        LOGGER.exception(e)
        raise e


def read_users(sess: db_session, user_id: int = None) -> Dict[str, Any]:
    # returns details of users and user_persona
    try:
        query = (
            sess.query(
                Users.user_id,
                Users.name,
                Users.email_id,
                Users.login_id,
                Users.pwd,
                Users.license_id,
                Users.creation_user_id,
                Users.creation_time,
                Users.last_modified,
                User_persona.role_id,
                User_persona.creation_time,
                User_persona.last_modified,
            )
            .join(User_persona, Users.user_id == User_persona.user_id, isouter=True)
            .filter(Users.is_deleted == False)
        )

        if user_id is not None:
            query = query.filter(Users.user_id == user_id)

        if query.count() != 0:
            user_list = []
            for i in query:
                user = {}
                user["user_id"] = i[0]
                user["name"] = i[1]
                user["email_id"] = i[2]
                user["login_id"] = i[3]
                user["pwd"] = i[4]
                user["license_id"] = i[5]
                user["creation_user_id"] = i[6]
                user["creation_time"] = i[7]
                user["last_modified"] = i[8]
                user["User_persona_id"] = i[9]
                user["Persona_creation_time"] = i[10]
                user["Persona_last_modified"] = i[11]
                user_list.append(user)
            return {"status": 1, "msg": user_list}
        else:
            return {"status": 0, "msg": "user(s) not found"}
    except Exception as e:
        LOGGER.exception(e)
        raise (e)


def delete_user_persona(
    sess: db_session, user_ids: list = None, role_ids: list = None, commit=True
) -> Dict[str, Any]:
    try:
        query = sess.query(User_persona).filter(User_persona.is_deleted == False)

        if user_ids is not None:
            query = sess.query(User_persona).filter(User_persona.user_id.in_(user_ids))
        if role_ids is not None:
            query = sess.query(User_persona).filter(User_persona.role_id.in_(role_ids))

        if query.count() != 0:
            query.update({"is_deleted": True})
            query.update({"last_modified": int(time.time())})
            if commit:
                sess.commit()
            else:
                sess.flush()
            return {"status": 1, "msg": user_ids}
        else:
            return {"status": 0, "msg": "User (User Persona combo) not found"}
    except Exception as e:
        LOGGER.exception(e)
        sess.rollback()
        raise e
