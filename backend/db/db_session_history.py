import time

from sqlalchemy import Column, ForeignKey, Integer
from sqlalchemy.dialects import postgresql
from sqlalchemy.orm import query
from sqlalchemy.sql.expression import func

from db.db_metadata import Auto_repr, Base
from db.db_users import Users
from utils.custom_typing import Any, Dict, db_session
from utils.logs import get_logger

LOGGER = get_logger()


class Session_history(Base, Auto_repr):
    __tablename__ = "session_history"
    user_id = Column(Integer, ForeignKey("users.user_id"))
    login_time = Column(postgresql.BIGINT, default=-1)
    session_token = Column(
        postgresql.VARCHAR(50), nullable=False, primary_key=True
    )  # check


def set_session_token(sess: db_session, user_id: int) -> Dict[str, Any]:
    try:
        session_history_ = Session_history()
        session_history_.user_id = user_id
        session_history_.login_time = int(time.time())
        token = str(session_history_.user_id) + "-" + str(session_history_.login_time)
        session_history_.session_token = token
        sess.add(session_history_)
        sess.commit()
        return {"status": 1, "msg": token}
    except Exception as e:
        LOGGER.exception(e)
        sess.rollback()
        raise (e)


def get_session_token(sess: db_session, user_id: int) -> Dict[str, Any]:
    try:
        token = (
            sess.query(Session_history)
            .filter(Session_history.user_id == user_id)
            .order_by(Session_history.login_time.desc())
        )
        if token.count() != 0:
            return {"status": 1, "msg": token.first().to_dict()}
        else:
            return {"status": 0, "msg": "no sess token found"}
    except Exception as e:
        LOGGER.exception(e)
        raise (e)


def get_login_time(sess: db_session, user_id: int = None):
    try:
        query = sess.query(
            func.max(Session_history.login_time), Session_history.user_id
        ).group_by(Session_history.user_id)
        if user_id is not None:
            query = query.filter(Session_history.user_id == user_id)
        login_time_list = []
        if query.count() != 0:
            for i in query:
                login_time_list.append(i)
            return {"status": 1, "msg": login_time_list}

        else:
            return {"status": 0, "msg": "No Login time found"}
    except Exception as e:
        LOGGER.exception(e)
        raise (e)
