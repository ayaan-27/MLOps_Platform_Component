import time

from sqlalchemy import Boolean, Column, ForeignKey, Integer, and_
from sqlalchemy.dialects import postgresql
from sqlalchemy.orm import query
from sqlalchemy.sql.elements import and_

from db.db_metadata import Auto_repr, Base
from utils.custom_typing import Any, Dict, db_session
from utils.logs import get_logger

LOGGER = get_logger()


class Model_hub(Base, Auto_repr):
    __tablename__ = "model_hub"
    model_id = Column(Integer, primary_key=True, autoincrement=True)
    model_name = Column(postgresql.VARCHAR(100))
    project_id = Column(Integer, ForeignKey("projects.project_id"))
    user_id = Column(Integer, ForeignKey("users.user_id"))
    is_deleted = Column(postgresql.BOOLEAN, default=False)
    visibility = Column(postgresql.BOOLEAN, default=True)

def create_model_hub(
    session: db_session,
    model_name: str,
    project_id: str,
    user_id: str,
    commit: Boolean = True,
) -> Dict[str, Any]:
    try:
        model_hub_ = Model_hub()
        model_hub_.model_name = model_name
        model_hub_.project_id = project_id
        model_hub_.user_id = user_id
        session.add(model_hub_)
        if commit:
            session.commit()
        else:
            session.flush()
        return {"status": 1, "msg": model_hub_.model_id}
    except Exception as e:
        LOGGER.exception(e)
        session.rollback()
        raise e

def delete_model(session: db_session, model_id: int, commit = True) -> Dict[str, Any]:
    try:
        query = session.query(Model_hub).filter(
            and_(
                Model_hub.model_id == model_id, Model_hub.is_deleted == False
                )
            )
        if query.count() != 0:
            query.update({"is_deleted": True})
            query.update({"last_modified": int(time.time())})
            if commit:
                session.commit()
            else:
                session.flush()
            return {"status": 1, "msg": "Model(s) Deleted"}
        else:
            return {"status": 0, "msg": "Model(s) not found"}
    except Exception as e:
        LOGGER.exception(e)
        session.rollback()
        raise e
